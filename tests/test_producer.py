import asyncio
import gc
import json
import time
import weakref
from unittest import mock

import pytest

from aiokafka.client import AIOKafkaClient
from aiokafka.cluster import ClusterMetadata
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.errors import (
    KafkaError,
    KafkaTimeoutError,
    LeaderNotAvailableError,
    MessageSizeTooLargeError,
    NotLeaderForPartitionError,
    ProducerClosed,
    RequestTimedOutError,
    UnknownTopicOrPartitionError,
)
from aiokafka.producer import AIOKafkaProducer
from aiokafka.protocol.produce import ProduceResponse_v0, ProduceResponse_v2
from aiokafka.util import create_future

from ._testutil import (
    KafkaIntegrationTestCase,
    kafka_versions,
    run_in_thread,
    run_until_complete,
)

LOG_APPEND_TIME = 1


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):
    @run_until_complete
    async def test_producer_start(self):
        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(acks=122)

        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()
        partitions = await producer.partitions_for("some_topic_name")
        self.assertEqual(len(partitions), 2)
        self.assertEqual(partitions, {0, 1})
        await producer.stop()
        self.assertEqual(producer._closed, True)

    @run_until_complete
    async def test_producer_warn_unclosed(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        producer_ref = weakref.ref(producer)
        await producer.start()

        with self.silence_loop_exception_handler():
            with self.assertWarnsRegex(ResourceWarning, "Unclosed AIOKafkaProducer"):
                del producer
                gc.collect()
        # Assure that the reference was properly collected
        self.assertIsNone(producer_ref())

    @run_until_complete
    async def test_producer_notopic(self):
        producer = AIOKafkaProducer(
            request_timeout_ms=200,
            bootstrap_servers=self.hosts,
        )
        await producer.start()
        with mock.patch.object(AIOKafkaClient, "_metadata_update") as mocked:

            async def dummy(*d, **kw):
                return

            mocked.side_effect = dummy
            with self.assertRaises(UnknownTopicOrPartitionError):
                await producer.send_and_wait("some_topic", b"hello")
        await producer.stop()

    @run_until_complete
    async def test_producer_send(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()
        self.add_cleanup(producer.stop)
        with self.assertRaises(TypeError):
            await producer.send(self.topic, "hello, Kafka!", partition=0)
        future = await producer.send(self.topic, b"hello, Kafka!", partition=0)
        resp = await future
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))
        self.assertEqual(resp.offset, 0)

        fut = await producer.send(self.topic, b"second msg", partition=1)
        resp = await fut
        self.assertEqual(resp.partition, 1)

        future = await producer.send(self.topic, b"value", key=b"KEY")
        resp = await future
        self.assertTrue(resp.partition in (0, 1))

        resp = await producer.send_and_wait(self.topic, b"value")
        self.assertTrue(resp.partition in (0, 1))

        await producer.stop()
        with self.assertRaises(ProducerClosed):
            await producer.send(self.topic, b"value", key=b"KEY")

    @run_in_thread
    def test_create_producer_no_running_loop(self):
        loop = asyncio.new_event_loop()
        with pytest.deprecated_call():
            producer = AIOKafkaProducer(bootstrap_servers=self.hosts, loop=loop)
        loop.run_until_complete(producer.start())
        try:
            future = loop.run_until_complete(
                producer.send(self.topic, b"hello, Kafka!", partition=0)
            )
            resp = loop.run_until_complete(future)
            self.assertEqual(resp.topic, self.topic)
            self.assertTrue(resp.partition in (0, 1))
            self.assertEqual(resp.offset, 0)
        finally:
            loop.run_until_complete(producer.stop())
            loop.close()

    @run_until_complete
    async def test_producer_context_manager(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        async with producer as prod:
            assert prod is producer
            assert producer._sender._sender_task is not None
            await producer.send(self.topic, b"value", key=b"KEY")
        assert producer._closed

        # Closes even on error
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        with pytest.raises(ValueError):
            async with producer as prod:
                assert prod is producer
                assert producer._sender._sender_task is not None
                await producer.send(self.topic, b"value", key=b"KEY")
                raise ValueError()
        assert producer._closed

    @run_until_complete
    async def test_producer_send_noack(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts, acks=0)
        await producer.start()
        fut1 = await producer.send(self.topic, b"hello, Kafka!", partition=0)
        fut2 = await producer.send(self.topic, b"hello, Kafka!", partition=1)
        done, _ = await asyncio.wait([fut1, fut2])
        for item in done:
            self.assertEqual(item.result(), None)
        await producer.stop()

    @run_until_complete
    async def test_producer_send_with_serializer(self):
        def key_serializer(val):
            return val.upper().encode()

        def serializer(val):
            return json.dumps(val).encode()

        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts,
            value_serializer=serializer,
            key_serializer=key_serializer,
            acks="all",
            max_request_size=1000,
        )
        await producer.start()
        key = "some key"
        value = {"strKey": 23523.443, 23: "STRval"}
        future = await producer.send(self.topic, value, key=key)
        resp = await future
        partition = resp.partition
        offset = resp.offset
        self.assertTrue(partition in (0, 1))  # partition

        future = await producer.send(self.topic, "some str", key=key)
        resp = await future
        # expect the same partition bcs the same key
        self.assertEqual(resp.partition, partition)
        # expect offset +1
        self.assertEqual(resp.offset, offset + 1)

        value[23] = "*VALUE" * 800
        with self.assertRaises(MessageSizeTooLargeError):
            await producer.send(self.topic, value, key=key)

        await producer.stop()
        await producer.stop()  # should be Ok

    @run_until_complete
    async def test_producer_send_with_compression(self):
        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(compression_type="my_custom")

        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts,
            compression_type="gzip",
        )

        await producer.start()

        # short message will not be compressed
        future = await producer.send(self.topic, b"this msg is too short for compress")
        resp = await future
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))

        # now message will be compressed
        resp = await producer.send_and_wait(self.topic, b"large_message-" * 100)
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))
        await producer.stop()

    @run_until_complete
    async def test_producer_send_leader_notfound(self):
        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts,
            request_timeout_ms=200,
        )
        await producer.start()

        with mock.patch.object(ClusterMetadata, "leader_for_partition") as mocked:
            mocked.return_value = -1
            future = await producer.send(self.topic, b"text")
            with self.assertRaises(LeaderNotAvailableError):
                await future

        with mock.patch.object(ClusterMetadata, "leader_for_partition") as mocked:
            mocked.return_value = None
            future = await producer.send(self.topic, b"text")
            with self.assertRaises(NotLeaderForPartitionError):
                await future

        await producer.stop()

    @run_until_complete
    async def test_producer_send_timeout(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()

        async def mocked_send(nodeid, req):
            raise KafkaTimeoutError()

        with mock.patch.object(producer.client, "send") as mocked:
            mocked.side_effect = mocked_send

            fut1 = await producer.send(self.topic, b"text1")
            fut2 = await producer.send(self.topic, b"text2")
            done, _ = await asyncio.wait([fut1, fut2])
            for item in done:
                with self.assertRaises(KafkaTimeoutError):
                    item.result()

        await producer.stop()

    @run_until_complete
    async def test_producer_send_error(self):
        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts,
            retry_backoff_ms=100,
            linger_ms=5,
            request_timeout_ms=400,
        )
        await producer.start()

        async def mocked_send(nodeid, req):
            # RequestTimedOutCode error for partition=0
            return ProduceResponse_v0([(self.topic, [(0, 7, 0), (1, 0, 111)])])

        with mock.patch.object(producer.client, "send") as mocked:
            mocked.side_effect = mocked_send
            fut1 = await producer.send(self.topic, b"text1", partition=0)
            fut2 = await producer.send(self.topic, b"text2", partition=1)
            with self.assertRaises(RequestTimedOutError):
                await fut1
            resp = await fut2
            self.assertEqual(resp.offset, 111)

        async def mocked_send_with_sleep(nodeid, req):
            # RequestTimedOutCode error for partition=0
            await asyncio.sleep(0.1)
            return ProduceResponse_v0([(self.topic, [(0, 7, 0)])])

        with mock.patch.object(producer.client, "send") as mocked:
            mocked.side_effect = mocked_send_with_sleep
            with self.assertRaises(RequestTimedOutError):
                future = await producer.send(self.topic, b"text1", partition=0)
                await future
        await producer.stop()

    @run_until_complete
    async def test_producer_send_batch(self):
        key = b"test key"
        value = b"test value"
        max_batch_size = 10000

        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts,
            max_batch_size=max_batch_size,
        )
        await producer.start()

        partitions = await producer.partitions_for(self.topic)
        partition = partitions.pop()

        # silly method to find current offset for this partition
        resp = await producer.send_and_wait(
            self.topic, value=b"discovering offset", partition=partition
        )
        offset = resp.offset

        # only fills up to its limits, then returns None
        batch = producer.create_batch()
        self.assertEqual(batch.record_count(), 0)
        num = 0
        while True:
            metadata = batch.append(key=key, value=value, timestamp=None)
            if metadata is None:
                break
            num += 1
        self.assertTrue(num > 0)
        self.assertEqual(batch.record_count(), num)

        # batch gets properly sent
        future = await producer.send_batch(batch, self.topic, partition=partition)
        resp = await future
        self.assertEqual(resp.topic, self.topic)
        self.assertEqual(resp.partition, partition)
        self.assertEqual(resp.offset, offset + 1)

        # batch accepts a too-large message if it's the first
        too_large = b"m" * (max_batch_size + 1)
        batch = producer.create_batch()
        metadata = batch.append(key=None, value=too_large, timestamp=None)
        self.assertIsNotNone(metadata)

        # batch rejects a too-large message if it's not the first
        batch = producer.create_batch()
        batch.append(key=None, value=b"short", timestamp=None)
        metadata = batch.append(key=None, value=too_large, timestamp=None)
        self.assertIsNone(metadata)
        await producer.stop()

        # batch can't be sent after closing time
        with self.assertRaises(ProducerClosed):
            await producer.send_batch(batch, self.topic, partition=partition)

    @run_until_complete
    async def test_producer_send_batch_with_serializer(self):
        def key_serializer(val):
            return val.upper().encode()

        def value_serializer(val):
            return json.dumps(val, separators=(",", ":")).encode()

        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        await producer.start()

        partitions = await producer.partitions_for(self.topic)
        partition = partitions.pop()

        batch = producer.create_batch()
        batch.append(key="key1", value={"value": 111}, timestamp=None)
        batch.append(key="key2", value={"value": 222}, timestamp=None)
        self.assertEqual(batch.record_count(), 2)

        # batch gets properly sent
        future = await producer.send_batch(batch, self.topic, partition=partition)
        resp = await future
        await producer.stop()
        self.assertEqual(resp.partition, partition)

        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await consumer.start()

        msg = await consumer.getone()
        self.assertEqual(msg.key, b"KEY1")
        self.assertEqual(msg.value, b'{"value":111}')

        msg = await consumer.getone()
        self.assertEqual(msg.key, b"KEY2")
        self.assertEqual(msg.value, b'{"value":222}')

        await consumer.stop()

    @pytest.mark.ssl
    @run_until_complete
    async def test_producer_ssl(self):
        # Produce by SSL consume by PLAINTEXT
        topic = "test_ssl_produce"
        context = self.create_ssl_context()
        producer = AIOKafkaProducer(
            bootstrap_servers=[f"{self.kafka_host}:{self.kafka_ssl_port}"],
            security_protocol="SSL",
            ssl_context=context,
        )
        await producer.start()
        await producer.send_and_wait(topic=topic, value=b"Super msg")
        await producer.stop()

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.hosts,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        msg = await consumer.getone()
        self.assertEqual(msg.value, b"Super msg")
        await consumer.stop()

    @run_until_complete
    async def test_producer_arguments(self):
        with self.assertRaisesRegex(
            ValueError, "`security_protocol` should be SSL or PLAINTEXT"
        ):
            AIOKafkaProducer(
                bootstrap_servers=self.hosts,
                security_protocol="SOME",
            )
        with self.assertRaisesRegex(
            ValueError, "`ssl_context` is mandatory if security_protocol=='SSL'"
        ):
            AIOKafkaProducer(
                bootstrap_servers=self.hosts,
                security_protocol="SSL",
                ssl_context=None,
            )

    @run_until_complete
    async def test_producer_flush_test(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()
        self.add_cleanup(producer.stop)

        fut1 = await producer.send("producer_flush_test", b"text1")
        fut2 = await producer.send("producer_flush_test", b"text2")
        self.assertFalse(fut1.done())
        self.assertFalse(fut2.done())

        await producer.flush()
        self.assertTrue(fut1.done())
        self.assertTrue(fut2.done())

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_producer_correct_time_returned(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()
        self.add_cleanup(producer.stop)

        send_time = time.time() * 1000
        res = await producer.send_and_wait("XXXX", b"text1", partition=0)
        self.assertLess(res.timestamp - send_time, 1000)  # 1s

        res = await producer.send_and_wait(
            "XXXX", b"text1", partition=0, timestamp_ms=123123123
        )
        self.assertEqual(res.timestamp, 123123123)

        expected_timestamp = 999999999

        async def mocked_send(*args, **kw):
            # There's no easy way to set LOG_APPEND_TIME on server, so use this
            # hack for now.
            return ProduceResponse_v2(
                topics=[("XXXX", [(0, 0, 0, expected_timestamp)])], throttle_time_ms=0
            )

        with mock.patch.object(producer.client, "send") as mocked:
            mocked.side_effect = mocked_send

            res = await producer.send_and_wait("XXXX", b"text1", partition=0)
            self.assertEqual(res.timestamp_type, LOG_APPEND_TIME)
            self.assertEqual(res.timestamp, expected_timestamp)

    @run_until_complete
    async def test_producer_send_empty_batch(self):
        # We trigger a unique case here, we don't send any messages, but the
        # ProduceBatch will be created. It should be discarded as it contains
        # 0 messages by sender routine.
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()
        self.add_cleanup(producer.stop)

        with self.assertRaises(TypeError):
            await producer.send(self.topic, "text1")

        send_mock = mock.Mock()
        send_mock.side_effect = producer._sender._send_produce_req
        producer._sender._send_produce_req = send_mock

        await producer.flush()
        self.assertEqual(send_mock.call_count, 0)

    @run_until_complete
    async def test_producer_send_reenque_resets_waiters(self):
        # See issue #409. If reenqueue method does not reset the waiter
        # properly new batches will raise RecursionError.

        producer = AIOKafkaProducer(bootstrap_servers=self.hosts, linger_ms=1000)
        await producer.start()
        self.add_cleanup(producer.stop)

        # 1st step is to force an error in produce sequense and force a
        # reenqueue on 1 batch.
        with mock.patch.object(producer.client, "send") as mocked:
            send_fut = create_future()

            async def mocked_func(node_id, request):
                if not send_fut.done():
                    send_fut.set_result(None)
                raise UnknownTopicOrPartitionError()

            mocked.side_effect = mocked_func

            fut = await producer.send(self.topic, b"Some MSG", partition=0)
            await send_fut
            # 100ms backoff time
            await asyncio.sleep(0.11)
        self.assertFalse(fut.done())
        # At this stage, the batch might have been drained again
        # Then we add another msg right after the reenqueue.
        # If drain_waiter is broken it will end up with a RecursionError.
        fut2 = await producer.send(self.topic, b"Some MSG 2", partition=0)

        await fut2
        self.assertTrue(fut.done())
        self.assertTrue(fut2.done())
        msg1 = await fut
        msg2 = await fut2

        # The order should be preserved
        self.assertLess(msg1.offset, msg2.offset)

    @run_until_complete
    async def test_producer_idempotence_configuration(self):
        with self.assertRaises(ValueError):
            AIOKafkaProducer(acks=1, enable_idempotence=True)
        producer = AIOKafkaProducer(enable_idempotence=True)
        self.add_cleanup(producer.stop)
        self.assertEqual(producer._sender._acks, -1)  # -1 is set for `all`
        self.assertIsNotNone(producer._txn_manager)

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_producer_indempotence_simple(self):
        # The test here will just check if we can do simple produce with
        # enable_idempotence option, as no specific API changes is expected.

        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts, enable_idempotence=True
        )
        await producer.start()
        self.add_cleanup(producer.stop)

        meta = await producer.send_and_wait(self.topic, b"hello, Kafka!")

        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)
        msg = await consumer.getone()
        self.assertEqual(msg.offset, meta.offset)
        self.assertEqual(msg.timestamp, meta.timestamp)
        self.assertEqual(msg.value, b"hello, Kafka!")
        self.assertEqual(msg.key, None)

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_producer_indempotence_no_duplicates(self):
        # Idempotent producer should retry produce in case of timeout error
        producer = AIOKafkaProducer(
            bootstrap_servers=self.hosts,
            enable_idempotence=True,
            request_timeout_ms=2000,
        )
        await producer.start()
        self.add_cleanup(producer.stop)

        original_send = producer.client.send
        retry = [0]

        async def mocked_send(*args, **kw):
            result = await original_send(*args, **kw)
            if result.API_KEY == ProduceResponse_v0.API_KEY and retry[0] < 2:
                retry[0] += 1
                raise RequestTimedOutError
            return result

        with mock.patch.object(producer.client, "send") as mocked:
            mocked.side_effect = mocked_send

            meta = await producer.send_and_wait(self.topic, b"hello, Kafka!")

        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)
        msg = await consumer.getone()
        self.assertEqual(msg.offset, meta.offset)
        self.assertEqual(msg.timestamp, meta.timestamp)
        self.assertEqual(msg.value, b"hello, Kafka!")
        self.assertEqual(msg.key, None)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(consumer.getone(), timeout=0.5)

    @run_until_complete
    async def test_producer_invalid_leader_retry_metadata(self):
        # See related issue #362. The metadata can have a new node in leader
        # set while we still don't have metadata for that node.

        producer = AIOKafkaProducer(bootstrap_servers=self.hosts, linger_ms=1000)
        await producer.start()
        self.add_cleanup(producer.stop)

        # Make sure we have fresh metadata for partitions
        await producer.partitions_for(self.topic)
        # Alter metadata to convince the producer, that leader or partition 0
        # is a different node
        topic_meta = producer._metadata._partitions[self.topic]
        topic_meta[0] = topic_meta[0]._replace(leader=topic_meta[0].leader + 1)

        meta = await producer.send_and_wait(self.topic, b"hello, Kafka!")
        self.assertTrue(meta)

    @run_until_complete
    async def test_producer_leader_change_preserves_order(self):
        # Before 0.5.0 we did not lock partition until a response came from
        # the server, but locked the node itself.
        # For example: Say the sender sent a request to node 1 and before an
        # failure answer came we updated metadata and leader become node 0.
        # This way we may send the next batch to node 0 without waiting for
        # node 1 batch to be reenqueued, resulting in out-of-order batches

        producer = AIOKafkaProducer(bootstrap_servers=self.hosts, linger_ms=1000)
        await producer.start()
        self.add_cleanup(producer.stop)

        # Alter metadata to convince the producer, that leader or partition 0
        # is a different node
        await producer.partitions_for(self.topic)
        topic_meta = producer._metadata._partitions[self.topic]
        real_leader = topic_meta[0].leader
        topic_meta[0] = topic_meta[0]._replace(leader=real_leader + 1)

        # Make sure the first request for produce takes more time
        original_send = producer.client.send

        async def mocked_send(node_id, request, *args, **kw):
            if node_id != real_leader and request.API_KEY == ProduceResponse_v0.API_KEY:
                await asyncio.sleep(2)

            result = await original_send(node_id, request, *args, **kw)
            return result

        producer.client.send = mocked_send

        # Send Batch 1. This will end up waiting for some time on fake leader
        batch = producer.create_batch()
        meta = batch.append(key=b"key", value=b"1", timestamp=None)
        batch.close()
        fut = await producer.send_batch(batch, self.topic, partition=0)

        # Make sure we sent the request
        await asyncio.sleep(0.1)
        # Update metadata to return leader to real one
        await producer.client.force_metadata_update()

        # Send Batch 2, that if it's bugged will go straight to the real node
        batch2 = producer.create_batch()
        meta2 = batch2.append(key=b"key", value=b"2", timestamp=None)
        batch2.close()
        fut2 = await producer.send_batch(batch2, self.topic, partition=0)

        batch_meta = await fut
        batch_meta2 = await fut2

        # Check the order of messages
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)
        msg = await consumer.getone()
        self.assertEqual(msg.offset, batch_meta.offset)
        self.assertEqual(msg.timestamp or -1, meta.timestamp)
        self.assertEqual(msg.value, b"1")
        self.assertEqual(msg.key, b"key")
        msg2 = await consumer.getone()
        self.assertEqual(msg2.offset, batch_meta2.offset)
        self.assertEqual(msg2.timestamp or -1, meta2.timestamp)
        self.assertEqual(msg2.value, b"2")
        self.assertEqual(msg2.key, b"key")

    @run_until_complete
    async def test_producer_sender_errors_propagate_to_producer(self):
        # Following on #362 there may be other unexpected errors in sender
        # routine that we want the user to see, rather than just get stuck.

        producer = AIOKafkaProducer(bootstrap_servers=self.hosts, linger_ms=1000)
        await producer.start()
        self.add_cleanup(producer.stop)

        with mock.patch.object(producer._sender, "_send_produce_req") as m:
            m.side_effect = KeyError

            with self.assertRaisesRegex(
                KafkaError, "Unexpected error during batch delivery"
            ):
                await producer.send_and_wait(self.topic, b"hello, Kafka!")

        with self.assertRaisesRegex(
            KafkaError, "Unexpected error during batch delivery"
        ):
            await producer.send_and_wait(self.topic, b"hello, Kafka!")

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_producer_send_with_headers(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()
        self.add_cleanup(producer.stop)

        fut = await producer.send(
            self.topic, b"msg", partition=0, headers=[("type", b"Normal")]
        )
        resp = await fut
        self.assertEqual(resp.partition, 0)

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_producer_send_and_wait_with_headers(self):
        producer = AIOKafkaProducer(bootstrap_servers=self.hosts)
        await producer.start()
        self.add_cleanup(producer.stop)

        resp = await producer.send_and_wait(
            self.topic, b"msg", partition=0, headers=[("type", b"Normal")]
        )
        self.assertEqual(resp.partition, 0)
