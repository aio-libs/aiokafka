import asyncio
import gc
import json
import pytest
import time
import weakref
from unittest import mock

from kafka.cluster import ClusterMetadata
from kafka.common import (KafkaTimeoutError,
                          UnknownTopicOrPartitionError,
                          MessageSizeTooLargeError,
                          NotLeaderForPartitionError,
                          LeaderNotAvailableError,
                          RequestTimedOutError)
from kafka.protocol.produce import ProduceResponse

from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, kafka_versions
)

from aiokafka.producer import AIOKafkaProducer
from aiokafka.client import AIOKafkaClient
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.errors import ProducerClosed
from aiokafka.util import PY_341

LOG_APPEND_TIME = 1


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):
    topic = 'test_produce_topic'

    @run_until_complete
    def test_producer_start(self):
        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(loop=self.loop, acks=122)

        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(loop=self.loop, api_version="3.4.5")

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        self.assertNotEqual(producer.client.api_version, 'auto')
        partitions = yield from producer.partitions_for('some_topic_name')
        self.assertEqual(len(partitions), 2)
        self.assertEqual(partitions, set([0, 1]))
        yield from producer.stop()
        self.assertEqual(producer._closed, True)

    @pytest.mark.skipif(not PY_341, reason="Not supported on older Python's")
    @run_until_complete
    def test_producer_warn_unclosed(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        producer_ref = weakref.ref(producer)
        yield from producer.start()

        with self.silence_loop_exception_handler():
            with self.assertWarnsRegex(
                    ResourceWarning, "Unclosed AIOKafkaProducer"):
                del producer
                # _sender_routine will contain a reference and will only be
                # freed after loop will spin once. Not sure why dou...
                yield from asyncio.sleep(0, loop=self.loop)
                gc.collect()
                # Assure that the reference was properly collected
                self.assertIsNone(producer_ref())

    @run_until_complete
    def test_producer_notopic(self):
        producer = AIOKafkaProducer(
            loop=self.loop, request_timeout_ms=200,
            bootstrap_servers=self.hosts)
        yield from producer.start()
        with mock.patch.object(
                AIOKafkaClient, '_metadata_update') as mocked:
            @asyncio.coroutine
            def dummy(*d, **kw):
                return
            mocked.side_effect = dummy
            with self.assertRaises(UnknownTopicOrPartitionError):
                yield from producer.send_and_wait('some_topic', b'hello')
        yield from producer.stop()

    @run_until_complete
    def test_producer_send(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        with self.assertRaises(TypeError):
            yield from producer.send(self.topic, 'hello, Kafka!')
        future = yield from producer.send(self.topic, b'hello, Kafka!')
        resp = yield from future
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))
        self.assertEqual(resp.offset, 0)

        fut = yield from producer.send(self.topic, b'second msg', partition=1)
        resp = yield from fut
        self.assertEqual(resp.partition, 1)

        future = yield from producer.send(self.topic, b'value', key=b'KEY')
        resp = yield from future
        self.assertTrue(resp.partition in (0, 1))

        resp = yield from producer.send_and_wait(self.topic, b'value')
        self.assertTrue(resp.partition in (0, 1))

        yield from producer.stop()
        with self.assertRaises(ProducerClosed):
            yield from producer.send(self.topic, b'value', key=b'KEY')

    @run_until_complete
    def test_producer_send_noack(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts, acks=0)
        yield from producer.start()
        fut1 = yield from producer.send(
            self.topic, b'hello, Kafka!', partition=0)
        fut2 = yield from producer.send(
            self.topic, b'hello, Kafka!', partition=1)
        done, _ = yield from asyncio.wait([fut1, fut2], loop=self.loop)
        for item in done:
            self.assertEqual(item.result(), None)
        yield from producer.stop()

    @run_until_complete
    def test_producer_send_with_serializer(self):
        def key_serializer(val):
            return val.upper().encode()

        def serializer(val):
            return json.dumps(val).encode()

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            value_serializer=serializer,
            key_serializer=key_serializer, acks='all',
            max_request_size=1000)
        yield from producer.start()
        key = 'some key'
        value = {'strKey': 23523.443, 23: 'STRval'}
        future = yield from producer.send(self.topic, value, key=key)
        resp = yield from future
        partition = resp.partition
        offset = resp.offset
        self.assertTrue(partition in (0, 1))  # partition

        future = yield from producer.send(self.topic, 'some str', key=key)
        resp = yield from future
        # expect the same partition bcs the same key
        self.assertEqual(resp.partition, partition)
        # expect offset +1
        self.assertEqual(resp.offset, offset + 1)

        value[23] = '*VALUE' * 800
        with self.assertRaises(MessageSizeTooLargeError):
            yield from producer.send(self.topic, value, key=key)

        yield from producer.stop()
        yield from producer.stop()  # shold be Ok

    @run_until_complete
    def test_producer_send_with_compression(self):
        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(
                loop=self.loop, compression_type='my_custom')

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type='gzip')

        yield from producer.start()

        # short message will not be compressed
        future = yield from producer.send(
            self.topic, b'this msg is too short for compress')
        resp = yield from future
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))

        # now message will be compressed
        resp = yield from producer.send_and_wait(
            self.topic, b'large_message-' * 100)
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))
        yield from producer.stop()

    @run_until_complete
    def test_producer_send_leader_notfound(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            request_timeout_ms=200)
        yield from producer.start()

        with mock.patch.object(
                ClusterMetadata, 'leader_for_partition') as mocked:
            mocked.return_value = -1
            future = yield from producer.send(self.topic, b'text')
            with self.assertRaises(LeaderNotAvailableError):
                yield from future

        with mock.patch.object(
                ClusterMetadata, 'leader_for_partition') as mocked:
            mocked.return_value = None
            future = yield from producer.send(self.topic, b'text')
            with self.assertRaises(NotLeaderForPartitionError):
                yield from future

        yield from producer.stop()

    @run_until_complete
    def test_producer_send_timeout(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()

        @asyncio.coroutine
        def mocked_send(nodeid, req):
            raise KafkaTimeoutError()

        with mock.patch.object(producer.client, 'send') as mocked:
            mocked.side_effect = mocked_send

            fut1 = yield from producer.send(self.topic, b'text1')
            fut2 = yield from producer.send(self.topic, b'text2')
            done, _ = yield from asyncio.wait([fut1, fut2], loop=self.loop)
            for item in done:
                with self.assertRaises(KafkaTimeoutError):
                    item.result()

        yield from producer.stop()

    @run_until_complete
    def test_producer_send_error(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            retry_backoff_ms=100,
            linger_ms=5, request_timeout_ms=400)
        yield from producer.start()

        @asyncio.coroutine
        def mocked_send(nodeid, req):
            # RequestTimedOutCode error for partition=0
            return ProduceResponse[0]([(self.topic, [(0, 7, 0), (1, 0, 111)])])

        with mock.patch.object(producer.client, 'send') as mocked:
            mocked.side_effect = mocked_send
            fut1 = yield from producer.send(self.topic, b'text1', partition=0)
            fut2 = yield from producer.send(self.topic, b'text2', partition=1)
            with self.assertRaises(RequestTimedOutError):
                yield from fut1
            resp = yield from fut2
            self.assertEqual(resp.offset, 111)

        @asyncio.coroutine
        def mocked_send_with_sleep(nodeid, req):
            # RequestTimedOutCode error for partition=0
            yield from asyncio.sleep(0.1, loop=self.loop)
            return ProduceResponse[0]([(self.topic, [(0, 7, 0)])])

        with mock.patch.object(producer.client, 'send') as mocked:
            mocked.side_effect = mocked_send_with_sleep
            with self.assertRaises(RequestTimedOutError):
                future = yield from producer.send(
                    self.topic, b'text1', partition=0)
                yield from future
        yield from producer.stop()

    @run_until_complete
    def test_producer_send_batch(self):
        key = b'test key'
        value = b'test value'
        max_batch_size = 10000

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            max_batch_size=max_batch_size)
        yield from producer.start()

        partitions = yield from producer.partitions_for(self.topic)
        partition = partitions.pop()

        # silly method to find current offset for this partition
        resp = yield from producer.send_and_wait(
            self.topic, value=b'discovering offset', partition=partition)
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
        future = yield from producer.send_batch(
            batch, self.topic, partition=partition)
        resp = yield from future
        self.assertEqual(resp.topic, self.topic)
        self.assertEqual(resp.partition, partition)
        self.assertEqual(resp.offset, offset + 1)

        # batch accepts a too-large message if it's the first
        too_large = b'm' * (max_batch_size + 1)
        batch = producer.create_batch()
        metadata = batch.append(key=None, value=too_large, timestamp=None)
        self.assertIsNotNone(metadata)

        # batch rejects a too-large message if it's not the first
        batch = producer.create_batch()
        batch.append(key=None, value=b"short", timestamp=None)
        metadata = batch.append(key=None, value=too_large, timestamp=None)
        self.assertIsNone(metadata)
        yield from producer.stop()

        # batch can't be sent after closing time
        with self.assertRaises(ProducerClosed):
            yield from producer.send_batch(
                batch, self.topic, partition=partition)

    @pytest.mark.ssl
    @run_until_complete
    def test_producer_ssl(self):
        # Produce by SSL consume by PLAINTEXT
        topic = "test_ssl_produce"
        context = self.create_ssl_context()
        producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=[
                "{}:{}".format(self.kafka_host, self.kafka_ssl_port)],
            security_protocol="SSL", ssl_context=context)
        yield from producer.start()
        yield from producer.send_and_wait(topic=topic, value=b"Super msg")
        yield from producer.stop()

        consumer = AIOKafkaConsumer(
            topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            enable_auto_commit=True,
            auto_offset_reset="earliest")
        yield from consumer.start()
        msg = yield from consumer.getone()
        self.assertEqual(msg.value, b"Super msg")
        yield from consumer.stop()

    def test_producer_arguments(self):
        with self.assertRaisesRegex(
                ValueError, "`security_protocol` should be SSL or PLAINTEXT"):
            AIOKafkaProducer(
                loop=self.loop,
                bootstrap_servers=self.hosts,
                security_protocol="SOME")
        with self.assertRaisesRegex(
                ValueError, "`ssl_context` is mandatory if "
                            "security_protocol=='SSL'"):
            AIOKafkaProducer(
                loop=self.loop,
                bootstrap_servers=self.hosts,
                security_protocol="SSL", ssl_context=None)

    @run_until_complete
    def test_producer_flush_test(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        self.add_cleanup(producer.stop)

        fut1 = yield from producer.send("producer_flush_test", b'text1')
        fut2 = yield from producer.send("producer_flush_test", b'text2')
        self.assertFalse(fut1.done())
        self.assertFalse(fut2.done())

        yield from producer.flush()
        self.assertTrue(fut1.done())
        self.assertTrue(fut2.done())

    @kafka_versions('>=0.10.0')
    @run_until_complete
    def test_producer_correct_time_returned(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        self.add_cleanup(producer.stop)

        send_time = (time.time() * 1000)
        res = yield from producer.send_and_wait(
            "XXXX", b'text1', partition=0)
        self.assertLess(res.timestamp - send_time, 1000)  # 1s

        res = yield from producer.send_and_wait(
            "XXXX", b'text1', partition=0, timestamp_ms=123123123)
        self.assertEqual(res.timestamp, 123123123)

        expected_timestamp = 999999999

        @asyncio.coroutine
        def mocked_send(*args, **kw):
            # There's no easy way to set LOG_APPEND_TIME on server, so use this
            # hack for now.
            return ProduceResponse[2](
                topics=[
                    ('XXXX', [(0, 0, 0, expected_timestamp)])],
                throttle_time_ms=0)

        with mock.patch.object(producer.client, 'send') as mocked:
            mocked.side_effect = mocked_send

            res = yield from producer.send_and_wait(
                "XXXX", b'text1', partition=0)
            self.assertEqual(res.timestamp_type, LOG_APPEND_TIME)
            self.assertEqual(res.timestamp, expected_timestamp)

    @run_until_complete
    def test_producer_send_empty_batch(self):
        # We trigger a unique case here, we don't send any messages, but the
        # ProduceBatch will be created. It should be discarded as it contains
        # 0 messages by sender routine.
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        self.add_cleanup(producer.stop)

        with self.assertRaises(TypeError):
            yield from producer.send(self.topic, 'text1')

        send_mock = mock.Mock()
        send_mock.side_effect = producer._send_produce_req
        producer._send_produce_req = send_mock

        yield from producer.flush()
        self.assertEqual(send_mock.call_count, 0)
