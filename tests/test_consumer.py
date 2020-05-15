import asyncio
import gc
import time
import json
from unittest import mock
from contextlib import contextmanager

import pytest

from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.consumer.fetcher import RecordTooLargeError, FetchRequest
from aiokafka.consumer import fetcher
from aiokafka.producer import AIOKafkaProducer
from aiokafka.record import MemoryRecords
from aiokafka.client import AIOKafkaClient
from aiokafka.util import ensure_future
from aiokafka.structs import (
    OffsetAndTimestamp, TopicPartition, OffsetAndMetadata
)
from aiokafka.errors import (
    IllegalStateError, OffsetOutOfRangeError, UnsupportedVersionError,
    KafkaTimeoutError, NoOffsetForPartitionError, ConsumerStoppedError,
    IllegalOperation, UnknownError, KafkaError, InvalidSessionTimeoutError,
    CorruptRecordException
)

from ._testutil import (
    KafkaIntegrationTestCase, StubRebalanceListener,
    run_until_complete, random_string, kafka_versions)


class TestConsumerIntegration(KafkaIntegrationTestCase):
    async def consumer_factory(self, **kwargs):
        enable_auto_commit = kwargs.pop('enable_auto_commit', True)
        auto_offset_reset = kwargs.pop('auto_offset_reset', 'earliest')
        group = kwargs.pop('group', 'group-%s' % self.id())
        consumer = AIOKafkaConsumer(
            self.topic, group_id=group,
            bootstrap_servers=self.hosts,
            enable_auto_commit=enable_auto_commit,
            auto_offset_reset=auto_offset_reset,
            **kwargs)
        await consumer.start()
        self.add_cleanup(consumer.stop)
        if group is not None:
            await consumer.seek_to_committed()
        return consumer

    @contextmanager
    def count_fetch_requests(self, consumer, count):
        records_class = fetcher.PartitionRecords
        with mock.patch.object(
                fetcher, "PartitionRecords") as mocked:
            call_count = [0]

            def factory(*args, **kw):
                res = records_class(*args, **kw)
                call_count[0] += 1
                return res

            mocked.side_effect = factory
            yield
            self.assertEqual(call_count[0], count)

    @run_until_complete
    async def test_simple_consumer(self):
        with self.assertRaises(ValueError):
            # check unsupported version
            consumer = await self.consumer_factory(api_version="0.8")

        now = time.time()
        await self.send_messages(0, list(range(0, 100)))
        await self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer = await self.consumer_factory()

        p0 = TopicPartition(self.topic, 0)
        p1 = TopicPartition(self.topic, 1)
        assignment = consumer.assignment()
        self.assertEqual(sorted(list(assignment)), [p0, p1])

        topics = await consumer.topics()
        self.assertTrue(self.topic in topics)

        parts = consumer.partitions_for_topic(self.topic)
        self.assertEqual(sorted(list(parts)), [0, 1])

        offset = await consumer.committed(
            TopicPartition("uknown-topic", 2))
        self.assertEqual(offset, None)

        offset = await consumer.committed(p0)
        if offset is None:
            offset = 0

        messages = []
        for i in range(200):
            message = await consumer.getone()
            messages.append(message)
        self.assert_message_count(messages, 200)

        h = consumer.highwater(p0)
        self.assertEqual(h, 100)
        t = consumer.last_poll_timestamp(p0)
        self.assertGreaterEqual(t, int(now * 1000))
        now = time.time()
        self.assertLessEqual(t, int(now * 1000))

        consumer.seek(p0, offset + 90)
        for i in range(10):
            m = await consumer.getone()
            self.assertEqual(m.value, str(i + 90).encode())
        await consumer.stop()

        # will ignore, no exception expected
        await consumer.stop()

    @run_until_complete
    async def test_consumer_context_manager(self):
        await self.send_messages(0, list(range(0, 10)))

        group = 'group-%s' % self.id()
        consumer = AIOKafkaConsumer(
            self.topic, group_id=group,
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
            auto_offset_reset="earliest")
        async with consumer as con:
            assert con is consumer
            assert consumer._fetcher is not None
            messages = []
            async for m in consumer:
                messages.append(m)
                if len(messages) == 10:
                    break
            self.assert_message_count(messages, 10)
        assert consumer._closed

        # Finilize on exception too
        consumer = AIOKafkaConsumer(
            self.topic, group_id=group,
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
            auto_offset_reset="earliest")
        with pytest.raises(ValueError):
            async with consumer as con:
                assert con is consumer
                assert consumer._fetcher is not None
                messages = []
                async for m in consumer:
                    messages.append(m)
                    if len(messages) == 10:
                        break
                self.assert_message_count(messages, 10)
                raise ValueError
        assert consumer._closed

    @run_until_complete
    async def test_consumer_api_version(self):
        await self.send_messages(0, list(range(0, 10)))
        for text_version, api_version in [
                ("auto", (0, 9, 0)),
                ("0.9.1", (0, 9, 1)),
                ("0.10.0", (0, 10, 0)),
                ("0.11", (0, 11, 0)),
                ("0.12.1", (0, 12, 1)),
                ("1.0.2", (1, 0, 2))]:
            consumer = AIOKafkaConsumer(
                loop=self.loop, bootstrap_servers=self.hosts,
                api_version=text_version)
            self.assertEqual(consumer._client.api_version, api_version)
            await consumer.stop()

        # invalid cases
        for version in ["0", "1", "0.10.0.1"]:
            with self.assertRaises(ValueError):
                AIOKafkaConsumer(
                    loop=self.loop, bootstrap_servers=self.hosts,
                    api_version=version)
        for version in [(0, 9), (0, 9, 1)]:
            with self.assertRaises(TypeError):
                AIOKafkaConsumer(
                    loop=self.loop, bootstrap_servers=self.hosts,
                    api_version=version)

    @run_until_complete
    async def test_consumer_warn_unclosed(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id=None,
            bootstrap_servers=self.hosts)
        await consumer.start()

        with self.silence_loop_exception_handler():
            with self.assertWarnsRegex(
                    ResourceWarning, "Unclosed AIOKafkaConsumer"):
                del consumer
                await asyncio.sleep(0, loop=self.loop)
                gc.collect()

    @run_until_complete
    async def test_get_by_partition(self):
        await self.send_messages(0, list(range(0, 100)))
        await self.send_messages(1, list(range(100, 200)))
        consumer = await self.consumer_factory()

        p0 = TopicPartition(self.topic, 0)
        p1 = TopicPartition(self.topic, 1)
        messages = []

        async def task(tp, messages):
            for i in range(100):
                m = await consumer.getone(tp)
                self.assertEqual(m.partition, tp.partition)
                messages.append(m)

        task1 = ensure_future(task(p0, messages), loop=self.loop)
        task2 = ensure_future(task(p1, messages), loop=self.loop)
        await asyncio.wait([task1, task2], loop=self.loop)
        self.assert_message_count(messages, 200)

    @run_until_complete
    async def test_none_group(self):
        await self.send_messages(0, list(range(0, 100)))
        await self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer1 = await self.consumer_factory(
            group=None, enable_auto_commit=False)
        consumer2 = await self.consumer_factory(group=None)

        messages = []
        for i in range(200):
            message = await consumer1.getone()
            messages.append(message)
        self.assert_message_count(messages, 200)
        with self.assertRaises(IllegalOperation):
            # commit does not supported for None group
            await consumer1.commit()

        messages = []
        for i in range(200):
            message = await consumer2.getone()
            messages.append(message)
        self.assert_message_count(messages, 200)

    @run_until_complete
    async def test_consumer_poll(self):
        await self.send_messages(0, list(range(0, 100)))
        await self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer = await self.consumer_factory()

        messages = []
        while True:
            resp = await consumer.getmany(timeout_ms=1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) >= 200:
                break
        self.assert_message_count(messages, 200)

        p0 = TopicPartition(self.topic, 0)
        p1 = TopicPartition(self.topic, 1)
        await self.send_messages(0, list(range(0, 100)))
        await self.send_messages(1, list(range(100, 200)))

        # Check consumption for a specific partition
        messages = []
        while True:
            resp = await consumer.getmany(p0, timeout_ms=1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) >= 100:
                break
        self.assert_message_count(messages, 100)

        while True:
            resp = await consumer.getmany(p1, timeout_ms=1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) >= 200:
                break
        self.assert_message_count(messages, 200)

    @run_until_complete
    async def test_large_messages(self):
        # Produce 10 "normal" size messages
        r_msgs = [str(x) for x in range(10)]
        small_messages = await self.send_messages(0, r_msgs)

        # Produce 10 messages that are large (bigger than default fetch size)
        l_msgs = [random_string(5000) for _ in range(10)]
        large_messages = await self.send_messages(0, l_msgs)

        # Consumer should still get all of them
        consumer = await self.consumer_factory()
        expected_messages = set(small_messages + large_messages)
        actual_messages = []
        for i in range(20):
            m = await consumer.getone()
            actual_messages.append(m)
        actual_messages = {m.value for m in actual_messages}
        self.assertEqual(expected_messages, set(actual_messages))

    @run_until_complete
    async def test_too_large_messages_getone(self):
        msgs = [
            random_string(10),  # small one
            random_string(50000),  # large one
            random_string(50)   # another small one
        ]
        messages = await self.send_messages(0, msgs)

        consumer = await self.consumer_factory(
            max_partition_fetch_bytes=4000)
        m = await consumer.getone()
        self.assertEqual(m.value, messages[0])

        # Starting from 0.10.1 we should be able to handle any message size,
        # as we use FetchRequest v3 or above
        if consumer._client.api_version <= (0, 10, 0):
            with self.assertRaises(RecordTooLargeError):
                await consumer.getone()
        else:
            m = await consumer.getone()
            self.assertEqual(m.value, messages[1])

        m = await consumer.getone()
        self.assertEqual(m.value, messages[2])

    @run_until_complete
    async def test_too_large_messages_getmany(self):
        msgs = [
            random_string(10),  # small one
            random_string(50000),  # large one
            random_string(50),   # another small one
        ]
        messages = await self.send_messages(0, msgs)
        tp = TopicPartition(self.topic, 0)

        consumer = await self.consumer_factory(
            max_partition_fetch_bytes=4000)

        # First fetch will get 1 small message and discard the large one
        m = await consumer.getmany(timeout_ms=1000)
        self.assertTrue(m)
        self.assertEqual(m[tp][0].value, messages[0])

        # Starting from 0.10.1 we should be able to handle any message size,
        # as we use FetchRequest v3 or above
        if consumer._client.api_version <= (0, 10, 0):
            with self.assertRaises(RecordTooLargeError):
                await consumer.getmany(timeout_ms=1000)
        else:
            m = await consumer.getmany(timeout_ms=1000)
            self.assertTrue(m)
            self.assertEqual(m[tp][0].value, messages[1])

        m = await consumer.getmany(timeout_ms=1000)
        self.assertTrue(m)
        self.assertEqual(m[tp][0].value, messages[2])

    @run_until_complete
    async def test_offset_behavior__resuming_behavior(self):
        msgs1 = await self.send_messages(0, range(0, 100))
        msgs2 = await self.send_messages(1, range(100, 200))

        available_msgs = msgs1 + msgs2
        # Start a consumer_factory
        consumer1 = await self.consumer_factory()
        consumer2 = await self.consumer_factory()
        result = []
        for i in range(10):
            msg = await consumer1.getone()
            result.append(msg.value)
        await consumer1.stop()

        # consumer2 should take both partitions after rebalance
        while True:
            msg = await consumer2.getone()
            result.append(msg.value)
            if len(result) == len(available_msgs):
                break

        self.assertEqual(set(available_msgs), set(result))

    @run_until_complete
    async def test_subscribe_manual(self):
        msgs1 = await self.send_messages(0, range(0, 10))
        msgs2 = await self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = await self.consumer_factory()
        pos = await consumer.position(TopicPartition(self.topic, 0))
        with self.assertRaises(IllegalStateError):
            consumer.assign([TopicPartition(self.topic, 0)])
        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 0)])
        result = []
        for i in range(10):
            msg = await consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(result), set(msgs1))
        await consumer.commit()
        pos = await consumer.position(TopicPartition(self.topic, 0))
        self.assertTrue(pos > 0)

        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 1)])
        for i in range(10):
            msg = await consumer.getone()
            result.append(msg.value)
        await consumer.stop()
        self.assertEqual(set(available_msgs), set(result))

    @run_until_complete
    async def test_manual_subscribe_pattern(self):
        msgs1 = await self.send_messages(0, range(0, 10))
        msgs2 = await self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id='test-group',
            bootstrap_servers=self.hosts, auto_offset_reset='earliest',
            enable_auto_commit=False)
        consumer.subscribe(pattern="topic-test_manual_subs*")
        await consumer.start()
        self.add_cleanup(consumer.stop)
        await consumer.seek_to_committed()
        result = []
        for i in range(20):
            msg = await consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(available_msgs), set(result))

        await consumer.commit(
            {TopicPartition(self.topic, 0): OffsetAndMetadata(9, '')})
        await consumer.seek_to_committed(TopicPartition(self.topic, 0))
        msg = await consumer.getone(TopicPartition(self.topic, 0))
        self.assertEqual(msg.value, b'9')
        await consumer.commit(
            {TopicPartition(self.topic, 0): OffsetAndMetadata(10, '')})
        await consumer.stop()

        # subscribe by topic
        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id='test-group',
            bootstrap_servers=self.hosts, auto_offset_reset='earliest',
            enable_auto_commit=False)
        consumer.subscribe(topics=(self.topic,))
        await consumer.start()
        self.add_cleanup(consumer.stop)
        await consumer.seek_to_committed()
        result = []
        for i in range(10):
            msg = await consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(msgs2), set(result))
        self.assertEqual(consumer.subscription(), set([self.topic]))

    @run_until_complete
    async def test_subscribe_errors(self):
        consumer = await self.consumer_factory()
        with self.assertRaises(ValueError):
            consumer.subscribe(topics=(self.topic, ), pattern="some")
        with self.assertRaises(ValueError):
            consumer.subscribe(topics=(), pattern=None)
        with self.assertRaises(ValueError):
            consumer.subscribe(pattern="^(spome(")
        with self.assertRaises(ValueError):
            consumer.subscribe("some_topic")  # should be a list
        with self.assertRaises(TypeError):
            consumer.subscribe(topics=["some_topic"], listener=object())

    @run_until_complete
    async def test_compress_decompress(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type="gzip")
        await producer.start()
        await self.wait_topic(producer.client, self.topic)
        msg1 = b'some-message' * 10
        msg2 = b'other-message' * 30
        await producer.send(self.topic, msg1, partition=1)
        await producer.send(self.topic, msg2, partition=1)
        await producer.stop()

        consumer = await self.consumer_factory()
        rmsg1 = await consumer.getone()
        self.assertEqual(rmsg1.value, msg1)
        rmsg2 = await consumer.getone()
        self.assertEqual(rmsg2.value, msg2)

    @run_until_complete
    async def test_compress_decompress_lz4(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type="lz4")
        await producer.start()
        await self.wait_topic(producer.client, self.topic)
        msg1 = b'some-message' * 10
        msg2 = b'other-message' * 30
        await producer.send(self.topic, msg1, partition=1)
        await producer.send(self.topic, msg2, partition=1)
        await producer.stop()

        consumer = await self.consumer_factory()
        rmsg1 = await consumer.getone()
        self.assertEqual(rmsg1.value, msg1)
        rmsg2 = await consumer.getone()
        self.assertEqual(rmsg2.value, msg2)

    @run_until_complete
    async def test_consumer_seek_backward(self):
        # Send 3 messages
        await self.send_messages(0, [1, 2, 3])

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = await self.consumer_factory()
        with self.count_fetch_requests(consumer, 1):
            rmsg1 = await consumer.getone()
            self.assertEqual(rmsg1.value, b'1')

        # Seek should invalidate the remaining messages
        tp = TopicPartition(self.topic, rmsg1.partition)
        consumer.seek(tp, rmsg1.offset)
        with self.count_fetch_requests(consumer, 1):
            rmsg2 = await consumer.getone()
            self.assertEqual(rmsg2.value, b'1')
            rmsg2 = await consumer.getone()
            self.assertEqual(rmsg2.value, b'2')
        # Same with getmany
        consumer.seek(tp, rmsg2.offset)
        with self.count_fetch_requests(consumer, 1):
            res = await consumer.getmany(timeout_ms=500)
            rmsg3 = res[tp][0]
            self.assertEqual(rmsg3.value, b'2')
            rmsg3 = res[tp][1]
            self.assertEqual(rmsg3.value, b'3')

    @run_until_complete
    async def test_consumer_seek_forward_getone(self):
        # Send 3 messages
        await self.send_messages(0, [1, 2, 3])

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = await self.consumer_factory()
        with self.count_fetch_requests(consumer, 1):
            rmsg1 = await consumer.getone()
            self.assertEqual(rmsg1.value, b'1')

        # Seek should invalidate the remaining message
        tp = TopicPartition(self.topic, rmsg1.partition)
        consumer.seek(tp, rmsg1.offset + 2)
        with self.count_fetch_requests(consumer, 1):
            rmsg2 = await consumer.getone()
            self.assertEqual(rmsg2.value, b'3')

        res = await consumer.getmany(timeout_ms=0)
        self.assertEqual(res, {})

    @run_until_complete
    async def test_consumer_seek_forward_getmany(self):
        # Send 3 messages
        await self.send_messages(0, [1, 2, 3])

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = await self.consumer_factory()
        with self.count_fetch_requests(consumer, 1):
            rmsg1 = await consumer.getone()
            self.assertEqual(rmsg1.value, b'1')

        # Seek should invalidate the remaining message
        tp = TopicPartition(self.topic, rmsg1.partition)
        consumer.seek(tp, rmsg1.offset + 2)
        with self.count_fetch_requests(consumer, 1):
            rmsg2 = await consumer.getmany(timeout_ms=500)
            rmsg2 = rmsg2[tp][0]
            self.assertEqual(rmsg2.value, b'3')

        res = await consumer.getmany(timeout_ms=0)
        self.assertEqual(res, {})

    @run_until_complete
    async def test_consumer_seek_to_beginning(self):
        # Send 3 messages
        await self.send_messages(0, [1, 2, 3])
        consumer = await self.consumer_factory()

        tp = TopicPartition(self.topic, 0)
        start_position = await consumer.position(tp)

        rmsg1 = await consumer.getone()
        await consumer.seek_to_beginning()
        rmsg2 = await consumer.getone()
        self.assertEqual(rmsg2.value, rmsg1.value)

        await consumer.seek_to_beginning(tp)
        rmsg3 = await consumer.getone()
        self.assertEqual(rmsg2.value, rmsg3.value)

        pos = await consumer.position(tp)
        self.assertEqual(pos, start_position + 1)

    @run_until_complete
    async def test_consumer_seek_to_end(self):
        # Send 3 messages
        await self.send_messages(0, [1, 2, 3])

        consumer = await self.consumer_factory()

        tp = TopicPartition(self.topic, 0)
        start_position = await consumer.position(tp)

        await consumer.seek_to_end()
        pos = await consumer.position(tp)
        self.assertEqual(pos, start_position + 3)
        task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(0.1, loop=self.loop)
        self.assertEqual(task.done(), False)

        await self.send_messages(0, [4, 5, 6])
        rmsg = await task
        self.assertEqual(rmsg.value, b"4")

        await consumer.seek_to_end(tp)
        task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(0.1, loop=self.loop)
        self.assertEqual(task.done(), False)

        await self.send_messages(0, [7, 8, 9])
        rmsg = await task
        self.assertEqual(rmsg.value, b"7")

        pos = await consumer.position(tp)
        self.assertEqual(pos, start_position + 7)

    @run_until_complete
    async def test_consumer_seek_on_unassigned(self):
        tp0 = TopicPartition(self.topic, 0)
        tp1 = TopicPartition(self.topic, 1)
        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id=None, bootstrap_servers=self.hosts)
        await consumer.start()
        self.add_cleanup(consumer.stop)
        consumer.assign([tp0])

        with self.assertRaises(IllegalStateError):
            await consumer.seek_to_beginning(tp1)
        with self.assertRaises(IllegalStateError):
            await consumer.seek_to_committed(tp1)
        with self.assertRaises(IllegalStateError):
            await consumer.seek_to_end(tp1)

    @run_until_complete
    async def test_consumer_seek_errors(self):
        consumer = await self.consumer_factory()
        tp = TopicPartition("topic", 0)

        with self.assertRaises(ValueError):
            consumer.seek(tp, -1)
        with self.assertRaises(ValueError):
            consumer.seek(tp, "")
        with self.assertRaises(TypeError):
            await consumer.seek_to_beginning(1)
        with self.assertRaises(TypeError):
            await consumer.seek_to_committed(1)
        with self.assertRaises(TypeError):
            await consumer.seek_to_end(1)

    @run_until_complete
    async def test_manual_subscribe_nogroup(self):
        msgs1 = await self.send_messages(0, range(0, 10))
        msgs2 = await self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id=None,
            bootstrap_servers=self.hosts, auto_offset_reset='earliest',
            enable_auto_commit=False)
        consumer.subscribe(topics=(self.topic,))
        await consumer.start()
        self.add_cleanup(consumer.stop)
        result = []
        for i in range(20):
            msg = await consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(available_msgs), set(result))

    @run_until_complete
    async def test_check_extended_message_record(self):
        s_time_ms = time.time() * 1000
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        await producer.start()
        await self.wait_topic(producer.client, self.topic)
        msg1 = b'some-message#1'
        await producer.send(self.topic, msg1, partition=1)
        await producer.stop()

        consumer = await self.consumer_factory()
        rmsg1 = await consumer.getone()
        self.assertEqual(rmsg1.value, msg1)
        self.assertEqual(rmsg1.serialized_key_size, -1)
        self.assertEqual(rmsg1.serialized_value_size, 14)
        if consumer._client.api_version >= (0, 10):
            self.assertNotEqual(rmsg1.timestamp, None)
            self.assertTrue(rmsg1.timestamp >= s_time_ms)
            self.assertEqual(rmsg1.timestamp_type, 0)
        else:
            self.assertEqual(rmsg1.timestamp, None)
            self.assertEqual(rmsg1.timestamp_type, None)

    @run_until_complete
    async def test_max_poll_records(self):
        await self.send_messages(0, list(range(100)))

        consumer = await self.consumer_factory(
            max_poll_records=48)

        data = await consumer.getmany(timeout_ms=1000)
        count = sum(map(len, data.values()))
        self.assertEqual(count, 48)
        data = await consumer.getmany(timeout_ms=1000, max_records=42)
        count = sum(map(len, data.values()))
        self.assertEqual(count, 42)
        data = await consumer.getmany(timeout_ms=1000, max_records=None)
        count = sum(map(len, data.values()))
        self.assertEqual(count, 10)

        await self.send_messages(0, list(range(1)))
        # Query more than we have
        data = await consumer.getmany(timeout_ms=1000, max_records=100)
        count = sum(map(len, data.values()))
        self.assertEqual(count, 1)

        with self.assertRaises(ValueError):
            data = await consumer.getmany(max_records=0)
        await consumer.stop()

        with self.assertRaises(ValueError):
            consumer = await self.consumer_factory(
                max_poll_records=0)

    @pytest.mark.ssl
    @run_until_complete
    async def test_ssl_consume(self):
        # Produce by PLAINTEXT, Consume by SSL
        # Send 3 messages
        await self.send_messages(0, [1, 2, 3])

        context = self.create_ssl_context()
        group = "group-{}".format(self.id())
        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop, group_id=group,
            bootstrap_servers=[
                "{}:{}".format(self.kafka_host, self.kafka_ssl_port)],
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            security_protocol="SSL", ssl_context=context)
        await consumer.start()
        self.add_cleanup(consumer.stop)
        results = await consumer.getmany(timeout_ms=1000)
        [msgs] = results.values()  # only 1 partition anyway
        msgs = [msg.value for msg in msgs]
        self.assertEqual(msgs, [b"1", b"2", b"3"])

    @run_until_complete
    async def test_consumer_arguments(self):
        with self.assertRaisesRegex(
                ValueError, "`security_protocol` should be SSL or PLAINTEXT"):
            AIOKafkaConsumer(
                self.topic, loop=self.loop,
                bootstrap_servers=self.hosts,
                security_protocol="SOME")
        with self.assertRaisesRegex(
                ValueError, "`ssl_context` is mandatory if "
                            "security_protocol=='SSL'"):
            AIOKafkaConsumer(
                self.topic, loop=self.loop,
                bootstrap_servers=self.hosts,
                security_protocol="SSL", ssl_context=None)
        with self.assertRaisesRegex(
                ValueError, "Incorrect isolation level READ_CCC"):
            consumer = AIOKafkaConsumer(
                self.topic, loop=self.loop,
                bootstrap_servers=self.hosts,
                isolation_level="READ_CCC")
            self.add_cleanup(consumer.stop)
            await consumer.start()
        with self.assertRaisesRegex(
                ValueError,
                "sasl_plain_username and sasl_plain_password required for "
                "PLAIN sasl"):
            consumer = AIOKafkaConsumer(
                self.topic, loop=self.loop,
                bootstrap_servers=self.hosts,
                security_protocol="SASL_PLAINTEXT")

    @run_until_complete
    async def test_consumer_commit_validation(self):
        consumer = await self.consumer_factory()
        self.add_cleanup(consumer.stop)

        tp = TopicPartition(self.topic, 0)
        offset = await consumer.position(tp)
        offset_and_metadata = OffsetAndMetadata(offset, "")

        with self.assertRaises(ValueError):
            await consumer.commit({})
        with self.assertRaises(ValueError):
            await consumer.commit("something")
        with self.assertRaises(ValueError):
            await consumer.commit({tp: (offset, "metadata", 100)})
        with self.assertRaisesRegex(
                ValueError, "Key should be TopicPartition instance"):
            await consumer.commit({"my_topic": offset_and_metadata})
        with self.assertRaisesRegex(
                ValueError, "Metadata should be a string"):
            await consumer.commit({tp: (offset, 1000)})
        with self.assertRaisesRegex(
                ValueError, "Metadata should be a string"):
            await consumer.commit({tp: (offset, b"\x00\x02")})

        with self.assertRaisesRegex(
                IllegalStateError, "Partition .* is not assigned"):
            await consumer.commit({TopicPartition(self.topic, 10): 1000})
        consumer.unsubscribe()
        with self.assertRaisesRegex(
                IllegalStateError, "Not subscribed to any topics"):
            await consumer.commit({tp: 1000})

        consumer = AIOKafkaConsumer(
            loop=self.loop,
            group_id='group-{}'.format(self.id()),
            bootstrap_servers=self.hosts)
        await consumer.start()
        self.add_cleanup(consumer.stop)

        consumer.subscribe(topics=set([self.topic]))
        with self.assertRaisesRegex(
                IllegalStateError, "No partitions assigned"):
            await consumer.commit({tp: 1000})

    @run_until_complete
    async def test_consumer_position(self):
        await self.send_messages(0, [1, 2, 3])

        consumer = await self.consumer_factory(enable_auto_commit=False)
        self.add_cleanup(consumer.stop)
        tp = TopicPartition(self.topic, 0)
        offset = await consumer.position(tp)
        self.assertEqual(offset, 0)
        await consumer.getone()
        offset = await consumer.position(tp)
        self.assertEqual(offset, 1)

        with self.assertRaises(IllegalStateError):
            await consumer.position(TopicPartition(self.topic, 1000))

        # If we lose assignment when waiting for position we should retry
        # with new assignment
        another_topic = self.topic + "-1"
        consumer.subscribe((self.topic, another_topic))
        await consumer._subscription.wait_for_assignment()
        assert tp in consumer.assignment()
        # At this moment the assignment is done, but position should be
        # undefined
        position_task = ensure_future(consumer.position(tp), loop=self.loop)
        await asyncio.sleep(0.0001, loop=self.loop)
        self.assertFalse(position_task.done())

        # We change subscription to imitate a rebalance
        consumer.subscribe((self.topic, ))
        offset = await position_task
        self.assertEqual(offset, 0)

        # Same case, but when we lose subscription
        consumer.subscribe((self.topic, another_topic))
        await consumer._subscription.wait_for_assignment()

        position_task = ensure_future(consumer.position(tp), loop=self.loop)
        await asyncio.sleep(0.0001, loop=self.loop)
        self.assertFalse(position_task.done())

        # We can't recover after subscription is lost
        consumer.unsubscribe()
        with self.assertRaises(IllegalStateError):
            await position_task

    @run_until_complete
    async def test_consumer_commit_no_group(self):
        consumer_no_group = await self.consumer_factory(group=None)
        tp = TopicPartition(self.topic, 0)
        offset = await consumer_no_group.position(tp)

        with self.assertRaises(IllegalOperation):
            await consumer_no_group.commit({tp: offset})
        with self.assertRaises(IllegalOperation):
            await consumer_no_group.committed(tp)

    @run_until_complete
    async def test_consumer_commit(self):
        await self.send_messages(0, [1, 2, 3])

        consumer = await self.consumer_factory()
        tp = TopicPartition(self.topic, 0)

        msg = await consumer.getone()
        # Commit by offset
        await consumer.commit({tp: msg.offset + 1})
        committed = await consumer.committed(tp)
        self.assertEqual(committed, msg.offset + 1)

        msg = await consumer.getone()
        # Commit by offset and metadata
        await consumer.commit({
            tp: (msg.offset + 2, "My metadata 2")
        })
        committed = await consumer.committed(tp)
        self.assertEqual(committed, msg.offset + 2)

    @run_until_complete
    async def test_consumer_group_without_subscription(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop,
            group_id='group-{}'.format(self.id()),
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            heartbeat_interval_ms=100)
        await consumer.start()
        await asyncio.sleep(0.2, loop=self.loop)
        await consumer.stop()

    @run_until_complete
    async def test_consumer_wait_topic(self):
        topic = "some-test-topic-for-autocreate"
        consumer = AIOKafkaConsumer(
            topic, loop=self.loop, bootstrap_servers=self.hosts)
        await consumer.start()
        self.add_cleanup(consumer.stop)
        consume_task = self.loop.create_task(consumer.getone())
        # just to be sure getone does not fail (before produce)
        await asyncio.sleep(0.5, loop=self.loop)
        self.assertFalse(consume_task.done())

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        await producer.start()
        await producer.send(topic, b'test msg')
        await producer.stop()

        data = await consume_task
        self.assertEqual(data.value, b'test msg')

    @run_until_complete
    async def test_consumer_subscribe_pattern_with_autocreate(self):
        pattern = "^some-autocreate-pattern-.*$"
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="some_group",
            fetch_max_wait_ms=50,
            auto_offset_reset="earliest")
        self.add_cleanup(consumer.stop)
        await consumer.start()
        consumer.subscribe(pattern=pattern)
        # Start getter for the topics. Should not create any topics
        consume_task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertEqual(consumer.subscription(), set())

        # Now lets autocreate the topic by fetching metadata for it.
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        self.add_cleanup(producer.stop)
        await producer.start()
        my_topic = "some-autocreate-pattern-1"
        await producer.client._wait_on_metadata(my_topic)
        # Wait for consumer to refresh metadata with new topic
        await asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >= {my_topic})
        self.assertEqual(consumer.subscription(), {my_topic})

        # Add another topic
        my_topic2 = "some-autocreate-pattern-2"
        await producer.client._wait_on_metadata(my_topic2)
        # Wait for consumer to refresh metadata with new topic
        await asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >=
                        {my_topic, my_topic2})
        self.assertEqual(consumer.subscription(), {my_topic, my_topic2})

        # Now lets actually produce some data and verify that it is consumed
        await producer.send(my_topic, b'test msg')
        data = await asyncio.wait_for(
            consume_task, timeout=2, loop=self.loop)
        self.assertEqual(data.value, b'test msg')

    @run_until_complete
    async def test_consumer_subscribe_pattern_autocreate_no_group_id(self):
        pattern = "^no-group-pattern-.*$"
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id=None,
            fetch_max_wait_ms=50,
            auto_offset_reset="earliest")
        self.add_cleanup(consumer.stop)
        await consumer.start()
        consumer.subscribe(pattern=pattern)
        # Start getter for the topics. Should not create any topics
        consume_task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertEqual(consumer.subscription(), set())

        # Now lets autocreate the topic by fetching metadata for it.
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        self.add_cleanup(producer.stop)
        await producer.start()
        my_topic = "no-group-pattern-1"
        await producer.client._wait_on_metadata(my_topic)
        # Wait for consumer to refresh metadata with new topic
        await asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >= {my_topic})
        self.assertEqual(consumer.subscription(), {my_topic})

        # Add another topic
        my_topic2 = "no-group-pattern-2"
        await producer.client._wait_on_metadata(my_topic2)
        # Wait for consumer to refresh metadata with new topic
        await asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >=
                        {my_topic, my_topic2})
        self.assertEqual(consumer.subscription(), {my_topic, my_topic2})

        # Now lets actually produce some data and verify that it is consumed
        await producer.send(my_topic, b'test msg')
        data = await asyncio.wait_for(
            consume_task, timeout=2, loop=self.loop)
        self.assertEqual(data.value, b'test msg')

    @run_until_complete
    async def test_consumer_rebalance_on_new_topic(self):
        # Test will create a consumer group and check if adding new topic
        # will trigger a group rebalance and assign partitions
        pattern = "^another-autocreate-pattern-.*$"
        client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=self.hosts,
            client_id="test_autocreate")
        await client.bootstrap()
        listener1 = StubRebalanceListener(loop=self.loop)
        listener2 = StubRebalanceListener(loop=self.loop)
        consumer1 = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="test-autocreate-rebalance",
            heartbeat_interval_ms=100)
        consumer1.subscribe(pattern=pattern, listener=listener1)
        await consumer1.start()
        consumer2 = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="test-autocreate-rebalance",
            heartbeat_interval_ms=100)
        consumer2.subscribe(pattern=pattern, listener=listener2)
        await consumer2.start()
        await asyncio.sleep(0.5, loop=self.loop)
        # bootstrap will take care of the initial group assignment
        self.assertEqual(consumer1.assignment(), set())
        self.assertEqual(consumer2.assignment(), set())
        listener1.reset()
        listener2.reset()

        # Lets force autocreation of a topic
        my_topic = "another-autocreate-pattern-1"
        await client._wait_on_metadata(my_topic)

        # Wait for group to stabilize
        assign1 = await listener1.wait_assign()
        assign2 = await listener2.wait_assign()
        # We expect 2 partitons for autocreated topics
        my_partitions = set([
            TopicPartition(my_topic, 0), TopicPartition(my_topic, 1)])
        self.assertEqual(assign1 | assign2, my_partitions)
        self.assertEqual(
            consumer1.assignment() | consumer2.assignment(),
            my_partitions)

        # Lets add another topic
        listener1.reset()
        listener2.reset()
        my_topic2 = "another-autocreate-pattern-2"
        await client._wait_on_metadata(my_topic2)

        # Wait for group to stabilize
        assign1 = await listener1.wait_assign()
        assign2 = await listener2.wait_assign()
        # We expect 2 partitons for autocreated topics
        my_partitions = set([
            TopicPartition(my_topic, 0), TopicPartition(my_topic, 1),
            TopicPartition(my_topic2, 0), TopicPartition(my_topic2, 1)])
        self.assertEqual(assign1 | assign2, my_partitions)
        self.assertEqual(
            consumer1.assignment() | consumer2.assignment(),
            my_partitions)

        await consumer1.stop()
        await consumer2.stop()
        await client.close()

    @run_until_complete
    async def test_consumer_stops_getone(self):
        # If we have a fetch in progress it should be cancelled if consumer is
        # stopped
        consumer = await self.consumer_factory()
        task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(0.1, loop=self.loop)
        # As we didn't input any data into Kafka
        self.assertFalse(task.done())

        await consumer.stop()
        # Check that pending call was cancelled
        with self.assertRaises(ConsumerStoppedError):
            await task
        # Check that any subsequent call will also raise ConsumerStoppedError
        with self.assertRaises(ConsumerStoppedError):
            await consumer.getone()

    @run_until_complete
    async def test_consumer_stops_getmany(self):
        # If we have a fetch in progress it should be cancelled if consumer is
        # stopped
        consumer = await self.consumer_factory()
        task = self.loop.create_task(consumer.getmany(timeout_ms=10000))
        await asyncio.sleep(0.1, loop=self.loop)
        # As we didn't input any data into Kafka
        self.assertFalse(task.done())

        await consumer.stop()
        # Interrupted call should just return 0 results. This will allow the
        # user to check for cancellation himself.
        self.assertTrue(task.done())
        self.assertEqual(task.result(), {})
        # Any later call will raise ConsumerStoppedError as consumer closed
        # all connections and can't continue operating.
        with self.assertRaises(ConsumerStoppedError):
            await self.loop.create_task(
                consumer.getmany(timeout_ms=10000))
        # Just check no spetial case on timeout_ms=0
        with self.assertRaises(ConsumerStoppedError):
            await self.loop.create_task(
                consumer.getmany(timeout_ms=0))

    @run_until_complete
    async def test_exclude_internal_topics(self):
        # Create random topic
        my_topic = "some_noninternal_topic"
        client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=self.hosts,
            client_id="test_autocreate")
        await client.bootstrap()
        await client._wait_on_metadata(my_topic)
        await client.close()

        # Check if only it will be subscribed
        pattern = "^.*$"
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="some_group_1",
            auto_offset_reset="earliest",
            exclude_internal_topics=False)
        consumer.subscribe(pattern=pattern)
        await consumer.start()
        self.assertIn("__consumer_offsets", consumer.subscription())
        await consumer._client.force_metadata_update()
        self.assertIn("__consumer_offsets", consumer.subscription())
        await consumer.stop()

    @run_until_complete
    async def test_offset_reset_manual(self):
        await self.send_messages(0, [1])

        consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="offset_reset_group",
            auto_offset_reset="none")
        await consumer.start()
        self.add_cleanup(consumer.stop)
        tp = TopicPartition(self.topic, 0)

        with self.assertRaises(NoOffsetForPartitionError):
            for x in range(2):
                await consumer.getmany(timeout_ms=1000)
        with self.assertRaises(NoOffsetForPartitionError):
            for x in range(2):
                await consumer.getone()

        consumer.seek(tp, 19999)
        with self.assertRaises(OffsetOutOfRangeError):
            for x in range(2):
                await consumer.getmany(tp, timeout_ms=1000)
        with self.assertRaises(OffsetOutOfRangeError):
            for x in range(2):
                await consumer.getone(tp)

    @run_until_complete
    async def test_consumer_cleanup_unassigned_data_getone(self):
        # Send 3 messages
        topic2 = self.topic + "_other"
        topic3 = self.topic + "_another"
        tp = TopicPartition(self.topic, 0)
        tp2 = TopicPartition(topic2, 0)
        tp3 = TopicPartition(topic3, 0)
        await self.send_messages(0, [1, 2, 3])
        await self.send_messages(0, [5, 6, 7], topic=topic2)
        await self.send_messages(0, [8], topic=topic3)

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = await self.consumer_factory()
        await consumer.getone()
        # Verify that we have some precached records
        self.assertIn(tp, consumer._fetcher._records)

        consumer.subscribe([topic2])
        res = await consumer.getone()
        self.assertEqual(res.value, b"5")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)

        # Same with an explicit partition
        consumer.subscribe([topic3])
        res = await consumer.getone(tp3)
        self.assertEqual(res.value, b"8")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)
        self.assertNotIn(tp2, consumer._fetcher._records)

    @run_until_complete
    async def test_consumer_cleanup_unassigned_data_getmany(self):
        # Send 3 messages
        topic2 = self.topic + "_other"
        topic3 = self.topic + "_another"
        tp = TopicPartition(self.topic, 0)
        tp2 = TopicPartition(topic2, 0)
        tp3 = TopicPartition(topic3, 0)
        await self.send_messages(0, [1, 2, 3])
        await self.send_messages(0, [5, 6, 7], topic=topic2)
        await self.send_messages(0, [8], topic=topic3)

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = await self.consumer_factory(
            heartbeat_interval_ms=200)
        await consumer.getone()
        # Verify that we have some precached records
        self.assertIn(tp, consumer._fetcher._records)

        consumer.subscribe([topic2])
        res = await consumer.getmany(timeout_ms=5000, max_records=1)
        self.assertEqual(res[tp2][0].value, b"5")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)

        # Same with an explicit partition
        consumer.subscribe([topic3])
        res = await consumer.getmany(tp3, timeout_ms=5000, max_records=1)
        self.assertEqual(res[tp3][0].value, b"8")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)
        self.assertNotIn(tp2, consumer._fetcher._records)

    @run_until_complete
    async def test_rebalance_listener_with_coroutines(self):
        await self.send_messages(0, list(range(0, 10)))
        await self.send_messages(1, list(range(10, 20)))

        main_self = self

        class SimpleRebalanceListener(ConsumerRebalanceListener):
            def __init__(self, consumer):
                self.consumer = consumer
                self.revoke_mock = mock.Mock()
                self.assign_mock = mock.Mock()

            async def on_partitions_revoked(self, revoked):
                self.revoke_mock(revoked)
                # If this commit would fail we will end up with wrong msgs
                # eturned in test below
                await self.consumer.commit()
                # Confirm that coordinator is actually waiting for callback to
                # complete
                await asyncio.sleep(0.2, loop=main_self.loop)
                main_self.assertTrue(
                    self.consumer._coordinator.needs_join_prepare)

            async def on_partitions_assigned(self, assigned):
                self.assign_mock(assigned)
                # Confirm that coordinator is actually waiting for callback to
                # complete
                await asyncio.sleep(0.2, loop=main_self.loop)
                main_self.assertFalse(
                    self.consumer._coordinator.needs_join_prepare)

        tp0 = TopicPartition(self.topic, 0)
        tp1 = TopicPartition(self.topic, 1)
        consumer1 = AIOKafkaConsumer(
            loop=self.loop, group_id="test_rebalance_listener_with_coroutines",
            bootstrap_servers=self.hosts, enable_auto_commit=False,
            auto_offset_reset="earliest")
        listener1 = SimpleRebalanceListener(consumer1)
        consumer1.subscribe([self.topic], listener=listener1)
        await consumer1.start()
        self.add_cleanup(consumer1.stop)

        msg = await consumer1.getone(tp0)
        self.assertEqual(msg.value, b"0")
        msg = await consumer1.getone(tp1)
        self.assertEqual(msg.value, b"10")
        listener1.revoke_mock.assert_called_with(set([]))
        listener1.assign_mock.assert_called_with(set([tp0, tp1]))

        # By adding a 2nd consumer we trigger rebalance
        consumer2 = AIOKafkaConsumer(
            loop=self.loop, group_id="test_rebalance_listener_with_coroutines",
            bootstrap_servers=self.hosts, enable_auto_commit=False,
            auto_offset_reset="earliest")
        listener2 = SimpleRebalanceListener(consumer2)
        consumer2.subscribe([self.topic], listener=listener2)
        await consumer2.start()
        self.add_cleanup(consumer2.stop)

        msg1 = await consumer1.getone()
        msg2 = await consumer2.getone()
        # We can't predict the assignment in test
        if consumer1.assignment() == set([tp1]):
            msg1, msg2 = msg2, msg1
            c1_assignment = set([tp1])
            c2_assignment = set([tp0])
        else:
            c1_assignment = set([tp0])
            c2_assignment = set([tp1])

        self.assertEqual(msg1.value, b"1")
        self.assertEqual(msg2.value, b"11")

        listener1.revoke_mock.assert_called_with(set([tp0, tp1]))
        self.assertEqual(listener1.revoke_mock.call_count, 2)
        listener1.assign_mock.assert_called_with(c1_assignment)
        self.assertEqual(listener1.assign_mock.call_count, 2)

        listener2.revoke_mock.assert_called_with(set([]))
        self.assertEqual(listener2.revoke_mock.call_count, 1)
        listener2.assign_mock.assert_called_with(c2_assignment)
        self.assertEqual(listener2.assign_mock.call_count, 1)

    @run_until_complete
    async def test_rebalance_listener_no_deadlock_callbacks(self):
        # Seek_to_end requires partitions to be assigned, so it waits for
        # rebalance to end before attempting seek
        tp0 = TopicPartition(self.topic, 0)

        class SimpleRebalanceListener(ConsumerRebalanceListener):
            def __init__(self, consumer):
                self.consumer = consumer
                self.seek_task = None

            async def on_partitions_revoked(self, revoked):
                pass

            async def on_partitions_assigned(self, assigned):
                self.seek_task = self.consumer._loop.create_task(
                    self._super_reseek())
                await self.seek_task

            async def _super_reseek(self):
                committed = await self.consumer.committed(tp0)
                position = await self.consumer.position(tp0)
                await self.consumer.seek_to_end(tp0)
                position2 = await self.consumer.position(tp0)
                return committed, position, position2

        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id="test_rebalance_listener_with_coroutines",
            bootstrap_servers=self.hosts, enable_auto_commit=False,
            auto_offset_reset="earliest")
        listener = SimpleRebalanceListener(consumer)
        consumer.subscribe([self.topic], listener=listener)
        await consumer.start()
        self.add_cleanup(consumer.stop)
        committed, position, position2 = await listener.seek_task
        self.assertIsNone(committed)
        self.assertIsNotNone(position)
        self.assertIsNotNone(position2)

    @run_until_complete
    async def test_commit_not_blocked_by_long_poll_fetch(self):
        await self.send_messages(0, list(range(0, 10)))

        consumer = await self.consumer_factory(
            fetch_max_wait_ms=10000)

        # This should prefetch next batch right away and long-poll
        await consumer.getmany(timeout_ms=1000)
        long_poll_task = self.loop.create_task(
            consumer.getmany(timeout_ms=1000))
        await asyncio.sleep(0.2, loop=self.loop)
        self.assertFalse(long_poll_task.done())

        start_time = self.loop.time()
        await consumer.commit()
        end_time = self.loop.time()

        self.assertFalse(long_poll_task.done())
        self.assertLess(end_time - start_time, 500)

    @kafka_versions('>=0.10.1')
    @run_until_complete
    async def test_offsets_for_times_single(self):
        high_time = int(self.loop.time() * 1000)
        middle_time = high_time - 1000
        low_time = high_time - 2000
        tp = TopicPartition(self.topic, 0)

        [msg1] = await self.send_messages(
            0, [1], timestamp_ms=low_time, return_inst=True)
        [msg2] = await self.send_messages(
            0, [1], timestamp_ms=high_time, return_inst=True)

        consumer = await self.consumer_factory()

        offsets = await consumer.offsets_for_times({tp: low_time})
        self.assertEqual(len(offsets), 1)
        self.assertEqual(offsets[tp].offset, msg1.offset)
        self.assertEqual(offsets[tp].timestamp, low_time)

        offsets = await consumer.offsets_for_times({tp: middle_time})
        self.assertEqual(offsets[tp].offset, msg2.offset)
        self.assertEqual(offsets[tp].timestamp, high_time)

        offsets = await consumer.offsets_for_times({tp: high_time})
        self.assertEqual(offsets[tp].offset, msg2.offset)
        self.assertEqual(offsets[tp].timestamp, high_time)

        # Out of bound timestamps check

        offsets = await consumer.offsets_for_times({tp: 0})
        self.assertEqual(offsets[tp].offset, msg1.offset)
        self.assertEqual(offsets[tp].timestamp, low_time)

        offsets = await consumer.offsets_for_times({tp: 9999999999999})
        self.assertEqual(offsets[tp], None)

        offsets = await consumer.offsets_for_times({})
        self.assertEqual(offsets, {})

        # Beginning and end offsets

        offsets = await consumer.beginning_offsets([tp])
        self.assertEqual(offsets, {
            tp: msg1.offset,
        })

        offsets = await consumer.end_offsets([tp])
        self.assertEqual(offsets, {
            tp: msg2.offset + 1,
        })

    @kafka_versions('>=0.10.1')
    @run_until_complete
    async def test_kafka_consumer_offsets_search_many_partitions(self):
        tp0 = TopicPartition(self.topic, 0)
        tp1 = TopicPartition(self.topic, 1)

        send_time = int(time.time() * 1000)
        [msg1] = await self.send_messages(
            0, [1], timestamp_ms=send_time, return_inst=True)
        [msg2] = await self.send_messages(
            1, [1], timestamp_ms=send_time, return_inst=True)

        consumer = await self.consumer_factory()
        offsets = await consumer.offsets_for_times({
            tp0: send_time,
            tp1: send_time
        })

        self.assertEqual(offsets, {
            tp0: OffsetAndTimestamp(msg1.offset, send_time),
            tp1: OffsetAndTimestamp(msg2.offset, send_time)
        })

        offsets = await consumer.beginning_offsets([tp0, tp1])
        self.assertEqual(offsets, {
            tp0: msg1.offset,
            tp1: msg2.offset
        })

        offsets = await consumer.end_offsets([tp0, tp1])
        self.assertEqual(offsets, {
            tp0: msg1.offset + 1,
            tp1: msg2.offset + 1
        })

    @kafka_versions('>=0.10.1')
    @run_until_complete
    async def test_kafka_consumer_offsets_errors(self):
        consumer = await self.consumer_factory()
        tp = TopicPartition(self.topic, 0)
        bad_tp = TopicPartition(self.topic, 100)

        with self.assertRaises(ValueError):
            await consumer.offsets_for_times({tp: -1})
        with self.assertRaises(KafkaTimeoutError):
            await consumer.offsets_for_times({bad_tp: 0})

    @kafka_versions('<0.10.1')
    @run_until_complete
    async def test_kafka_consumer_offsets_old_brokers(self):
        consumer = await self.consumer_factory()
        tp = TopicPartition(self.topic, 0)

        with self.assertRaises(UnsupportedVersionError):
            await consumer.offsets_for_times({tp: int(time.time())})
        with self.assertRaises(UnsupportedVersionError):
            await consumer.beginning_offsets(tp)
        with self.assertRaises(UnsupportedVersionError):
            await consumer.end_offsets(tp)

    @run_until_complete
    async def test_kafka_consumer_sets_coordinator_values(self):
        group = "test-group-%s" % self.id()
        session_timeout_ms = 12345
        heartbeat_interval_ms = 3456
        retry_backoff_ms = 567
        auto_commit_interval_ms = 6789
        exclude_internal_topics = False
        enable_auto_commit = True

        consumer = await self.consumer_factory(
            group=group,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            retry_backoff_ms=retry_backoff_ms,
            auto_commit_interval_ms=auto_commit_interval_ms,
            exclude_internal_topics=exclude_internal_topics,
            enable_auto_commit=enable_auto_commit)
        coordinator = consumer._coordinator

        self.assertEqual(
            coordinator.group_id, group)
        self.assertEqual(
            coordinator._session_timeout_ms, session_timeout_ms)
        self.assertEqual(
            coordinator._heartbeat_interval_ms, heartbeat_interval_ms)
        self.assertEqual(
            coordinator._retry_backoff_ms, retry_backoff_ms)
        self.assertEqual(
            coordinator._auto_commit_interval_ms, auto_commit_interval_ms)
        self.assertEqual(
            coordinator._exclude_internal_topics, exclude_internal_topics)
        self.assertEqual(
            coordinator._enable_auto_commit, enable_auto_commit)

    @run_until_complete
    async def test_consumer_fast_unsubscribe(self):
        # Unsubscribe before coordination finishes
        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id="test_consumer_fast_unsubscribe",
            bootstrap_servers=self.hosts)
        await consumer.start()
        consumer.subscribe([self.topic])
        await asyncio.sleep(0.01, loop=self.loop)
        consumer.unsubscribe()
        await consumer.stop()

    @run_until_complete
    async def test_consumer_manual_assignment_with_group(self):
        # Following issue #394 we seemed to mix subscription with manual
        # assignment. The main test above probably missed this scenario cause
        # it was initialized for subscription.
        await self.send_messages(0, list(range(0, 10)))

        consumer = AIOKafkaConsumer(
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id='group-%s' % self.id(), bootstrap_servers=self.hosts)
        tp = TopicPartition(self.topic, 0)
        consumer.assign([tp])
        await consumer.start()
        self.add_cleanup(consumer.stop)

        for i in range(5):
            msg = await consumer.getone()
            self.assertEqual(msg.value, str(i).encode())

        await consumer.commit()
        await consumer.stop()

        # Start the next consumer after closing this one. It should have
        # committed the offset to the group
        consumer = AIOKafkaConsumer(
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id='group-%s' % self.id(), bootstrap_servers=self.hosts)
        tp = TopicPartition(self.topic, 0)
        consumer.assign([tp])
        await consumer.start()
        self.add_cleanup(consumer.stop)

        for i in range(5, 10):
            msg = await consumer.getone()
            self.assertEqual(msg.value, str(i).encode())

    @run_until_complete
    async def test_consumer_manual_assignment_no_group_before_start(self):
        # Following issue #394 we seemed to mix subscription with manual
        # assignment. The main test above probably missed this scenario cause
        # it was initialized for subscription.
        await self.send_messages(0, list(range(0, 10)))

        consumer = AIOKafkaConsumer(
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id=None, bootstrap_servers=self.hosts)
        tp = TopicPartition(self.topic, 0)
        consumer.assign([tp])
        await consumer.start()
        self.add_cleanup(consumer.stop)

        for i in range(10):
            msg = await consumer.getone()
            self.assertEqual(msg.value, str(i).encode())

    @run_until_complete
    async def test_consumer_manual_assignment_no_group_after_start(self):
        # Following issue #394 we seemed to mix subscription with manual
        # assignment. The main test above probably missed this scenario cause
        # it was initialized for subscription.
        await self.send_messages(0, list(range(0, 10)))

        consumer = AIOKafkaConsumer(
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id=None, bootstrap_servers=self.hosts)
        tp = TopicPartition(self.topic, 0)
        await consumer.start()
        consumer.assign([tp])
        self.add_cleanup(consumer.stop)

        for i in range(10):
            msg = await consumer.getone()
            self.assertEqual(msg.value, str(i).encode())

    @run_until_complete
    async def test_consumer_invalid_session_timeout(self):
        # Following issue #344 it seems more critical. There may be
        # more cases, where aiokafka just does not handle correctly in
        # coordination (like ConnectionError is said issue).
        # Original issue #294
        await self.send_messages(0, list(range(0, 10)))

        consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id="group-" + self.id(), bootstrap_servers=self.hosts,
            session_timeout_ms=200, heartbeat_interval_ms=100)
        self.add_cleanup(consumer.stop)
        with self.assertRaises(InvalidSessionTimeoutError):
            await consumer.start()

    @run_until_complete
    async def test_consumer_invalid_crc_in_records(self):
        consumer = await self.consumer_factory()
        orig_send = consumer._client.send
        with mock.patch.object(consumer._client, "send") as m:
            corrupted = []

            async def mock_send(node_id, req, group=None):
                res = await orig_send(node_id, req, group=group)
                if res.API_KEY == FetchRequest[0].API_KEY and not corrupted:
                    for topic, partitions in res.topics:
                        for index, partition_data in enumerate(partitions):
                            partition_data = list(partition_data)
                            records_data = bytearray(partition_data[-1])
                            if records_data:
                                records_data[-1] ^= 0xff
                                partition_data[-1] = bytes(records_data)
                                partitions[index] = tuple(partition_data)
                                corrupted.append(index)
                return res
            m.side_effect = mock_send
            # Make sure we do the mocked send, not wait for old fetch
            await asyncio.sleep(0.5, loop=self.loop)

            await self.send_messages(0, [0])

            # We should be able to continue if next time we get normal record
            with self.assertRaises(CorruptRecordException):
                await consumer.getone()

            # All other calls should succeed
            res = await consumer.getmany(timeout_ms=2000)
            self.assertEqual(len(list(res.values())[0]), 1)

    @run_until_complete
    async def test_consumer_compacted_topic(self):
        await self.send_messages(0, list(range(0, 10)))

        consumer = await self.consumer_factory()
        with mock.patch.object(
                fetcher.PartitionRecords, "__next__", autospec=True) as m:
            def mock_next(self):
                try:
                    res = next(self._records_iterator)
                except StopIteration:
                    self._records_iterator = None
                    raise
                # Say offsets 1, 3 and 4 were compacted out
                if res.offset in [1, 3, 4, 9]:
                    return mock_next(self)
                return res
            m.side_effect = mock_next

            # All other calls should succeed
            res = await consumer.getmany(timeout_ms=2000)
            self.assertEqual(len(list(res.values())[0]), 6)
            # Even thou 9'th offset was compacted out we still need to proceed
            # from 10th as record batch contains information about that and
            # the same batch will be returned over and over if we try to fetch
            # 9th again.
            pos = await consumer.position(TopicPartition(self.topic, 0))
            self.assertEqual(pos, 10)

    @run_until_complete
    async def test_consumer_serialize_deserialize(self):

        def serialize(value):
            if value is None:
                return None
            return json.dumps(value).encode()

        def deserialize(value):
            if value is None:
                return None
            return json.loads(value.decode())

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            key_serializer=serialize, value_serializer=serialize)
        await producer.start()
        self.add_cleanup(producer.stop)

        await producer.send_and_wait(
            self.topic, key={"key": 1}, value=["value1", "value2"])

        consumer = await self.consumer_factory(
            key_deserializer=deserialize, value_deserializer=deserialize)

        msg = await consumer.getone()
        self.assertEqual(msg.key, {"key": 1})
        self.assertEqual(msg.value, ["value1", "value2"])

    @run_until_complete
    async def test_consumer_compressed_returns_older_msgs(self):
        # If a batch contains 10 elements and we request offset of 1 in the
        # middle the broker will return the WHOLE batch, including old offsets
        # Those should be omitted and not returned to user.
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type="gzip")
        await producer.start()
        self.add_cleanup(producer.stop)
        await self.wait_topic(producer.client, self.topic)

        # We must be sure that we will end up with 1 and only 1 batch
        batch = producer.create_batch()
        for i in range(10):
            batch.append(key=b"123", value=str(i).encode(), timestamp=None)
        fut = await producer.send_batch(
            batch, topic=self.topic, partition=0)
        batch_meta = await fut

        consumer = await self.consumer_factory()
        consumer.seek(TopicPartition(self.topic, 0), batch_meta.offset + 5)

        orig_send = consumer._client.send
        with mock.patch.object(consumer._client, "send") as m:
            recv_records = []

            async def mock_send(node_id, req, group=None, test_case=self):
                res = await orig_send(node_id, req, group=group)
                if res.API_KEY == FetchRequest[0].API_KEY:
                    for topic, partitions in res.topics:
                        for partition_data in partitions:
                            data = partition_data[-1]
                            # Manually do unpack using internal tools so that
                            # we can count how many were actually passed from
                            # broker
                            records = MemoryRecords(data)
                            while records.has_next():
                                recv_records.extend(records.next_batch())
                return res
            m.side_effect = mock_send

            res = await consumer.getmany(timeout_ms=2000)
            self.assertEqual(len(list(res.values())[0]), 5)
            self.assertEqual(len(recv_records), 10)

        pos = await consumer.position(TopicPartition(self.topic, 0))
        self.assertEqual(pos, batch_meta.offset + 10)

    @run_until_complete
    async def test_consumer_propagates_coordinator_errors(self):
        # Following issue #344 it seems more critical. There may be
        # more cases, where aiokafka just does not handle correctly in
        # coordination (like ConnectionError in said issue).
        # Original issue #294

        consumer = AIOKafkaConsumer(
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id="group-" + self.id(),
            bootstrap_servers=self.hosts)
        await consumer.start()
        self.add_cleanup(consumer.stop)

        with mock.patch.object(consumer._coordinator, "_send_req") as m:
            async def mock_send_req(request):
                res = mock.Mock()
                res.error_code = UnknownError.errno
                return res
            m.side_effect = mock_send_req

            consumer.subscribe([self.topic])  # Force join error
            with self.assertRaises(KafkaError):
                await consumer.getone()

            # This time we won't kill the fetch waiter, we will check errors
            # before waiting
            with self.assertRaises(KafkaError):
                await consumer.getone()

            # Error in aiokafka code case, should be raised to user too
            m.side_effect = ValueError
            with self.assertRaises(KafkaError):
                await consumer.getone()

        # Even after error should be stopped we already have a broken
        # coordination routine
        with self.assertRaises(KafkaError):
            await consumer.getone()

    @run_until_complete
    async def test_consumer_propagates_heartbeat_errors(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id="group-" + self.id(),
            bootstrap_servers=self.hosts)
        await consumer.start()
        self.add_cleanup(consumer.stop)

        with mock.patch.object(consumer._coordinator, "_do_heartbeat") as m:
            m.side_effect = UnknownError

            consumer.subscribe([self.topic])  # Force join error
            with self.assertRaises(KafkaError):
                await consumer.getone()

            # This time we won't kill the fetch waiter, we will check errors
            # before waiting
            with self.assertRaises(KafkaError):
                await consumer.getone()

    @run_until_complete
    async def test_consumer_propagates_commit_refresh_errors(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            group_id="group-" + self.id(),
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=500)
        await consumer.start()
        self.add_cleanup(consumer.stop)

        with mock.patch.object(
                consumer._coordinator, "_do_fetch_commit_offsets") as m:
            m.side_effect = UnknownError

            consumer.subscribe([self.topic])  # Force join error
            subscription = consumer._subscription.subscription
            with self.assertRaises(KafkaError):
                await consumer.getone()

            # This time we won't kill the fetch waiter, we will check errors
            # before waiting
            with self.assertRaises(KafkaError):
                await consumer.getone()

            refresh_event = subscription.assignment.commit_refresh_needed
            self.assertTrue(refresh_event.is_set())

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_consumer_with_headers(self):
        await self.send_messages(
            0, [0], headers=[("header1", b"17")])
        # Start a consumer_factory
        consumer = await self.consumer_factory()

        message = await consumer.getone()
        self.assertEqual(message.value, b"0")
        self.assertEqual(message.headers, (("header1", b"17"), ))

    @run_until_complete
    async def test_consumer_pause_resume(self):
        await self.send_messages(0, range(5))
        await self.send_messages(1, range(5))

        consumer = await self.consumer_factory()
        tp0 = TopicPartition(self.topic, 0)

        self.assertEqual(consumer.paused(), set())
        seen_partitions = set()
        for _ in range(10):
            msg = await consumer.getone()
            seen_partitions.add(msg.partition)
        self.assertEqual(seen_partitions, {0, 1})

        await consumer.seek_to_beginning()
        consumer.pause(tp0)
        self.assertEqual(consumer.paused(), {tp0})
        seen_partitions = set()
        for _ in range(5):
            msg = await consumer.getone()
            seen_partitions.add(msg.partition)
        self.assertEqual(seen_partitions, {1})

        await consumer.seek_to_beginning()
        consumer.resume(tp0)
        self.assertEqual(consumer.paused(), set())
        seen_partitions = set()
        for _ in range(10):
            msg = await consumer.getone()
            seen_partitions.add(msg.partition)
        self.assertEqual(seen_partitions, {0, 1})

        # Message send in fetch process
        get_task = ensure_future(consumer.getone(), loop=self.loop)
        await asyncio.sleep(0.1, loop=self.loop)
        self.assertFalse(get_task.done())

        # NOTE: we pause after sending fetch requests. We just don't return
        # message to the user
        consumer.pause(tp0)
        await self.send_messages(0, [10])

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(get_task, timeout=0.5, loop=self.loop)

    @run_until_complete
    async def test_max_poll_interval_ms(self):
        await self.send_messages(0, list(range(0, 10)))
        await self.send_messages(1, list(range(10, 20)))
        # Start a consumer_factory
        consumer1 = await self.consumer_factory(
            max_poll_interval_ms=3000, client_id="c1",
            heartbeat_interval_ms=100)
        consumer2 = await self.consumer_factory(
            heartbeat_interval_ms=100, client_id="c2")

        class MyListener(ConsumerRebalanceListener):
            def __init__(self, loop):
                self.revoked = []
                self.assigned = []
                self.assignment_ready = asyncio.Event(loop=loop)

            async def on_partitions_revoked(self, revoked):
                self.revoked.append(revoked)
                self.assignment_ready.clear()

            async def on_partitions_assigned(self, assigned):
                self.assigned.append(assigned)
                self.assignment_ready.set()

        listener1 = MyListener(self.loop)
        listener2 = MyListener(self.loop)
        consumer1.subscribe([self.topic], listener=listener1)
        consumer2.subscribe([self.topic], listener=listener2)

        # Make sure we rebalanced and ready for processing each of it's part
        await listener1.assignment_ready.wait()
        await listener2.assignment_ready.wait()
        self.assertTrue(consumer1.assignment())
        self.assertTrue(consumer2.assignment())

        # After 3 seconds the first consumer should be considered stuck and
        # leave the group as per configuration.
        start_time = self.loop.time()
        seen = []
        for i in range(20):
            msg = await consumer2.getone()
            seen.append(int(msg.value))

        self.assertEqual(set(seen), set(range(0, 20)))

        took = self.loop.time() - start_time
        self.assertAlmostEqual(took, 3, delta=1)

        # The first consumer should be able to consume messages if it's
        # unstuck later on
        await self.send_messages(0, list(range(20, 30)))
        await self.send_messages(1, list(range(30, 40)))

        for i in range(10):
            msg = await consumer1.getone()
            self.assertGreaterEqual(int(msg.value), 20)
