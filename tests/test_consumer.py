import asyncio
import time
from unittest import mock
from contextlib import contextmanager

import pytest

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.consumer.fetcher import RecordTooLargeError
from aiokafka.producer import AIOKafkaProducer
from aiokafka.client import AIOKafkaClient
from aiokafka import (
    ConsumerStoppedError, IllegalOperation, ConsumerRebalanceListener
)
from aiokafka.structs import (
    OffsetAndTimestamp, TopicPartition, OffsetAndMetadata
)
from aiokafka.errors import (
    IllegalStateError, UnknownTopicOrPartitionError, OffsetOutOfRangeError,
    UnsupportedVersionError, KafkaTimeoutError
)

from ._testutil import (
    KafkaIntegrationTestCase, StubRebalanceListener,
    run_until_complete, random_string, kafka_versions)


class TestConsumerIntegration(KafkaIntegrationTestCase):
    @asyncio.coroutine
    def consumer_factory(self, **kwargs):
        enable_auto_commit = kwargs.pop('enable_auto_commit', True)
        auto_offset_reset = kwargs.pop('auto_offset_reset', 'earliest')
        group = kwargs.pop('group', 'group-%s' % self.id())
        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop, group_id=group,
            bootstrap_servers=self.hosts,
            enable_auto_commit=enable_auto_commit,
            auto_offset_reset=auto_offset_reset,
            **kwargs)
        yield from consumer.start()
        if group is not None:
            yield from consumer.seek_to_committed()
        return consumer

    @contextmanager
    def count_fetch_requests(self, consumer, count):
        orig = consumer._fetcher._proc_fetch_request
        with mock.patch.object(
                consumer._fetcher, "_proc_fetch_request") as mocked:
            mocked.side_effect = orig
            yield
            self.assertEqual(mocked.call_count, count)

    @run_until_complete
    def test_simple_consumer(self):
        with self.assertRaises(ValueError):
            # check unsupported version
            consumer = yield from self.consumer_factory(api_version="0.8")

        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer = yield from self.consumer_factory()

        p0 = TopicPartition(self.topic, 0)
        p1 = TopicPartition(self.topic, 1)
        assignment = consumer.assignment()
        self.assertEqual(sorted(list(assignment)), [p0, p1])

        topics = yield from consumer.topics()
        self.assertTrue(self.topic in topics)

        parts = consumer.partitions_for_topic(self.topic)
        self.assertEqual(sorted(list(parts)), [0, 1])

        offset = yield from consumer.committed(
            TopicPartition("uknown-topic", 2))
        self.assertEqual(offset, None)

        offset = yield from consumer.committed(p0)
        if offset is None:
            offset = 0

        messages = []
        for i in range(200):
            message = yield from consumer.getone()
            messages.append(message)
        self.assert_message_count(messages, 200)

        h = consumer.highwater(p0)
        self.assertEqual(h, 100)

        consumer.seek(p0, offset + 90)
        for i in range(10):
            m = yield from consumer.getone()
            self.assertEqual(m.value, str(i + 90).encode())
        yield from consumer.stop()

        # will ignore, no exception expected
        yield from consumer.stop()

    @run_until_complete
    def test_get_by_partition(self):
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        consumer = yield from self.consumer_factory()

        p0 = TopicPartition(self.topic, 0)
        p1 = TopicPartition(self.topic, 1)
        messages = []

        @asyncio.coroutine
        def task(tp, messages):
            for i in range(100):
                m = yield from consumer.getone(tp)
                self.assertEqual(m.partition, tp.partition)
                messages.append(m)

        task1 = asyncio.async(task(p0, messages), loop=self.loop)
        task2 = asyncio.async(task(p1, messages), loop=self.loop)
        yield from asyncio.wait([task1, task2], loop=self.loop)
        self.assert_message_count(messages, 200)
        yield from consumer.stop()

    @run_until_complete
    def test_none_group(self):
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer1 = yield from self.consumer_factory(
            group=None, enable_auto_commit=False)
        consumer2 = yield from self.consumer_factory(group=None)

        messages = []
        for i in range(200):
            message = yield from consumer1.getone()
            messages.append(message)
        self.assert_message_count(messages, 200)
        with self.assertRaises(IllegalOperation):
            # commit does not supported for None group
            yield from consumer1.commit()

        messages = []
        for i in range(200):
            message = yield from consumer2.getone()
            messages.append(message)
        self.assert_message_count(messages, 200)
        yield from consumer1.stop()
        yield from consumer2.stop()

    @run_until_complete
    def test_consumer_poll(self):
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer = yield from self.consumer_factory()

        messages = []
        while True:
            resp = yield from consumer.getmany(timeout_ms=1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) >= 200:
                break
        self.assert_message_count(messages, 200)

        p0 = TopicPartition(self.topic, 0)
        p1 = TopicPartition(self.topic, 1)
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))

        # Check consumption for a specific partition
        messages = []
        while True:
            resp = yield from consumer.getmany(p0, timeout_ms=1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) >= 100:
                break
        self.assert_message_count(messages, 100)

        while True:
            resp = yield from consumer.getmany(p1, timeout_ms=1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) >= 200:
                break
        self.assert_message_count(messages, 200)

        yield from consumer.stop()

    @run_until_complete
    def test_large_messages(self):
        # Produce 10 "normal" size messages
        r_msgs = [str(x) for x in range(10)]
        small_messages = yield from self.send_messages(0, r_msgs)

        # Produce 10 messages that are large (bigger than default fetch size)
        l_msgs = [random_string(5000) for _ in range(10)]
        large_messages = yield from self.send_messages(0, l_msgs)

        # Consumer should still get all of them
        consumer = yield from self.consumer_factory()
        expected_messages = set(small_messages + large_messages)
        actual_messages = []
        for i in range(20):
            m = yield from consumer.getone()
            actual_messages.append(m)
        actual_messages = {m.value for m in actual_messages}
        self.assertEqual(expected_messages, set(actual_messages))
        yield from consumer.stop()

    @run_until_complete
    def test_too_large_messages_getone(self):
        msgs = [
            random_string(10),  # small one
            random_string(50000),  # large one
            random_string(50)   # another small one
        ]
        messages = yield from self.send_messages(0, msgs)

        consumer = yield from self.consumer_factory(
            max_partition_fetch_bytes=4000)
        m = yield from consumer.getone()
        self.assertEqual(m.value, messages[0])

        # Large request will just be skipped
        with self.assertRaises(RecordTooLargeError):
            yield from consumer.getone()

        m = yield from consumer.getone()
        self.assertEqual(m.value, messages[2])
        yield from consumer.stop()

    @run_until_complete
    def test_too_large_messages_getmany(self):
        msgs = [
            random_string(10),  # small one
            random_string(50000),  # large one
            random_string(50),   # another small one
        ]
        messages = yield from self.send_messages(0, msgs)
        tp = TopicPartition(self.topic, 0)

        consumer = yield from self.consumer_factory(
            max_partition_fetch_bytes=4000)

        # First fetch will get 1 small message and discard the large one
        m = yield from consumer.getmany(timeout_ms=1000)
        self.assertTrue(m)
        self.assertEqual(m[tp][0].value, messages[0])

        # Second will only get a large one, so will raise an error
        with self.assertRaises(RecordTooLargeError):
            yield from consumer.getmany(timeout_ms=1000)

        m = yield from consumer.getmany(timeout_ms=1000)
        self.assertTrue(m)
        self.assertEqual(m[tp][0].value, messages[2])
        yield from consumer.stop()

    @run_until_complete
    def test_offset_behavior__resuming_behavior(self):
        msgs1 = yield from self.send_messages(0, range(0, 100))
        msgs2 = yield from self.send_messages(1, range(100, 200))

        available_msgs = msgs1 + msgs2
        # Start a consumer_factory
        consumer1 = yield from self.consumer_factory()
        consumer2 = yield from self.consumer_factory()
        result = []
        for i in range(10):
            msg = yield from consumer1.getone()
            result.append(msg.value)
        yield from consumer1.stop()

        # consumer2 should take both partitions after rebalance
        while True:
            msg = yield from consumer2.getone()
            result.append(msg.value)
            if len(result) == len(available_msgs):
                break

        self.assertEqual(set(available_msgs), set(result))
        yield from consumer1.stop()
        yield from consumer2.stop()

    @run_until_complete
    def test_subscribe_manual(self):
        msgs1 = yield from self.send_messages(0, range(0, 10))
        msgs2 = yield from self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = yield from self.consumer_factory()
        pos = yield from consumer.position(TopicPartition(self.topic, 0))
        with self.assertRaises(IllegalStateError):
            consumer.assign([TopicPartition(self.topic, 0)])
        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 0)])
        result = []
        for i in range(10):
            msg = yield from consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(result), set(msgs1))
        yield from consumer.commit()
        pos = yield from consumer.position(TopicPartition(self.topic, 0))
        self.assertTrue(pos > 0)

        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 1)])
        for i in range(10):
            msg = yield from consumer.getone()
            result.append(msg.value)
        yield from consumer.stop()
        self.assertEqual(set(available_msgs), set(result))

    @run_until_complete
    def test_manual_subscribe_pattern(self):
        msgs1 = yield from self.send_messages(0, range(0, 10))
        msgs2 = yield from self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id='test-group',
            bootstrap_servers=self.hosts, auto_offset_reset='earliest',
            enable_auto_commit=False)
        consumer.subscribe(pattern="topic-test_manual_subs*")
        yield from consumer.start()
        yield from consumer.seek_to_committed()
        result = []
        for i in range(20):
            msg = yield from consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(available_msgs), set(result))

        yield from consumer.commit(
            {TopicPartition(self.topic, 0): OffsetAndMetadata(9, '')})
        yield from consumer.seek_to_committed(TopicPartition(self.topic, 0))
        msg = yield from consumer.getone(TopicPartition(self.topic, 0))
        self.assertEqual(msg.value, b'9')
        yield from consumer.commit(
            {TopicPartition(self.topic, 0): OffsetAndMetadata(10, '')})
        yield from consumer.stop()

        # subscribe by topic
        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id='test-group',
            bootstrap_servers=self.hosts, auto_offset_reset='earliest',
            enable_auto_commit=False)
        consumer.subscribe(topics=(self.topic,))
        yield from consumer.start()
        yield from consumer.seek_to_committed()
        result = []
        for i in range(10):
            msg = yield from consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(msgs2), set(result))
        self.assertEqual(consumer.subscription(), set([self.topic]))
        yield from consumer.stop()

    @run_until_complete
    def test_subscribe_errors(self):
        consumer = yield from self.consumer_factory()
        with self.assertRaises(ValueError):
            consumer.subscribe(topics=(self.topic, ), pattern="some")
        with self.assertRaises(ValueError):
            consumer.subscribe(topics=(), pattern=None)
        with self.assertRaises(ValueError):
            consumer.subscribe(pattern="^(spome(")

    @run_until_complete
    def test_compress_decompress(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type="gzip")
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)
        msg1 = b'some-message' * 10
        msg2 = b'other-message' * 30
        yield from producer.send(self.topic, msg1, partition=1)
        yield from producer.send(self.topic, msg2, partition=1)
        yield from producer.stop()

        consumer = yield from self.consumer_factory()
        rmsg1 = yield from consumer.getone()
        self.assertEqual(rmsg1.value, msg1)
        rmsg2 = yield from consumer.getone()
        self.assertEqual(rmsg2.value, msg2)
        yield from consumer.stop()

    @run_until_complete
    def test_compress_decompress_lz4(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type="lz4")
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)
        msg1 = b'some-message' * 10
        msg2 = b'other-message' * 30
        yield from producer.send(self.topic, msg1, partition=1)
        yield from producer.send(self.topic, msg2, partition=1)
        yield from producer.stop()

        consumer = yield from self.consumer_factory()
        rmsg1 = yield from consumer.getone()
        self.assertEqual(rmsg1.value, msg1)
        rmsg2 = yield from consumer.getone()
        self.assertEqual(rmsg2.value, msg2)
        yield from consumer.stop()

    @run_until_complete
    def test_consumer_seek_backward(self):
        # Send 3 messages
        yield from self.send_messages(0, [1, 2, 3])

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = yield from self.consumer_factory()
        with self.count_fetch_requests(consumer, 1):
            rmsg1 = yield from consumer.getone()
            self.assertEqual(rmsg1.value, b'1')

        # Seek should invalidate the remaining messages
        tp = TopicPartition(self.topic, rmsg1.partition)
        consumer.seek(tp, rmsg1.offset)
        with self.count_fetch_requests(consumer, 1):
            rmsg2 = yield from consumer.getone()
            self.assertEqual(rmsg2.value, b'1')
            rmsg2 = yield from consumer.getone()
            self.assertEqual(rmsg2.value, b'2')
        # Same with getmany
        consumer.seek(tp, rmsg2.offset)
        with self.count_fetch_requests(consumer, 1):
            res = yield from consumer.getmany(timeout_ms=500)
            rmsg3 = res[tp][0]
            self.assertEqual(rmsg3.value, b'2')
            rmsg3 = res[tp][1]
            self.assertEqual(rmsg3.value, b'3')
        yield from consumer.stop()

    @run_until_complete
    def test_consumer_seek_forward_getone(self):
        # Send 3 messages
        yield from self.send_messages(0, [1, 2, 3])

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = yield from self.consumer_factory()
        with self.count_fetch_requests(consumer, 1):
            rmsg1 = yield from consumer.getone()
            self.assertEqual(rmsg1.value, b'1')

        # Seek should invalidate the remaining message
        tp = TopicPartition(self.topic, rmsg1.partition)
        consumer.seek(tp, rmsg1.offset + 2)
        with self.count_fetch_requests(consumer, 0):
            rmsg2 = yield from consumer.getone()
            self.assertEqual(rmsg2.value, b'3')

        res = yield from consumer.getmany(timeout_ms=0)
        self.assertEqual(res, {})
        yield from consumer.stop()

    @run_until_complete
    def test_consumer_seek_forward_getmany(self):
        # Send 3 messages
        yield from self.send_messages(0, [1, 2, 3])

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = yield from self.consumer_factory()
        with self.count_fetch_requests(consumer, 1):
            rmsg1 = yield from consumer.getone()
            self.assertEqual(rmsg1.value, b'1')

        # Seek should invalidate the remaining message
        tp = TopicPartition(self.topic, rmsg1.partition)
        consumer.seek(tp, rmsg1.offset + 2)
        with self.count_fetch_requests(consumer, 0):
            rmsg2 = yield from consumer.getmany(timeout_ms=500)
            rmsg2 = rmsg2[tp][0]
            self.assertEqual(rmsg2.value, b'3')

        res = yield from consumer.getmany(timeout_ms=0)
        self.assertEqual(res, {})
        yield from consumer.stop()

    @run_until_complete
    def test_consumer_seek_to_beginning(self):
        # Send 3 messages
        yield from self.send_messages(0, [1, 2, 3])

        consumer = yield from self.consumer_factory()
        self.add_cleanup(consumer.stop)

        tp = TopicPartition(self.topic, 0)
        start_position = yield from consumer.position(tp)

        rmsg1 = yield from consumer.getone()
        yield from consumer.seek_to_beginning()
        rmsg2 = yield from consumer.getone()
        self.assertEqual(rmsg2.value, rmsg1.value)

        yield from consumer.seek_to_beginning(tp)
        rmsg3 = yield from consumer.getone()
        self.assertEqual(rmsg2.value, rmsg3.value)

        pos = yield from consumer.position(tp)
        self.assertEqual(pos, start_position + 1)

    @pytest.mark.skip(reason="Raises AssertionError, see issue #199")
    @run_until_complete
    def test_consumer_seek_to_end(self):
        # Send 3 messages
        yield from self.send_messages(0, [1, 2, 3])

        consumer = yield from self.consumer_factory()
        self.add_cleanup(consumer.stop)

        tp = TopicPartition(self.topic, 0)
        start_position = yield from consumer.position(tp)

        yield from consumer.seek_to_end()
        pos = yield from consumer.position(tp)
        self.assertEqual(pos, start_position + 3)
        task = self.loop.create_task(consumer.getone())
        yield from asyncio.sleep(0.1, loop=self.loop)
        self.assertEqual(task.done(), False)

        yield from self.send_messages(0, [4, 5, 6])
        rmsg = yield from task
        self.assertEqual(rmsg.value, b"4")

        yield from consumer.seek_to_end(tp)
        task = self.loop.create_task(consumer.getone())
        yield from asyncio.sleep(0.1, loop=self.loop)
        self.assertEqual(task.done(), False)

        yield from self.send_messages(0, [7, 8, 9])
        rmsg = yield from task
        self.assertEqual(rmsg.value, b"7")

        pos = yield from consumer.position(tp)
        self.assertEqual(pos, start_position + 7)

    @run_until_complete
    def test_consumer_seek_on_unassigned(self):
        tp0 = TopicPartition(self.topic, 0)
        tp1 = TopicPartition(self.topic, 1)
        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id=None, bootstrap_servers=self.hosts)
        yield from consumer.start()
        self.add_cleanup(consumer.stop)
        consumer.assign([tp0])

        with self.assertRaises(IllegalStateError):
            yield from consumer.seek_to_beginning(tp1)
        with self.assertRaises(IllegalStateError):
            yield from consumer.seek_to_committed(tp1)
        with self.assertRaises(IllegalStateError):
            yield from consumer.seek_to_end(tp1)

    @run_until_complete
    def test_consumer_seek_type_errors(self):
        consumer = yield from self.consumer_factory()
        self.add_cleanup(consumer.stop)

        with self.assertRaises(TypeError):
            yield from consumer.seek_to_beginning(1)
        with self.assertRaises(TypeError):
            yield from consumer.seek_to_committed(1)
        with self.assertRaises(TypeError):
            yield from consumer.seek_to_end(1)

    @run_until_complete
    def test_manual_subscribe_nogroup(self):
        msgs1 = yield from self.send_messages(0, range(0, 10))
        msgs2 = yield from self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id=None,
            bootstrap_servers=self.hosts, auto_offset_reset='earliest',
            enable_auto_commit=False)
        consumer.subscribe(topics=(self.topic,))
        yield from consumer.start()
        result = []
        for i in range(20):
            msg = yield from consumer.getone()
            result.append(msg.value)
        self.assertEqual(set(available_msgs), set(result))
        yield from consumer.stop()

    @pytest.mark.xfail
    @run_until_complete
    def test_unknown_topic_or_partition(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id=None,
            bootstrap_servers=self.hosts, auto_offset_reset='earliest',
            enable_auto_commit=False)
        yield from consumer.start()

        with self.assertRaises(UnknownTopicOrPartitionError):
            yield from consumer.assign([TopicPartition(self.topic, 2222)])
        yield from consumer.stop()

    @run_until_complete
    def test_check_extended_message_record(self):
        s_time_ms = time.time() * 1000
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)
        msg1 = b'some-message#1'
        yield from producer.send(self.topic, msg1, partition=1)
        yield from producer.stop()

        consumer = yield from self.consumer_factory()
        rmsg1 = yield from consumer.getone()
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
        yield from consumer.stop()

    @run_until_complete
    def test_equal_consumption(self):
        # A strange use case of kafka-python, that can be reproduced in
        # aiokafka https://github.com/dpkp/kafka-python/issues/675
        yield from self.send_messages(0, list(range(200)))
        yield from self.send_messages(1, list(range(200)))

        partition_consumption = [0, 0]
        for x in range(10):
            consumer = yield from self.consumer_factory(
                max_partition_fetch_bytes=10000)
            for x in range(10):
                msg = yield from consumer.getone()
                partition_consumption[msg.partition] += 1
            yield from consumer.stop()

        diff = abs(partition_consumption[0] - partition_consumption[1])
        # We are good as long as it's not 100%, as we do rely on randomness of
        # a shuffle in code. Ideally it should be 50/50 (0 diff) thou
        self.assertLess(diff / sum(partition_consumption), 1.0)

    @run_until_complete
    def test_max_poll_records(self):
        # A strange use case of kafka-python, that can be reproduced in
        # aiokafka https://github.com/dpkp/kafka-python/issues/675
        yield from self.send_messages(0, list(range(100)))

        consumer = yield from self.consumer_factory(
            max_poll_records=48)
        data = yield from consumer.getmany(timeout_ms=1000)
        count = sum(map(len, data.values()))
        self.assertEqual(count, 48)
        data = yield from consumer.getmany(timeout_ms=1000, max_records=42)
        count = sum(map(len, data.values()))
        self.assertEqual(count, 42)
        data = yield from consumer.getmany(timeout_ms=1000, max_records=None)
        count = sum(map(len, data.values()))
        self.assertEqual(count, 10)

        with self.assertRaises(ValueError):
            data = yield from consumer.getmany(max_records=0)
        yield from consumer.stop()

        with self.assertRaises(ValueError):
            consumer = yield from self.consumer_factory(
                max_poll_records=0)

    @run_until_complete
    def test_ssl_consume(self):
        # Produce by PLAINTEXT, Consume by SSL
        # Send 3 messages
        yield from self.send_messages(0, [1, 2, 3])

        context = self.create_ssl_context()
        group = "group-{}".format(self.id())
        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop, group_id=group,
            bootstrap_servers=[
                "{}:{}".format(self.kafka_host, self.kafka_ssl_port)],
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            security_protocol="SSL", ssl_context=context)
        yield from consumer.start()
        results = yield from consumer.getmany(timeout_ms=1000)
        [msgs] = results.values()  # only 1 partition anyway
        msgs = [msg.value for msg in msgs]
        self.assertEqual(msgs, [b"1", b"2", b"3"])
        yield from consumer.stop()

    def test_consumer_arguments(self):
        with self.assertRaisesRegexp(
                ValueError, "`security_protocol` should be SSL or PLAINTEXT"):
            AIOKafkaConsumer(
                self.topic, loop=self.loop,
                bootstrap_servers=self.hosts,
                security_protocol="SOME")
        with self.assertRaisesRegexp(
                ValueError, "`ssl_context` is mandatory if "
                            "security_protocol=='SSL'"):
            AIOKafkaConsumer(
                self.topic, loop=self.loop,
                bootstrap_servers=self.hosts,
                security_protocol="SSL", ssl_context=None)

    @run_until_complete
    def test_consumer_commit_validation(self):
        consumer = yield from self.consumer_factory()

        tp = TopicPartition(self.topic, 0)
        offset = yield from consumer.position(tp)
        offset_and_metadata = OffsetAndMetadata(offset, "")

        with self.assertRaises(ValueError):
            yield from consumer.commit({})
        with self.assertRaises(ValueError):
            yield from consumer.commit("something")
        with self.assertRaisesRegexp(
                ValueError, "Key should be TopicPartition instance"):
            yield from consumer.commit({"my_topic": offset_and_metadata})
        with self.assertRaisesRegexp(
                ValueError, "Metadata should be a string"):
            yield from consumer.commit({tp: (offset, 1000)})
        with self.assertRaisesRegexp(
                ValueError, "Metadata should be a string"):
            yield from consumer.commit({tp: (offset, b"\x00\x02")})

    @run_until_complete
    def test_consumer_commit_no_group(self):
        consumer_no_group = yield from self.consumer_factory(group=None)
        tp = TopicPartition(self.topic, 0)
        offset = yield from consumer_no_group.position(tp)

        with self.assertRaises(IllegalOperation):
            yield from consumer_no_group.commit({tp: offset})
        with self.assertRaises(IllegalOperation):
            yield from consumer_no_group.committed(tp)

    @run_until_complete
    def test_consumer_commit(self):
        yield from self.send_messages(0, [1, 2, 3])

        consumer = yield from self.consumer_factory()
        tp = TopicPartition(self.topic, 0)

        msg = yield from consumer.getone()
        # Commit by offset
        yield from consumer.commit({tp: msg.offset + 1})
        committed = yield from consumer.committed(tp)
        self.assertEqual(committed, msg.offset + 1)

        msg = yield from consumer.getone()
        # Commit by offset and metadata
        yield from consumer.commit({
            tp: (msg.offset + 2, "My metadata 2")
        })
        committed = yield from consumer.committed(tp)
        self.assertEqual(committed, msg.offset + 2)

    @run_until_complete
    def test_consumer_group_without_subscription(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop,
            group_id='group-{}'.format(self.id()),
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            heartbeat_interval_ms=100)
        yield from consumer.start()
        yield from asyncio.sleep(0.2, loop=self.loop)
        yield from consumer.stop()

    @run_until_complete
    def test_consumer_wait_topic(self):
        topic = "some-test-topic-for-autocreate"
        consumer = AIOKafkaConsumer(
            topic, loop=self.loop, bootstrap_servers=self.hosts)
        yield from consumer.start()
        consume_task = self.loop.create_task(consumer.getone())
        # just to be sure getone does not fail (before produce)
        yield from asyncio.sleep(0.5, loop=self.loop)
        self.assertFalse(consume_task.done())

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        yield from producer.send(topic, b'test msg')
        yield from producer.stop()

        data = yield from consume_task
        self.assertEqual(data.value, b'test msg')
        yield from consumer.stop()

    @run_until_complete
    def test_consumer_subscribe_pattern_with_autocreate(self):
        pattern = "^some-autocreate-pattern-.*$"
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="some_group",
            fetch_max_wait_ms=50,
            auto_offset_reset="earliest")
        self.add_cleanup(consumer.stop)
        yield from consumer.start()
        consumer.subscribe(pattern=pattern)
        # Start getter for the topics. Should not create any topics
        consume_task = self.loop.create_task(consumer.getone())
        yield from asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertEqual(consumer.subscription(), set())

        # Now lets autocreate the topic by fetching metadata for it.
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        self.add_cleanup(producer.stop)
        yield from producer.start()
        my_topic = "some-autocreate-pattern-1"
        yield from producer.client._wait_on_metadata(my_topic)
        # Wait for consumer to refresh metadata with new topic
        yield from asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >= {my_topic})
        self.assertEqual(consumer.subscription(), {my_topic})

        # Add another topic
        my_topic2 = "some-autocreate-pattern-2"
        yield from producer.client._wait_on_metadata(my_topic2)
        # Wait for consumer to refresh metadata with new topic
        yield from asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >=
                        {my_topic, my_topic2})
        self.assertEqual(consumer.subscription(), {my_topic, my_topic2})

        # Now lets actualy produce some data and verify that it is consumed
        yield from producer.send(my_topic, b'test msg')
        data = yield from consume_task
        self.assertEqual(data.value, b'test msg')

    @run_until_complete
    def test_consumer_subscribe_pattern_autocreate_no_group_id(self):
        pattern = "^no-group-pattern-.*$"
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id=None,
            fetch_max_wait_ms=50,
            auto_offset_reset="earliest")
        self.add_cleanup(consumer.stop)
        yield from consumer.start()
        consumer.subscribe(pattern=pattern)
        # Start getter for the topics. Should not create any topics
        consume_task = self.loop.create_task(consumer.getone())
        yield from asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertEqual(consumer.subscription(), set())

        # Now lets autocreate the topic by fetching metadata for it.
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        self.add_cleanup(producer.stop)
        yield from producer.start()
        my_topic = "no-group-pattern-1"
        yield from producer.client._wait_on_metadata(my_topic)
        # Wait for consumer to refresh metadata with new topic
        yield from asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >= {my_topic})
        self.assertEqual(consumer.subscription(), {my_topic})

        # Add another topic
        my_topic2 = "no-group-pattern-2"
        yield from producer.client._wait_on_metadata(my_topic2)
        # Wait for consumer to refresh metadata with new topic
        yield from asyncio.sleep(0.3, loop=self.loop)
        self.assertFalse(consume_task.done())
        self.assertTrue(consumer._client.cluster.topics() >=
                        {my_topic, my_topic2})
        self.assertEqual(consumer.subscription(), {my_topic, my_topic2})

        # Now lets actualy produce some data and verify that it is consumed
        yield from producer.send(my_topic, b'test msg')
        data = yield from consume_task
        self.assertEqual(data.value, b'test msg')

    @run_until_complete
    def test_consumer_rebalance_on_new_topic(self):
        # Test will create a consumer group and check if adding new topic
        # will trigger a group rebalance and assign partitions
        pattern = "^another-autocreate-pattern-.*$"
        client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=self.hosts,
            client_id="test_autocreate")
        yield from client.bootstrap()
        listener1 = StubRebalanceListener(loop=self.loop)
        listener2 = StubRebalanceListener(loop=self.loop)
        consumer1 = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="test-autocreate-rebalance",
            heartbeat_interval_ms=100)
        consumer1.subscribe(pattern=pattern, listener=listener1)
        yield from consumer1.start()
        consumer2 = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="test-autocreate-rebalance",
            heartbeat_interval_ms=100)
        consumer2.subscribe(pattern=pattern, listener=listener2)
        yield from consumer2.start()
        yield from asyncio.sleep(0.5, loop=self.loop)
        # bootstrap will take care of the initial group assignment
        self.assertEqual(consumer1.assignment(), set())
        self.assertEqual(consumer2.assignment(), set())
        listener1.reset()
        listener2.reset()

        # Lets force autocreation of a topic
        my_topic = "another-autocreate-pattern-1"
        yield from client._wait_on_metadata(my_topic)

        # Wait for group to stabilize
        assign1 = yield from listener1.wait_assign()
        assign2 = yield from listener2.wait_assign()
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
        yield from client._wait_on_metadata(my_topic2)

        # Wait for group to stabilize
        assign1 = yield from listener1.wait_assign()
        assign2 = yield from listener2.wait_assign()
        # We expect 2 partitons for autocreated topics
        my_partitions = set([
            TopicPartition(my_topic, 0), TopicPartition(my_topic, 1),
            TopicPartition(my_topic2, 0), TopicPartition(my_topic2, 1)])
        self.assertEqual(assign1 | assign2, my_partitions)
        self.assertEqual(
            consumer1.assignment() | consumer2.assignment(),
            my_partitions)

        yield from consumer1.stop()
        yield from consumer2.stop()
        yield from client.close()

    @run_until_complete
    def test_consumer_stops_getone(self):
        # If we have a fetch in progress it should be cancelled if consumer is
        # stoped
        consumer = yield from self.consumer_factory()
        task = self.loop.create_task(consumer.getone())
        yield from asyncio.sleep(0.1, loop=self.loop)
        # As we didn't input any data into Kafka
        self.assertFalse(task.done())

        yield from consumer.stop()
        # Check that pending call was cancelled
        with self.assertRaises(ConsumerStoppedError):
            yield from task
        # Check that any subsequent call will also raise ConsumerStoppedError
        with self.assertRaises(ConsumerStoppedError):
            yield from consumer.getone()

    @run_until_complete
    def test_consumer_stops_getmany(self):
        # If we have a fetch in progress it should be cancelled if consumer is
        # stoped
        consumer = yield from self.consumer_factory()
        task = self.loop.create_task(consumer.getmany(timeout_ms=10000))
        yield from asyncio.sleep(0.1, loop=self.loop)
        # As we didn't input any data into Kafka
        self.assertFalse(task.done())

        yield from consumer.stop()
        # Interrupted call should just return 0 results. This will allow the
        # user to check for cancellation himself.
        self.assertTrue(task.done())
        self.assertEqual(task.result(), {})
        # Any later call will raise ConsumerStoppedError as consumer closed
        # all connections and can't continue operating.
        with self.assertRaises(ConsumerStoppedError):
            yield from self.loop.create_task(
                consumer.getmany(timeout_ms=10000))
        # Just check no spetial case on timeout_ms=0
        with self.assertRaises(ConsumerStoppedError):
            yield from self.loop.create_task(
                consumer.getmany(timeout_ms=0))

    @run_until_complete
    def test_exclude_internal_topics(self):
        # Create random topic
        my_topic = "some_noninternal_topic"
        client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=self.hosts,
            client_id="test_autocreate")
        yield from client.bootstrap()
        yield from client._wait_on_metadata(my_topic)
        yield from client.close()

        # Check if only it will be subscribed
        pattern = "^.*$"
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="some_group_1",
            auto_offset_reset="earliest",
            exclude_internal_topics=False)
        consumer.subscribe(pattern=pattern)
        yield from consumer.start()
        self.assertIn("__consumer_offsets", consumer.subscription())
        yield from consumer._client.force_metadata_update()
        self.assertIn("__consumer_offsets", consumer.subscription())
        yield from consumer.stop()

    @run_until_complete
    def test_offset_reset_manual(self):
        yield from self.send_messages(0, [1])

        consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop, bootstrap_servers=self.hosts,
            metadata_max_age_ms=200, group_id="offset_reset_group",
            auto_offset_reset="none")
        yield from consumer.start()
        self.add_cleanup(consumer.stop)

        with self.assertRaises(OffsetOutOfRangeError):
            for x in range(2):
                yield from consumer.getmany(timeout_ms=1000)

        with self.assertRaises(OffsetOutOfRangeError):
            for x in range(2):
                yield from consumer.getone()

    @run_until_complete
    def test_consumer_cleanup_unassigned_data_getone(self):
        # Send 3 messages
        topic2 = self.topic + "_other"
        topic3 = self.topic + "_another"
        tp = TopicPartition(self.topic, 0)
        tp2 = TopicPartition(topic2, 0)
        tp3 = TopicPartition(topic3, 0)
        yield from self.send_messages(0, [1, 2, 3])
        yield from self.send_messages(0, [5, 6, 7], topic=topic2)
        yield from self.send_messages(0, [8], topic=topic3)

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = yield from self.consumer_factory()
        yield from consumer.getone()
        # Verify that we have some precached records
        self.assertIn(tp, consumer._fetcher._records)

        consumer.subscribe([topic2])
        res = yield from consumer.getone()
        self.assertEqual(res.value, b"5")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)

        # Same with an explicit partition
        consumer.subscribe([topic3])
        res = yield from consumer.getone(tp3)
        self.assertEqual(res.value, b"8")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)
        self.assertNotIn(tp2, consumer._fetcher._records)

        yield from consumer.stop()

    @run_until_complete
    def test_consumer_cleanup_unassigned_data_getmany(self):
        # Send 3 messages
        topic2 = self.topic + "_other"
        topic3 = self.topic + "_another"
        tp = TopicPartition(self.topic, 0)
        tp2 = TopicPartition(topic2, 0)
        tp3 = TopicPartition(topic3, 0)
        yield from self.send_messages(0, [1, 2, 3])
        yield from self.send_messages(0, [5, 6, 7], topic=topic2)
        yield from self.send_messages(0, [8], topic=topic3)

        # Read first. 3 are delivered at a time, so 2 will remain
        consumer = yield from self.consumer_factory(
            heartbeat_interval_ms=200)
        yield from consumer.getone()
        # Verify that we have some precached records
        self.assertIn(tp, consumer._fetcher._records)

        consumer.subscribe([topic2])
        res = yield from consumer.getmany(timeout_ms=5000, max_records=1)
        self.assertEqual(res[tp2][0].value, b"5")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)

        # Same with an explicit partition
        consumer.subscribe([topic3])
        res = yield from consumer.getmany(tp3, timeout_ms=5000, max_records=1)
        self.assertEqual(res[tp3][0].value, b"8")
        # Verify that we have no more precached records
        self.assertNotIn(tp, consumer._fetcher._records)
        self.assertNotIn(tp2, consumer._fetcher._records)

        yield from consumer.stop()

    @run_until_complete
    def test_rebalance_listener_with_coroutines(self):
        yield from self.send_messages(0, list(range(0, 10)))
        yield from self.send_messages(1, list(range(10, 20)))

        main_self = self

        class SimpleRebalanceListener(ConsumerRebalanceListener):
            def __init__(self, consumer):
                self.consumer = consumer
                self.revoke_mock = mock.Mock()
                self.assign_mock = mock.Mock()

            @asyncio.coroutine
            def on_partitions_revoked(self, revoked):
                self.revoke_mock(revoked)
                # If this commit would fail we will end up with wrong msgs
                # eturned in test below
                yield from self.consumer.commit()
                # Confirm that coordinator is actually waiting for callback to
                # complete
                yield from asyncio.sleep(0.2, loop=main_self.loop)
                main_self.assertTrue(
                    self.consumer._coordinator.needs_join_prepare)

            @asyncio.coroutine
            def on_partitions_assigned(self, assigned):
                self.assign_mock(assigned)
                # Confirm that coordinator is actually waiting for callback to
                # complete
                yield from asyncio.sleep(0.2, loop=main_self.loop)
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
        yield from consumer1.start()
        self.add_cleanup(consumer1.stop)

        msg = yield from consumer1.getone(tp0)
        self.assertEqual(msg.value, b"0")
        msg = yield from consumer1.getone(tp1)
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
        yield from consumer2.start()
        self.add_cleanup(consumer2.stop)

        msg1 = yield from consumer1.getone()
        msg2 = yield from consumer2.getone()
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
    def test_rebalance_listener_no_deadlock_callbacks(self):
        # Seek_to_end requires partitions to be assigned, so it waits for
        # rebalance to end before attempting seek
        tp0 = TopicPartition(self.topic, 0)

        class SimpleRebalanceListener(ConsumerRebalanceListener):
            def __init__(self, consumer):
                self.consumer = consumer
                self.seek_task = None

            @asyncio.coroutine
            def on_partitions_revoked(self, revoked):
                pass

            @asyncio.coroutine
            def on_partitions_assigned(self, assigned):
                self.seek_task = self.consumer._loop.create_task(
                    self.consumer.seek_to_end(tp0))
                yield from self.seek_task

        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id="test_rebalance_listener_with_coroutines",
            bootstrap_servers=self.hosts, enable_auto_commit=False,
            auto_offset_reset="earliest")
        listener = SimpleRebalanceListener(consumer)
        consumer.subscribe([self.topic], listener=listener)
        yield from consumer.start()
        self.assertTrue(listener.seek_task.done())

    @run_until_complete
    def test_consumer_stop_cancels_pending_position_fetches(self):
        consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop, bootstrap_servers=self.hosts,
            group_id='group-%s' % self.id())
        yield from consumer.start()
        self.add_cleanup(consumer.stop)

        self.assertTrue(consumer._pending_position_fetches)
        pending_task = list(consumer._pending_position_fetches)[0]
        yield from consumer.stop()
        self.assertTrue(pending_task.cancelled())

    @run_until_complete
    def test_commit_not_blocked_by_long_poll_fetch(self):
        yield from self.send_messages(0, list(range(0, 10)))

        consumer = yield from self.consumer_factory(
            fetch_max_wait_ms=10000)

        # This should prefetch next batch right away and long-poll
        yield from consumer.getmany(timeout_ms=1000)
        long_poll_task = self.loop.create_task(
            consumer.getmany(timeout_ms=1000))
        yield from asyncio.sleep(0.2, loop=self.loop)
        self.assertFalse(long_poll_task.done())

        start_time = self.loop.time()
        yield from consumer.commit()
        end_time = self.loop.time()

        self.assertFalse(long_poll_task.done())
        self.assertLess(end_time - start_time, 500)

    @kafka_versions('>=0.10.1')
    @run_until_complete
    def test_offsets_for_times_single(self):
        high_time = int(self.loop.time() * 1000)
        middle_time = high_time - 1000
        low_time = high_time - 2000
        tp = TopicPartition(self.topic, 0)

        [msg1] = yield from self.send_messages(
            0, [1], timestamp_ms=low_time, return_inst=True)
        [msg2] = yield from self.send_messages(
            0, [1], timestamp_ms=high_time, return_inst=True)

        consumer = yield from self.consumer_factory()

        offsets = yield from consumer.offsets_for_times({tp: low_time})
        self.assertEqual(len(offsets), 1)
        self.assertEqual(offsets[tp].offset, msg1.offset)
        self.assertEqual(offsets[tp].timestamp, low_time)

        offsets = yield from consumer.offsets_for_times({tp: middle_time})
        self.assertEqual(offsets[tp].offset, msg2.offset)
        self.assertEqual(offsets[tp].timestamp, high_time)

        offsets = yield from consumer.offsets_for_times({tp: high_time})
        self.assertEqual(offsets[tp].offset, msg2.offset)
        self.assertEqual(offsets[tp].timestamp, high_time)

        # Out of bound timestamps check

        offsets = yield from consumer.offsets_for_times({tp: 0})
        self.assertEqual(offsets[tp].offset, msg1.offset)
        self.assertEqual(offsets[tp].timestamp, low_time)

        offsets = yield from consumer.offsets_for_times({tp: 9999999999999})
        self.assertEqual(offsets[tp], None)

        offsets = yield from consumer.offsets_for_times({})
        self.assertEqual(offsets, {})

        # Beginning and end offsets

        offsets = yield from consumer.beginning_offsets([tp])
        self.assertEqual(offsets, {
            tp: msg1.offset,
        })

        offsets = yield from consumer.end_offsets([tp])
        self.assertEqual(offsets, {
            tp: msg2.offset + 1,
        })

    @kafka_versions('>=0.10.1')
    @run_until_complete
    def test_kafka_consumer_offsets_search_many_partitions(self):
        tp0 = TopicPartition(self.topic, 0)
        tp1 = TopicPartition(self.topic, 1)

        send_time = int(time.time() * 1000)
        [msg1] = yield from self.send_messages(
            0, [1], timestamp_ms=send_time, return_inst=True)
        [msg2] = yield from self.send_messages(
            1, [1], timestamp_ms=send_time, return_inst=True)

        consumer = yield from self.consumer_factory()
        offsets = yield from consumer.offsets_for_times({
            tp0: send_time,
            tp1: send_time
        })

        self.assertEqual(offsets, {
            tp0: OffsetAndTimestamp(msg1.offset, send_time),
            tp1: OffsetAndTimestamp(msg2.offset, send_time)
        })

        offsets = yield from consumer.beginning_offsets([tp0, tp1])
        self.assertEqual(offsets, {
            tp0: msg1.offset,
            tp1: msg2.offset
        })

        offsets = yield from consumer.end_offsets([tp0, tp1])
        self.assertEqual(offsets, {
            tp0: msg1.offset + 1,
            tp1: msg2.offset + 1
        })

    @kafka_versions('>=0.10.1')
    @run_until_complete
    def test_kafka_consumer_offsets_errors(self):
        consumer = yield from self.consumer_factory()
        tp = TopicPartition(self.topic, 0)
        bad_tp = TopicPartition(self.topic, 100)

        with self.assertRaises(ValueError):
            yield from consumer.offsets_for_times({tp: -1})
        with self.assertRaises(KafkaTimeoutError):
            yield from consumer.offsets_for_times({bad_tp: 0})

    @kafka_versions('<0.10.1')
    @run_until_complete
    def test_kafka_consumer_offsets_old_brokers(self):
        consumer = yield from self.consumer_factory()
        tp = TopicPartition(self.topic, 0)

        with self.assertRaises(UnsupportedVersionError):
            yield from consumer.offsets_for_times({tp: int(time.time())})
        with self.assertRaises(UnsupportedVersionError):
            yield from consumer.beginning_offsets(tp)
        with self.assertRaises(UnsupportedVersionError):
            yield from consumer.end_offsets(tp)
