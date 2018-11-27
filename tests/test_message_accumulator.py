import asyncio
import pytest
import unittest
from unittest import mock

from kafka.cluster import ClusterMetadata
from kafka.common import (TopicPartition, KafkaTimeoutError,
                          NotLeaderForPartitionError,
                          LeaderNotAvailableError)
from ._testutil import run_until_complete
from aiokafka.util import ensure_future
from aiokafka.producer.message_accumulator import (
    MessageAccumulator, MessageBatch, BatchBuilder
)


@pytest.mark.usefixtures('setup_test_class_serverless')
class TestMessageAccumulator(unittest.TestCase):
    @run_until_complete
    def test_basic(self):
        cluster = ClusterMetadata(metadata_max_age_ms=10000)
        ma = MessageAccumulator(cluster, 1000, 0, 30, loop=self.loop)
        data_waiter = ma.data_waiter()
        done, _ = yield from asyncio.wait(
            [data_waiter], timeout=0.2, loop=self.loop)
        self.assertFalse(bool(done))  # no data in accumulator yet...

        tp0 = TopicPartition("test-topic", 0)
        tp1 = TopicPartition("test-topic", 1)
        yield from ma.add_message(tp0, b'key', b'value', timeout=2)
        yield from ma.add_message(tp1, None, b'value without key', timeout=2)

        done, _ = yield from asyncio.wait(
            [data_waiter], timeout=0.2, loop=self.loop)
        self.assertTrue(bool(done))

        batches, unknown_leaders_exist = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(batches, {})
        self.assertEqual(unknown_leaders_exist, True)

        def mocked_leader_for_partition(tp):
            if tp == tp0:
                return 0
            if tp == tp1:
                return 1
            return -1

        cluster.leader_for_partition = mock.MagicMock()
        cluster.leader_for_partition.side_effect = mocked_leader_for_partition
        batches, unknown_leaders_exist = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(len(batches), 2)
        self.assertEqual(unknown_leaders_exist, False)
        m_set0 = batches[0].get(tp0)
        self.assertEqual(type(m_set0), MessageBatch)
        m_set1 = batches[1].get(tp1)
        self.assertEqual(type(m_set1), MessageBatch)
        self.assertEqual(m_set0.expired(), False)

        data_waiter = ensure_future(ma.data_waiter(), loop=self.loop)
        done, _ = yield from asyncio.wait(
            [data_waiter], timeout=0.2, loop=self.loop)
        self.assertFalse(bool(done))  # no data in accumulator again...

        # testing batch overflow
        tp2 = TopicPartition("test-topic", 2)
        yield from ma.add_message(
            tp0, None, b'some short message', timeout=2)
        yield from ma.add_message(
            tp0, None, b'some other short message', timeout=2)
        yield from ma.add_message(
            tp1, None, b'0123456789' * 70, timeout=2)
        yield from ma.add_message(
            tp2, None, b'message to unknown leader', timeout=2)
        # next we try to add message with len=500,
        # as we have buffer_size=1000 coroutine will block until data will be
        # drained
        add_task = ensure_future(
            ma.add_message(tp1, None, b'0123456789' * 50, timeout=2),
            loop=self.loop)
        done, _ = yield from asyncio.wait(
            [add_task], timeout=0.2, loop=self.loop)
        self.assertFalse(bool(done))

        batches, unknown_leaders_exist = ma.drain_by_nodes(ignore_nodes=[1, 2])
        self.assertEqual(unknown_leaders_exist, True)
        m_set0 = batches[0].get(tp0)
        self.assertEqual(m_set0._builder._relative_offset, 2)
        m_set1 = batches[1].get(tp1)
        self.assertEqual(m_set1, None)

        done, _ = yield from asyncio.wait(
            [add_task], timeout=0.1, loop=self.loop)
        self.assertFalse(bool(done))  # we stil not drained data for tp1

        batches, unknown_leaders_exist = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(unknown_leaders_exist, True)
        m_set0 = batches[0].get(tp0)
        self.assertEqual(m_set0, None)
        m_set1 = batches[1].get(tp1)
        self.assertEqual(m_set1._builder._relative_offset, 1)

        done, _ = yield from asyncio.wait(
            [add_task], timeout=0.2, loop=self.loop)
        self.assertTrue(bool(done))
        batches, unknown_leaders_exist = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(unknown_leaders_exist, True)
        m_set1 = batches[1].get(tp1)
        self.assertEqual(m_set1._builder._relative_offset, 1)

    @run_until_complete
    def test_batch_done(self):
        tp0 = TopicPartition("test-topic", 0)
        tp1 = TopicPartition("test-topic", 1)
        tp2 = TopicPartition("test-topic", 2)
        tp3 = TopicPartition("test-topic", 3)

        def mocked_leader_for_partition(tp):
            if tp == tp0:
                return 0
            if tp == tp1:
                return 1
            if tp == tp2:
                return -1
            return None

        cluster = ClusterMetadata(metadata_max_age_ms=10000)
        cluster.leader_for_partition = mock.MagicMock()
        cluster.leader_for_partition.side_effect = mocked_leader_for_partition

        ma = MessageAccumulator(cluster, 1000, 0, 1, loop=self.loop)
        fut1 = yield from ma.add_message(
            tp2, None, b'msg for tp@2', timeout=2)
        fut2 = yield from ma.add_message(
            tp3, None, b'msg for tp@3', timeout=2)
        yield from ma.add_message(tp1, None, b'0123456789'*70, timeout=2)
        with self.assertRaises(KafkaTimeoutError):
            yield from ma.add_message(tp1, None, b'0123456789'*70, timeout=2)
        batches, _ = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(batches[1][tp1].expired(), True)
        with self.assertRaises(LeaderNotAvailableError):
            yield from fut1
        with self.assertRaises(NotLeaderForPartitionError):
            yield from fut2

        fut01 = yield from ma.add_message(
            tp0, b'key0', b'value#0', timeout=2)
        fut02 = yield from ma.add_message(
            tp0, b'key1', b'value#1', timeout=2)
        fut10 = yield from ma.add_message(
            tp1, None, b'0123456789'*70, timeout=2)
        batches, _ = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(batches[0][tp0].expired(), False)
        self.assertEqual(batches[1][tp1].expired(), False)
        batch_data = batches[0][tp0].get_data_buffer()
        self.assertEqual(type(batch_data), bytearray)
        batches[0][tp0].done(base_offset=10)

        class TestException(Exception):
            pass

        batches[1][tp1].failure(exception=TestException())

        res = yield from fut01
        self.assertEqual(res.topic, "test-topic")
        self.assertEqual(res.partition, 0)
        self.assertEqual(res.offset, 10)
        res = yield from fut02
        self.assertEqual(res.topic, "test-topic")
        self.assertEqual(res.partition, 0)
        self.assertEqual(res.offset, 11)
        with self.assertRaises(TestException):
            yield from fut10

        fut01 = yield from ma.add_message(
            tp0, b'key0', b'value#0', timeout=2)
        batches, _ = ma.drain_by_nodes(ignore_nodes=[])
        batches[0][tp0].done_noack()
        res = yield from fut01
        self.assertEqual(res, None)

        # cancelling future
        fut01 = yield from ma.add_message(
            tp0, b'key0', b'value#2', timeout=2)
        batches, _ = ma.drain_by_nodes(ignore_nodes=[])
        fut01.cancel()
        batches[0][tp0].done(base_offset=21)  # no error in this case

    @run_until_complete
    def test_message_batch_builder_basic(self):
        magic = 0
        batch_size = 1000
        msg_count = 3
        key = b"test key"
        value = b"test value"
        builder = BatchBuilder(magic, batch_size, 0, is_transactional=False)
        self.assertEqual(builder._relative_offset, 0)
        self.assertIsNone(builder._buffer)
        self.assertFalse(builder._closed)
        self.assertEqual(builder.size(), 0)
        self.assertEqual(builder.record_count(), 0)

        # adding messages returns size and increments appropriate values
        for num in range(1, msg_count + 1):
            old_size = builder.size()
            metadata = builder.append(key=key, value=value, timestamp=None)
            self.assertIsNotNone(metadata)
            msg_size = metadata.size
            self.assertTrue(msg_size > 0)
            self.assertEqual(builder.size(), old_size + msg_size)
            self.assertEqual(builder.record_count(), num)
        old_size = builder.size()
        old_count = builder.record_count()

        # close the builder
        buf = builder._build()
        self.assertIsNotNone(builder._buffer)
        self.assertEqual(buf, builder._buffer)
        self.assertTrue(builder._closed)

        # nothing can be added after the builder has been closed
        old_size = builder.size()
        metadata = builder.append(key=key, value=value, timestamp=None)
        self.assertIsNone(metadata)
        self.assertEqual(builder.size(), old_size)
        self.assertEqual(builder.record_count(), old_count)

    @run_until_complete
    def test_add_batch_builder(self):
        tp0 = TopicPartition("test-topic", 0)
        tp1 = TopicPartition("test-topic", 1)

        def mocked_leader_for_partition(tp):
            if tp == tp0:
                return 0
            if tp == tp1:
                return 1
            return None

        cluster = ClusterMetadata(metadata_max_age_ms=10000)
        cluster.leader_for_partition = mock.MagicMock()
        cluster.leader_for_partition.side_effect = mocked_leader_for_partition

        ma = MessageAccumulator(cluster, 1000, 0, 1, loop=self.loop)
        builder0 = ma.create_builder()
        builder1_1 = ma.create_builder()
        builder1_2 = ma.create_builder()

        # batches may queued one-per-TP
        self.assertFalse(ma._wait_data_future.done())
        yield from ma.add_batch(builder0, tp0, 1)
        self.assertTrue(ma._wait_data_future.done())
        self.assertEqual(len(ma._batches[tp0]), 1)

        yield from ma.add_batch(builder1_1, tp1, 1)
        self.assertEqual(len(ma._batches[tp1]), 1)
        with self.assertRaises(KafkaTimeoutError):
            yield from ma.add_batch(builder1_2, tp1, 0.1)
        self.assertTrue(ma._wait_data_future.done())
        self.assertEqual(len(ma._batches[tp1]), 1)

        # second batch gets added once the others are cleared out
        self.loop.call_later(0.1, ma.drain_by_nodes, [])
        yield from ma.add_batch(builder1_2, tp1, 1)
        self.assertTrue(ma._wait_data_future.done())
        self.assertEqual(len(ma._batches[tp0]), 0)
        self.assertEqual(len(ma._batches[tp1]), 1)

    @run_until_complete
    def test_batch_pending_batch_list(self):
        # In message accumulator we have _pending_batches list, that stores
        # batches when those are delivered to node. We must be sure we never
        # lose a batch during retries and that we don't produce duplicate batch
        # links in the process

        tp0 = TopicPartition("test-topic", 0)

        def mocked_leader_for_partition(tp):
            if tp == tp0:
                return 0
            return None

        cluster = ClusterMetadata(metadata_max_age_ms=10000)
        cluster.leader_for_partition = mock.MagicMock()
        cluster.leader_for_partition.side_effect = mocked_leader_for_partition

        ma = MessageAccumulator(cluster, 1000, 0, 1, loop=self.loop)
        fut1 = yield from ma.add_message(
            tp0, b'key', b'value', timeout=2)

        # Drain and Reenqueu
        batches, _ = ma.drain_by_nodes(ignore_nodes=[])
        batch = batches[0][tp0]
        self.assertIn(batch, ma._pending_batches)
        self.assertFalse(ma._batches)
        self.assertFalse(fut1.done())

        ma.reenqueue(batch)
        self.assertEqual(batch.retry_count, 1)
        self.assertFalse(ma._pending_batches)
        self.assertIn(batch, ma._batches[tp0])
        self.assertFalse(fut1.done())

        # Drain and Reenqueu again. We check for repeated call
        batches, _ = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(batches[0][tp0], batch)
        self.assertEqual(batch.retry_count, 2)
        self.assertIn(batch, ma._pending_batches)
        self.assertFalse(ma._batches)
        self.assertFalse(fut1.done())

        ma.reenqueue(batch)
        self.assertEqual(batch.retry_count, 2)
        self.assertFalse(ma._pending_batches)
        self.assertIn(batch, ma._batches[tp0])
        self.assertFalse(fut1.done())

        # Drain and mark as done. Check that no link to batch remained
        batches, _ = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(batches[0][tp0], batch)
        self.assertEqual(batch.retry_count, 3)
        self.assertIn(batch, ma._pending_batches)
        self.assertFalse(ma._batches)
        self.assertFalse(fut1.done())

        if hasattr(batch.future, "_callbacks"):  # Vanilla asyncio
            self.assertEqual(len(batch.future._callbacks), 1)

        batch.done_noack()
        yield from asyncio.sleep(0.01, loop=self.loop)
        self.assertEqual(batch.retry_count, 3)
        self.assertFalse(ma._pending_batches)
        self.assertFalse(ma._batches)
