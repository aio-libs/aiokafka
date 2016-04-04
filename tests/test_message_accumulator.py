import io
import asyncio
import pytest
import unittest
from unittest import mock

from kafka.cluster import ClusterMetadata
from kafka.common import (TopicPartition, KafkaTimeoutError,
                          NotLeaderForPartitionError,
                          LeaderNotAvailableError)
from ._testutil import run_until_complete
from aiokafka.message_accumulator import MessageAccumulator, MessageBatch


@pytest.mark.usefixtures('setup_test_class_serverless')
class TestMessageAccumulator(unittest.TestCase):
    @run_until_complete
    def test_basic(self):
        cluster = ClusterMetadata(metadata_max_age_ms=10000)
        ma = MessageAccumulator(cluster, 1000, None, 30, self.loop)
        data_waiter = asyncio.async(ma.data_waiter(), loop=self.loop)
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

        data_waiter = asyncio.async(ma.data_waiter(), loop=self.loop)
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
            tp1, None, b'0123456789'*70, timeout=2)
        yield from ma.add_message(
            tp2, None, b'message to unknown leader', timeout=2)
        # next we try to add message with len=500,
        # as we have buffer_size=1000 coroutine will block until data will be
        # drained
        add_task = asyncio.async(
            ma.add_message(tp1, None, b'0123456789'*50, timeout=2),
            loop=self.loop)
        done, _ = yield from asyncio.wait(
            [add_task], timeout=0.2, loop=self.loop)
        self.assertFalse(bool(done))

        batches, unknown_leaders_exist = ma.drain_by_nodes(ignore_nodes=[1, 2])
        self.assertEqual(unknown_leaders_exist, True)
        m_set0 = batches[0].get(tp0)
        self.assertEqual(m_set0._relative_offset, 2)
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
        self.assertEqual(m_set1._relative_offset, 1)

        done, _ = yield from asyncio.wait(
            [add_task], timeout=0.2, loop=self.loop)
        self.assertTrue(bool(done))
        batches, unknown_leaders_exist = ma.drain_by_nodes(ignore_nodes=[])
        self.assertEqual(unknown_leaders_exist, True)
        m_set1 = batches[1].get(tp1)
        self.assertEqual(m_set1._relative_offset, 1)

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

        ma = MessageAccumulator(cluster, 1000, None, 1, self.loop)
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
        batch_data = batches[0][tp0].data()
        self.assertEqual(type(batch_data), io.BytesIO)
        batches[0][tp0].done(base_offset=10)

        class TestException(Exception):
            pass

        batches[1][tp1].done(exception=TestException())

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
        batches[0][tp0].done(base_offset=None)
        res = yield from fut01
        self.assertEqual(res, None)
