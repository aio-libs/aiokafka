import asyncio
import pytest
import unittest
from collections import deque
from unittest import mock

from kafka.common import (TopicPartition, TopicAuthorizationFailedError,
                          UnknownError, UnknownTopicOrPartitionError,
                          OffsetOutOfRangeError)
from kafka.consumer.subscription_state import (
    SubscriptionState, TopicPartitionState)
from kafka.protocol.offset import (
    OffsetResetStrategy, OffsetResponse_v0 as OffsetResponse)
from kafka.protocol.fetch import (
    FetchRequest_v0 as FetchRequest, FetchResponse_v0 as FetchResponse)
from kafka.protocol.message import Message

from aiokafka.client import AIOKafkaClient
from aiokafka.fetcher import Fetcher, FetchResult, FetchError, ConsumerRecord
from ._testutil import run_until_complete


@pytest.mark.usefixtures('setup_test_class_serverless')
class TestFetcher(unittest.TestCase):
    @run_until_complete
    def test_update_fetch_positions(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        subscriptions = SubscriptionState('latest')
        fetcher = Fetcher(client, subscriptions, loop=self.loop)
        partition = TopicPartition('test', 0)
        # partition is not assigned, should be ignored
        yield from fetcher.update_fetch_positions([partition])

        state = TopicPartitionState()
        state.seek(0)
        subscriptions.assignment[partition] = state
        # partition is fetchable, no need to update position
        yield from fetcher.update_fetch_positions([partition])

        client.ready = mock.MagicMock()
        client.ready.side_effect = asyncio.coroutine(lambda a: True)
        client.force_metadata_update = mock.MagicMock()
        client.force_metadata_update.side_effect = asyncio.coroutine(
            lambda: False)
        client.send = mock.MagicMock()
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: OffsetResponse([('test', [(0, 0, [4])])]))
        state.await_reset(OffsetResetStrategy.LATEST)
        client.cluster.leader_for_partition = mock.MagicMock()
        client.cluster.leader_for_partition.side_effect = [None, -1, 0]
        yield from fetcher.update_fetch_positions([partition])
        self.assertEqual(state.position, 4)

        client.cluster.leader_for_partition = mock.MagicMock()
        client.cluster.leader_for_partition.return_value = 1
        client.send = mock.MagicMock()
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: OffsetResponse([('test', [(0, 3, [])])]))
        state.await_reset(OffsetResetStrategy.LATEST)
        with self.assertRaises(UnknownTopicOrPartitionError):
            yield from fetcher.update_fetch_positions([partition])

        client.send.side_effect = asyncio.coroutine(
            lambda n, r: OffsetResponse([('test', [(0, -1, [])])]))
        with self.assertRaises(UnknownError):
            yield from fetcher.update_fetch_positions([partition])
        yield from fetcher.close()

    @run_until_complete
    def test_proc_fetch_request(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        subscriptions = SubscriptionState('latest')
        fetcher = Fetcher(client, subscriptions, loop=self.loop)

        tp = TopicPartition('test', 0)
        tp_info = (tp.topic, [(tp.partition, 155, 100000)])
        req = FetchRequest(
            -1,  # replica_id
            100, 100, [tp_info])

        client.ready = mock.MagicMock()
        client.ready.side_effect = asyncio.coroutine(lambda a: True)
        client.force_metadata_update = mock.MagicMock()
        client.force_metadata_update.side_effect = asyncio.coroutine(
            lambda: False)
        client.send = mock.MagicMock()
        msg = Message(b"test msg")
        msg._encode_self()
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 0, 9, [(4, 10, msg)])])]))
        fetcher._in_flight.add(0)
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, False)

        state = TopicPartitionState()
        state.seek(0)
        subscriptions.assignment[tp] = state
        subscriptions.needs_partition_assignment = False
        fetcher._in_flight.add(0)
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, True)
        buf = fetcher._records[tp]
        self.assertEqual(buf.getone(), None)  # invalid offset, msg is ignored

        state.seek(4)
        fetcher._in_flight.add(0)
        fetcher._records.clear()
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, True)
        buf = fetcher._records[tp]
        self.assertEqual(buf.getone().value, b"test msg")

        # error -> no partition found
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 3, 9, [(4, 10, msg)])])]))
        fetcher._in_flight.add(0)
        fetcher._records.clear()
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, False)

        # error -> topic auth failed
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 29, 9, [(4, 10, msg)])])]))
        fetcher._in_flight.add(0)
        fetcher._records.clear()
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, True)
        with self.assertRaises(TopicAuthorizationFailedError):
            yield from fetcher.next_record([])

        # error -> unknown
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, -1, 9, [(4, 10, msg)])])]))
        fetcher._in_flight.add(0)
        fetcher._records.clear()
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, False)

        # error -> offset out of range
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 1, 9, [(4, 10, msg)])])]))
        fetcher._in_flight.add(0)
        fetcher._records.clear()
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, False)
        self.assertEqual(state.is_fetchable(), False)

        state.seek(4)
        subscriptions._default_offset_reset_strategy = OffsetResetStrategy.NONE
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 1, 9, [(4, 10, msg)])])]))
        fetcher._in_flight.add(0)
        fetcher._records.clear()
        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, True)
        with self.assertRaises(OffsetOutOfRangeError):
            yield from fetcher.next_record([])

        yield from fetcher.close()

    def _setup_error_after_data(self):
        subscriptions = SubscriptionState('latest')
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        fetcher = Fetcher(client, subscriptions, loop=self.loop)
        tp1 = TopicPartition('some_topic', 0)
        tp2 = TopicPartition('some_topic', 1)

        state = TopicPartitionState()
        state.seek(0)
        subscriptions.assignment[tp1] = state
        state = TopicPartitionState()
        state.seek(0)
        subscriptions.assignment[tp2] = state
        subscriptions.needs_partition_assignment = False

        # Add some data
        messages = [ConsumerRecord(
            topic="some_topic", partition=1, offset=0, timestamp=0,
            timestamp_type=0, key=None, value=b"some", checksum=None,
            serialized_key_size=0, serialized_value_size=4)]
        fetcher._records[tp2] = FetchResult(
            tp2, subscriptions=subscriptions, loop=self.loop,
            messages=deque(messages), backoff=0)
        # Add some error
        fetcher._records[tp1] = FetchError(
            loop=self.loop, error=OffsetOutOfRangeError({}), backoff=0)
        return fetcher, tp1, tp2, messages

    @run_until_complete
    def test_fetched_records_error_after_data(self):
        # Test error after some data. fetched_records should not discard data.
        fetcher, tp1, tp2, messages = self._setup_error_after_data()

        msg = yield from fetcher.fetched_records([])
        self.assertEqual(msg, {tp2: messages})

        with self.assertRaises(OffsetOutOfRangeError):
            msg = yield from fetcher.fetched_records([])

        msg = yield from fetcher.fetched_records([])
        self.assertEqual(msg, {})

    @run_until_complete
    def test_next_record_error_after_data(self):
        # Test error after some data. next_record should not discard data.
        fetcher, tp1, tp2, messages = self._setup_error_after_data()

        msg = yield from fetcher.next_record([])
        self.assertEqual(msg, messages[0])

        with self.assertRaises(OffsetOutOfRangeError):
            msg = yield from fetcher.next_record([])

        with self.assertRaises(asyncio.TimeoutError):
            yield from asyncio.wait_for(
                fetcher.next_record([]), timeout=0.1, loop=self.loop)

    @run_until_complete
    def test_compacted_topic_consumption(self):
        # Compacted topics can have offsets skipped
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        client.ready = mock.MagicMock()
        client.ready.side_effect = asyncio.coroutine(lambda a: True)
        client.force_metadata_update = mock.MagicMock()
        client.force_metadata_update.side_effect = asyncio.coroutine(
            lambda: False)
        client.send = mock.MagicMock()

        subscriptions = SubscriptionState('latest')
        fetcher = Fetcher(client, subscriptions, loop=self.loop)

        tp = TopicPartition('test', 0)
        req = FetchRequest(
            -1,  # replica_id
            100, 100, [(tp.topic, [(tp.partition, 155, 100000)])])
        msg1 = Message(b"12345", key=b"1")
        msg1._encode_self()
        msg2 = Message(b"23456", key=b"2")
        msg2._encode_self()
        msg3 = Message(b"34567", key=b"3")
        msg3._encode_self()
        resp = FetchResponse(
            [('test', [(
                0, 0, 3000,  # partition, error_code, highwater_offset
                [(160, 5, msg1),  # offset, len_bytes, bytes
                 (162, 5, msg2),
                 (167, 5, msg3),
                 ]
            )])])

        client.send.side_effect = asyncio.coroutine(lambda n, r: resp)
        state = TopicPartitionState()
        state.seek(155)
        state.drop_pending_message_set = False
        subscriptions.assignment[tp] = state
        subscriptions.needs_partition_assignment = False
        fetcher._in_flight.add(0)

        needs_wake_up = yield from fetcher._proc_fetch_request(0, req)
        self.assertEqual(needs_wake_up, True)
        buf = fetcher._records[tp]
        # Test successful getone
        first = buf.getone()
        self.assertEqual(state.position, 161)
        self.assertEqual(
            (first.value, first.key, first.offset),
            (msg1.value, msg1.key, 160))

        # Test successful getmany
        second, third = buf.getall()
        self.assertEqual(state.position, 168)
        self.assertEqual(
            (second.value, second.key, second.offset),
            (msg2.value, msg2.key, 162))
        self.assertEqual(
            (third.value, third.key, third.offset),
            (msg3.value, msg3.key, 167))
