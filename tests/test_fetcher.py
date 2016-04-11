import asyncio
import pytest
import unittest
from unittest import mock

from kafka.common import (TopicPartition, TopicAuthorizationFailedError,
                          UnknownError, UnknownTopicOrPartitionError,
                          OffsetOutOfRangeError)
from kafka.consumer.subscription_state import (
    SubscriptionState, TopicPartitionState)
from kafka.protocol.offset import OffsetResetStrategy, OffsetResponse
from kafka.protocol.fetch import FetchRequest, FetchResponse
from kafka.protocol.message import Message

from aiokafka.client import AIOKafkaClient
from aiokafka.fetcher import Fetcher
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
