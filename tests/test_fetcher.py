import asyncio
import pytest
import unittest
from unittest import mock

from kafka.protocol.offset import OffsetResponse
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder

from aiokafka.protocol.fetch import (
    FetchRequest_v0 as FetchRequest, FetchResponse_v0 as FetchResponse)
from aiokafka.errors import (
    TopicAuthorizationFailedError, UnknownError, UnknownTopicOrPartitionError,
    OffsetOutOfRangeError, KafkaTimeoutError, NotLeaderForPartitionError
)
from aiokafka.structs import (
    TopicPartition, OffsetAndTimestamp, OffsetAndMetadata
)
from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.fetcher import (
    Fetcher, FetchResult, FetchError, ConsumerRecord, OffsetResetStrategy,
    PartitionRecords, READ_UNCOMMITTED
)
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.util import ensure_future
from ._testutil import run_until_complete


def test_offset_reset_strategy():
    assert OffsetResetStrategy.from_str("latest") == OffsetResetStrategy.LATEST
    assert OffsetResetStrategy.from_str("earliest") == \
        OffsetResetStrategy.EARLIEST
    assert OffsetResetStrategy.from_str("none") == OffsetResetStrategy.NONE
    assert OffsetResetStrategy.from_str("123") == OffsetResetStrategy.NONE

    assert OffsetResetStrategy.to_str(OffsetResetStrategy.LATEST) == "latest"
    assert OffsetResetStrategy.to_str(OffsetResetStrategy.EARLIEST) == \
        "earliest"
    assert OffsetResetStrategy.to_str(OffsetResetStrategy.NONE) == "none"
    assert OffsetResetStrategy.to_str(100) == "timestamp(100)"


def test_fetch_result_and_error(loop):
    # Add some data
    result = FetchResult(
        TopicPartition("test", 0), assignment=mock.Mock(), loop=loop,
        partition_records=mock.Mock(next_fetch_offset=0), backoff=0)

    assert repr(result) == "<FetchResult position=0>"
    error = FetchError(
        loop=loop, error=OffsetOutOfRangeError({}), backoff=0)

    # Python3.7 got rid of trailing comma in exceptions, which makes the line
    # diffrent between 3.6 and 3.7.
    assert repr(error) in [
        "<FetchError error=OffsetOutOfRangeError({},)>",
        "<FetchError error=OffsetOutOfRangeError({})>"
    ]


@pytest.mark.usefixtures('setup_test_class_serverless')
class TestFetcher(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self._cleanup = []

    def tearDown(self):
        super().tearDown()
        for coro, args, kw in reversed(self._cleanup):
            task = asyncio.wait_for(coro(*args, **kw), 30, loop=self.loop)
            self.loop.run_until_complete(task)

    def add_cleanup(self, cb_or_coro, *args, **kw):
        self._cleanup.append((cb_or_coro, args, kw))

    @run_until_complete
    async def test_fetcher__update_fetch_positions(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        subscriptions = SubscriptionState(loop=self.loop)
        fetcher = Fetcher(client, subscriptions, loop=self.loop)
        self.add_cleanup(fetcher.close)
        # Disable backgroud task
        fetcher._fetch_task.cancel()
        try:
            await fetcher._fetch_task
        except asyncio.CancelledError:
            pass
        fetcher._fetch_task = ensure_future(
            asyncio.sleep(1000000, loop=self.loop), loop=self.loop)

        partition = TopicPartition('test', 0)
        offsets = {partition: OffsetAndTimestamp(12, -1)}

        async def _proc_offset_request(node_id, topic_data):
            return offsets

        fetcher._proc_offset_request = mock.Mock()
        fetcher._proc_offset_request.side_effect = _proc_offset_request

        def reset_assignment():
            subscriptions.assign_from_user({partition})
            assignment = subscriptions.subscription.assignment
            tp_state = assignment.state_value(partition)
            return assignment, tp_state
        assignment, tp_state = reset_assignment()

        self.assertIsNone(tp_state._position)

        # CASE: reset from committed
        # In basic case we will need to wait for committed
        update_task = ensure_future(
            fetcher._update_fetch_positions(assignment, 0, [partition]),
            loop=self.loop
        )
        await asyncio.sleep(0.1, loop=self.loop)
        self.assertFalse(update_task.done())
        # Will continue only after committed is resolved
        tp_state.update_committed(OffsetAndMetadata(4, ""))
        needs_wakeup = await update_task
        self.assertFalse(needs_wakeup)
        self.assertEqual(tp_state._position, 4)
        self.assertEqual(fetcher._proc_offset_request.call_count, 0)

        # CASE: will not query committed if position already present
        await fetcher._update_fetch_positions(assignment, 0, [partition])
        self.assertEqual(tp_state._position, 4)
        self.assertEqual(fetcher._proc_offset_request.call_count, 0)

        # CASE: awaiting_reset for the partition
        tp_state.await_reset(OffsetResetStrategy.LATEST)
        self.assertIsNone(tp_state._position)
        await fetcher._update_fetch_positions(assignment, 0, [partition])
        self.assertEqual(tp_state._position, 12)
        self.assertEqual(fetcher._proc_offset_request.call_count, 1)

        # CASE: seeked while waiting for committed to be resolved
        assignment, tp_state = reset_assignment()
        update_task = ensure_future(
            fetcher._update_fetch_positions(assignment, 0, [partition]),
            loop=self.loop
        )
        await asyncio.sleep(0.1, loop=self.loop)
        self.assertFalse(update_task.done())

        tp_state.seek(8)
        tp_state.update_committed(OffsetAndMetadata(4, ""))
        await update_task
        self.assertEqual(tp_state._position, 8)
        self.assertEqual(fetcher._proc_offset_request.call_count, 1)

        # CASE: awaiting_reset during waiting for committed
        assignment, tp_state = reset_assignment()
        update_task = ensure_future(
            fetcher._update_fetch_positions(assignment, 0, [partition]),
            loop=self.loop
        )
        await asyncio.sleep(0.1, loop=self.loop)
        self.assertFalse(update_task.done())

        tp_state.await_reset(OffsetResetStrategy.LATEST)
        tp_state.update_committed(OffsetAndMetadata(4, ""))
        await update_task
        self.assertEqual(tp_state._position, 12)
        self.assertEqual(fetcher._proc_offset_request.call_count, 2)

        # CASE: reset using default strategy if committed offset undefined
        assignment, tp_state = reset_assignment()
        self.loop.call_later(
            0.01, tp_state.update_committed, OffsetAndMetadata(-1, ""))
        await fetcher._update_fetch_positions(assignment, 0, [partition])
        self.assertEqual(tp_state._position, 12)
        self.assertEqual(fetcher._records, {})

        # CASE: set error if _default_reset_strategy = OffsetResetStrategy.NONE
        assignment, tp_state = reset_assignment()
        self.loop.call_later(
            0.01, tp_state.update_committed, OffsetAndMetadata(-1, ""))
        fetcher._default_reset_strategy = OffsetResetStrategy.NONE
        needs_wakeup = await fetcher._update_fetch_positions(
            assignment, 0, [partition])
        self.assertTrue(needs_wakeup)
        self.assertIsNone(tp_state._position)
        self.assertIsInstance(fetcher._records[partition], FetchError)
        fetcher._records.clear()

        # CASE: if _proc_offset_request errored, we will retry on another spin
        fetcher._proc_offset_request.side_effect = UnknownError()
        assignment, tp_state = reset_assignment()
        tp_state.await_reset(OffsetResetStrategy.LATEST)
        await fetcher._update_fetch_positions(assignment, 0, [partition])
        self.assertIsNone(tp_state._position)
        self.assertTrue(tp_state.awaiting_reset)

        # CASE: reset 2 partitions separately, 1 will raise, 1 will get
        #       committed
        fetcher._proc_offset_request.side_effect = _proc_offset_request
        partition2 = TopicPartition('test', 1)
        subscriptions.assign_from_user({partition, partition2})
        assignment = subscriptions.subscription.assignment
        tp_state = assignment.state_value(partition)
        tp_state2 = assignment.state_value(partition2)
        tp_state.await_reset(OffsetResetStrategy.LATEST)
        self.loop.call_later(
            0.01, tp_state2.update_committed, OffsetAndMetadata(5, ""))
        await fetcher._update_fetch_positions(
            assignment, 0, [partition, partition2])
        self.assertEqual(tp_state.position, 12)
        self.assertEqual(tp_state2.position, 5)

    @run_until_complete
    async def test_proc_fetch_request(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        subscriptions = SubscriptionState(loop=self.loop)
        fetcher = Fetcher(
            client, subscriptions, auto_offset_reset="latest", loop=self.loop)

        tp = TopicPartition('test', 0)
        tp_info = (tp.topic, [(tp.partition, 4, 100000)])
        req = FetchRequest(
            -1,  # replica_id
            100, 100, [tp_info])

        client.ready = mock.MagicMock()
        client.ready.side_effect = asyncio.coroutine(lambda a: True)
        client.force_metadata_update = mock.MagicMock()
        client.force_metadata_update.side_effect = asyncio.coroutine(
            lambda: False)
        client.send = mock.MagicMock()

        builder = LegacyRecordBatchBuilder(
            magic=1, compression_type=0, batch_size=99999999)
        builder.append(offset=4, value=b"test msg", key=None, timestamp=None)
        raw_batch = bytes(builder.build())

        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 0, 9, raw_batch)])]))
        subscriptions.assign_from_user({tp})
        assignment = subscriptions.subscription.assignment
        tp_state = assignment.state_value(tp)

        # The partition has no active position, so will ignore result
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, False)
        self.assertEqual(fetcher._records, {})

        # The partition's position does not match request's fetch offset
        subscriptions.seek(tp, 0)
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, False)
        self.assertEqual(fetcher._records, {})

        subscriptions.seek(tp, 4)
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, True)
        buf = fetcher._records[tp]
        self.assertEqual(buf.getone().value, b"test msg")

        # If position changed after fetch request passed
        subscriptions.seek(tp, 4)
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        subscriptions.seek(tp, 10)
        self.assertIsNone(buf.getone())

        # If assignment is lost after fetch request passed
        subscriptions.seek(tp, 4)
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        subscriptions.unsubscribe()
        self.assertIsNone(buf.getone())

        subscriptions.assign_from_user({tp})
        assignment = subscriptions.subscription.assignment
        tp_state = assignment.state_value(tp)

        # error -> no partition found (UnknownTopicOrPartitionError)
        subscriptions.seek(tp, 4)
        fetcher._records.clear()
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 3, 9, raw_batch)])]))
        cc = client.force_metadata_update.call_count
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, False)
        self.assertEqual(client.force_metadata_update.call_count, cc + 1)

        # error -> topic auth failed (TopicAuthorizationFailedError)
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 29, 9, raw_batch)])]))
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, True)
        with self.assertRaises(TopicAuthorizationFailedError):
            await fetcher.next_record([])

        # error -> unknown
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, -1, 9, raw_batch)])]))
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, False)

        # error -> offset out of range with offset strategy
        client.send.side_effect = asyncio.coroutine(
            lambda n, r: FetchResponse(
                [('test', [(0, 1, 9, raw_batch)])]))
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, False)
        self.assertEqual(tp_state.has_valid_position, False)
        self.assertEqual(tp_state.awaiting_reset, True)
        self.assertEqual(tp_state.reset_strategy, OffsetResetStrategy.LATEST)

        # error -> offset out of range without offset strategy
        subscriptions.seek(tp, 4)
        fetcher._default_reset_strategy = OffsetResetStrategy.NONE
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, True)
        with self.assertRaises(OffsetOutOfRangeError):
            await fetcher.next_record([])

        await fetcher.close()

    def _setup_error_after_data(self):
        subscriptions = SubscriptionState(loop=self.loop)
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        fetcher = Fetcher(client, subscriptions, loop=self.loop)
        tp1 = TopicPartition('some_topic', 0)
        tp2 = TopicPartition('some_topic', 1)

        subscriptions.subscribe(set(["some_topic"]))
        subscriptions.assign_from_subscribed({tp1, tp2})
        assignment = subscriptions.subscription.assignment
        subscriptions.seek(tp1, 0)
        subscriptions.seek(tp2, 0)

        # Add some data
        messages = [ConsumerRecord(
            topic="some_topic", partition=1, offset=0, timestamp=0,
            timestamp_type=0, key=None, value=b"some", checksum=None,
            serialized_key_size=0, serialized_value_size=4, headers=[])]
        partition_records = PartitionRecords(
            tp2, mock.Mock(), [], 0,
            None, None, False, READ_UNCOMMITTED)
        partition_records._records_iterator = iter(messages)
        fetcher._records[tp2] = FetchResult(
            tp2, assignment=assignment, loop=self.loop,
            partition_records=partition_records, backoff=0)
        # Add some error
        fetcher._records[tp1] = FetchError(
            loop=self.loop, error=OffsetOutOfRangeError({}), backoff=0)
        return fetcher, tp1, tp2, messages

    @run_until_complete
    async def test_fetched_records_error_after_data(self):
        # Test error after some data. fetched_records should not discard data.
        fetcher, tp1, tp2, messages = self._setup_error_after_data()

        msg = await fetcher.fetched_records([])
        self.assertEqual(msg, {tp2: messages})

        with self.assertRaises(OffsetOutOfRangeError):
            msg = await fetcher.fetched_records([])

        msg = await fetcher.fetched_records([])
        self.assertEqual(msg, {})

    @run_until_complete
    async def test_next_record_error_after_data(self):
        # Test error after some data. next_record should not discard data.
        fetcher, tp1, tp2, messages = self._setup_error_after_data()

        msg = await fetcher.next_record([])
        self.assertEqual(msg, messages[0])

        with self.assertRaises(OffsetOutOfRangeError):
            msg = await fetcher.next_record([])

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(
                fetcher.next_record([]), timeout=0.1, loop=self.loop)

    @run_until_complete
    async def test_compacted_topic_consumption(self):
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

        subscriptions = SubscriptionState(loop=self.loop)
        fetcher = Fetcher(client, subscriptions, loop=self.loop)

        tp = TopicPartition('test', 0)
        req = FetchRequest(
            -1,  # replica_id
            100, 100, [(tp.topic, [(tp.partition, 155, 100000)])])

        builder = LegacyRecordBatchBuilder(
            magic=1, compression_type=0, batch_size=99999999)
        builder.append(160, value=b"12345", key=b"1", timestamp=None)
        builder.append(162, value=b"23456", key=b"2", timestamp=None)
        builder.append(167, value=b"34567", key=b"3", timestamp=None)
        batch = bytes(builder.build())

        resp = FetchResponse(
            [('test', [(
                0, 0, 3000,  # partition, error_code, highwater_offset
                batch  # Batch raw bytes
            )])])

        subscriptions.assign_from_user({tp})
        assignment = subscriptions.subscription.assignment
        tp_state = assignment.state_value(tp)
        client.send.side_effect = asyncio.coroutine(lambda n, r: resp)

        tp_state.seek(155)
        fetcher._in_flight.add(0)
        needs_wake_up = await fetcher._proc_fetch_request(
            assignment, 0, req)
        self.assertEqual(needs_wake_up, True)
        buf = fetcher._records[tp]
        # Test successful getone, the closest in batch offset=160
        first = buf.getone()
        self.assertEqual(tp_state.position, 161)
        self.assertEqual(
            (first.value, first.key, first.offset),
            (b"12345", b"1", 160))

        # Test successful getmany
        second, third = buf.getall()
        self.assertEqual(tp_state.position, 168)
        self.assertEqual(
            (second.value, second.key, second.offset),
            (b"23456", b"2", 162))
        self.assertEqual(
            (third.value, third.key, third.offset),
            (b"34567", b"3", 167))

    @run_until_complete
    async def test_fetcher_offsets_for_times(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=[])
        client.ready = mock.MagicMock()
        client.ready.side_effect = asyncio.coroutine(lambda a: True)
        client._maybe_wait_metadata = mock.MagicMock()
        client._maybe_wait_metadata.side_effect = asyncio.coroutine(
            lambda: False)
        client.cluster.leader_for_partition = mock.MagicMock()
        client.cluster.leader_for_partition.return_value = 0
        client._api_version = (0, 10, 1)

        subscriptions = SubscriptionState(loop=self.loop)
        fetcher = Fetcher(client, subscriptions, loop=self.loop)
        tp0 = TopicPartition("topic", 0)
        tp1 = TopicPartition("topic", 1)

        # Timeouting will result in KafkaTimeoutError
        with mock.patch.object(fetcher, "_proc_offset_requests") as mocked:
            mocked.side_effect = asyncio.TimeoutError

            with self.assertRaises(KafkaTimeoutError):
                await fetcher.get_offsets_by_times({tp0: 0}, 1000)

        # Broker returns UnsupportedForMessageFormatError
        with mock.patch.object(client, "send") as mocked:
            async def mock_send(node_id, request):
                return OffsetResponse[1]([
                    ("topic", [(0, 43, -1, -1)]),
                    ("topic", [(1, 0, 1000, 9999)])
                ])
            mocked.side_effect = mock_send
            offsets = await fetcher.get_offsets_by_times(
                {tp0: 0, tp1: 0}, 1000)
            self.assertEqual(offsets, {
                tp0: None,
                tp1: OffsetAndTimestamp(9999, 1000),
            })
        # Brokers returns NotLeaderForPartitionError
        with mock.patch.object(client, "send") as mocked:
            async def mock_send(node_id, request):
                return OffsetResponse[1]([
                    ("topic", [(0, 6, -1, -1)]),
                ])
            mocked.side_effect = mock_send
            with self.assertRaises(NotLeaderForPartitionError):
                await fetcher._proc_offset_request(
                    0, {"topic": (0, 1000)})

        # Broker returns UnknownTopicOrPartitionError
        with mock.patch.object(client, "send") as mocked:
            async def mock_send(node_id, request):
                return OffsetResponse[1]([
                    ("topic", [(0, 3, -1, -1)]),
                ])
            mocked.side_effect = mock_send
            with self.assertLogs("aiokafka.consumer.fetcher", "WARN") as cm:
                with self.assertRaises(UnknownTopicOrPartitionError):
                    await fetcher._proc_offset_request(
                        0, {"topic": (0, 1000)})
            if cm is not None:
                self.assertIn(
                    "Received unknown topic or partition error", cm.output[0])
