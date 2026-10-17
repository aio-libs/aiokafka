import asyncio
import time
from unittest import mock

import pytest

from aiokafka import (
    AIOKafkaConsumer,
    ConsumerMetricsCollector,
    ProducerMetricsCollector,
)
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.errors import (
    CorruptRecordException,
    KafkaConnectionError,
    NotLeaderForPartitionError,
    OffsetOutOfRangeError,
    RecordTooLargeError,
    TopicAuthorizationFailedError,
)
from aiokafka.protocol.commit import OffsetCommitResponse_v2
from aiokafka.protocol.fetch import FetchRequest_v4, FetchResponse_v4
from aiokafka.record.default_records import DefaultRecordBatchBuilder
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder
from aiokafka.record.util import calc_crc32c
from aiokafka.structs import OffsetAndMetadata, TopicPartition

TP = TopicPartition("metrics", 0)
OTHER_TP = TopicPartition("metrics", 1)


@pytest.fixture
async def consumer(monkeypatch):
    # Exercise the public wiring and poll paths without starting broker tasks.
    monkeypatch.setattr(Fetcher, "_fetch_requests_routine", mock.AsyncMock())
    monkeypatch.setattr(GroupCoordinator, "_coordination_routine", mock.AsyncMock())
    monkeypatch.setattr(
        GroupCoordinator, "start_commit_offsets_refresh_task", mock.Mock()
    )
    collector = mock.create_autospec(ConsumerMetricsCollector, instance=True)
    consumer = AIOKafkaConsumer(
        group_id="metrics-group",
        enable_auto_commit=False,
        auto_offset_reset="none",
        retry_backoff_ms=0,
        metrics_collector=collector,
        value_deserializer=lambda value: {"value": value},
    )
    consumer.assign([TP, OTHER_TP])
    monkeypatch.setattr(consumer._client, "bootstrap", mock.AsyncMock())
    monkeypatch.setattr(consumer, "_wait_topics", mock.AsyncMock())
    await consumer.start()
    consumer.seek(TP, 4)
    consumer.seek(OTHER_TP, 4)
    yield consumer
    await consumer.stop()


@pytest.fixture
def clock(monkeypatch):
    now = [10.0]
    for module in ("fetcher", "group_coordinator"):
        timer = mock.Mock(wraps=time)
        timer.monotonic.side_effect = lambda: now[0]
        monkeypatch.setattr(f"aiokafka.consumer.{module}.time", timer)
    return now


def batch_data(magic=2, compression=0, transactional=False, control=False):
    if magic == 2:
        builder = DefaultRecordBatchBuilder(
            magic=2,
            compression_type=compression,
            is_transactional=transactional,
            producer_id=12 if transactional else -1,
            producer_epoch=0,
            base_sequence=0,
            batch_size=10000,
        )
    else:
        builder = LegacyRecordBatchBuilder(
            magic=magic, compression_type=compression, batch_size=10000
        )
    # A gap in offsets must not inflate record_count.
    builder.append(offset=4, timestamp=1, key=b"k", value=b"value", headers=[])
    builder.append(offset=8, timestamp=2, key=None, value=None, headers=[])
    data = bytes(builder.build())
    if control:
        # Set the control bit, then replace the CRC for a valid control batch.
        data = bytearray(data)
        data[22] |= 0x20
        data[17:21] = calc_crc32c(data[21:]).to_bytes(4, "big")
        data = bytes(data)
    return data


async def fetch(consumer, data=b"", error_code=0, clock=None, before_response=None):
    request = FetchRequest_v4(-1, 100, 1, 10000, 0, [(TP.topic, [(0, 4, 10000)])])
    response = FetchResponse_v4(0, [(TP.topic, [(0, error_code, 12, 10, [], data)])])

    async def send(node, request):
        if clock is not None:
            clock[0] += 0.25
        if before_response is not None:
            before_response()
        return response

    consumer._client.send = mock.AsyncMock(side_effect=send)
    assignment = consumer._subscription.subscription.assignment
    return await consumer._fetcher._proc_fetch_request(assignment, 0, request)


def test_partial_collector_and_typo_warning():
    class LagCollector(ConsumerMetricsCollector):
        def on_records_lag(self, *, topic, partition, lag):
            self.lag = lag

    collector = LagCollector()
    collector.on_fetch_failed(topic="t", partition=0, exception=ValueError())
    collector.on_records_lag(topic="t", partition=0, lag=3)
    assert collector.lag == 3
    with pytest.warns(
        UserWarning, match="Unknown consumer metrics callbacks.*on_fecth"
    ):

        class TypoCollector(ConsumerMetricsCollector):
            def on_fecth(self):
                pass


@pytest.mark.parametrize("collector", [object(), ProducerMetricsCollector()])
async def test_invalid_collector(collector):
    with pytest.raises(TypeError, match="instance of ConsumerMetricsCollector"):
        AIOKafkaConsumer(metrics_collector=collector)


async def test_default_collector():
    consumer = AIOKafkaConsumer()
    try:
        assert type(consumer._metrics_collector) is ConsumerMetricsCollector
    finally:
        await consumer.stop()


async def test_wiring(consumer):
    assert consumer._fetcher._metrics_collector is consumer._metrics_collector
    assert consumer._coordinator._metrics_collector is consumer._metrics_collector


@pytest.mark.parametrize("magic", [0, 1, 2])
@pytest.mark.parametrize("compression", [0, 1])
async def test_fetch_and_poll_counts(consumer, clock, magic, compression):
    data = batch_data(magic, compression)
    await fetch(consumer, data, clock=clock)
    collector = consumer._metrics_collector
    collector.on_fetch_completed.assert_called_once_with(
        topic=TP.topic,
        partition=0,
        fetch_latency_seconds=0.25,
        record_count=2,
        bytes_fetched=len(data),
    )
    collector.on_records_lag.assert_called_once_with(topic=TP.topic, partition=0, lag=8)
    collector.on_records_consumed.assert_not_called()
    result = await consumer.getmany(max_records=1)
    assert len(result[TP]) == 1
    # Deserialization changes the value type, but bytes use serialized sizes.
    assert result[TP][0].value == {"value": b"value"}
    collector.on_records_consumed.assert_called_once_with(
        topic=TP.topic,
        partition=0,
        record_count=1,
        bytes_consumed=6,
    )
    collector.on_records_consumed.reset_mock()
    record = await consumer.__anext__()
    assert record.value == {"value": None}
    collector.on_records_consumed.assert_called_once_with(
        topic=TP.topic,
        partition=0,
        record_count=1,
        bytes_consumed=0,
    )
    collector.on_records_consumed.reset_mock()
    assert await consumer.getmany() == {}
    collector.on_records_consumed.assert_not_called()
    # Consumption does not emit a second fetch or lag event.
    assert collector.on_fetch_completed.call_count == 1
    assert collector.on_records_lag.call_count == 1


async def test_empty_fetch(consumer, clock):
    await fetch(consumer, clock=clock)
    consumer._metrics_collector.on_fetch_completed.assert_called_once_with(
        topic=TP.topic,
        partition=0,
        fetch_latency_seconds=0.25,
        record_count=0,
        bytes_fetched=0,
    )
    assert await consumer.getmany() == {}
    consumer._metrics_collector.on_records_consumed.assert_not_called()


@pytest.mark.parametrize("change", ["seek", "unassign"])
async def test_stale_fetch_is_ignored(consumer, change):
    def change_assignment():
        if change == "seek":
            consumer.seek(TP, 9)
        else:
            consumer.unsubscribe()

    await fetch(consumer, batch_data(), before_response=change_assignment)
    assert consumer._metrics_collector.mock_calls == []


async def test_paused_and_discarded_records_not_consumed(consumer):
    await fetch(consumer, batch_data())
    consumer.pause(TP)
    assert await consumer.getmany() == {}
    consumer.resume(TP)
    await fetch(consumer, batch_data())
    consumer.seek(TP, 9)
    assert await consumer.getmany() == {}
    consumer._metrics_collector.on_records_consumed.assert_not_called()


@pytest.mark.parametrize(
    "error", [TopicAuthorizationFailedError, OffsetOutOfRangeError]
)
async def test_terminal_fetch_failure_reported_once(consumer, error):
    await fetch(consumer, error_code=error.errno)
    collector = consumer._metrics_collector
    collector.on_fetch_completed.assert_not_called()
    collector.on_records_lag.assert_not_called()
    collector.on_fetch_failed.assert_called_once()
    failure = collector.on_fetch_failed.call_args.kwargs
    assert (failure["topic"], failure["partition"]) == TP
    assert isinstance(failure["exception"], error)
    with pytest.raises(error):
        await consumer.getmany()
    assert collector.on_fetch_failed.call_count == 1


async def test_recoverable_fetch_failure_not_reported(consumer):
    consumer._client.force_metadata_update = mock.Mock()
    await fetch(consumer, error_code=NotLeaderForPartitionError.errno)
    assert consumer._metrics_collector.mock_calls == []
    consumer._fetcher._default_reset_strategy = -2
    await fetch(consumer, error_code=OffsetOutOfRangeError.errno)
    assert consumer._metrics_collector.mock_calls == []


@pytest.mark.parametrize("error", [KafkaConnectionError(), asyncio.CancelledError()])
async def test_fetch_transport_retry_and_cancellation(consumer, error):
    consumer._client.send = mock.AsyncMock(side_effect=error)
    request = FetchRequest_v4(-1, 100, 1, 10000, 0, [(TP.topic, [(0, 4, 10000)])])
    assignment = consumer._subscription.subscription.assignment
    assert not await consumer._fetcher._proc_fetch_request(assignment, 0, request)
    assert consumer._metrics_collector.mock_calls == []


@pytest.mark.parametrize("poll", ["getone", "getmany"])
async def test_corrupt_record_failure(consumer, poll):
    data = bytearray(batch_data())
    data[-1] ^= 1
    await fetch(consumer, bytes(data))
    with pytest.raises(CorruptRecordException):
        await getattr(consumer, poll)()
    collector = consumer._metrics_collector
    collector.on_fetch_failed.assert_called_once()
    collector.on_records_consumed.assert_not_called()


async def test_fetch_metrics_exception_does_not_break_poll(consumer, caplog):
    collector = consumer._metrics_collector
    for name in ("on_fetch_completed", "on_records_lag", "on_records_consumed"):
        getattr(collector, name).side_effect = RuntimeError("collector failed")
    await fetch(consumer, batch_data())
    assert len((await consumer.getmany())[TP]) == 2
    assert caplog.text.count("Consumer metrics collector callback failed") == 3


async def test_no_counting_for_default_fetch_callback(consumer):
    collector = ConsumerMetricsCollector()
    consumer._fetcher._metrics_collector = collector
    with mock.patch("aiokafka.consumer.fetcher.MemoryRecords") as records:
        consumer._fetcher._record_fetch_metrics(TP, b"not a record set", 0.1)
    records.assert_not_called()


@pytest.mark.parametrize("kind", ["aborted", "control"])
async def test_filtered_batches_still_count_as_fetched(consumer, kind):
    data = batch_data(transactional=kind == "aborted", control=kind == "control")
    if kind == "aborted":
        consumer._fetcher._isolation_level = 1
        response = FetchResponse_v4(0, [(TP.topic, [(0, 0, 12, 10, [(12, 0)], data)])])
        consumer._client.send = mock.AsyncMock(return_value=response)
        request = FetchRequest_v4(-1, 100, 1, 10000, 1, [(TP.topic, [(0, 4, 10000)])])
        await consumer._fetcher._proc_fetch_request(
            consumer._subscription.subscription.assignment, 0, request
        )
    else:
        await fetch(consumer, data)
    assert (
        consumer._metrics_collector.on_fetch_completed.call_args.kwargs["record_count"]
        == 2
    )
    assert await consumer.getmany() == {}
    consumer._metrics_collector.on_records_consumed.assert_not_called()


async def commit_response(consumer, clock, codes=(0, 0), transport_error=None):
    async def send(request):
        clock[0] += 0.5
        if transport_error is not None:
            raise transport_error
        return OffsetCommitResponse_v2([(TP.topic, [(0, codes[0]), (1, codes[1])])])

    consumer._coordinator._send_req = mock.AsyncMock(side_effect=send)
    consumer._coordinator.ensure_coordinator_known = mock.AsyncMock()


async def test_commit_partial_success(consumer, clock):
    await commit_response(
        consumer, clock, codes=(0, TopicAuthorizationFailedError.errno)
    )
    with pytest.raises(TopicAuthorizationFailedError):
        await consumer.commit({TP: 5, OTHER_TP: 9})
    collector = consumer._metrics_collector
    collector.on_commit_completed.assert_called_once_with(
        topic=TP.topic,
        partition=0,
        offset=5,
        commit_latency_seconds=0.5,
    )
    collector.on_commit_failed.assert_called_once()
    failure = collector.on_commit_failed.call_args.kwargs
    assert failure["partition"] == 1
    assert failure["offset"] == 9
    assert failure["commit_latency_seconds"] == 0.5
    assert isinstance(failure["exception"], TopicAuthorizationFailedError)


async def test_commit_transport_retry(consumer, clock):
    await commit_response(consumer, clock)
    send = consumer._coordinator._send_req.side_effect
    attempts = 0

    async def retry(request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            clock[0] += 0.25
            raise KafkaConnectionError()
        return await send(request)

    consumer._coordinator._send_req.side_effect = retry
    await consumer.commit({TP: 5, OTHER_TP: 9})
    collector = consumer._metrics_collector
    assert collector.on_commit_failed.call_count == 2
    assert collector.on_commit_completed.call_count == 2
    assert all(
        call.kwargs["commit_latency_seconds"] == 0.25
        for call in collector.on_commit_failed.call_args_list
    )
    assert all(
        call.kwargs["commit_latency_seconds"] == 0.5
        for call in collector.on_commit_completed.call_args_list
    )


@pytest.mark.parametrize("mode", ["manual", "periodic", "final"])
async def test_commit_collector_exception_ignored(consumer, clock, mode, caplog):
    await commit_response(consumer, clock)
    collector = consumer._metrics_collector
    collector.on_commit_completed.side_effect = RuntimeError("collector failed")
    if mode != "manual":
        coordinator = consumer._coordinator
        coordinator._enable_auto_commit = True
        coordinator._next_autocommit_deadline = 0
        assignment = consumer._subscription.subscription.assignment
        if mode == "periodic":
            await coordinator._maybe_do_autocommit(assignment)
        else:
            await coordinator._maybe_do_last_autocommit(assignment)
    else:
        await consumer.commit({TP: 4, OTHER_TP: 4})
    assert collector.on_commit_completed.call_count == 2
    assert caplog.text.count("Consumer metrics collector callback failed") == 2


async def test_empty_and_cancelled_commits(consumer, clock):
    coordinator = consumer._coordinator
    await commit_response(consumer, clock, transport_error=asyncio.CancelledError())
    assignment = consumer._subscription.subscription.assignment
    await coordinator._do_commit_offsets(assignment, {})
    coordinator._send_req.assert_not_called()
    with pytest.raises(asyncio.CancelledError):
        await coordinator._do_commit_offsets(assignment, {TP: OffsetAndMetadata(5, "")})
    assert consumer._metrics_collector.mock_calls == []


async def test_failure_callback_exceptions_preserve_original_error(consumer, clock):
    collector = consumer._metrics_collector
    collector.on_fetch_failed.side_effect = RuntimeError("collector failed")
    collector.on_commit_failed.side_effect = RuntimeError("collector failed")
    await fetch(consumer, error_code=TopicAuthorizationFailedError.errno)
    with pytest.raises(TopicAuthorizationFailedError):
        await consumer.getone()
    await commit_response(
        consumer, clock, transport_error=TopicAuthorizationFailedError(TP.topic)
    )
    with pytest.raises(TopicAuthorizationFailedError):
        await consumer.commit({TP: 5, OTHER_TP: 9})
    assert collector.on_fetch_failed.call_count == 1
    assert collector.on_commit_failed.call_count == 2


async def test_truncated_record_set_does_not_complete_fetch(consumer):
    await fetch(consumer, b"partial")
    consumer._metrics_collector.on_fetch_completed.assert_not_called()
    consumer._metrics_collector.on_fetch_failed.assert_called_once()
    with pytest.raises(RecordTooLargeError):
        await consumer.getmany()


async def test_counting_error_does_not_break_fetch_task(consumer):
    # Enough bytes for a legacy message, but too few for a modern header.
    data = bytearray(26)
    data[8:12] = (14).to_bytes(4, "big")
    data[16] = 2
    assert await fetch(consumer, bytes(data))
    consumer._metrics_collector.on_fetch_completed.assert_not_called()
    consumer._metrics_collector.on_records_consumed.assert_not_called()
    with pytest.raises(CorruptRecordException, match="header is incomplete"):
        await consumer.getmany()
    consumer._metrics_collector.on_fetch_failed.assert_called_once()


async def test_poll_counts_each_partition(consumer):
    data = batch_data()
    response = FetchResponse_v4(
        0, [(TP.topic, [(p, 0, 12, 10, [], data) for p in (0, 1)])]
    )
    consumer._client.send = mock.AsyncMock(return_value=response)
    request = FetchRequest_v4(
        -1, 100, 1, 10000, 0, [(TP.topic, [(p, 4, 10000) for p in (0, 1)])]
    )
    await consumer._fetcher._proc_fetch_request(
        consumer._subscription.subscription.assignment, 0, request
    )
    result = await consumer.getmany()
    assert set(result) == {TP, OTHER_TP}
    consumer._metrics_collector.on_records_consumed.assert_has_calls(
        [
            mock.call(topic=TP.topic, partition=p, record_count=2, bytes_consumed=6)
            for p in (0, 1)
        ]
    )


async def test_partial_collector_skips_unused_consumption_count(consumer):
    class LagCollector(ConsumerMetricsCollector):
        def on_records_lag(self, *, topic, partition, lag):
            pass

    consumer._metrics_collector = LagCollector()
    # No attribute reads or iteration are needed for an unused callback.
    consumer._record_consumed_metrics(TP.topic, TP.partition, [object()])
