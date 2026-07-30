import asyncio
import logging
from unittest import mock

import pytest

from aiokafka import (
    AIOKafkaProducer,
    NullProducerMetricsCollector,
    ProducerMetricsCollector,
)
from aiokafka.cluster import ClusterMetadata
from aiokafka.errors import KafkaTimeoutError, NotLeaderForPartitionError
from aiokafka.producer.message_accumulator import MessageAccumulator
from aiokafka.producer.sender import SendProduceReqHandler
from aiokafka.structs import TopicPartition


class RecordingMetricsCollector(ProducerMetricsCollector):
    def __init__(self):
        self.events = []

    def on_batch_dispatched(
        self,
        *,
        topic,
        partition,
        queue_time_seconds,
        batch_size_bytes,
        record_count,
        attempt,
    ):
        self.events.append(
            (
                "batch_dispatched",
                topic,
                partition,
                queue_time_seconds,
                batch_size_bytes,
                record_count,
                attempt,
            )
        )

    def on_batch_completed(
        self,
        *,
        topic,
        partition,
        send_to_completion_seconds,
        record_count,
        acknowledged,
        batch_age_seconds,
    ):
        self.events.append(
            (
                "batch_completed",
                topic,
                partition,
                send_to_completion_seconds,
                record_count,
                acknowledged,
                batch_age_seconds,
            )
        )

    def on_batch_failed(
        self, *, topic, partition, exception, record_count, batch_age_seconds
    ):
        self.events.append(
            (
                "batch_failed",
                topic,
                partition,
                exception,
                record_count,
                batch_age_seconds,
            )
        )

    def on_buffer_wait(self, *, topic, partition, wait_seconds):
        self.events.append(("buffer_wait", topic, partition, wait_seconds))


class RaisingMetricsCollector(RecordingMetricsCollector):
    def on_batch_dispatched(
        self,
        *,
        topic,
        partition,
        queue_time_seconds,
        batch_size_bytes,
        record_count,
        attempt,
    ):
        raise RuntimeError("collector failed")


def make_cluster():
    cluster = ClusterMetadata(metadata_max_age_ms=10000)
    cluster.leader_for_partition = mock.Mock(return_value=0)
    return cluster


def make_cluster_without_leader():
    cluster = ClusterMetadata(metadata_max_age_ms=10000)
    cluster.leader_for_partition = mock.Mock(return_value=None)
    return cluster


@pytest.mark.asyncio
async def test_metrics_collector_records_batch_lifecycle():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    clock = [10.0]

    with mock.patch(
        "aiokafka.producer.message_accumulator.time.monotonic",
        side_effect=lambda: clock[0],
    ):
        accumulator = MessageAccumulator(
            make_cluster(),
            batch_size=1000,
            compression_type=0,
            batch_ttl=30,
            metrics_collector=collector,
        )

        future = await accumulator.add_message(
            tp, b"key", b"value", timeout=2, timestamp_ms=1
        )
        clock[0] = 10.75
        batches, unknown_leaders_exist = accumulator.drain_by_nodes(ignore_nodes=[])
        assert not unknown_leaders_exist

        batch = batches[0][tp]
        clock[0] = 11.0
        batch.done(base_offset=10)

    metadata = await future
    assert metadata.topic == "test-topic"
    assert collector.events[0][:4] == ("batch_dispatched", "test-topic", 0, 0.75)
    assert collector.events[0][4] > 0
    assert collector.events[0][5] == 1
    assert collector.events[0][6] == 1
    assert collector.events[1] == (
        "batch_completed",
        "test-topic",
        0,
        0.25,
        1,
        True,
        1.0,
    )


@pytest.mark.asyncio
async def test_metrics_collector_records_each_dispatch_attempt():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    clock = [10.0]

    with mock.patch(
        "aiokafka.producer.message_accumulator.time.monotonic",
        side_effect=lambda: clock[0],
    ):
        accumulator = MessageAccumulator(
            make_cluster(),
            batch_size=1000,
            compression_type=0,
            batch_ttl=30,
            metrics_collector=collector,
        )

        future = await accumulator.add_message(tp, None, b"value", timeout=2)

        clock[0] = 10.5
        batches, _ = accumulator.drain_by_nodes(ignore_nodes=[])
        batch = batches[0][tp]

        clock[0] = 11.0
        accumulator.reenqueue(batch)

        clock[0] = 11.25
        batches, _ = accumulator.drain_by_nodes(ignore_nodes=[])
        assert batches[0][tp] is batch

        clock[0] = 12.0
        batch.done(base_offset=10)

    await future
    dispatched_events = [
        event for event in collector.events if event[0] == "batch_dispatched"
    ]
    assert len(dispatched_events) == 2
    assert dispatched_events[0][3] == 0.5
    assert dispatched_events[0][6] == 1
    assert dispatched_events[1][3] == 0.25
    assert dispatched_events[1][6] == 2
    assert collector.events[-1] == (
        "batch_completed",
        "test-topic",
        0,
        0.75,
        1,
        True,
        2.0,
    )


@pytest.mark.asyncio
async def test_metrics_collector_records_unacknowledged_completion():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    clock = [20.0]

    with mock.patch(
        "aiokafka.producer.message_accumulator.time.monotonic",
        side_effect=lambda: clock[0],
    ):
        accumulator = MessageAccumulator(
            make_cluster(),
            batch_size=1000,
            compression_type=0,
            batch_ttl=30,
            metrics_collector=collector,
        )

        future = await accumulator.add_message(tp, None, b"value", timeout=2)
        clock[0] = 20.5
        batches, _ = accumulator.drain_by_nodes(ignore_nodes=[])
        clock[0] = 20.75
        batches[0][tp].done_noack()

    assert await future is None
    assert collector.events[-1] == (
        "batch_completed",
        "test-topic",
        0,
        0.25,
        1,
        False,
        0.75,
    )


@pytest.mark.asyncio
async def test_metrics_collector_records_acks_zero_completion():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    producer = AIOKafkaProducer(acks=0, metrics_collector=collector)
    producer._metadata.leader_for_partition = mock.Mock(return_value=0)
    producer.client.send = mock.AsyncMock(return_value=None)

    try:
        future = await producer._message_accumulator.add_message(
            tp, None, b"value", timeout=2
        )
        batches, _ = producer._message_accumulator.drain_by_nodes(ignore_nodes=[])
        handler = SendProduceReqHandler(producer._sender, batches[0])

        await handler.do(0)

        assert await future is None
        completed_events = [
            event for event in collector.events if event[0] == "batch_completed"
        ]
        assert len(completed_events) == 1
        assert completed_events[0][5] is False
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_metrics_collector_records_buffer_wait():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=90,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )

    await accumulator.add_message(tp, None, b"hello", timeout=2)
    await accumulator.add_message(tp, None, b"hello", timeout=2)

    add_task = asyncio.create_task(
        accumulator.add_message(tp, None, b"hello", timeout=2)
    )
    await asyncio.sleep(0.02)
    assert not add_task.done()

    accumulator.drain_by_nodes(ignore_nodes=[])
    await add_task

    buffer_wait_events = [
        event for event in collector.events if event[0] == "buffer_wait"
    ]
    assert len(buffer_wait_events) == 1
    assert buffer_wait_events[0][1] == "test-topic"
    assert buffer_wait_events[0][2] == 0
    assert buffer_wait_events[0][3] > 0


@pytest.mark.asyncio
async def test_metrics_collector_records_add_batch_buffer_wait():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )

    await accumulator.add_message(tp, None, b"value", timeout=2)
    add_task = asyncio.create_task(
        accumulator.add_batch(accumulator.create_builder(), tp, timeout=2)
    )
    await asyncio.sleep(0.02)
    assert not add_task.done()

    batches, _ = accumulator.drain_by_nodes(ignore_nodes=[])
    batches[0][tp].done_noack()
    batch_future = await add_task

    batches, _ = accumulator.drain_by_nodes(ignore_nodes=[])
    assert batches == {}
    assert await batch_future is None

    buffer_wait_events = [
        event for event in collector.events if event[0] == "buffer_wait"
    ]
    assert len(buffer_wait_events) == 1
    assert buffer_wait_events[0][1:3] == ("test-topic", 0)
    assert buffer_wait_events[0][3] > 0


@pytest.mark.asyncio
async def test_message_accumulator_uses_null_metrics_collector_by_default():
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
    )

    assert isinstance(accumulator._metrics_collector, NullProducerMetricsCollector)


@pytest.mark.asyncio
async def test_metrics_collector_exceptions_are_logged_and_ignored(caplog):
    tp = TopicPartition("test-topic", 0)
    collector = RaisingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )
    caplog.set_level(logging.ERROR, logger="aiokafka.producer.message_accumulator")

    future = await accumulator.add_message(tp, None, b"value", timeout=2)
    batches, unknown_leaders_exist = accumulator.drain_by_nodes(ignore_nodes=[])
    batch = batches[0][tp]

    assert not unknown_leaders_exist
    assert not future.done()
    assert "Producer metrics collector callback failed" in caplog.text

    batch.done(base_offset=10)
    metadata = await future
    assert metadata.topic == "test-topic"


@pytest.mark.asyncio
async def test_metrics_not_reported_after_batch_already_completed():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )

    future = await accumulator.add_message(tp, None, b"value", timeout=2)
    batches, _ = accumulator.drain_by_nodes(ignore_nodes=[])
    batch = batches[0][tp]

    batch.drain_ready()
    batch.done_noack()
    batch.done_noack()
    batch.done(base_offset=10)
    batch.failure(RuntimeError("late failure"))

    assert await future is None
    assert [event[0] for event in collector.events] == [
        "batch_dispatched",
        "batch_completed",
    ]
    assert collector.events[1][5] is False


@pytest.mark.asyncio
async def test_metrics_not_dispatched_for_expired_batch_without_leader():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster_without_leader(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=-1,
        metrics_collector=collector,
    )

    future = await accumulator.add_message(tp, None, b"value", timeout=2)
    batches, unknown_leaders_exist = accumulator.drain_by_nodes(ignore_nodes=[])

    assert batches == {}
    assert unknown_leaders_exist
    assert future.done()
    future_exception = future.exception()
    assert isinstance(future_exception, NotLeaderForPartitionError)
    assert [event[0] for event in collector.events] == ["batch_failed"]
    assert collector.events[0][5] >= 0


@pytest.mark.asyncio
async def test_metrics_not_dispatched_for_empty_batch():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )

    future = await accumulator.add_batch(accumulator.create_builder(), tp, timeout=2)
    batches, unknown_leaders_exist = accumulator.drain_by_nodes(ignore_nodes=[])

    assert batches == {}
    assert not unknown_leaders_exist
    assert await future is None
    assert collector.events == []


@pytest.mark.asyncio
async def test_metrics_collector_records_batch_failure_before_drain():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )

    future = await accumulator.add_message(tp, None, b"value", timeout=2)
    exc = RuntimeError("sender failed")
    accumulator.fail_all(exc)

    with pytest.raises(RuntimeError, match="sender failed"):
        await future
    failure_events = [event for event in collector.events if event[0] == "batch_failed"]
    assert len(failure_events) == 1
    assert failure_events[0][:5] == ("batch_failed", "test-topic", 0, exc, 1)
    assert failure_events[0][5] >= 0


@pytest.mark.asyncio
async def test_metrics_collector_records_buffer_wait_on_timeout():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=90,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )

    await accumulator.add_message(tp, None, b"hello", timeout=2)
    await accumulator.add_message(tp, None, b"hello", timeout=2)

    with pytest.raises(KafkaTimeoutError):
        await accumulator.add_message(tp, None, b"hello", timeout=0.001)

    buffer_wait_events = [
        event for event in collector.events if event[0] == "buffer_wait"
    ]
    assert len(buffer_wait_events) == 1
    assert buffer_wait_events[0][1] == "test-topic"
    assert buffer_wait_events[0][2] == 0
    assert buffer_wait_events[0][3] > 0


@pytest.mark.asyncio
async def test_metrics_collector_records_batch_failure():
    tp = TopicPartition("test-topic", 0)
    collector = RecordingMetricsCollector()
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
        metrics_collector=collector,
    )

    future = await accumulator.add_message(tp, None, b"value", timeout=2)
    batches, _ = accumulator.drain_by_nodes(ignore_nodes=[])

    exc = RuntimeError("delivery failed")
    batches[0][tp].failure(exc)

    assert future.done()
    future_exception = future.exception()
    assert isinstance(future_exception, RuntimeError)
    assert str(future_exception) == "delivery failed"
    failure_events = [event for event in collector.events if event[0] == "batch_failed"]
    assert len(failure_events) == 1
    assert failure_events[0][:5] == ("batch_failed", "test-topic", 0, exc, 1)
    assert failure_events[0][5] >= 0


@pytest.mark.asyncio
async def test_producer_passes_metrics_collector_to_accumulator():
    collector = RecordingMetricsCollector()
    producer = AIOKafkaProducer(metrics_collector=collector)
    try:
        assert producer._metrics_collector is collector
        assert producer._message_accumulator._metrics_collector is collector
    finally:
        await producer.stop()


@pytest.mark.asyncio
async def test_producer_rejects_invalid_metrics_collector():
    with pytest.raises(
        TypeError,
        match="metrics_collector must be an instance of ProducerMetricsCollector",
    ):
        AIOKafkaProducer(metrics_collector=object())


def test_null_metrics_collector_is_abc_implementation():
    collector = NullProducerMetricsCollector()

    assert isinstance(collector, ProducerMetricsCollector)
    collector.on_batch_dispatched(
        topic="topic",
        partition=0,
        queue_time_seconds=0.1,
        batch_size_bytes=1,
        record_count=1,
        attempt=1,
    )
    collector.on_batch_completed(
        topic="topic",
        partition=0,
        send_to_completion_seconds=0.2,
        record_count=1,
        acknowledged=True,
        batch_age_seconds=0.3,
    )
    collector.on_batch_failed(
        topic="topic",
        partition=0,
        exception=RuntimeError("boom"),
        record_count=1,
        batch_age_seconds=0.4,
    )
    collector.on_buffer_wait(topic="topic", partition=0, wait_seconds=0.3)


def test_producer_metrics_collector_is_abstract():
    with pytest.raises(TypeError, match="abstract class"):
        ProducerMetricsCollector()
