import asyncio
import logging
from unittest import mock

import pytest

from aiokafka import AIOKafkaProducer, NullMetricsCollector, ProducerMetricsCollector
from aiokafka.cluster import ClusterMetadata
from aiokafka.errors import KafkaTimeoutError
from aiokafka.producer.message_accumulator import MessageAccumulator
from aiokafka.structs import TopicPartition


class RecordingMetricsCollector:
    def __init__(self):
        self.events = []

    def on_batch_drained(
        self, topic, queue_time_seconds, batch_size_bytes, record_count
    ):
        self.events.append(
            (
                "batch_drained",
                topic,
                queue_time_seconds,
                batch_size_bytes,
                record_count,
            )
        )

    def on_batch_done(self, topic, request_latency_seconds, record_count):
        self.events.append(("batch_done", topic, request_latency_seconds, record_count))

    def on_batch_failure(self, topic, exception, record_count):
        self.events.append(("batch_failure", topic, exception, record_count))

    def on_buffer_wait(self, topic, wait_seconds):
        self.events.append(("buffer_wait", topic, wait_seconds))


class RaisingMetricsCollector(RecordingMetricsCollector):
    def on_batch_drained(
        self, topic, queue_time_seconds, batch_size_bytes, record_count
    ):
        raise RuntimeError("collector failed")


def make_cluster():
    cluster = ClusterMetadata(metadata_max_age_ms=10000)
    cluster.leader_for_partition = mock.Mock(return_value=0)
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
    assert collector.events[0][:3] == ("batch_drained", "test-topic", 0.75)
    assert collector.events[0][3] > 0
    assert collector.events[0][4] == 1
    assert collector.events[1] == ("batch_done", "test-topic", 0.25, 1)


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
    await asyncio.sleep(0)
    assert not add_task.done()

    accumulator.drain_by_nodes(ignore_nodes=[])
    await add_task

    buffer_wait_events = [
        event for event in collector.events if event[0] == "buffer_wait"
    ]
    assert len(buffer_wait_events) == 1
    assert buffer_wait_events[0][1] == "test-topic"
    assert buffer_wait_events[0][2] >= 0


@pytest.mark.asyncio
async def test_message_accumulator_uses_null_metrics_collector_by_default():
    accumulator = MessageAccumulator(
        make_cluster(),
        batch_size=1000,
        compression_type=0,
        batch_ttl=30,
    )

    assert isinstance(accumulator._metrics_collector, NullMetricsCollector)


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
        "batch_drained",
        "batch_drained",
    ]


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
    assert ("batch_failure", "test-topic", exc, 1) in collector.events


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
    assert buffer_wait_events
    assert all(event[1] == "test-topic" for event in buffer_wait_events)
    assert all(event[2] >= 0 for event in buffer_wait_events)


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
    assert ("batch_failure", "test-topic", exc, 1) in collector.events


@pytest.mark.asyncio
async def test_producer_passes_metrics_collector_to_accumulator():
    collector = RecordingMetricsCollector()
    producer = AIOKafkaProducer(metrics_collector=collector)
    try:
        assert producer._metrics_collector is collector
        assert producer._message_accumulator._metrics_collector is collector
    finally:
        await producer.stop()


def test_null_metrics_collector_is_protocol_implementation():
    collector = NullMetricsCollector()

    assert isinstance(collector, ProducerMetricsCollector)
    collector.on_batch_drained("topic", 0.1, 1, 1)
    collector.on_batch_done("topic", 0.2, 1)
    collector.on_batch_failure("topic", RuntimeError("boom"), 1)
    collector.on_buffer_wait("topic", 0.3)
