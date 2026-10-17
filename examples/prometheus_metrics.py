import argparse
import asyncio

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConsumerMetricsCollector,
    ProducerMetricsCollector,
)

queue_time = Histogram(
    "aiokafka_producer_batch_queue_time_seconds",
    "Time batches spent queued per dispatch attempt.",
    ["topic", "partition"],
)
send_to_completion = Histogram(
    "aiokafka_producer_send_to_completion_seconds",
    "Time from final batch dispatch to producer-side completion.",
    ["topic", "partition", "acknowledged"],
)
batch_age = Histogram(
    "aiokafka_producer_batch_age_seconds",
    "Time from batch creation to terminal outcome.",
    ["topic", "partition", "outcome"],
)
batch_size = Histogram(
    "aiokafka_producer_batch_size_bytes",
    "Producer batch size in bytes per dispatch attempt.",
    ["topic", "partition"],
)
batch_retries = Counter(
    "aiokafka_producer_batch_retries_total",
    "Producer batch retry attempts.",
    ["topic", "partition"],
)
records_completed = Counter(
    "aiokafka_producer_records_completed_total",
    "Records successfully completed by the producer.",
    ["topic", "partition", "acknowledged"],
)
batch_failures = Counter(
    "aiokafka_producer_batch_failures_total",
    "Producer batches that ultimately failed.",
    ["topic", "partition", "exception"],
)
buffer_wait = Counter(
    "aiokafka_producer_buffer_wait_seconds_total",
    "Total time spent waiting for producer accumulator space.",
    ["topic", "partition"],
)


class PrometheusProducerMetrics(ProducerMetricsCollector):
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
        queue_time.labels(topic, partition).observe(queue_time_seconds)
        batch_size.labels(topic, partition).observe(batch_size_bytes)
        if attempt > 1:
            batch_retries.labels(topic, partition).inc()

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
        acknowledged_label = str(acknowledged).lower()
        outcome = "acknowledged" if acknowledged else "unacknowledged"
        send_to_completion.labels(topic, partition, acknowledged_label).observe(
            send_to_completion_seconds
        )
        records_completed.labels(topic, partition, acknowledged_label).inc(record_count)
        batch_age.labels(topic, partition, outcome).observe(batch_age_seconds)

    def on_batch_failed(
        self,
        *,
        topic,
        partition,
        exception,
        record_count,
        batch_age_seconds,
    ):
        batch_failures.labels(topic, partition, type(exception).__name__).inc()
        batch_age.labels(topic, partition, "failed").observe(batch_age_seconds)

    def on_buffer_wait(self, *, topic, partition, wait_seconds):
        buffer_wait.labels(topic, partition).inc(wait_seconds)


async def produce():
    start_http_server(8000)
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        metrics_collector=PrometheusProducerMetrics(),
    )
    await producer.start()
    try:
        while True:
            await producer.send_and_wait("test-topic", b"Super message")
            await asyncio.sleep(1)
    finally:
        await producer.stop()


fetch_latency = Histogram(
    "aiokafka_consumer_fetch_latency_seconds",
    "Fetch request round trip per partition, including broker wait.",
    ["topic", "partition"],
)
records_fetched = Counter(
    "aiokafka_consumer_records_fetched_total",
    "Records fetched when the count is available in batch headers.",
    ["topic", "partition"],
)
fetched_bytes = Counter(
    "aiokafka_consumer_bytes_fetched_total",
    "Encoded record set bytes fetched, including repeated partial batches.",
    ["topic", "partition"],
)
fetch_failures = Counter(
    "aiokafka_consumer_fetch_failures_total",
    "Partition fetch errors surfaced to the application.",
    ["topic", "partition", "exception"],
)
records_consumed = Counter(
    "aiokafka_consumer_records_consumed_total",
    "Records handed to the application.",
    ["topic", "partition"],
)
consumed_bytes = Counter(
    "aiokafka_consumer_bytes_consumed_total",
    "Serialized key and value bytes handed to the application.",
    ["topic", "partition"],
)
records_lag = Gauge(
    "aiokafka_consumer_records_lag",
    "Highwater minus position at the latest fetch update, clamped to zero.",
    ["topic", "partition"],
)
commit_latency = Histogram(
    "aiokafka_consumer_commit_latency_seconds",
    "Offset commit request round trip per partition and attempt.",
    ["topic", "partition", "outcome"],
)
commit_failures = Counter(
    "aiokafka_consumer_commit_failures_total",
    "Failed partition offset commit attempts, including retries.",
    ["topic", "partition", "exception"],
)


class PrometheusConsumerMetrics(ConsumerMetricsCollector):
    def on_fetch_completed(
        self, *, topic, partition, fetch_latency_seconds, record_count, bytes_fetched
    ):
        fetch_latency.labels(topic, partition).observe(fetch_latency_seconds)
        if record_count is not None:
            records_fetched.labels(topic, partition).inc(record_count)
        fetched_bytes.labels(topic, partition).inc(bytes_fetched)

    def on_fetch_failed(self, *, topic, partition, exception):
        fetch_failures.labels(topic, partition, type(exception).__name__).inc()

    def on_records_consumed(self, *, topic, partition, record_count, bytes_consumed):
        records_consumed.labels(topic, partition).inc(record_count)
        consumed_bytes.labels(topic, partition).inc(bytes_consumed)

    def on_records_lag(self, *, topic, partition, lag):
        records_lag.labels(topic, partition).set(max(0, lag))

    def on_commit_completed(self, *, topic, partition, offset, commit_latency_seconds):
        commit_latency.labels(topic, partition, "completed").observe(
            commit_latency_seconds
        )

    def on_commit_failed(
        self, *, topic, partition, offset, commit_latency_seconds, exception
    ):
        commit_latency.labels(topic, partition, "failed").observe(
            commit_latency_seconds
        )
        commit_failures.labels(topic, partition, type(exception).__name__).inc()


async def consume():
    start_http_server(8000)
    async with AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers="localhost:9092",
        group_id="prometheus-example",
        auto_offset_reset="earliest",
        metrics_collector=PrometheusConsumerMetrics(),
    ) as consumer:
        while True:
            await consumer.getmany(timeout_ms=1000)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("producer", "consumer"), default="producer")
    args = parser.parse_args()
    asyncio.run(consume() if args.mode == "consumer" else produce())
