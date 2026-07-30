import asyncio

from prometheus_client import Counter, Histogram, start_http_server

from aiokafka import AIOKafkaProducer, ProducerMetricsCollector

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


asyncio.run(produce())
