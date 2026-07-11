import asyncio

from prometheus_client import Counter, Histogram, start_http_server

from aiokafka import AIOKafkaProducer

queue_time = Histogram(
    "aiokafka_producer_record_queue_time_seconds",
    "Time records spent queued in the producer accumulator.",
    ["topic"],
)
request_latency = Histogram(
    "aiokafka_producer_request_latency_seconds",
    "Time between sending a produce request and receiving acknowledgment.",
    ["topic"],
)
batch_size = Histogram(
    "aiokafka_producer_batch_size_bytes",
    "Producer batch size in bytes.",
    ["topic"],
)
records_sent = Counter(
    "aiokafka_producer_records_sent_total",
    "Records acknowledged by the broker.",
    ["topic"],
)
batch_failures = Counter(
    "aiokafka_producer_batch_failures_total",
    "Producer batches that ultimately failed.",
    ["topic", "exception"],
)
buffer_wait = Counter(
    "aiokafka_producer_buffer_wait_seconds_total",
    "Total time spent waiting for producer accumulator space.",
    ["topic"],
)


class PrometheusProducerMetrics:
    def on_batch_drained(
        self, topic, queue_time_seconds, batch_size_bytes, record_count
    ):
        queue_time.labels(topic).observe(queue_time_seconds)
        batch_size.labels(topic).observe(batch_size_bytes)

    def on_batch_done(self, topic, request_latency_seconds, record_count):
        request_latency.labels(topic).observe(request_latency_seconds)
        records_sent.labels(topic).inc(record_count)

    def on_batch_failure(self, topic, exception, record_count):
        batch_failures.labels(topic, type(exception).__name__).inc()

    def on_buffer_wait(self, topic, wait_seconds):
        buffer_wait.labels(topic).inc(wait_seconds)


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
