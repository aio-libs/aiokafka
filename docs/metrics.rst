.. _metrics:

Producer metrics
================

``AIOKafkaProducer`` can emit lightweight producer batch lifecycle metrics via
the ``metrics_collector`` constructor argument. The collector is a synchronous
callback object; aiokafka does not aggregate, sample, export, or depend on a
metrics backend.

Implementations should keep callbacks fast and non-blocking. If you need to do
asynchronous work, push the event into an internal queue and process it from a
separate task. Exceptions raised by collectors are logged and ignored.

.. code:: python

    from aiokafka import AIOKafkaProducer


    class MetricsCollector:
        def on_batch_drained(
            self, topic, queue_time_seconds, batch_size_bytes, record_count
        ):
            ...

        def on_batch_done(self, topic, request_latency_seconds, record_count):
            ...

        def on_batch_failure(self, topic, exception, record_count):
            ...

        def on_buffer_wait(self, topic, wait_seconds):
            ...


    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        metrics_collector=MetricsCollector(),
    )

Callbacks
---------

``on_batch_drained(topic, queue_time_seconds, batch_size_bytes, record_count)``
    Called when a batch leaves the producer accumulator and is handed to the
    sender. ``queue_time_seconds`` measures time from batch creation to drain.

``on_batch_done(topic, request_latency_seconds, record_count)``
    Called when the broker acknowledges a batch. ``request_latency_seconds``
    measures time from drain to acknowledgment.

``on_batch_failure(topic, exception, record_count)``
    Called when a batch ultimately fails.

``on_buffer_wait(topic, wait_seconds)``
    Called when ``send()`` had to wait for accumulator space before appending a
    record.

All durations are reported in seconds. ``topic`` is always passed; collector
implementations decide whether to use it as a label.

Prometheus example
------------------

.. code:: python

    from prometheus_client import Counter, Histogram


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


API reference
-------------

.. autoclass:: aiokafka.metrics.ProducerMetricsCollector
    :members:
    :no-index:

.. autoclass:: aiokafka.metrics.NullProducerMetricsCollector
    :members:
    :no-index:
