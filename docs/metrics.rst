.. _metrics:

Producer and consumer metrics
=============================

.. warning::

    The metrics collector APIs are experimental and may change without a
    deprecation period.

``AIOKafkaProducer`` can emit lightweight producer batch lifecycle metrics via
the ``metrics_collector`` constructor argument. The collector is a synchronous
callback object derived from ``ProducerMetricsCollector``; aiokafka does not
aggregate, sample, export, or depend on a metrics backend.

Implementations should keep callbacks fast and non-blocking. If you need to do
asynchronous work, push the event into an internal queue and process it from a
separate task. Exceptions raised by collectors are logged and ignored.
Every callback has a no-op default, so implementations only need to override
the callbacks they use. Defining an unknown method whose name starts with
``on_`` emits a warning to help catch callback name typos.

.. code:: python

    from aiokafka import AIOKafkaProducer, ProducerMetricsCollector


    class CompletionMetrics(ProducerMetricsCollector):
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
            print(
                topic,
                partition,
                record_count,
                acknowledged,
                batch_age_seconds,
            )


    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        metrics_collector=CompletionMetrics(),
    )

Callbacks
---------

``on_batch_dispatched(*, topic, partition, queue_time_seconds, batch_size_bytes, record_count, attempt)``
    Called every time a batch leaves the accumulator and is handed to the
    sender. The first attempt is ``1``. Retried batches emit this callback
    again with an incremented attempt.

    ``queue_time_seconds`` covers only the time spent in the accumulator for
    this attempt. Retry backoff and earlier attempts are excluded.
    ``batch_size_bytes`` and ``record_count`` are therefore observed per
    attempt. Count successfully completed records in a terminal callback
    instead of this callback if retries must not be double-counted.

``on_batch_completed(*, topic, partition, send_to_completion_seconds, record_count, acknowledged, batch_age_seconds)``
    Called once when the producer successfully completes a batch.
    ``send_to_completion_seconds`` measures time from the final dispatch until
    producer-side completion.

    ``acknowledged`` is ``True`` when a broker acknowledgment was received.
    With ``acks=0`` it is ``False`` and completion means that the producer
    finished sending the request without waiting for a broker response.

    ``batch_age_seconds`` measures time from batch creation to completion and
    includes retries.

``on_batch_failed(*, topic, partition, exception, record_count, batch_age_seconds)``
    Called once when a batch ultimately fails. ``batch_age_seconds`` measures
    time from batch creation to failure and includes retries.

``on_buffer_wait(*, topic, partition, wait_seconds)``
    Called once after a ``send()`` or ``send_batch()`` operation that waited
    for accumulator space. ``wait_seconds`` is accumulated across all wait
    loops in that operation. It is also reported when the operation ultimately
    times out or otherwise fails after waiting.

Granularity
-----------

The dispatch, completion, and failure callbacks are batch-level events.
``on_buffer_wait`` is an operation-level event for a single ``send()`` or
``send_batch()`` call.

``batch_age_seconds`` is an upper bound for per-message latency because records
can be appended after their batch was created. The API does not track append
timestamps for individual records and therefore cannot provide exact
per-message end-to-end latency.

Empty batches emit no lifecycle metrics.

All durations are reported in seconds. ``topic`` and ``partition`` are always
passed; collector implementations decide whether to use them as labels.

Prometheus example
------------------

.. code:: python

    from aiokafka import ProducerMetricsCollector
    from prometheus_client import Counter, Histogram


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
            send_to_completion.labels(
                topic, partition, acknowledged_label
            ).observe(send_to_completion_seconds)
            records_completed.labels(
                topic, partition, acknowledged_label
            ).inc(record_count)
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
            batch_failures.labels(
                topic, partition, type(exception).__name__
            ).inc()
            batch_age.labels(topic, partition, "failed").observe(batch_age_seconds)

        def on_buffer_wait(self, *, topic, partition, wait_seconds):
            buffer_wait.labels(topic, partition).inc(wait_seconds)


API reference
-------------

.. autoclass:: aiokafka.metrics.ProducerMetricsCollector
    :members:
    :no-index:


Consumer metrics
----------------

``AIOKafkaConsumer`` accepts a ``metrics_collector`` derived from
``ConsumerMetricsCollector``. It follows the same synchronous, non-blocking,
no-op default and exception-handling conventions as the producer collector.
Callbacks use keyword-only arguments, and durations are in seconds. No metrics
backend is required. ``topic`` and ``partition`` are always supplied; choose
labels in your collector to control cardinality.

.. code:: python

    from aiokafka import AIOKafkaConsumer, ConsumerMetricsCollector


    class LagMetrics(ConsumerMetricsCollector):
        def on_records_lag(self, *, topic, partition, lag):
            print(topic, partition, lag)


    consumer = AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers="localhost:9092",
        group_id="example",
        metrics_collector=LagMetrics(),
    )

``on_fetch_completed(*, topic, partition, fetch_latency_seconds, record_count, bytes_fetched)``
    Called per accepted successful partition response, including empty responses.
    Latency measures the request round trip (including the broker's fetch wait),
    so partitions in one request share the same latency. Responses discarded
    after reassignment or a position change are excluded.

    ``bytes_fetched`` is the encoded record set size, including batch overhead
    and compression. ``record_count`` counts records in the returned batches,
    including aborted transactions, control records, and records before the
    requested offset. It does not use offset distance, which can overcount
    compacted records. Modern batch counts come from their headers without
    decompression; legacy compressed batches require decoding to count records.
    This counting is skipped when ``on_fetch_completed`` is not overridden.
    If counting fails, this callback is omitted; normal consumption still
    handles decoding errors. A successful fetch does not guarantee successful
    CRC validation or deserialization during a later poll.

``on_fetch_failed(*, topic, partition, exception)``
    Called when the fetcher queues a partition error for the application, or
    when polling encounters a Kafka error while decoding records. Internal
    retries, recoverable routing errors, automatic offset resets, and
    cancellation do not emit failures. The fetcher retries transport failures
    indefinitely, so those failures have no terminal callback. Each queued
    error is reported once; a subsequent fetch can fail again.

``on_records_consumed(*, topic, partition, record_count, bytes_consumed)``
    Called once per non-empty partition returned by ``getmany()``, or once for
    each record returned by ``getone()`` (including async iteration). Counts
    represent records handed to the application, not successfully processed
    or committed records. Paused, discarded, and buffered records are excluded.

    ``bytes_consumed`` sums serialized key and value sizes before user
    deserialization. Null keys and values contribute zero. Headers, record
    overhead, and compression are excluded, so these bytes are not directly
    comparable to ``bytes_fetched``.

``on_records_lag(*, topic, partition, lag)``
    Called after each accepted successful fetch updates the highwater, even
    for empty responses. Lag is ``highwater - position`` at that moment and
    includes buffered records. It is an offset distance, not an exact record
    count, and uses highwater even with ``read_committed`` isolation. Lag is
    not updated on each consumed record, seek, or commit. Collectors should
    expire labels for partitions no longer assigned to the consumer.

``on_commit_completed(*, topic, partition, offset, commit_latency_seconds)``
    Called per successfully committed partition in each offset-commit request
    attempt. ``offset`` is the committed next offset to consume. Latency
    measures the request round trip, excluding coordinator discovery, lock
    wait, retry backoff, and earlier attempts.

``on_commit_failed(*, topic, partition, offset, commit_latency_seconds, exception)``
    Called per failed partition in each offset-commit request attempt. Transport
    failures emit a failure for every requested partition. Mixed responses can
    emit both success and failure callbacks for different partitions.

    Both commit callbacks cover manual, periodic automatic, and final automatic
    commits. Retries are separate attempts: a partition successfully committed
    in a partially failed request can be reported again on retry. Empty commits,
    validation failures before a request, and cancelled requests emit no event.

See ``examples/prometheus_metrics.py`` for producer and consumer collectors
using Prometheus histograms, counters, and a lag gauge. Run it with
``--mode consumer`` to expose consumer metrics on port 8000.

.. autoclass:: aiokafka.metrics.ConsumerMetricsCollector
    :members:
    :no-index:
