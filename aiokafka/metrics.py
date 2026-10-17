"""Experimental producer and consumer metrics collector APIs."""

import logging
import warnings

_CALLBACK_NAMES = frozenset(
    {
        "on_batch_completed",
        "on_batch_dispatched",
        "on_batch_failed",
        "on_buffer_wait",
    }
)


class ProducerMetricsCollector:
    """Receive experimental producer batch lifecycle events.

    All methods are synchronous and called from the producer hot path.
    Implementations should be fast and non-blocking. If asynchronous work is
    required, defer it via an internal queue.

    Exceptions raised by collectors are logged and ignored.

    Each callback has a no-op default implementation. Subclasses only need to
    override the callbacks they use. Unknown methods starting with ``on_``
    emit a warning when the subclass is defined to help detect callback name
    typos.

    All time-valued arguments are in seconds.

    .. warning::
        This API is experimental and may change without a deprecation period.
    """

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        unknown_callbacks = sorted(
            name
            for name in vars(cls)
            if name.startswith("on_") and name not in _CALLBACK_NAMES
        )
        if unknown_callbacks:
            warnings.warn(
                "Unknown producer metrics callbacks (possible typo): "
                f"{', '.join(unknown_callbacks)}",
                UserWarning,
                stacklevel=2,
            )

    def on_batch_dispatched(
        self,
        *,
        topic: str,
        partition: int,
        queue_time_seconds: float,
        batch_size_bytes: int,
        record_count: int,
        attempt: int,
    ) -> None:
        """Called each time a batch is handed to the sender.

        ``queue_time_seconds`` covers only the queue time for this attempt.
        ``attempt`` starts at 1. Batch size and record count are reported per
        attempt and can therefore be observed more than once for retried
        batches.
        """

    def on_batch_completed(
        self,
        *,
        topic: str,
        partition: int,
        send_to_completion_seconds: float,
        record_count: int,
        acknowledged: bool,
        batch_age_seconds: float,
    ) -> None:
        """Called when the producer successfully completes a batch.

        ``acknowledged`` is false when the producer is configured with
        ``acks=0``. ``batch_age_seconds`` is an upper bound for the latency of
        records in the batch and includes retries.
        """

    def on_batch_failed(
        self,
        *,
        topic: str,
        partition: int,
        exception: BaseException,
        record_count: int,
        batch_age_seconds: float,
    ) -> None:
        """Called when a batch ultimately fails.

        ``batch_age_seconds`` is an upper bound for the latency of records in
        the batch and includes retries.
        """

    def on_buffer_wait(
        self, *, topic: str, partition: int, wait_seconds: float
    ) -> None:
        """Called once when a send operation waited for accumulator space."""


_CONSUMER_CALLBACK_NAMES = frozenset(
    {
        "on_fetch_completed",
        "on_fetch_failed",
        "on_records_consumed",
        "on_records_lag",
        "on_commit_completed",
        "on_commit_failed",
    }
)


class ConsumerMetricsCollector:
    """Receive experimental consumer fetch, consumption, and commit events.

    Callbacks are synchronous and should be fast and non-blocking. Defer
    asynchronous work via an internal queue. Exceptions raised by collectors
    are logged and ignored. All durations are in seconds.

    Each callback has a no-op default. Subclasses need only override the
    callbacks they use. Unknown methods starting with ``on_`` emit a warning
    when the subclass is defined to help detect callback name typos.

    .. warning::
        This API is experimental and may change without a deprecation period.
    """

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        unknown_callbacks = sorted(
            name
            for name in vars(cls)
            if name.startswith("on_") and name not in _CONSUMER_CALLBACK_NAMES
        )
        if unknown_callbacks:
            warnings.warn(
                "Unknown consumer metrics callbacks (possible typo): "
                f"{', '.join(unknown_callbacks)}",
                UserWarning,
                stacklevel=2,
            )

    def on_fetch_completed(
        self,
        *,
        topic: str,
        partition: int,
        fetch_latency_seconds: float,
        record_count: int,
        bytes_fetched: int,
    ) -> None:
        """Called for each accepted successful partition fetch, even if empty.

        Latency measures the request round trip, including broker fetch wait.
        Bytes are the encoded record set size, including batch overhead.
        Record count includes control and aborted records and records before
        the requested offset. It need not equal the count delivered to the
        application. Discarded responses and record sets that cannot be counted
        are omitted.
        """

    def on_fetch_failed(
        self, *, topic: str, partition: int, exception: BaseException
    ) -> None:
        """Called when a partition fetch or record decoding error is surfaced.

        Internal retries, automatic offset resets, and cancellation are
        excluded. Only Kafka errors are reported from record decoding.
        """

    def on_records_consumed(
        self,
        *,
        topic: str,
        partition: int,
        record_count: int,
        bytes_consumed: int,
    ) -> None:
        """Called per partition when records are returned to the application.

        Bytes are serialized key and value sizes, excluding headers and record
        overhead. Null keys and values contribute zero bytes.
        """

    def on_records_lag(self, *, topic: str, partition: int, lag: int) -> None:
        """Called after a fetch updates the highwater: highwater minus position.

        Position is the next offset to consume, so buffered records contribute
        to lag. This is offset distance, not a count of deliverable records;
        it uses highwater even with ``read_committed`` isolation.
        """

    def on_commit_completed(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        commit_latency_seconds: float,
    ) -> None:
        """Called per successful partition in an offset-commit request attempt.

        Includes manual and automatic commits. Latency measures the request
        round trip, excluding coordinator discovery, lock wait, and retries.
        """

    def on_commit_failed(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        commit_latency_seconds: float,
        exception: BaseException,
    ) -> None:
        """Called per failed partition in an offset-commit request attempt.

        Retried attempts are reported individually. Transport errors are
        reported for every requested partition. Cancellation is excluded.
        """


def _call_consumer_metrics(callback, **kwargs):
    try:
        callback(**kwargs)
    except Exception:
        logging.getLogger(__name__).exception(
            "Consumer metrics collector callback failed"
        )
