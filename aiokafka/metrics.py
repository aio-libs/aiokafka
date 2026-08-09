"""Experimental producer metrics collector API."""

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
