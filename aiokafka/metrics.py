import abc


class ProducerMetricsCollector(abc.ABC):
    """Receive producer batch lifecycle events.

    All methods are synchronous and called from the producer hot path.
    Implementations should be fast and non-blocking. If asynchronous work is
    required, defer it via an internal queue.

    Exceptions raised by collectors are logged and ignored.

    All time-valued arguments are in seconds.
    """

    @abc.abstractmethod
    def on_batch_drained(
        self,
        *,
        topic: str,
        partition: int,
        queue_time_seconds: float,
        batch_size_bytes: int,
        record_count: int,
    ) -> None:
        """Called when a batch leaves the accumulator and is handed to sender."""

    @abc.abstractmethod
    def on_batch_done(
        self,
        *,
        topic: str,
        partition: int,
        request_latency_seconds: float,
        record_count: int,
    ) -> None:
        """Called when the broker acknowledges a batch."""

    @abc.abstractmethod
    def on_batch_failure(
        self,
        *,
        topic: str,
        partition: int,
        exception: BaseException,
        record_count: int,
    ) -> None:
        """Called when a batch ultimately fails."""

    @abc.abstractmethod
    def on_buffer_wait(
        self, *, topic: str, partition: int, wait_seconds: float
    ) -> None:
        """Called when appending a record waited for accumulator space."""


class NullProducerMetricsCollector(ProducerMetricsCollector):
    """Default no-op producer metrics collector."""

    def on_batch_drained(
        self,
        *,
        topic: str,
        partition: int,
        queue_time_seconds: float,
        batch_size_bytes: int,
        record_count: int,
    ) -> None:
        pass

    def on_batch_done(
        self,
        *,
        topic: str,
        partition: int,
        request_latency_seconds: float,
        record_count: int,
    ) -> None:
        pass

    def on_batch_failure(
        self,
        *,
        topic: str,
        partition: int,
        exception: BaseException,
        record_count: int,
    ) -> None:
        pass

    def on_buffer_wait(
        self, *, topic: str, partition: int, wait_seconds: float
    ) -> None:
        pass
