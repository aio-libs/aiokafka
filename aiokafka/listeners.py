"""Experimental listeners for connection and producer events."""

from collections.abc import Awaitable, Callable, Iterable
from typing import Any, TypeVar

from aiokafka.structs import ConnectionCloseReason, ConnectionInfo

__all__ = ["ConnectionListener", "ProducerListener"]

_ListenerT = TypeVar("_ListenerT")


class ConnectionListener:
    """Observe Kafka connections through optional method overrides.

    Each method has a no-op default. Overrides may be synchronous or return
    an awaitable. Methods run in background tasks, in order per connection.
    Exceptions are logged and do not affect Kafka requests or recovery.
    This API is experimental and may change without a deprecation period.
    See :ref:`connection-lifecycle` for capacity and shutdown semantics.
    """

    def on_connection_opened(
        self, connection: ConnectionInfo
    ) -> Awaitable[None] | None:
        """Called after connection establishment, including TLS/SASL negotiation."""

    def on_connection_closed(
        self,
        connection: ConnectionInfo,
        reason: ConnectionCloseReason | None,
        error: BaseException | None,
    ) -> Awaitable[None] | None:
        """Called when an established connection closes."""

    def on_connection_failed(
        self, connection: ConnectionInfo, error: Exception
    ) -> Awaitable[None] | None:
        """Called when connection establishment fails."""


class ProducerListener(ConnectionListener):
    """Receive connection and producer batch events through optional overrides.

    Batch callbacks are synchronous and run on the producer hot path. Keep them
    fast and non-blocking; defer asynchronous work to an application queue.
    Connection callbacks use the background dispatch described on the base
    class. Callback exceptions are logged and ignored. All durations are seconds.

    This API is experimental and may change without a deprecation period.
    """

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


def _validate_listeners(
    listeners: Iterable[_ListenerT], base: type[_ListenerT]
) -> tuple[_ListenerT, ...]:
    """Snapshot registration and reject duplicate instances to avoid double delivery."""
    result = tuple(listeners)
    if any(not isinstance(listener, base) for listener in result):
        raise TypeError(f"listeners must contain {base.__name__} instances")
    if len({id(listener) for listener in result}) != len(result):
        raise ValueError("listeners must not contain the same instance more than once")
    return result


def _collect_callbacks(
    listeners: Iterable[object], base: type[object], names: Iterable[str]
) -> dict[str, tuple[Callable[..., Any], ...]]:
    """Validate overrides once, leaving no default-method calls on the hot path."""
    listeners = tuple(listeners)
    callbacks = {}
    for name in names:
        overridden = []
        for listener in listeners:
            if not isinstance(listener, base):
                continue
            callback = getattr(listener, name)
            if not callable(callback):
                raise TypeError(f"{name} must be callable")
            if getattr(callback, "__func__", None) is not getattr(base, name):
                overridden.append(callback)
        if overridden:
            callbacks[name] = tuple(overridden)
    return callbacks
