.. _connection-lifecycle:

Connection lifecycle callbacks
==============================

:class:`~aiokafka.AIOKafkaConsumer`, :class:`~aiokafka.AIOKafkaProducer`, and
``AIOKafkaClient`` accept an optional ``listeners`` iterable. Subclass
:class:`~aiokafka.ConnectionListener` and override any of its no-op methods:

* ``on_connection_opened(connection)`` runs after a connection is established,
  including API version discovery and any TLS/SASL negotiation.
* ``on_connection_closed(connection, reason, error)`` reports an established
  connection closing. ``reason`` is a :class:`~aiokafka.ConnectionCloseReason`
  or ``None``; ``error`` is the underlying exception when available, otherwise
  ``None``.
* ``on_connection_failed(connection, error)`` reports a failed establishment
  attempt, including failures during bootstrap and background recovery.
  A cancelled attempt does not emit a failure notification. Request errors
  after establishment retain their existing handling; they do not emit this
  notification.

Each callback receives an immutable :class:`~aiokafka.ConnectionInfo` with
``client_id``, ``connection_id``, ``node_id``, ``host``, ``port``, and
``connection_group``. Bootstrap connections have ``node_id=None``, even if
they are retained for subsequent metadata requests. ``ConnectionGroup.DEFAULT``
identifies ordinary broker connections and ``ConnectionGroup.COORDINATION``
identifies coordination connections. Both enums and ``ConnectionInfo`` are
available directly from ``aiokafka`` and in ``aiokafka.structs``.
Low-level callers previously importing ``aiokafka.conn.CloseReason`` should
use ``aiokafka.structs.ConnectionCloseReason`` instead; the enum values are
unchanged.

A connection's opening and closing notifications share the same opaque
``connection_id``. Each new attempt, including retries and replacements, gets
a new identifier. Reusing a connection emits no additional opening event.
Failed establishment emits only a failed notification, even if a socket was
opened before negotiation failed.

These notifications describe individual connections. An opening notification
does not establish cluster-wide availability, completed group coordination, or
resumed message processing. Bootstrap sockets may close normally as soon as
metadata is obtained. A close with ``reason=None`` can represent this normal
cleanup. ``IDLE_DROP`` identifies idle removal, ``SHUTDOWN`` identifies deliberate
client shutdown, and ``CONNECTION_BROKEN`` or ``CONNECTION_TIMEOUT`` identify
transport failures or request timeouts. ``OUT_OF_SYNC`` reports a mismatched
request/response correlation ID. ``AUTH_FAILURE`` is retained for compatibility
with the low-level connection's closing reasons. Authentication fails before
establishment completes, so public observers receive it only through
``on_connection_failed(connection, error)``. Handle authentication alerts there;
``on_connection_closed`` never reports ``AUTH_FAILURE``.

For example, the same listener can be supplied to a consumer or producer:

.. code-block:: python

    import logging

    from aiokafka import (
        AIOKafkaProducer, ConnectionCloseReason, ConnectionInfo, ConnectionListener,
    )

    logger = logging.getLogger("kafka.connections")


    class LoggingListener(ConnectionListener):
        def on_connection_opened(self, connection: ConnectionInfo) -> None:
            logger.info("Opened Kafka connection %s to %s:%s",
                        connection.connection_id, connection.host, connection.port)

        async def on_connection_closed(
            self,
            connection: ConnectionInfo,
            reason: ConnectionCloseReason | None,
            error: BaseException | None,
        ) -> None:
            logger.info("Closed Kafka connection %s: %s (%r)",
                        connection.connection_id, reason, error)

        def on_connection_failed(
            self, connection: ConnectionInfo, error: Exception,
        ) -> None:
            logger.warning("Kafka connection attempt %s failed: %r",
                           connection.connection_id, error)


    async def send():
        producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9092",
            listeners=[LoggingListener()],
        )
        try:
            await producer.start()
            await producer.send_and_wait("events", b"hello")
        finally:
            await producer.stop()

Dispatch and shutdown
---------------------

Listener methods may be synchronous or return awaitables, including tasks.
Override only the methods you need. Unoverridden methods are skipped without
creating tasks or consuming observer capacity.
See :ref:`listeners` for the shared producer/consumer listener hierarchy.

Observers run in managed background tasks. Their exceptions are logged and do
not change connection management, retries, or request results. For each
connection, notifications run in order: all listeners' opening callbacks finish
before closing callbacks begin. Listeners run in registration order for each
event, and an exception in one does not skip the next. Different connections
have independent dispatch and no ordering guarantee. By the time a callback
runs, the connection may already have closed. Callback completion is not
required for ``start()`` or a request to return.

New observer work is admitted while fewer than **128 connections have pending
callbacks per client**. Each attempt emits at most an opening and a closing
event, or a single failure event. Once all opening callbacks finish, their task
and queue are released. Idle connections do not consume pending-work capacity.

When the registered listeners include both opening and closing overrides, an
accepted opening retains only its connection identifier to reserve delivery of
its closing event. A dropped opening also drops its closing event, even if capacity later
returns. Reserved closing events are always accepted and can temporarily take
pending work above 128. This additional work is limited to previously admitted
live connections; new openings and failures remain blocked at capacity.
Overload therefore does not split opening/closing pairs. Admission is shared
across all listeners on the client, not counted separately per listener. If
none overrides the opening method, admission is checked at closure.

A warning is logged on the first dropped notification in each overload episode.
Admission resumes when pending work falls below the limit; dropped events are
not replayed. Kafka requests and connection attempts do not wait for observers.
These events are best-effort observations and cannot provide an exact count of
all sockets. Shutdown cancellation can still discard an accepted closing event.

Keep callbacks short and avoid blocking the event loop. Defer longer work to
application tasks. Callbacks are never awaited by connection establishment,
including while its connection lock is held.

During ``AIOKafkaClient.close()``, connections close first, then pending
observers have up to **five seconds in total** to finish. Remaining observer
tasks are cancelled, and their queued notifications are discarded. Cancellation
while awaiting transport closure or draining observers skips the remaining
grace period and requests cancellation of pending observers. This grace period
is independent of Kafka's ``request_timeout_ms``.

The five-second limit applies only to observer draining. Producer ``stop()``
also waits for queued records and sender shutdown, and consumer ``stop()``
closes its coordinator and fetcher before reaching ``AIOKafkaClient.close()``.
Cancelling either ``stop()`` during those earlier stages can leave connections
and observers running. Calling ``stop()`` again returns immediately because
shutdown has already been marked as started. A timeout around ``stop()``
therefore does not guarantee connection or observer cleanup.

Cancellation is cooperative: callbacks must propagate cancellation promptly;
shutdown does not wait indefinitely for a callback that suppresses it. A
callback that stops its own client should directly await ``stop()`` or return
the task performing shutdown. These forms avoid waiting for the callback that
is awaiting shutdown. The exclusion applies to that connection's dispatch task;
its remaining listeners and queued closing callbacks finish after ``stop()``
returns and must also be kept short. A detached application task that stops the
client does not exempt the callback that spawned it: that callback is drained
or cancelled under the normal shutdown policy.
A callback that suppresses cancellation can outlive ``stop()`` and retains its
running task until it finishes. Stop the client before closing its event loop,
and ensure callbacks propagate cancellation after any short cleanup. Arbitrary
callback code cannot be forcibly terminated by asyncio. Closing a low-level
connection between runs of its owning loop queues the notification on that
loop; callbacks cannot be delivered after the loop has closed.

Connection events can arrive before ``start()`` completes. Listeners should
notify application code if shutdown is needed during startup. The application
must coordinate startup and shutdown: calling ``stop()`` concurrently with
``start()`` is not supported, including from an opening callback. These
notifications do not change the existing startup or bootstrap cleanup behavior.

With ``listeners=()`` (the default), no observer dispatcher or tasks are created.
