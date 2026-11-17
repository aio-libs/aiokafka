"""Managed background dispatch for connection listener events."""

import asyncio
import inspect
import logging
from collections import deque
from collections.abc import Iterable
from functools import partial
from typing import Any

from aiokafka.listeners import (
    ConnectionListener,
    _collect_callbacks,
)
from aiokafka.structs import ConnectionInfo

log = logging.getLogger(__name__)


class _ConnectionCallbacks:
    # One deadline for all observers, independent of request_timeout_ms.
    _SHUTDOWN_TIMEOUT = 5.0
    # Limit admission of new work. Previously admitted closes retain delivery.
    _MAX_PENDING_CONNECTIONS = 128

    def __init__(
        self,
        listeners: Iterable[ConnectionListener],
        *,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        callbacks = _collect_callbacks(
            listeners,
            ConnectionListener,
            ("on_connection_opened", "on_connection_closed", "on_connection_failed"),
        )
        self._callbacks = {
            name.removeprefix("on_connection_"): methods
            for name, methods in callbacks.items()
        }
        self._loop = loop if loop is not None else asyncio.get_running_loop()
        self._queues: dict[str, deque[tuple[str, tuple[Any, ...]]]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._awaited_tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False
        # Only a connection id is retained once its opening callback finishes.
        self._observed: set[str] = set()
        self._overflow_warned = False

    def emit(self, event: str, info: ConnectionInfo, *args: Any) -> None:
        if self._closed or event not in self._callbacks:
            return
        if self._loop.is_closed():
            log.warning(
                "Dropping on_connection_%s callback: event loop is closed", event
            )
            return
        connection_id = info.connection_id
        admitted_close = event == "closed" and connection_id in self._observed
        if event == "closed":
            if "opened" in self._callbacks and not admitted_close:
                return
            self._observed.discard(connection_id)
        if connection_id not in self._queues:
            at_capacity = len(self._tasks) >= self._MAX_PENDING_CONNECTIONS
            if at_capacity and not admitted_close:
                if not self._overflow_warned:
                    self._overflow_warned = True
                    log.warning(
                        "Connection observer limit reached (%s pending connections); "
                        "dropping notifications for other connections",
                        self._MAX_PENDING_CONNECTIONS,
                    )
                return
            if not at_capacity:
                self._overflow_warned = False
            self._queues[connection_id] = deque()
        if event == "opened" and "closed" in self._callbacks:
            self._observed.add(connection_id)
        self._queues[connection_id].append((event, (info, *args)))
        if connection_id not in self._tasks:
            task = self._loop.create_task(self._run(connection_id))
            self._tasks[connection_id] = task
            task.add_done_callback(partial(self._discard, connection_id))

    async def _run(self, connection_id: str) -> None:
        try:
            # Also defer synchronous callbacks with an eager task factory.
            await asyncio.sleep(0)
            queue = self._queues[connection_id]
            while queue:
                event, args = queue.popleft()
                for callback in self._callbacks[event]:
                    try:
                        result = callback(*args)
                        if inspect.isawaitable(result):
                            if isinstance(result, asyncio.Task):
                                self._awaited_tasks[connection_id] = result
                            try:
                                await result
                            finally:
                                self._awaited_tasks.pop(connection_id, None)
                    except asyncio.CancelledError:
                        task = asyncio.current_task()
                        if task is not None and task.cancelling():
                            raise
                        log.exception("Error in on_connection_%s callback", event)
                    except Exception:
                        log.exception("Error in on_connection_%s callback", event)
        finally:
            self._discard(connection_id, asyncio.current_task())

    def _discard(self, connection_id: str, task: asyncio.Task[None] | None) -> None:
        # The done callback also covers cancellation before _run starts. A new
        # event may already have installed a different task for this connection.
        if self._tasks.get(connection_id) is task:
            self._tasks.pop(connection_id, None)
            self._queues.pop(connection_id, None)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._observed.clear()
        # Exclude direct self-stop and callbacks awaiting this shutdown task.
        # Detached tasks spawned by a callback do not exempt their creator.
        current = asyncio.current_task()
        pending = {task for task in self._tasks.values() if task is not current}
        if not pending:
            return
        try:
            # close() may be entered from a finally block after its caller has
            # already received CancelledError while closing transports.
            if current is None or not current.cancelling():
                # An eager shutdown task may run before the callback returns it.
                # Give the dispatcher a turn to record that await relationship.
                await asyncio.sleep(0)
                pending.difference_update(
                    self._tasks[connection_id]
                    for connection_id, awaited in self._awaited_tasks.items()
                    if awaited is current
                )
                if pending:
                    _, pending = await asyncio.wait(
                        pending, timeout=self._SHUTDOWN_TIMEOUT
                    )
        finally:
            if pending:
                for connection_id, task in self._tasks.items():
                    if task in pending:
                        self._queues[connection_id].clear()
                for task in pending:
                    task.cancel()
                # Allow cooperative cleanup without waiting indefinitely for
                # a callback that suppresses cancellation.
                await asyncio.sleep(0)
                for connection_id, task in tuple(self._tasks.items()):
                    if task.done():
                        self._discard(connection_id, task)
