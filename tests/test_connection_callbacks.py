import asyncio
import struct
import weakref
from dataclasses import FrozenInstanceError
from unittest import mock

import pytest

from aiokafka import (
    AIOKafkaClient,
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConnectionCloseReason,
    ConnectionGroup,
    ConnectionInfo,
    ConnectionListener,
    ProducerListener,
)
from aiokafka.conn import AIOKafkaConnection
from aiokafka.errors import (
    CorrelationIdError,
    KafkaConnectionError,
    RequestTimedOutError,
)
from aiokafka.lifecycle import _ConnectionCallbacks
from aiokafka.protocol.metadata import MetadataRequest, MetadataResponse_v0


def listener_mock(**callbacks):
    listener = mock.Mock(spec=ConnectionListener)
    for event, callback in callbacks.items():
        setattr(listener, f"on_connection_{event}", callback)
    return listener


@pytest.fixture
async def transport_factory(monkeypatch):
    """Exercise real connection setup and teardown without a Kafka broker."""

    async def connect(factory, host, port, **kwargs):
        protocol = factory()
        transport = mock.Mock(spec=asyncio.Transport)
        transport.get_extra_info.return_value = None
        transport.is_closing.return_value = False

        def close():
            if not transport.is_closing():
                transport.is_closing.return_value = True
                protocol.connection_lost(None)

        transport.close.side_effect = close
        protocol.connection_made(transport)
        return transport, protocol

    factory = mock.AsyncMock(side_effect=connect)
    monkeypatch.setattr(asyncio.get_running_loop(), "create_connection", factory)
    monkeypatch.setattr(AIOKafkaConnection, "_do_version_lookup", mock.AsyncMock())
    monkeypatch.setattr(AIOKafkaConnection, "_do_sasl_handshake", mock.AsyncMock())
    return factory


@pytest.fixture
async def client(transport_factory):
    client = AIOKafkaClient(
        client_id="observer-test",
        listeners=[
            listener_mock(opened=mock.Mock(), closed=mock.Mock(), failed=mock.Mock())
        ],
    )
    client.cluster.update_metadata(
        MetadataResponse_v0([(1, "broker", 9092), (2, "other", 9093)], [])
    )
    try:
        yield client
    finally:
        await client.close()


async def drain(client):
    tasks = tuple(client._connection_callbacks._tasks.values())
    if tasks:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)


def callback(client, event):
    return client._connection_callbacks._callbacks[event][0]


def test_public_types():
    info = ConnectionInfo("client", "id", None, "host", 9092, ConnectionGroup.DEFAULT)
    with pytest.raises(FrozenInstanceError):
        info.node_id = 1


@pytest.mark.parametrize("cls", [AIOKafkaClient, AIOKafkaConsumer, AIOKafkaProducer])
async def test_public_constructor_forwarding(cls):
    opened, closed, failed = mock.Mock(), mock.Mock(), mock.Mock()
    obj = cls(
        listeners=[listener_mock(opened=opened, closed=closed, failed=failed)],
    )
    client = (
        obj if cls is AIOKafkaClient else getattr(obj, "_client", None) or obj.client
    )
    try:
        assert client._connection_callbacks._callbacks == {
            "opened": (opened,),
            "closed": (closed,),
            "failed": (failed,),
        }
    finally:
        if cls is AIOKafkaClient:
            await obj.close()
        else:
            await obj.stop()


@pytest.mark.parametrize("event", ["opened", "closed", "failed"])
async def test_invalid_callback(event):
    listener = listener_mock(**{event: False})
    with pytest.raises(TypeError, match=f"on_connection_{event} must be callable"):
        AIOKafkaClient(listeners=[listener])


async def test_open_reuse_and_shutdown(client):
    conn = await client._get_conn(1)
    assert await client._get_conn(1) is conn
    await drain(client)
    opened = callback(client, "opened")
    opened.assert_called_once()
    info = opened.call_args.args[0]
    assert info == ConnectionInfo(
        "observer-test", info.connection_id, 1, "broker", 9092, ConnectionGroup.DEFAULT
    )
    await client.close()
    callback(client, "closed").assert_called_once_with(
        info, ConnectionCloseReason.SHUTDOWN, None
    )
    assert not client._connection_callbacks._tasks


async def test_coordinator_connection_is_distinct(client):
    conn = await client._get_conn(1)
    coordinator = await client._get_conn(1, group=ConnectionGroup.COORDINATION)
    assert coordinator is not conn
    await drain(client)
    first, second = [call.args[0] for call in callback(client, "opened").call_args_list]
    assert first.connection_id != second.connection_id
    assert first.connection_group is ConnectionGroup.DEFAULT
    assert second.connection_group is ConnectionGroup.COORDINATION


@pytest.mark.parametrize("brokers", [[], [(1, "broker", 9092)]])
async def test_bootstrap_lifecycle(transport_factory, monkeypatch, brokers):
    monkeypatch.setattr(
        AIOKafkaConnection,
        "send",
        mock.AsyncMock(return_value=MetadataResponse_v0(brokers, [])),
    )
    opened, closed = mock.Mock(), mock.Mock()
    client = AIOKafkaClient(
        bootstrap_servers="bootstrap:9092",
        listeners=[listener_mock(opened=opened, closed=closed)],
    )
    try:
        await client.bootstrap()
        await drain(client)
        opened.assert_called_once()
        info = opened.call_args.args[0]
        assert info.node_id is None
        assert info.host == "bootstrap"
        assert info.connection_group is ConnectionGroup.DEFAULT
        if brokers:
            closed.assert_called_once_with(info, None, None)
        else:
            closed.assert_not_called()
            assert (
                await client._get_conn("bootstrap")
                is client._conns[("bootstrap", ConnectionGroup.DEFAULT)]
            )
            await client.close()
            opened.assert_called_once()
            closed.assert_called_once_with(info, ConnectionCloseReason.SHUTDOWN, None)
    finally:
        await client.close()


async def test_failed_bootstrap_attempts(transport_factory):
    error = OSError("unavailable")
    transport_factory.side_effect = error
    failed = mock.Mock()
    client = AIOKafkaClient(
        bootstrap_servers=["one:9092", "two:9093"],
        listeners=[listener_mock(failed=failed)],
    )
    try:
        with pytest.raises(KafkaConnectionError):
            await client.bootstrap()
        await drain(client)
        assert failed.call_count == 2
        infos = [call.args[0] for call in failed.call_args_list]
        assert {info.host for info in infos} == {"one", "two"}
        assert len({info.connection_id for info in infos}) == 2
        assert all(info.node_id is None for info in infos)
        assert all(call.args[1] is error for call in failed.call_args_list)
    finally:
        await client.close()


async def test_interruption_failed_retries_and_recovery(client, transport_factory):
    conn = await client._get_conn(1)
    await drain(client)
    error = OSError("connection lost")
    closed = asyncio.Event()
    callback(client, "closed").side_effect = lambda *args: closed.set()
    with mock.patch.object(client, "force_metadata_update") as refresh:
        conn._reader.set_exception(error)
        await asyncio.wait_for(closed.wait(), timeout=1)
        refresh.assert_called_once()
    old_info = callback(client, "opened").call_args.args[0]
    callback(client, "closed").assert_called_once_with(
        old_info, ConnectionCloseReason.CONNECTION_BROKEN, error
    )

    connect = transport_factory.side_effect
    transport_factory.side_effect = OSError("still unavailable")
    assert await client._get_conn(1) is None
    assert await client._get_conn(1) is None
    await drain(client)
    assert callback(client, "failed").call_count == 2
    transport_factory.side_effect = connect
    replacement = await client._get_conn(1)
    assert replacement is not conn
    await drain(client)
    assert callback(client, "opened").call_count == 2
    infos = [call.args[0] for call in callback(client, "opened").call_args_list]
    infos += [call.args[0] for call in callback(client, "failed").call_args_list]
    assert len({info.connection_id for info in infos}) == 4


async def test_idle_closure(client):
    conn = await client._get_conn(1)
    conn._max_idle_ms = 0
    conn._idle_handle.cancel()
    conn._idle_check(weakref.ref(conn))
    await drain(client)
    callback(client, "closed").assert_called_once_with(
        callback(client, "opened").call_args.args[0],
        ConnectionCloseReason.IDLE_DROP,
        None,
    )


async def test_timeout_preserves_error(client):
    conn = await client._get_conn(1)
    error = TimeoutError("request expired")
    with (
        mock.patch.object(conn, "send", mock.AsyncMock(side_effect=error)),
        pytest.raises(RequestTimedOutError),
    ):
        await client.send(1, MetadataRequest([]))
    await drain(client)
    callback(client, "closed").assert_called_once_with(
        callback(client, "opened").call_args.args[0],
        ConnectionCloseReason.CONNECTION_TIMEOUT,
        error,
    )
    callback(client, "failed").assert_not_called()


async def test_write_failure_preserves_error(client):
    conn = await client._get_conn(1)
    conn._versions = {MetadataRequest.API_KEY: (0, 0)}
    error = OSError("write failed")
    conn._writer.transport.write.side_effect = error
    with pytest.raises(KafkaConnectionError):
        conn.send(MetadataRequest([]))
    await drain(client)
    assert callback(client, "closed").call_args.args[2] is error


async def test_out_of_sync_preserves_error(client):
    conn = await client._get_conn(1)
    conn._versions = {MetadataRequest.API_KEY: (0, 0)}
    response = conn.send(MetadataRequest([]))
    conn._handle_frame(struct.pack(">i", -1))
    with pytest.raises(CorrelationIdError) as raised:
        await response
    await drain(client)
    callback(client, "closed").assert_called_once_with(
        callback(client, "opened").call_args.args[0],
        ConnectionCloseReason.OUT_OF_SYNC,
        raised.value,
    )


@pytest.mark.parametrize("stage", ["transport", "version", "authentication"])
async def test_establishment_failure_only_emits_failed(
    client, transport_factory, monkeypatch, stage
):
    error = KafkaConnectionError("handshake failed")
    if stage == "transport":
        transport_factory.side_effect = error
    else:
        method = "_do_version_lookup" if stage == "version" else "_do_sasl_handshake"
        monkeypatch.setattr(
            AIOKafkaConnection, method, mock.AsyncMock(side_effect=error)
        )
        client._security_protocol = "SASL_PLAINTEXT"
    assert await client._get_conn(1) is None
    await drain(client)
    callback(client, "failed").assert_called_once()
    assert callback(client, "failed").call_args.args[1] is error
    callback(client, "opened").assert_not_called()
    callback(client, "closed").assert_not_called()


async def test_open_waits_for_authentication(client, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def handshake(conn):
        entered.set()
        await release.wait()

    client._security_protocol = "SASL_PLAINTEXT"
    monkeypatch.setattr(AIOKafkaConnection, "_do_sasl_handshake", handshake)
    task = asyncio.create_task(client._get_conn(1))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        callback(client, "opened").assert_not_called()
    finally:
        release.set()
        await task
    await drain(client)
    callback(client, "opened").assert_called_once()


async def test_cancelled_establishment_is_not_a_failure(client, monkeypatch):
    monkeypatch.setattr(
        AIOKafkaConnection,
        "_do_version_lookup",
        mock.AsyncMock(side_effect=asyncio.CancelledError),
    )
    with pytest.raises(asyncio.CancelledError):
        await client._get_conn(1)
    await drain(client)
    for event in ("opened", "closed", "failed"):
        callback(client, event).assert_not_called()


async def test_close_at_end_of_handshake_keeps_order(client, monkeypatch):
    events = []
    callback(client, "opened").side_effect = lambda *args: events.append("opened")
    callback(client, "closed").side_effect = lambda *args: events.append("closed")

    async def lookup(conn):
        conn.close(ConnectionCloseReason.IDLE_DROP)

    monkeypatch.setattr(AIOKafkaConnection, "_do_version_lookup", lookup)
    await client._get_conn(1)
    await drain(client)
    assert events == ["opened", "closed"]


async def test_callbacks_are_ordered_per_connection_and_do_not_block_others(client):
    entered, release = asyncio.Event(), asyncio.Event()
    events = []

    async def opened(info):
        if info.node_id == 1:
            entered.set()
            await release.wait()
        events.append(("opened", info.node_id))

    callback(client, "opened").side_effect = opened
    callback(client, "closed").side_effect = lambda info, *_: events.append(
        ("closed", info.node_id)
    )
    first = await client._get_conn(1)
    await asyncio.wait_for(entered.wait(), timeout=1)
    first.close()
    second = await client._get_conn(2)
    second.close()
    first_info = next(
        call.args[0]
        for call in callback(client, "opened").call_args_list
        if call.args[0].node_id == 1
    )
    other_tasks = [
        task
        for key, task in client._connection_callbacks._tasks.items()
        if key != first_info.connection_id
    ]
    await asyncio.wait_for(asyncio.gather(*other_tasks), timeout=1)
    assert events == [("opened", 2), ("closed", 2)]
    release.set()
    await drain(client)
    assert events == [("opened", 2), ("closed", 2), ("opened", 1), ("closed", 1)]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "error", [ValueError("observer failed"), asyncio.CancelledError()]
)
async def test_callback_errors_are_isolated(
    client, transport_factory, caplog, asynchronous, error
):
    def broken(*args):
        raise error

    async def async_broken(*args):
        raise error

    for event in ("opened", "closed", "failed"):
        callback(client, event).side_effect = async_broken if asynchronous else broken
    conn = await client._get_conn(1)
    assert conn.connected()
    with mock.patch.object(conn, "send", mock.AsyncMock(return_value="response")):
        assert await client.send(1, MetadataRequest([])) == "response"
    transport_factory.side_effect = OSError("unavailable")
    assert await client._get_conn(2) is None
    await client.close()
    assert "Error in on_connection_opened callback" in caplog.text
    assert "Error in on_connection_closed callback" in caplog.text
    assert "Error in on_connection_failed callback" in caplog.text
    assert not client._connection_callbacks._tasks


async def test_shutdown_cancels_slow_observers(client, monkeypatch):
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def opened(info):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    callback(client, "opened").side_effect = opened
    monkeypatch.setattr(_ConnectionCallbacks, "_SHUTDOWN_TIMEOUT", 0.01)
    await client._get_conn(1)
    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.wait_for(client.close(), timeout=1)
    assert cancelled.is_set()
    assert not client._connection_callbacks._tasks
    callback(client, "closed").assert_not_called()


async def test_callback_can_close_client(client):
    stopped = asyncio.Event()

    async def opened(info):
        await client.close()
        stopped.set()

    callback(client, "opened").side_effect = opened
    await client._get_conn(1)
    await asyncio.wait_for(stopped.wait(), timeout=1)
    await drain(client)
    callback(client, "closed").assert_called_once()


async def test_callback_can_acquire_connection_lock(client):
    async def opened(info):
        async with client._get_conn_lock:
            assert client._conns[(info.node_id, info.connection_group)].connected()

    callback(client, "opened").side_effect = opened
    await client._get_conn(1)
    await drain(client)


async def test_disabled_callbacks_create_no_dispatcher(transport_factory):
    client = AIOKafkaClient()
    assert client._connection_callbacks is None
    client.cluster.update_metadata(MetadataResponse_v0([(1, "broker", 9092)], []))
    try:
        conn = await client._get_conn(1)
        assert conn._on_close_cb == client._on_connection_closed
    finally:
        await client.close()


async def test_bootstrap_metadata_error_is_not_establishment_failure(
    client, monkeypatch
):
    monkeypatch.setattr(
        AIOKafkaConnection,
        "send",
        mock.AsyncMock(side_effect=KafkaConnectionError("metadata request failed")),
    )
    with pytest.raises(KafkaConnectionError):
        await client.bootstrap()
    await drain(client)
    callback(client, "opened").assert_called_once()
    callback(client, "closed").assert_called_once()
    callback(client, "failed").assert_not_called()


async def test_missing_metadata_is_not_establishment_failure(client):
    assert await client._get_conn(999) is None
    await drain(client)
    callback(client, "failed").assert_not_called()


async def test_concurrent_connection_requests_emit_once(client):
    first, second = await asyncio.gather(client._get_conn(1), client._get_conn(1))
    assert first is second
    await drain(client)
    callback(client, "opened").assert_called_once()


async def test_callback_returning_future_preserves_order(client):
    future = asyncio.get_running_loop().create_future()
    callback(client, "opened").return_value = future
    conn = await client._get_conn(1)
    conn.close()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    callback(client, "closed").assert_not_called()
    future.set_result(None)
    await drain(client)
    callback(client, "closed").assert_called_once()


@pytest.mark.skipif(not hasattr(asyncio, "eager_task_factory"), reason="Python 3.12+")
async def test_eager_task_factory_defers_callbacks(client):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        async with client._get_conn_lock:
            conn = await client._create_connection("broker", 9092, node_id=1)
            client._conns[(1, ConnectionGroup.DEFAULT)] = conn
            callback(client, "opened").assert_not_called()
        await drain(client)
        callback(client, "opened").assert_called_once()
    finally:
        loop.set_task_factory(previous)


async def test_multiple_callbacks_can_close_client(client):
    stopped = []

    async def opened(info):
        await client.close()
        stopped.append(info.node_id)

    callback(client, "opened").side_effect = opened
    await client._get_conn(1)
    await client._get_conn(2)
    await drain(client)
    assert sorted(stopped) == [1, 2]


async def test_shutdown_does_not_wait_for_suppressed_cancellation(client, monkeypatch):
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def opened(info):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()

    callback(client, "opened").side_effect = opened
    monkeypatch.setattr(_ConnectionCallbacks, "_SHUTDOWN_TIMEOUT", 0.01)
    await client._get_conn(1)
    await asyncio.wait_for(entered.wait(), timeout=1)
    try:
        await asyncio.wait_for(client.close(), timeout=1)
        assert cancelled.is_set()
    finally:
        release.set()
        await drain(client)
    assert not client._connection_callbacks._tasks
    callback(client, "closed").assert_not_called()


async def test_cancelling_shutdown_cancels_observers(client):
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def opened(info):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    callback(client, "opened").side_effect = opened
    await client._get_conn(1)
    await asyncio.wait_for(entered.wait(), timeout=1)
    # Start shutdown directly on the dispatcher to cancel while it is draining.
    closing = asyncio.create_task(client._connection_callbacks.close())
    await asyncio.sleep(0)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert cancelled.is_set()
    assert not client._connection_callbacks._tasks


@pytest.mark.parametrize("entrypoint", ["client", "consumer", "producer"])
async def test_cancel_shutdown_during_transport_close(client, entrypoint):
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def opened(info):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    callback(client, "opened").side_effect = opened
    conn = await client._get_conn(1)
    await asyncio.wait_for(entered.wait(), timeout=1)
    # Transport shutdown is still pending when the caller cancels stop().
    transport_closed = conn._closed_fut
    conn._closed_fut = asyncio.get_running_loop().create_future()
    if entrypoint == "consumer":
        obj = AIOKafkaConsumer()
        obj._client = client
        stop = obj.stop
    elif entrypoint == "producer":
        obj = AIOKafkaProducer()
        obj.client = client
        stop = obj.stop
    else:
        stop = client.close
    closing = asyncio.create_task(stop())
    try:
        await asyncio.sleep(0)
        assert not conn.connected()
        closing.cancel()
        done, _ = await asyncio.wait({closing}, timeout=0.1)
        assert closing in done, "Cancellation waited for the observer grace period"
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert cancelled.is_set()
        assert not client._connection_callbacks._tasks
    finally:
        if not closing.done():
            closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        conn._closed_fut = transport_closed


@pytest.mark.parametrize(
    "reason", [None, ConnectionCloseReason.SHUTDOWN, ConnectionCloseReason.IDLE_DROP]
)
def test_connection_close_between_loop_runs(reason):
    loop = asyncio.new_event_loop()
    closed = mock.Mock()

    async def setup():
        client = AIOKafkaClient(listeners=[listener_mock(closed=closed)])

        async def connect(host, port, **kwargs):
            conn = AIOKafkaConnection(host, port, on_close=kwargs["on_close"])
            conn._reader = mock.Mock()
            conn._reader.at_eof.return_value = False
            conn._writer = mock.Mock()
            conn._read_task = loop.create_future()
            conn._read_task.set_result(None)
            conn._closed_fut = loop.create_future()
            conn._closed_fut.set_result(None)
            conn._idle_handle = loop.call_later(60, lambda: None)
            return conn

        with mock.patch("aiokafka.client.create_conn", side_effect=connect):
            conn = await client._create_connection("broker", 9092, node_id=1)
        client._conns[(1, ConnectionGroup.DEFAULT)] = conn
        return client, conn

    client, conn = loop.run_until_complete(setup())
    try:
        conn.close(reason)
        assert conn._on_close_cb is None
        assert conn._idle_handle.cancelled()
        closed.assert_not_called()
        loop.run_until_complete(client.close())
        closed.assert_called_once()
        assert closed.call_args.args[1] is reason
        assert not client._connection_callbacks._tasks
    finally:
        loop.run_until_complete(client.close())
        loop.close()


def test_callbacks_use_explicit_client_loop(caplog):
    loop = asyncio.new_event_loop()
    opened = mock.Mock()
    client = AIOKafkaClient(loop=loop, listeners=[listener_mock(opened=opened)])
    info = ConnectionInfo("client", "id", None, "host", 9092, ConnectionGroup.DEFAULT)
    try:
        client._connection_callbacks.emit("opened", info)
        opened.assert_not_called()
        loop.run_until_complete(drain(client))
        opened.assert_called_once_with(info)
        client._connection_callbacks.emit("closed", info, None, None)
        loop.run_until_complete(drain(client))
    finally:
        loop.close()
    # A closed loop cannot run observers. No coroutine or task should be created.
    client._connection_callbacks.emit("opened", info)
    assert not client._connection_callbacks._tasks
    assert not client._connection_callbacks._queues
    assert "event loop is closed" in caplog.text
    opened.assert_called_once()


async def test_close_exception_is_released_after_notification(client):
    conn = await client._get_conn(1)
    error = OSError("connection lost")
    conn.close(ConnectionCloseReason.CONNECTION_BROKEN, exc=error)
    assert conn._close_exception is None
    await drain(client)
    assert callback(client, "closed").call_args.args[2] is error


async def test_low_level_close_hook_remains_compatible(client):
    conn = await client._get_conn(1)
    received = []

    def on_close(connection, reason):
        received.append((connection, reason))

    conn._on_close_cb = on_close
    conn.close(ConnectionCloseReason.SHUTDOWN, exc=OSError("shutdown"))
    assert received == [(conn, ConnectionCloseReason.SHUTDOWN)]
    assert conn._close_exception is None


async def test_cancel_observers_before_dispatch_starts():
    current = asyncio.current_task()
    current.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.sleep(0)
        opened = mock.Mock()
        callbacks = _ConnectionCallbacks([listener_mock(opened=opened)])
        info = ConnectionInfo(
            "client", "id", None, "host", 9092, ConnectionGroup.DEFAULT
        )
        callbacks.emit("opened", info)
        await callbacks.close()
        assert not callbacks._tasks
        assert not callbacks._queues
        opened.assert_not_called()
    finally:
        current.uncancel()


@pytest.mark.parametrize("dispatch_started", [False, True])
async def test_failed_retries_bound_stalled_observers(
    client, transport_factory, monkeypatch, caplog, dispatch_started
):
    limit = 3
    monkeypatch.setattr(_ConnectionCallbacks, "_MAX_PENDING_CONNECTIONS", limit)
    release = asyncio.Event()

    async def failed(info, error):
        await release.wait()

    callback(client, "failed").side_effect = failed
    transport_factory.side_effect = OSError("broker unavailable")
    dispatcher = client._connection_callbacks
    try:
        for _ in range(limit):
            assert await client._get_conn(1) is None
        if dispatch_started:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert callback(client, "failed").call_count == limit
        for _ in range(20):
            assert await client._get_conn(1) is None
        assert transport_factory.await_count == limit + 20
        assert len(dispatcher._tasks) == limit
        assert len(dispatcher._queues) == limit
        queued = sum(len(queue) for queue in dispatcher._queues.values())
        assert queued == (0 if dispatch_started else limit)
        assert caplog.text.count("Connection observer limit reached") == 1
    finally:
        release.set()
        await drain(client)

    assert callback(client, "failed").call_count == limit
    assert not dispatcher._tasks
    assert not dispatcher._queues
    # Capacity recovers without replaying notifications dropped under overload.
    assert await client._get_conn(1) is None
    await drain(client)
    assert callback(client, "failed").call_count == limit + 1


async def test_overload_preserves_accepted_connection_order(client, monkeypatch):
    monkeypatch.setattr(_ConnectionCallbacks, "_MAX_PENDING_CONNECTIONS", 1)
    entered, release = asyncio.Event(), asyncio.Event()
    events = []

    async def opened(info):
        entered.set()
        await release.wait()
        events.append(("opened", info.node_id))

    callback(client, "opened").side_effect = opened
    callback(client, "closed").side_effect = lambda info, *_: events.append(
        ("closed", info.node_id)
    )
    first = await client._get_conn(1)
    await asyncio.wait_for(entered.wait(), timeout=1)
    try:
        second = await client._get_conn(2)
        assert second.connected()
        first.close()
        assert len(client._connection_callbacks._tasks) == 1
        assert events == []
    finally:
        release.set()
        await drain(client)
    assert events == [("opened", 1), ("closed", 1)]
    # Reject the whole lifecycle even if capacity returns before closure.
    second.close()
    await drain(client)
    assert events == [("opened", 1), ("closed", 1)]


@pytest.mark.parametrize("cls", [AIOKafkaClient, AIOKafkaConsumer, AIOKafkaProducer])
async def test_invalid_listener(cls):
    with pytest.raises(TypeError, match="listeners must contain ConnectionListener"):
        cls(listeners=[False])


async def test_listener_defaults_and_partial_override(transport_factory):
    received = []

    class Listener(ConnectionListener):
        async def on_connection_closed(self, connection, reason, error):
            received.append((connection, reason, error))

    client = AIOKafkaClient(listeners=[Listener()])
    client.cluster.update_metadata(MetadataResponse_v0([(1, "broker", 9092)], []))
    try:
        await client._get_conn(1)
        await drain(client)
    finally:
        await client.close()
    assert len(received) == 1
    info, reason, error = received[0]
    assert info.node_id == 1
    assert reason is ConnectionCloseReason.SHUTDOWN
    assert error is None
    assert ConnectionListener().on_connection_failed(info, OSError()) is None


async def test_callback_returning_shutdown_task(client):
    shutdown_tasks = []

    def opened(info):
        task = asyncio.create_task(client.close())
        shutdown_tasks.append(task)
        return task

    callback(client, "opened").side_effect = opened
    conn = await client._get_conn(1)
    await drain(client)
    assert len(shutdown_tasks) == 1
    assert shutdown_tasks[0].done()
    assert not shutdown_tasks[0].cancelled()
    assert shutdown_tasks[0].exception() is None
    assert not conn.connected()
    callback(client, "closed").assert_called_once()
    assert not client._connection_callbacks._tasks
    assert not client._connection_callbacks._queues


@pytest.mark.skipif(not hasattr(asyncio, "eager_task_factory"), reason="Python 3.12+")
async def test_returned_eager_shutdown_task_before_connection_is_pooled(
    client, monkeypatch
):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    monkeypatch.setattr(_ConnectionCallbacks, "_SHUTDOWN_TIMEOUT", 0.05)
    shutdown_tasks = []

    def opened(info):
        task = asyncio.create_task(client.close())
        shutdown_tasks.append(task)
        return task

    callback(client, "opened").side_effect = opened
    info = ConnectionInfo("client", "id", None, "host", 9092, ConnectionGroup.DEFAULT)
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        # No pooled transports make close() reach callback draining synchronously.
        client._connection_callbacks.emit("opened", info)
        await drain(client)
        assert len(shutdown_tasks) == 1
        assert shutdown_tasks[0].done()
        assert not shutdown_tasks[0].cancelled()
        assert shutdown_tasks[0].exception() is None
        assert not client._connection_callbacks._awaited_tasks
    finally:
        loop.set_task_factory(previous)


@pytest.mark.parametrize("finish_during_grace", [False, True])
@pytest.mark.parametrize("returned_waiter", [False, True])
async def test_detached_shutdown_drains_its_originating_callback(
    client, monkeypatch, finish_during_grace, returned_waiter
):
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    closing = None
    dispatcher = client._connection_callbacks
    monkeypatch.setattr(_ConnectionCallbacks, "_SHUTDOWN_TIMEOUT", 0.05)
    wait = mock.AsyncMock(wraps=asyncio.wait)
    monkeypatch.setattr(asyncio, "wait", wait)

    async def close_later():
        await entered.wait()
        await client.close()

    async def wait_for_logger():
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    def opened(info):
        nonlocal closing
        closing = asyncio.create_task(close_later())
        if returned_waiter:
            return asyncio.create_task(wait_for_logger())
        return wait_for_logger()

    callback(client, "opened").side_effect = opened
    await client._get_conn(1)
    await asyncio.wait_for(entered.wait(), timeout=1)
    observer = next(iter(dispatcher._tasks.values()))
    try:
        # Let shutdown enter the drain phase while the callback is still blocked.
        for _ in range(10):
            if wait.await_count or closing.done():
                break
            await asyncio.sleep(0)
        wait.assert_awaited_once()
        assert observer in wait.call_args.args[0]
        assert not closing.done()
        if finish_during_grace:
            release.set()
        await asyncio.wait_for(closing, timeout=1)
        assert cancelled.is_set() is not finish_during_grace
        assert not dispatcher._tasks
        assert not dispatcher._queues
        assert not dispatcher._awaited_tasks
        if finish_during_grace:
            callback(client, "closed").assert_called_once()
        else:
            callback(client, "closed").assert_not_called()
    finally:
        release.set()
        await asyncio.wait_for(closing, timeout=1)
        await drain(client)


async def test_callbacks_can_return_the_same_existing_shutdown_task(client):
    ready = asyncio.Event()
    opened_nodes = []

    async def close_later():
        await ready.wait()
        await client.close()

    closing = asyncio.create_task(close_later())

    def opened(info):
        opened_nodes.append(info.node_id)
        if len(opened_nodes) == 2:
            ready.set()
        return closing

    callback(client, "opened").side_effect = opened
    try:
        await client._get_conn(1)
        await client._get_conn(2)
        await drain(client)
        assert sorted(opened_nodes) == [1, 2]
        assert closing.done()
        assert not closing.cancelled()
        assert closing.exception() is None
        assert callback(client, "closed").call_count == 2
        assert not client._connection_callbacks._awaited_tasks
    finally:
        ready.set()
        await asyncio.wait_for(closing, timeout=1)


async def test_overload_preserves_reserved_closes_without_counting_idle_connections(
    client, monkeypatch
):
    monkeypatch.setattr(_ConnectionCallbacks, "_MAX_PENDING_CONNECTIONS", 1)
    closing, release = asyncio.Event(), asyncio.Event()

    async def closed(info, reason, error):
        closing.set()
        await release.wait()

    callback(client, "closed").side_effect = closed
    dispatcher = client._connection_callbacks
    first = await client._get_conn(1)
    await drain(client)
    assert not dispatcher._tasks
    assert not dispatcher._queues
    assert len(dispatcher._observed) == 1
    second = await client._get_conn(2)
    await drain(client)
    assert callback(client, "opened").call_count == 2
    first.close()
    try:
        await asyncio.wait_for(closing.wait(), timeout=1)
        assert len(dispatcher._queues) == 1
        assert len(dispatcher._tasks) == 1
        # Closure stays admitted even with the only pending-work slot occupied.
        second.close()
        assert len(dispatcher._tasks) == 2
        replacement_during_overload = await client._get_conn(1)
        assert replacement_during_overload.connected()
        assert len(dispatcher._tasks) == 2
        assert callback(client, "opened").call_count == 2
        replacement_during_overload.close()
    finally:
        release.set()
        await drain(client)
    assert callback(client, "closed").call_count == 2
    assert not dispatcher._queues
    replacement = await client._get_conn(2)
    await drain(client)
    replacement.close()
    await drain(client)
    opened_ids = [
        call.args[0].connection_id for call in callback(client, "opened").call_args_list
    ]
    closed_ids = [
        call.args[0].connection_id for call in callback(client, "closed").call_args_list
    ]
    assert len(opened_ids) == 3
    assert opened_ids == closed_ids
    assert not dispatcher._tasks
    assert not dispatcher._queues
    assert not dispatcher._observed


async def test_more_than_128_idle_connections_keep_observation(client, caplog):
    count = _ConnectionCallbacks._MAX_PENDING_CONNECTIONS + 2
    client.cluster.update_metadata(
        MetadataResponse_v0([(node, "broker", 9092) for node in range(count)], [])
    )
    dispatcher = client._connection_callbacks
    for node in range(count):
        await client._get_conn(node)
        await drain(client)
        assert not dispatcher._tasks
        assert not dispatcher._queues
    assert len(dispatcher._observed) == count
    assert callback(client, "opened").call_count == count
    assert "Connection observer limit reached" not in caplog.text
    await client.close()
    assert callback(client, "closed").call_count == count
    assert not dispatcher._observed
    assert not dispatcher._queues
    assert not dispatcher._tasks


async def test_default_listener_creates_no_work():
    dispatcher = _ConnectionCallbacks([ConnectionListener()])
    info = ConnectionInfo("client", "id", None, "host", 9092, ConnectionGroup.DEFAULT)
    loop = asyncio.get_running_loop()
    with mock.patch.object(loop, "create_task", wraps=loop.create_task) as create:
        dispatcher.emit("opened", info)
        dispatcher.emit("closed", info, None, None)
        dispatcher.emit("failed", info, OSError())
        create.assert_not_called()
    assert not dispatcher._callbacks
    assert not dispatcher._observed
    assert not dispatcher._queues
    await dispatcher.close()


async def test_failure_only_listener_skips_successful_lifecycles():
    failed = mock.Mock()

    class FailureListener(ConnectionListener):
        def on_connection_failed(self, info, error):
            failed(info, error)

    class InheritedListener(FailureListener):
        pass

    dispatcher = _ConnectionCallbacks([InheritedListener()])
    loop = asyncio.get_running_loop()
    with mock.patch.object(loop, "create_task", wraps=loop.create_task) as create:
        for number in range(130):
            info = ConnectionInfo(
                "client", str(number), None, "host", 9092, ConnectionGroup.DEFAULT
            )
            dispatcher.emit("opened", info)
            dispatcher.emit("closed", info, None, None)
        create.assert_not_called()
        assert not dispatcher._queues
        assert not dispatcher._observed
        error = OSError("failure")
        dispatcher.emit("failed", info, error)
        create.assert_called_once()
    await dispatcher.close()
    failed.assert_called_once_with(info, error)


async def test_instance_listener_override_is_dispatched():
    listener = ConnectionListener()
    listener.on_connection_opened = mock.Mock()
    dispatcher = _ConnectionCallbacks([listener])
    info = ConnectionInfo("client", "id", None, "host", 9092, ConnectionGroup.DEFAULT)
    dispatcher.emit("opened", info)
    await dispatcher.close()
    listener.on_connection_opened.assert_called_once_with(info)
    assert not dispatcher._observed


@pytest.mark.parametrize("cls", [AIOKafkaConsumer, AIOKafkaProducer])
async def test_connection_events_with_specialized_listeners(cls, transport_factory):
    base = ConnectionListener if cls is AIOKafkaConsumer else ProducerListener
    events = []

    class First(base):
        async def on_connection_opened(self, info):
            await asyncio.sleep(0)
            events.append(("first opened", info.connection_id))
            raise RuntimeError("observer failed")

        def on_connection_closed(self, info, reason, error):
            events.append(("first closed", info.connection_id))

    class Second(ConnectionListener):
        def on_connection_opened(self, info):
            events.append(("second opened", info.connection_id))

        async def on_connection_closed(self, info, reason, error):
            events.append(("second closed", info.connection_id))

    listeners = [First(), Second()]
    obj = cls(listeners=iter(listeners))
    client = obj._client if cls is AIOKafkaConsumer else obj.client
    listeners.clear()
    try:
        conn = await client._create_connection("broker", 9092)
        conn.close()
        await drain(client)
        assert [event for event, _ in events] == [
            "first opened",
            "second opened",
            "first closed",
            "second closed",
        ]
        assert len({connection_id for _, connection_id in events}) == 1
    finally:
        await obj.stop()


async def test_multiple_listeners_preserve_overload_event_pairs(monkeypatch):
    monkeypatch.setattr(_ConnectionCallbacks, "_MAX_PENDING_CONNECTIONS", 1)
    first = listener_mock(opened=mock.Mock(), closed=mock.Mock())
    second = listener_mock(opened=mock.Mock(), closed=mock.Mock())
    dispatcher = _ConnectionCallbacks([first, second])
    accepted = ConnectionInfo(
        "client", "accepted", None, "host", 9092, ConnectionGroup.DEFAULT
    )
    rejected = ConnectionInfo(
        "client", "rejected", None, "host", 9092, ConnectionGroup.DEFAULT
    )
    dispatcher.emit("opened", accepted)
    dispatcher.emit("opened", rejected)
    await asyncio.gather(*dispatcher._tasks.values())
    dispatcher.emit("closed", rejected, None, None)
    dispatcher.emit("closed", accepted, None, None)
    await dispatcher.close()
    for listener in (first, second):
        listener.on_connection_opened.assert_called_once_with(accepted)
        listener.on_connection_closed.assert_called_once_with(accepted, None, None)


async def test_listener_shutdown_preserves_following_listener_events(transport_factory):
    events = []
    first = ConnectionListener()
    second = ConnectionListener()
    first.on_connection_opened = lambda info: asyncio.create_task(client.close())
    second.on_connection_opened = lambda info: events.append("opened")
    second.on_connection_closed = lambda *args: events.append("closed")
    client = AIOKafkaClient(listeners=[first, second])
    client.cluster.update_metadata(MetadataResponse_v0([(1, "broker", 9092)], []))
    try:
        await client._get_conn(1)
        await drain(client)
        assert events == ["opened", "closed"]
        assert not client._connection_callbacks._tasks
    finally:
        await client.close()
