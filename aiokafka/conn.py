import asyncio
import collections
import functools
import logging
import struct
import sys
import traceback
import warnings
import weakref

from kafka.protocol.api import RequestHeader
from kafka.protocol.admin import (
    SaslHandShakeRequest, SaslAuthenticateRequest, ApiVersionRequest
)
from kafka.protocol.commit import (
    GroupCoordinatorResponse_v0 as GroupCoordinatorResponse)

import aiokafka.errors as Errors
from aiokafka.util import ensure_future, create_future, PY_36

try:
    import gssapi
except ImportError:
    gssapi = None

__all__ = ['AIOKafkaConnection', 'create_conn']


READER_LIMIT = 2 ** 16
SASL_QOP_AUTH = 1


class CloseReason:

    CONNECTION_BROKEN = 0
    CONNECTION_TIMEOUT = 1
    OUT_OF_SYNC = 2
    IDLE_DROP = 3
    SHUTDOWN = 4
    AUTH_FAILURE = 5


class VersionInfo:

    def __init__(self, versions):
        self._versions = versions

    def pick_best(self, request_versions):
        api_key = request_versions[0].API_KEY
        supported_versions = self._versions.get(api_key)
        if supported_versions is None:
            return request_versions[0]
        else:
            for req_klass in reversed(request_versions):
                if supported_versions[0] <= req_klass.API_VERSION and \
                        req_klass.API_VERSION <= supported_versions[1]:
                    return req_klass
        raise Errors.KafkaError(
            "Could not pick a version for API_KEY={} from {}. ".format(
                api_key, supported_versions)
        )


async def create_conn(
    host, port, *, loop=None, client_id='aiokafka',
    request_timeout_ms=40000, api_version=(0, 8, 2),
    ssl_context=None, security_protocol="PLAINTEXT",
    max_idle_ms=None, on_close=None,
    sasl_mechanism=None,
    sasl_plain_username=None,
    sasl_plain_password=None,
    sasl_kerberos_service_name='kafka',
    sasl_kerberos_domain_name=None,
    version_hint=None
):
    if loop is None:
        loop = asyncio.get_event_loop()
    conn = AIOKafkaConnection(
        host, port, loop=loop, client_id=client_id,
        request_timeout_ms=request_timeout_ms,
        api_version=api_version,
        ssl_context=ssl_context, security_protocol=security_protocol,
        max_idle_ms=max_idle_ms, on_close=on_close,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        sasl_kerberos_service_name=sasl_kerberos_service_name,
        sasl_kerberos_domain_name=sasl_kerberos_domain_name,
        version_hint=version_hint)
    await conn.connect()
    return conn


class AIOKafkaProtocol(asyncio.StreamReaderProtocol):

    def __init__(self, closed_fut, *args, loop, **kw):
        self._closed_fut = closed_fut
        super().__init__(*args, loop=loop, **kw)

    def connection_lost(self, exc):
        super().connection_lost(exc)
        if not self._closed_fut.cancelled():
            self._closed_fut.set_result(None)


class AIOKafkaConnection:
    """Class for manage connection to Kafka node"""

    log = logging.getLogger(__name__)

    _reader = None  # For __del__ to work properly, just in case
    _source_traceback = None

    def __init__(self, host, port, *, loop, client_id='aiokafka',
                 request_timeout_ms=40000, api_version=(0, 8, 2),
                 ssl_context=None, security_protocol='PLAINTEXT',
                 max_idle_ms=None, on_close=None, sasl_mechanism=None,
                 sasl_plain_password=None, sasl_plain_username=None,
                 sasl_kerberos_service_name='kafka',
                 sasl_kerberos_domain_name=None,
                 version_hint=None):
        if sasl_mechanism == "GSSAPI":
            assert gssapi is not None, "gssapi library required"

        self._loop = loop
        self._host = host
        self._port = port
        self._request_timeout = request_timeout_ms / 1000
        self._api_version = api_version
        self._client_id = client_id
        self._ssl_context = ssl_context
        self._security_protocol = security_protocol
        self._sasl_mechanism = sasl_mechanism
        self._sasl_plain_username = sasl_plain_username
        self._sasl_plain_password = sasl_plain_password
        self._sasl_kerberos_service_name = sasl_kerberos_service_name
        self._sasl_kerberos_domain_name = sasl_kerberos_domain_name

        # Version hint is the version determined by initial client bootstrap
        self._version_hint = version_hint
        self._version_info = VersionInfo({})

        self._reader = self._writer = self._protocol = None
        # Even on small size seems to be a bit faster than list.
        # ~2x on size of 2 in Python3.6
        self._requests = collections.deque()
        self._read_task = None
        self._correlation_id = 0
        self._closed_fut = None

        self._max_idle_ms = max_idle_ms
        self._last_action = loop.time()
        self._idle_handle = None

        self._on_close_cb = on_close

        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    # Warn and try to close. We can close synchroniously, so will attempt
    # that
    def __del__(self, _warnings=warnings):
        if self.connected():
            if PY_36:
                kwargs = {'source': self}
            else:
                kwargs = {}
            _warnings.warn("Unclosed AIOKafkaConnection {!r}".format(self),
                           ResourceWarning,
                           **kwargs)
            if self._loop.is_closed():
                return

            # We don't need to call callback in this case. Just release
            # sockets and stop connections.
            self._on_close_cb = None
            self.close()

            context = {'conn': self,
                       'message': 'Unclosed AIOKafkaConnection'}
            if self._source_traceback is not None:
                context['source_traceback'] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def connect(self):
        loop = self._loop
        self._closed_fut = create_future(loop=loop)
        if self._security_protocol in ["PLAINTEXT", "SASL_PLAINTEXT"]:
            ssl = None
        else:
            assert self._security_protocol in ["SSL", "SASL_SSL"]
            assert self._ssl_context is not None
            ssl = self._ssl_context
        # Create streams same as `open_connection`, but using custom protocol
        reader = asyncio.StreamReader(limit=READER_LIMIT, loop=loop)
        protocol = AIOKafkaProtocol(self._closed_fut, reader, loop=loop)
        transport, _ = await asyncio.wait_for(
            loop.create_connection(
                lambda: protocol, self.host, self.port, ssl=ssl),
            loop=loop, timeout=self._request_timeout)
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        self._reader, self._writer, self._protocol = reader, writer, protocol

        # Start reader task.
        self._read_task = self._create_reader_task()

        # Start idle checker
        if self._max_idle_ms is not None:
            self._idle_handle = self._loop.call_soon(
                self._idle_check, weakref.ref(self))

        if self._version_hint and self._version_hint >= (0, 10):
            await self._do_version_lookup()

        if self._security_protocol in ["SASL_SSL", "SASL_PLAINTEXT"]:
            await self._do_sasl_handshake()

        return reader, writer

    async def _do_version_lookup(self):
        version_req = ApiVersionRequest[0]()
        response = await self.send(version_req)
        versions = {}
        for api_key, min_version, max_version in response.api_versions:
            assert min_version <= max_version, (
                "{} should be less than or equal to {} for {}".format(
                    min_version, max_version, api_key)
            )
            versions[api_key] = (min_version, max_version)
        self._version_info = VersionInfo(versions)

    async def _do_sasl_handshake(self):
        # NOTE: We will only fallback to v0.9 gssapi scheme if user explicitly
        #       stated, that api_version is "0.9"
        if self._version_hint and self._version_hint < (0, 10):
            handshake_klass = None
            assert self._sasl_mechanism == 'GSSAPI', (
                "Only GSSAPI supported for v0.9"
            )
        else:
            handshake_klass = self._version_info.pick_best(
                SaslHandShakeRequest)

            sasl_handshake = handshake_klass(self._sasl_mechanism)
            response = await self.send(sasl_handshake)
            error_type = Errors.for_code(response.error_code)
            if error_type is not Errors.NoError:
                error = error_type(self)
                self.close(reason=CloseReason.AUTH_FAILURE, exc=error)
                raise error

            if self._sasl_mechanism not in response.enabled_mechanisms:
                exc = Errors.UnsupportedSaslMechanismError(
                    'Kafka broker does not support %s sasl mechanism. '
                    'Enabled mechanisms are: %s'
                    % (self._sasl_mechanism, response.enabled_mechanisms))
                self.close(reason=CloseReason.AUTH_FAILURE, exc=exc)
                raise exc

        assert self._sasl_mechanism in ('PLAIN', 'GSSAPI')
        if self._security_protocol == 'SASL_PLAINTEXT' and \
           self._sasl_mechanism == 'PLAIN':
            self.log.warning(
                'Sending username and password in the clear')

        if self._sasl_mechanism == 'GSSAPI':
            authenticator = self.authenticator_gssapi()
        else:
            authenticator = self.authenticator_plain()

        if handshake_klass is not None and sasl_handshake.API_VERSION > 0:
            auth_klass = self._version_info.pick_best(SaslAuthenticateRequest)
        else:
            auth_klass = None

        auth_bytes = None
        expect_response = True

        while True:
            res = await authenticator.step(auth_bytes)
            if res is None:
                break
            payload, expect_response = res

            # Before Kafka 1.0.0 Authentication bytes for SASL were send
            # without a Kafka Header, only with Length. This made error
            # handling hard, so they made SaslAuthenticateRequest to properly
            # pass error messages to clients on source of error.
            if auth_klass is None:
                auth_bytes = await self._send_sasl_token(
                    payload, expect_response
                )
            else:
                req = auth_klass(payload)
                resp = await self.send(req)
                error_type = Errors.for_code(resp.error_code)
                if error_type is not Errors.NoError:
                    exc = error_type(resp.error_message)
                    self.close(reason=CloseReason.AUTH_FAILURE, exc=exc)
                    raise exc
                auth_bytes = resp.sasl_auth_bytes

        if self._sasl_mechanism == 'GSSAPI':
            self.log.info(
                'Authenticated as %s via GSSAPI',
                self.sasl_principal)
        else:
            self.log.info('Authenticated as %s via PLAIN',
                          self._sasl_plain_username)

    def authenticator_plain(self):
        return SaslPlainAuthenticator(
            loop=self._loop,
            sasl_plain_password=self._sasl_plain_password,
            sasl_plain_username=self._sasl_plain_username)

    def authenticator_gssapi(self):
        return SaslGSSAPIAuthenticator(
            loop=self._loop,
            principal=self.sasl_principal)

    @property
    def sasl_principal(self):
        service = self._sasl_kerberos_service_name
        domain = self._sasl_kerberos_domain_name or self.host

        return "{service}@{domain}".format(service=service, domain=domain)

    @staticmethod
    def _on_read_task_error(self_ref, read_task):
        # We don't want to react to cancelled errors
        if read_task.cancelled():
            return

        try:
            read_task.result()
        except (OSError, EOFError, ConnectionError) as exc:
            self_ref().close(reason=CloseReason.CONNECTION_BROKEN, exc=exc)
        except Exception as exc:
            self = self_ref()
            self.log.exception("Unexpected exception in AIOKafkaConnection")
            self.close(reason=CloseReason.CONNECTION_BROKEN, exc=exc)

    @staticmethod
    def _idle_check(self_ref):
        self = self_ref()
        idle_for = self._loop.time() - self._last_action
        timeout = self._max_idle_ms / 1000
        # If we have any pending requests, we are assumed to be not idle.
        # it's up to `request_timeout_ms` to break those.
        if (idle_for >= timeout) and not self._requests:
            self.close(CloseReason.IDLE_DROP)
        else:
            if self._requests:
                # We must wait at least max_idle_ms anyway. Mostly this setting
                # is quite high so we shouldn't spend many CPU on this
                wake_up_in = timeout
            else:
                wake_up_in = timeout - idle_for
            self._idle_handle = self._loop.call_later(
                wake_up_in, self._idle_check, self_ref)

    def __repr__(self):
        return "<AIOKafkaConnection host={0.host} port={0.port}>".format(self)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def send(self, request, expect_response=True):
        if self._writer is None:
            raise Errors.ConnectionError(
                "No connection to broker at {0}:{1}"
                .format(self._host, self._port))

        correlation_id = self._next_correlation_id()
        header = RequestHeader(request,
                               correlation_id=correlation_id,
                               client_id=self._client_id)
        message = header.encode() + request.encode()
        size = struct.pack(">i", len(message))
        try:
            self._writer.write(size + message)
        except OSError as err:
            self.close(reason=CloseReason.CONNECTION_BROKEN)
            raise Errors.ConnectionError(
                "Connection at {0}:{1} broken: {2}".format(
                    self._host, self._port, err))

        self.log.debug(
            '%s Request %d: %s', self, correlation_id, request)

        if not expect_response:
            return self._writer.drain()
        fut = create_future(loop=self._loop)
        self._requests.append((correlation_id, request.RESPONSE_TYPE, fut))
        return asyncio.wait_for(fut, self._request_timeout, loop=self._loop)

    def _send_sasl_token(self, payload, expect_response=True):
        if self._writer is None:
            raise Errors.ConnectionError(
                "No connection to broker at {0}:{1}"
                .format(self._host, self._port))

        size = struct.pack(">i", len(payload))
        try:
            self._writer.write(size + payload)
        except OSError as err:
            self.close(reason=CloseReason.CONNECTION_BROKEN)
            raise Errors.ConnectionError(
                "Connection at {0}:{1} broken: {2}".format(
                    self._host, self._port, err))

        if not expect_response:
            return self._writer.drain()

        fut = create_future(loop=self._loop)
        self._requests.append((None, None, fut))
        return asyncio.wait_for(fut, self._request_timeout, loop=self._loop)

    def connected(self):
        return bool(self._reader is not None and not self._reader.at_eof())

    def close(self, reason=None, exc=None):
        self.log.debug("Closing connection at %s:%s", self._host, self._port)
        if self._reader is not None:
            self._writer.close()
            self._writer = self._reader = None
            if not self._read_task.done():
                self._read_task.cancel()
                self._read_task = None
            for _, _, fut in self._requests:
                if not fut.done():
                    error = Errors.ConnectionError(
                        "Connection at {0}:{1} closed".format(
                            self._host, self._port))
                    if exc is not None:
                        error.__cause__ = exc
                        error.__context__ = exc
                    fut.set_exception(error)
            self._requests = collections.deque()
            if self._on_close_cb is not None:
                self._on_close_cb(self, reason)
                self._on_close_cb = None
        if self._idle_handle is not None:
            self._idle_handle.cancel()

        # transport.close() will close socket, but not right ahead. Return
        # a future in case we need to wait on it.
        return self._closed_fut

    def _create_reader_task(self):
        self_ref = weakref.ref(self)
        read_task = ensure_future(self._read(self_ref), loop=self._loop)
        read_task.add_done_callback(
            functools.partial(self._on_read_task_error, self_ref))
        return read_task

    @classmethod
    async def _read(cls, self_ref):
        # XXX: I know that it become a bit more ugly once cyclic references
        # were removed, but it's needed to allow connections to properly
        # release resources if leaked.
        # NOTE: all errors will be handled by done callback

        reader = self_ref()._reader
        while True:
            resp = await reader.readexactly(4)
            size, = struct.unpack(">i", resp)

            resp = await reader.readexactly(size)
            self_ref()._handle_frame(resp)

    def _handle_frame(self, resp):
        correlation_id, resp_type, fut = self._requests[0]

        if correlation_id is None:  # Is a SASL packet, just pass it though
            if not fut.done():
                fut.set_result(resp)
        else:

            recv_correlation_id, = struct.unpack_from(">i", resp, 0)

            if (self._api_version == (0, 8, 2) and
                    resp_type is GroupCoordinatorResponse and
                    correlation_id != 0 and recv_correlation_id == 0):
                self.log.warning(
                    'Kafka 0.8.2 quirk -- GroupCoordinatorResponse'
                    ' coorelation id does not match request. This'
                    ' should go away once at least one topic has been'
                    ' initialized on the broker')

            elif correlation_id != recv_correlation_id:
                error = Errors.CorrelationIdError(
                    'Correlation ids do not match: sent {}, recv {}'
                    .format(correlation_id, recv_correlation_id))
                if not fut.done():
                    fut.set_exception(error)
                self.close(reason=CloseReason.OUT_OF_SYNC)
                return

            if not fut.done():
                response = resp_type.decode(resp[4:])
                self.log.debug(
                    '%s Response %d: %s', self, correlation_id, response)
                fut.set_result(response)

        # Update idle timer.
        self._last_action = self._loop.time()
        # We should clear the request future only after all code is done and
        # future is resolved. If any fails it's up to close() method to fail
        # this future.
        self._requests.popleft()

    def _next_correlation_id(self):
        self._correlation_id = (self._correlation_id + 1) % 2**31
        return self._correlation_id


class BaseSaslAuthenticator:

    def step(self, payload):
        return self._loop.run_in_executor(None, self._step, payload)

    def _step(self, payload):
        """ Process next token in sequence and return with:
            ``None`` if it was the last needed exchange
            ``tuple`` tuple with new token and a boolean whether it requires an
                answer token
        """
        try:
            data = self._authenticator.send(payload)
        except StopIteration:
            return
        else:
            return data


class SaslPlainAuthenticator(BaseSaslAuthenticator):

    def __init__(self, *, loop, sasl_plain_password, sasl_plain_username):
        self._loop = loop
        self._sasl_plain_username = sasl_plain_username
        self._sasl_plain_password = sasl_plain_password
        self._authenticator = self.authenticator_plain()

    def authenticator_plain(self):
        """ Automaton to authenticate with SASL tokens
        """
        # Send PLAIN credentials per RFC-4616
        data = '\0'.join([
            self._sasl_plain_username,
            self._sasl_plain_username,
            self._sasl_plain_password]
        ).encode("utf-8")

        resp = yield data, True

        assert resp == b"", (
            "Server should either close or send an empty response"
        )


class SaslGSSAPIAuthenticator(BaseSaslAuthenticator):

    def __init__(self, *, loop, principal):
        self._loop = loop
        self._principal = principal
        self._authenticator = self.authenticator_gssapi()

    def authenticator_gssapi(self):
        name = gssapi.Name(self._principal,
                           name_type=gssapi.NameType.hostbased_service)
        cname = name.canonicalize(gssapi.MechType.kerberos)

        client_ctx = gssapi.SecurityContext(name=cname, usage='initiate')

        server_token = None
        while not client_ctx.complete:
            client_token = client_ctx.step(server_token)
            client_token = client_token or b''

            server_token = yield client_token, True

        msg = client_ctx.unwrap(server_token).message

        qop = struct.pack('b', SASL_QOP_AUTH & msg[0])
        msg = qop + msg[1:]
        msg = client_ctx.wrap(msg + self._principal.encode(), False).message

        yield (msg, False)
