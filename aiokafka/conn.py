import asyncio
import struct
import socket
import logging
import copy
from kafka.common import ConnectionError
import kafka.common as Errors
from kafka.protocol.api import RequestHeader
from kafka.protocol.commit import GroupCoordinatorResponse
from aiokafka import __version__

__all__ = ['AIOKafkaConnection', 'create_conn']


@asyncio.coroutine
def create_conn(host, port, *, loop=None, **config):
    if loop is None:
        loop = asyncio.get_event_loop()
    conn = AIOKafkaConnection(host, port, loop=loop, **config)
    yield from conn._connect()
    return conn


class AIOKafkaConnection:
    HEADER = struct.Struct('>i')

    DEFAULT_CONFIG = {
        'client_id': 'kafka-python-' + __version__,
        'request_timeout_ms': 40000,
        'api_version': (0, 8, 2),  # default to most restrictive
    }

    log = logging.getLogger(__name__)

    def __init__(self, host, port, *, loop, **config):
        self._host = host
        self._port = port
        self._reader = self._writer = None
        self._loop = loop
        self._requests = []
        self._read_task = None
        self._correlation_id = 0
        self._config = copy.copy(self.DEFAULT_CONFIG)
        for key in self._config:
            if key in config:
                self._config[key] = config[key]

    @asyncio.coroutine
    def _connect(self):
        future = asyncio.open_connection(self.host, self.port, loop=self._loop)
        self._reader, self._writer = yield from asyncio.wait_for(
            future, self._config['request_timeout_ms']/1000)
        self._read_task = asyncio.ensure_future(self._read(), loop=self._loop)

    def __repr__(self):
        return "<AIOKafkaConnection host={0.host} port={0.port}>".format(self)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def send(self, request, expect_response=True):
        correlation_id = self._next_correlation_id()
        header = RequestHeader(request,
                               correlation_id=correlation_id,
                               client_id=self._config['client_id'])
        message = b''.join([header.encode(), request.encode()])
        size = self.HEADER.pack(len(message))
        try:
            self._writer.write(size)
            self._writer.write(message)
        except OSError as err:
            self.close()
            raise ConnectionError(
                "Connection at {0}:{1} broken: {2}".format(
                    self._host, self._port, err))

        fut = asyncio.Future(loop=self._loop)
        if not expect_response:
            fut.set_result(None)
            return fut
        self._requests.append((correlation_id, request.RESPONSE_TYPE, fut))
        return asyncio.wait_for(fut, self._config['request_timeout_ms']/1000)

    def connected(self):
        return self._reader and not self._reader.at_eof()

    def close(self):
        if self._reader:
            self._writer.close()
            self._reader = self._writer = None
            self._read_task.cancel()
            for _, _, fut in self._requests:
                fut.cancel()
            self._requests = []

    @asyncio.coroutine
    def _read(self):
        try:
            while True:
                resp = yield from self._reader.readexactly(4)
                size, = self.HEADER.unpack(resp)

                resp = yield from self._reader.readexactly(size)

                recv_correlation_id, = self.HEADER.unpack(resp[:4])

                correlation_id, resp_type, fut  = self._requests.pop(0)
                if (self._config['api_version'] == (0, 8, 2) and
                    resp_type is GroupCoordinatorResponse and
                    correlation_id != 0 and
                    recv_correlation_id == 0):
                    self.log.warning(
                        'Kafka 0.8.2 quirk -- GroupCoordinatorResponse'
                        ' coorelation id does not match request. This'
                        ' should go away once at least one topic has been'
                        ' initialized on the broker')

                if correlation_id != recv_correlation_id:
                    error = Errors.CorrelationIdError(
                        'Correlation ids do not match: sent {}, recv {}'
                        .format(correlation_id, recv_correlation_id))
                    fut.set_exception(error)
                    self.close()
                    break

                if not fut.done():
                    response = resp_type.decode(resp[4:])
                    self.log.debug('%s Response %d: %s',
                                   self, correlation_id, response)
                    fut.set_result(response)
        except OSError as exc:
            conn_exc = ConnectionError("Connection at {0}:{1} broken".format(
                self._host, self._port))
            conn_exc.__cause__ = exc
            conn_exc.__context__ = exc
            _, _, fut  = self._requests.pop(0)
            fut.set_exception(conn_exc)
            self.close()

    def _next_correlation_id(self):
        self._correlation_id = (self._correlation_id + 1) % 2**31
        return self._correlation_id
