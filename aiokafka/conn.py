import asyncio
import struct
from kafka.common import ConnectionError

__all__ = ['AIOKafkaConnection', 'create_conn']


@asyncio.coroutine
def create_conn(host, port, *, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    conn = AIOKafkaConnection(host, port, loop=loop)
    yield from conn._connect()
    return conn


class AIOKafkaConnection:
    HEADER = struct.Struct('>i')

    def __init__(self, host, port, *, loop):
        self._host = host
        self._port = port
        self._reader = self._writer = None
        self._loop = loop
        self._requests = []
        self._read_task = None

    def _connect(self):
        self._reader, self._writer = yield from asyncio.open_connection(
            self.host, self.port, loop=self._loop)
        self._read_task = asyncio.async(self._read(), loop=self._loop)

    def __repr__(self):
        return "<KafkaConnection host={0.host} port={0.port}>".format(self)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def send(self, payload, no_ack=False):
        self._writer.write(payload)
        fut = asyncio.Future(loop=self._loop)
        if no_ack:
            fut.set_result(None)
            return fut
        self._requests.append(fut)
        return fut

    @asyncio.coroutine
    def _read(self):
        try:
            while True:
                resp = yield from self._reader.readexactly(4)
                size, = self.HEADER.unpack(resp)

                resp = yield from self._reader.readexactly(size)

                fut = self._requests.pop(0)
                if not fut.cancelled():
                    fut.set_result(resp)
        except OSError as exc:
            conn_exc = ConnectionError("Kafka at {0}:{1} went away".format(
                self._host, self._port))
            conn_exc.__cause__ = exc
            conn_exc.__context__ = exc
            fut = self._requests.pop(0)
            fut.set_exception(conn_exc)
            self.close()

    def close(self):
        if self._reader:
            self._writer.close()
            self._reader = self._writer = None
            self._read_task.cancel()
            for fut in self._requests:
                fut.cancel()
            self._requests = []
