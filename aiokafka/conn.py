import asyncio
import struct
from collections import deque


MAX_CHUNK_SIZE = 65536


class Reader:

    HEADER = struct.Struct('>i')
    HEADER_SIZE = 4

    def __init__(self):
        self._buffer = bytearray()
        self._is_header = False
        self._payload_size = None

    def feed(self, chunk):
        if not chunk:
            return
        self._buffer.extend(chunk)

    def gets(self):
        buffer_size = len(self._buffer)
        if not self._is_header and buffer_size >= self.HEADER_SIZE:
            self._payload_size, = self.HEADER.unpack(
                self._buffer[:self.HEADER_SIZE])
            self._is_header = True

        if (self._is_header and buffer_size >=
                self.HEADER_SIZE + self._payload_size):
            start = self.HEADER_SIZE
            end = self.HEADER_SIZE + self._payload_size
            raw_resp = self._buffer[start:end]
            self._reset()
            return bytes(raw_resp)
        return False

    def _reset(self):
        start = self.HEADER_SIZE + self._payload_size
        self._buffer = self._buffer[start:]
        self._is_header = False
        self._payload_size = None


@asyncio.coroutine
def create_connection(host, port, *, loop=None):
    conn = AIOKafkaConnection(host, port, loop=loop)
    yield from conn._connect()
    return conn


class AIOKafkaConnection:
    HEADER = struct.Struct('>i')

    def __init__(self, host, port, *, loop=None):
        self._host = host
        self._port = port
        self._reader = self._writer = None
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        self._parser = Reader()
        self._waiters = deque()

    def _connect(self):
        self._reader, self._writer = yield from asyncio.open_connection(
            self.host, self.port, loop=self._loop)
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)

    def __repr__(self):
        return "<KafkaConnection host={0.host} port={0.port}>".format(self)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    def execute(self, request_id, payload, no_response=False):

        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        fut = asyncio.Future(loop=self._loop)
        self._waiters.append((fut, None))
        self._writer.write(payload)

        if no_response:
            fut.set_result(None)
        return fut

    def close(self):
        self._reader_task.cancel()
        if self._reader:
            self._writer.close()
            self._reader = self._writer = None

    @asyncio.coroutine
    def _read_data(self):
        """Responses reader task."""
        while not self._reader.at_eof():
            data = yield from self._reader.read(MAX_CHUNK_SIZE)
            self._parser.feed(data)
            while True:
                try:
                    raw_obj = self._parser.gets()
                except Exception as exc:
                    # ProtocolError is fatal
                    # so connection must be closed

                    self._loop.call_soon(self.close, exc)
                    raise
                else:
                    if raw_obj is False:
                        break
                    fut, decoder = self._waiters.popleft()

                    if decoder is not None:
                        try:
                            obj = decoder(raw_obj)
                            fut.set_result(obj)
                        except Exception as exc:
                            fut.set_exception(exc)
                            continue
                    else:
                        fut.set_result(raw_obj)

        self._closing = True
        self._loop.call_soon(self.close, None)
