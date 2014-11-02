import asyncio
import copy
import struct


class KafkaConnection:
    HEADER = struct.Struct('>i')

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._reader = self._writer = None

    def __repr__(self):
        return "<KafkaConnection host={0.host} port={0.port}>".format(self)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @asyncio.coroutine
    def send(self, request_id, payload):
        if self._writer is None:
            yield from self.reinit()

        self._writer.write(payload)

    @asyncio.coroutine
    def recv(self, request_id):
        if self._reader is None:
            yield from self.reinit()

        try:
            resp = yield from self._reader.readexactly(4)
            size, = self.HEADER.unpack(resp)

            resp = yield from self._reader.readexactly(size)
            return resp
        except OSError:
            self._writer.close()
            self._reader = self._writer = None
            raise

    def copy(self):
        c = copy.deepcopy(self)
        c._reader = c._writer = None
        return c

    def close(self):
        if self._reader:
            self._writer.close()
            self._reader = self._writer = None

    def reinit(self):
        if self._reader:
            self.close()
        self._reader, self._writer = yield from asyncio.open_connection(
            self.host, self.port)
