import asyncio
import gc
import pytest
import struct
import unittest
from unittest import mock
from kafka.protocol.produce import ProduceRequest_v0 as ProduceRequest
from kafka.protocol.message import Message
from kafka.protocol.metadata import (
    MetadataRequest_v0 as MetadataRequest,
    MetadataResponse_v0 as MetadataResponse)
from kafka.protocol.commit import (
    GroupCoordinatorRequest_v0 as GroupCoordinatorRequest,
    GroupCoordinatorResponse_v0 as GroupCoordinatorResponse)

from aiokafka.conn import AIOKafkaConnection, create_conn
from aiokafka.errors import ConnectionError, CorrelationIdError
from aiokafka.util import PY_341
from ._testutil import KafkaIntegrationTestCase, run_until_complete


@pytest.mark.usefixtures('setup_test_class')
class ConnTest(unittest.TestCase):

    def test_ctor(self):
        conn = AIOKafkaConnection('localhost', 1234, loop=self.loop)
        self.assertEqual('localhost', conn.host)
        self.assertEqual(1234, conn.port)
        self.assertTrue('KafkaConnection' in conn.__repr__())
        self.assertIsNone(conn._reader)
        self.assertIsNone(conn._writer)


@pytest.mark.usefixtures('setup_test_class')
class ConnIntegrationTest(KafkaIntegrationTestCase):

    @run_until_complete
    def test_global_loop_for_create_conn(self):
        asyncio.set_event_loop(self.loop)
        try:
            host, port = self.kafka_host, self.kafka_port
            conn = yield from create_conn(host, port)
            self.assertIs(conn._loop, self.loop)
            conn.close()
            # make sure second closing does nothing and we have full coverage
            # of *if self._reader:* condition
            conn.close()
        finally:
            asyncio.set_event_loop(None)

    @pytest.mark.skipif(not PY_341, reason="Not supported on older Python's")
    @run_until_complete
    def test_conn_warn_unclosed(self):
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(
            host, port, loop=self.loop, max_idle_ms=100000)

        with self.silence_loop_exception_handler():
            with self.assertWarnsRegex(
                    ResourceWarning, "Unclosed AIOKafkaConnection"):
                del conn
                gc.collect()

    @run_until_complete
    def test_basic_connection_load_meta(self):
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port, loop=self.loop)

        self.assertEqual(conn.connected(), True)
        request = MetadataRequest([])
        response = yield from conn.send(request)
        conn.close()
        self.assertIsInstance(response, MetadataResponse)

    @run_until_complete
    def test_connections_max_idle_ms(self):
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(
            host, port, loop=self.loop, max_idle_ms=200)
        self.assertEqual(conn.connected(), True)
        yield from asyncio.sleep(0.1, loop=self.loop)
        # Do some work
        request = MetadataRequest([])
        yield from conn.send(request)
        yield from asyncio.sleep(0.15, loop=self.loop)
        # Check if we're stil connected after 250ms, as we were not idle
        self.assertEqual(conn.connected(), True)

        # It shouldn't break if we have a long running call either
        readexactly = conn._reader.readexactly
        with mock.patch.object(conn._reader, 'readexactly') as mocked:
            @asyncio.coroutine
            def long_read(n):
                yield from asyncio.sleep(0.2, loop=self.loop)
                return (yield from readexactly(n))
            mocked.side_effect = long_read
            yield from conn.send(MetadataRequest([]))
        self.assertEqual(conn.connected(), True)

        yield from asyncio.sleep(0.2, loop=self.loop)
        self.assertEqual(conn.connected(), False)

    @run_until_complete
    def test_send_without_response(self):
        """Imitate producer without acknowledge, in this case client produces
        messages and kafka does not send response, and we make sure that
        futures do not stuck in queue forever"""

        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port, loop=self.loop)

        # prepare message
        msg = Message(b'foo')
        request = ProduceRequest(
            required_acks=0, timeout=10 * 1000,
            topics=[(b'foo', [(0, [(0, msg.encode())])])])

        # produce messages without acknowledge
        for i in range(100):
            conn.send(request, expect_response=False)
        # make sure futures no stuck in queue
        self.assertEqual(len(conn._requests), 0)
        conn.close()

    @run_until_complete
    def test_send_to_closed(self):
        host, port = self.kafka_host, self.kafka_port
        conn = AIOKafkaConnection(host=host, port=port, loop=self.loop)
        request = MetadataRequest([])
        with self.assertRaises(ConnectionError):
            yield from conn.send(request)

        conn._writer = mock.MagicMock()
        conn._writer.write.side_effect = OSError('mocked writer is closed')

        with self.assertRaises(ConnectionError):
            yield from conn.send(request)

    @run_until_complete
    def test_invalid_correlation_id(self):
        host, port = self.kafka_host, self.kafka_port

        request = MetadataRequest([])

        # setup connection with mocked reader and writer
        conn = AIOKafkaConnection(host=host, port=port, loop=self.loop)

        # setup reader
        reader = mock.MagicMock()
        int32 = struct.Struct('>i')
        resp = MetadataResponse(brokers=[], topics=[])
        resp = resp.encode()
        resp = int32.pack(999) + resp  # set invalid correlation id
        reader.readexactly.side_effect = [
            asyncio.coroutine(lambda *a, **kw: int32.pack(len(resp)))(),
            asyncio.coroutine(lambda *a, **kw: resp)()]
        writer = mock.MagicMock()

        conn._reader = reader
        conn._writer = writer
        # invoke reader task
        conn._read_task = conn._create_reader_task()

        with self.assertRaises(CorrelationIdError):
            yield from conn.send(request)

    @run_until_complete
    def test_correlation_id_on_group_coordinator_req(self):
        host, port = self.kafka_host, self.kafka_port

        request = GroupCoordinatorRequest(consumer_group='test')

        # setup connection with mocked reader and writer
        conn = AIOKafkaConnection(host=host, port=port, loop=self.loop)

        # setup reader
        reader = mock.MagicMock()
        int32 = struct.Struct('>i')
        resp = GroupCoordinatorResponse(
            error_code=0, coordinator_id=22,
            host='127.0.0.1', port=3333)
        resp = resp.encode()
        resp = int32.pack(0) + resp  # set correlation id to 0
        reader.readexactly.side_effect = [
            asyncio.coroutine(lambda *a, **kw: int32.pack(len(resp)))(),
            asyncio.coroutine(lambda *a, **kw: resp)()]
        writer = mock.MagicMock()

        conn._reader = reader
        conn._writer = writer
        # invoke reader task
        conn._read_task = conn._create_reader_task()

        response = yield from conn.send(request)
        self.assertIsInstance(response, GroupCoordinatorResponse)
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.coordinator_id, 22)
        self.assertEqual(response.host, '127.0.0.1')
        self.assertEqual(response.port, 3333)

    @run_until_complete
    def test_osserror_in_reader_task(self):
        host, port = self.kafka_host, self.kafka_port

        @asyncio.coroutine
        def invoke_osserror(*a, **kw):
            yield from asyncio.sleep(0.1, loop=self.loop)
            raise OSError('test oserror')

        request = MetadataRequest([])
        # setup connection with mocked reader and writer
        conn = AIOKafkaConnection(host=host, port=port, loop=self.loop)

        # setup reader
        reader = mock.MagicMock()
        reader.readexactly.return_value = invoke_osserror()
        writer = mock.MagicMock()

        conn._reader = reader
        conn._writer = writer
        # invoke reader task
        conn._read_task = conn._create_reader_task()

        with self.assertRaises(ConnectionError):
            yield from conn.send(request)
        self.assertEqual(conn.connected(), False)

    @run_until_complete
    def test_close_disconnects_connection(self):
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port, loop=self.loop)
        self.assertTrue(conn.connected())
        conn.close()
        self.assertFalse(conn.connected())
