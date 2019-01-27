import asyncio
import gc
import pytest
import struct
import unittest
from unittest import mock
from kafka.protocol.metadata import (
    MetadataRequest_v0 as MetadataRequest,
    MetadataResponse_v0 as MetadataResponse)
from kafka.protocol.commit import (
    GroupCoordinatorRequest_v0 as GroupCoordinatorRequest,
    GroupCoordinatorResponse_v0 as GroupCoordinatorResponse)
from kafka.protocol.admin import (
    SaslHandShakeRequest, SaslHandShakeResponse, SaslAuthenticateRequest,
    SaslAuthenticateResponse
)

from aiokafka.conn import AIOKafkaConnection, create_conn, VersionInfo
from aiokafka.errors import (
    ConnectionError, CorrelationIdError, KafkaError, NoError, UnknownError,
    UnsupportedSaslMechanismError, IllegalSaslStateError
)
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder
from aiokafka.util import PY_341
from ._testutil import KafkaIntegrationTestCase, run_until_complete
from aiokafka.protocol.produce import ProduceRequest_v0 as ProduceRequest


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
        builder = LegacyRecordBatchBuilder(
            magic=1, compression_type=0, batch_size=99999999)
        builder.append(offset=0, value=b"foo", key=None, timestamp=None)
        request = ProduceRequest(
            required_acks=0, timeout=10 * 1000,
            topics=[(b'foo', [(0, bytes(builder.build()))])])

        # produce messages without acknowledge
        req = []
        for i in range(10):
            req.append(conn.send(request, expect_response=False))
        # make sure futures no stuck in queue
        self.assertEqual(len(conn._requests), 0)
        for x in req:
            yield from x
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

    def test_connection_version_info(self):
        # All version supported
        version_info = VersionInfo({
            SaslHandShakeRequest[0].API_KEY: [0, 1]
        })
        self.assertEqual(
            version_info.pick_best(SaslHandShakeRequest),
            SaslHandShakeRequest[1])

        # Broker only supports the lesser version
        version_info = VersionInfo({
            SaslHandShakeRequest[0].API_KEY: [0, 0]
        })
        self.assertEqual(
            version_info.pick_best(SaslHandShakeRequest),
            SaslHandShakeRequest[0])

        # We don't support any version compatible with the broker
        version_info = VersionInfo({
            SaslHandShakeRequest[0].API_KEY: [2, 3]
        })
        with self.assertRaises(KafkaError):
            self.assertEqual(
                version_info.pick_best(SaslHandShakeRequest),
                SaslHandShakeRequest[1])

        # No information on the supported versions
        version_info = VersionInfo({})
        self.assertEqual(
            version_info.pick_best(SaslHandShakeRequest),
            SaslHandShakeRequest[0])

    @run_until_complete
    def test__do_sasl_handshake_v0(self):
        host, port = self.kafka_host, self.kafka_port

        # setup connection with mocked send and send_bytes
        conn = AIOKafkaConnection(
            host=host, port=port, loop=self.loop,
            sasl_mechanism="PLAIN",
            sasl_plain_username="admin",
            sasl_plain_password="123"
        )
        conn.close = close_mock = mock.MagicMock()

        supported_mechanisms = ["PLAIN"]
        error_class = NoError

        @asyncio.coroutine
        def mock_send(request, expect_response=True):
            return SaslHandShakeResponse[0](
                error_code=error_class.errno,
                enabled_mechanisms=supported_mechanisms
            )

        @asyncio.coroutine
        def mock_sasl_send(payload, expect_response):
            return b""

        conn.send = mock.Mock(side_effect=mock_send)
        conn._send_sasl_token = mock.Mock(side_effect=mock_sasl_send)
        conn._version_info = VersionInfo({
            SaslHandShakeRequest[0].API_KEY: [0, 0]
        })

        yield from conn._do_sasl_handshake()

        supported_mechanisms = ["GSSAPI"]
        with self.assertRaises(UnsupportedSaslMechanismError):
            yield from conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)

        error_class = UnknownError
        close_mock.reset()

        with self.assertRaises(UnknownError):
            yield from conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)

    @run_until_complete
    def test__do_sasl_handshake_v1(self):
        host, port = self.kafka_host, self.kafka_port

        # setup connection with mocked send and send_bytes
        conn = AIOKafkaConnection(
            host=host, port=port, loop=self.loop,
            sasl_mechanism="PLAIN",
            sasl_plain_username="admin",
            sasl_plain_password="123",
            security_protocol="SASL_PLAINTEXT"
        )
        conn.close = close_mock = mock.MagicMock()

        supported_mechanisms = ["PLAIN"]
        error_class = NoError
        auth_error_class = NoError

        @asyncio.coroutine
        def mock_send(request, expect_response=True):
            if request.API_KEY == SaslHandShakeRequest[0].API_KEY:
                assert request.API_VERSION == 1
                return SaslHandShakeResponse[1](
                    error_code=error_class.errno,
                    enabled_mechanisms=supported_mechanisms
                )
            else:
                assert request.API_KEY == SaslAuthenticateRequest[0].API_KEY
                return SaslAuthenticateResponse[0](
                    error_code=auth_error_class.errno,
                    error_message="",
                    sasl_auth_bytes=b""
                )

        conn.send = mock.Mock(side_effect=mock_send)
        conn._version_info = VersionInfo({
            SaslHandShakeRequest[0].API_KEY: [0, 1]
        })

        yield from conn._do_sasl_handshake()

        supported_mechanisms = ["GSSAPI"]
        with self.assertRaises(UnsupportedSaslMechanismError):
            yield from conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)
        supported_mechanisms = ["PLAIN"]

        auth_error_class = IllegalSaslStateError
        close_mock.reset()

        with self.assertRaises(IllegalSaslStateError):
            yield from conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)
        auth_error_class = NoError

        error_class = UnknownError
        close_mock.reset()

        with self.assertRaises(UnknownError):
            yield from conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)

    @run_until_complete
    def test__send_sasl_token(self):
        # Before Kafka 1.0.0 SASL was performed on the wire without
        # KAFKA_HEADER in the protocol. So we needed another private
        # function to send `raw` data with only length prefixed

        # setup connection with mocked transport and protocol
        conn = AIOKafkaConnection(
            host="", port=9999, loop=self.loop
        )
        conn.close = mock.MagicMock()
        conn._writer = mock.MagicMock()
        out_buffer = []
        conn._writer.write = mock.Mock(side_effect=out_buffer.append)
        conn._reader = mock.MagicMock()
        self.assertEqual(len(conn._requests), 0)

        # Successful send
        fut = conn._send_sasl_token(b"Super data")
        self.assertEqual(b''.join(out_buffer), b"\x00\x00\x00\nSuper data")
        self.assertEqual(len(conn._requests), 1)
        out_buffer.clear()

        # Resolve the request
        conn._requests[0][2].set_result(None)
        conn._requests.clear()
        yield from fut

        # Broken pipe error
        conn._writer.write.side_effect = OSError
        with self.assertRaises(ConnectionError):
            conn._send_sasl_token(b"Super data")
        self.assertEqual(out_buffer, [])
        self.assertEqual(len(conn._requests), 0)
        self.assertEqual(conn.close.call_count, 1)

        conn._writer = None
        with self.assertRaises(ConnectionError):
            conn._send_sasl_token(b"Super data")
        # We don't need to close 2ce
        self.assertEqual(conn.close.call_count, 1)
