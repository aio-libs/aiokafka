import asyncio
import gc
import struct
from typing import Any
from unittest import mock

import pytest

from aiokafka.conn import AIOKafkaConnection, create_conn
from aiokafka.errors import (
    CorrelationIdError,
    IllegalSaslStateError,
    KafkaConnectionError,
    NoError,
    UnknownError,
    UnsupportedSaslMechanismError,
)
from aiokafka.protocol.admin import (
    CreateTopicsRequest,
    SaslAuthenticateRequest,
    SaslAuthenticateResponse_v0,
    SaslHandShakeRequest,
    SaslHandShakeResponse_v0,
    SaslHandShakeResponse_v1,
)
from aiokafka.protocol.coordination import (
    FindCoordinatorRequest,
    FindCoordinatorResponse_v0,
)
from aiokafka.protocol.metadata import MetadataRequest
from aiokafka.protocol.metadata import MetadataResponse_v0 as MetadataResponse
from aiokafka.protocol.produce import ProduceRequest
from aiokafka.record.default_records import DefaultRecordBatchBuilder
from aiokafka.util import get_running_loop

from ._testutil import KafkaIntegrationTestCase, run_until_complete


@pytest.mark.usefixtures("setup_test_class")
class ConnIntegrationTest(KafkaIntegrationTestCase):
    @run_until_complete
    async def test_ctor(self):
        conn = AIOKafkaConnection("localhost", 1234)
        self.assertEqual("localhost", conn.host)
        self.assertEqual(1234, conn.port)
        self.assertTrue("KafkaConnection" in conn.__repr__())
        self.assertIsNone(conn._reader)
        self.assertIsNone(conn._writer)

    @run_until_complete
    async def test_global_loop_for_create_conn(self):
        loop = get_running_loop()
        host, port = self.kafka_host, self.kafka_port
        conn = await create_conn(host, port)
        self.assertIs(conn._loop, loop)
        conn.close()
        # make sure second closing does nothing and we have full coverage
        # of *if self._reader:* condition
        conn.close()

    @run_until_complete
    async def test_conn_warn_unclosed(self):
        host, port = self.kafka_host, self.kafka_port
        conn = await create_conn(host, port, max_idle_ms=100000)

        with self.silence_loop_exception_handler():
            with self.assertWarnsRegex(ResourceWarning, "Unclosed AIOKafkaConnection"):
                del conn
                gc.collect()

    @run_until_complete
    async def test_basic_connection_load_meta(self):
        host, port = self.kafka_host, self.kafka_port
        conn = await create_conn(host, port)

        self.assertEqual(conn.connected(), True)
        request = MetadataRequest([])
        response = await conn.send(request)
        conn.close()
        self.assertEqual(response.API_KEY, 3)

    @run_until_complete
    async def test_connections_max_idle_ms(self):
        host, port = self.kafka_host, self.kafka_port
        conn = await create_conn(host, port, max_idle_ms=200)
        self.assertEqual(conn.connected(), True)
        await asyncio.sleep(0.1)
        # Do some work
        request = MetadataRequest([])
        await conn.send(request)
        await asyncio.sleep(0.15)
        # Check if we're still connected after 250ms, as we were not idle
        self.assertEqual(conn.connected(), True)

        # It shouldn't break if we have a long running call either
        readexactly = conn._reader.readexactly
        with mock.patch.object(conn._reader, "readexactly") as mocked:

            async def long_read(n):
                await asyncio.sleep(0.2)
                return await readexactly(n)

            mocked.side_effect = long_read
            await conn.send(MetadataRequest([]))
        self.assertEqual(conn.connected(), True)

        await asyncio.sleep(0.2)
        self.assertEqual(conn.connected(), False)

    @run_until_complete
    async def test_send_without_response(self):
        """Imitate producer without acknowledge, in this case client produces
        messages and kafka does not send response, and we make sure that
        futures do not stuck in queue forever"""

        host, port = self.kafka_host, self.kafka_port
        conn = await create_conn(host, port)

        # create a topic
        async def create_topic(topic):
            await conn.send(
                CreateTopicsRequest(
                    create_topic_requests=[("foo", 1, 1, [], [])],
                    timeout=1000,
                    validate_only=False,
                )
            )
            for _ in range(5):
                ok = await conn.send(
                    MetadataRequest(["foo"], allow_auto_topic_creation=False)
                )
                if ok:
                    ok = topic in (t for _, t, *_ in ok.topics)
                if not ok:
                    await asyncio.sleep(1)
                else:
                    return
            raise AssertionError(f'No topic "{topic}" exists')

        await create_topic("foo")

        # prepare message
        builder = DefaultRecordBatchBuilder(
            magic=2,
            compression_type=0,
            batch_size=99999999,
            is_transactional=0,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=0,
        )
        builder.append(offset=0, value=b"foo", key=None, timestamp=None, headers=[])
        request = ProduceRequest(
            required_acks=0,
            timeout=10 * 1000,
            topics=[("foo", [(0, bytes(builder.build()))])],
            transactional_id=None,
        )

        # produce messages without acknowledge
        req = [
            asyncio.create_task(conn.send(request, expect_response=False))
            for _ in range(10)
        ]
        # make sure futures no stuck in queue
        self.assertEqual(len(conn._requests), 0)
        for x in req:
            await x
        conn.close()

    @run_until_complete
    async def test_send_to_closed(self):
        host, port = self.kafka_host, self.kafka_port
        conn = AIOKafkaConnection(host=host, port=port)
        conn._versions = {MetadataRequest.API_KEY: (0, 0)}
        request = MetadataRequest([])
        with self.assertRaises(KafkaConnectionError):
            await conn.send(request)

        conn._writer = mock.MagicMock()
        conn._writer.write.side_effect = OSError("mocked writer is closed")

        with self.assertRaises(KafkaConnectionError):
            await conn.send(request)

    @run_until_complete
    async def test_invalid_correlation_id(self):
        host, port = self.kafka_host, self.kafka_port

        request = MetadataRequest([])

        # setup connection with mocked reader and writer
        conn = AIOKafkaConnection(host=host, port=port)
        conn._versions = {MetadataRequest.API_KEY: (0, 0)}

        # setup reader
        reader = mock.MagicMock()
        int32 = struct.Struct(">i")
        resp = MetadataResponse(brokers=[], topics=[])
        resp = resp.encode()
        resp = int32.pack(999) + resp  # set invalid correlation id

        async def first_resp(*args: Any, **kw: Any):
            return int32.pack(len(resp))

        async def second_resp(*args: Any, **kw: Any):
            return resp

        reader.readexactly.side_effect = [first_resp(), second_resp()]
        writer = mock.MagicMock()

        conn._reader = reader
        conn._writer = writer
        # invoke reader task
        conn._read_task = conn._create_reader_task()

        with self.assertRaises(CorrelationIdError):
            await conn.send(request)

    @run_until_complete
    async def test_correlation_id_on_group_coordinator_req(self):
        host, port = self.kafka_host, self.kafka_port

        request = FindCoordinatorRequest(coordinator_key="test", coordinator_type=0)

        # setup connection with mocked reader and writer
        conn = AIOKafkaConnection(host=host, port=port)
        conn._versions = {FindCoordinatorRequest.API_KEY: (0, 0)}

        # setup reader
        reader = mock.MagicMock()
        int32 = struct.Struct(">i")
        resp = FindCoordinatorResponse_v0(
            error_code=0, coordinator_id=22, host="127.0.0.1", port=3333
        )
        resp = resp.encode()
        resp = int32.pack(0) + resp  # set correlation id to 0

        async def first_resp(*args: Any, **kw: Any):
            return int32.pack(len(resp))

        async def second_resp(*args: Any, **kw: Any):
            return resp

        reader.readexactly.side_effect = [first_resp(), second_resp()]
        writer = mock.MagicMock()

        conn._reader = reader
        conn._writer = writer
        # invoke reader task
        conn._read_task = conn._create_reader_task()

        response = await conn.send(request)
        self.assertIsInstance(response, FindCoordinatorResponse_v0)
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.coordinator_id, 22)
        self.assertEqual(response.host, "127.0.0.1")
        self.assertEqual(response.port, 3333)

    @run_until_complete
    async def test_osserror_in_reader_task(self):
        host, port = self.kafka_host, self.kafka_port

        async def invoke_osserror(*a, **kw):
            await asyncio.sleep(0.1)
            raise OSError("test oserror")

        request = MetadataRequest([])
        # setup connection with mocked reader and writer
        conn = AIOKafkaConnection(host=host, port=port)
        conn._versions = {MetadataRequest.API_KEY: (0, 0)}

        # setup reader
        reader = mock.MagicMock()
        reader.readexactly.return_value = invoke_osserror()
        writer = mock.MagicMock()

        conn._reader = reader
        conn._writer = writer
        # invoke reader task
        conn._read_task = conn._create_reader_task()

        with self.assertRaises(KafkaConnectionError):
            await conn.send(request)
        self.assertEqual(conn.connected(), False)

    @run_until_complete
    async def test_close_disconnects_connection(self):
        host, port = self.kafka_host, self.kafka_port
        conn = await create_conn(host, port)
        self.assertTrue(conn.connected())
        conn.close()
        self.assertFalse(conn.connected())

    @run_until_complete
    async def test__do_sasl_handshake_v0(self):
        host, port = self.kafka_host, self.kafka_port

        # setup connection with mocked send and send_bytes
        conn = AIOKafkaConnection(
            host=host,
            port=port,
            sasl_mechanism="PLAIN",
            sasl_plain_username="admin",
            sasl_plain_password="123",
        )
        conn.close = close_mock = mock.MagicMock()

        supported_mechanisms = ["PLAIN"]
        error_class = NoError

        async def mock_send(request, expect_response=True):
            return SaslHandShakeResponse_v0(
                error_code=error_class.errno, enabled_mechanisms=supported_mechanisms
            )

        async def mock_sasl_send(payload, expect_response):
            return b""

        conn.send = mock.Mock(side_effect=mock_send)
        conn._send_sasl_token = mock.Mock(side_effect=mock_sasl_send)

        await conn._do_sasl_handshake()

        supported_mechanisms = ["GSSAPI"]
        with self.assertRaises(UnsupportedSaslMechanismError):
            await conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)

        error_class = UnknownError
        close_mock.reset()

        with self.assertRaises(UnknownError):
            await conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)

    @run_until_complete
    async def test__do_sasl_handshake_v1(self):
        host, port = self.kafka_host, self.kafka_port

        # setup connection with mocked send and send_bytes
        conn = AIOKafkaConnection(
            host=host,
            port=port,
            sasl_mechanism="PLAIN",
            sasl_plain_username="admin",
            sasl_plain_password="123",
            security_protocol="SASL_PLAINTEXT",
        )
        conn.close = close_mock = mock.MagicMock()

        supported_mechanisms = ["PLAIN"]
        error_class = NoError
        auth_error_class = NoError

        async def mock_send(request, expect_response=True):
            if request.API_KEY == SaslHandShakeRequest.API_KEY:
                assert (
                    request.prepare({SaslHandShakeRequest.API_KEY: [0, 1]}).API_VERSION
                    == 1
                )
                return SaslHandShakeResponse_v1(
                    error_code=error_class.errno,
                    enabled_mechanisms=supported_mechanisms,
                )
            else:
                assert request.API_KEY == SaslAuthenticateRequest.API_KEY
                return SaslAuthenticateResponse_v0(
                    error_code=auth_error_class.errno,
                    error_message="",
                    sasl_auth_bytes=b"",
                )

        conn.send = mock.Mock(side_effect=mock_send)

        await conn._do_sasl_handshake()

        supported_mechanisms = ["GSSAPI"]
        with self.assertRaises(UnsupportedSaslMechanismError):
            await conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)
        supported_mechanisms = ["PLAIN"]

        auth_error_class = IllegalSaslStateError
        close_mock.reset()

        with self.assertRaises(IllegalSaslStateError):
            await conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)
        auth_error_class = NoError

        error_class = UnknownError
        close_mock.reset()

        with self.assertRaises(UnknownError):
            await conn._do_sasl_handshake()
        self.assertTrue(close_mock.call_count)

    @run_until_complete
    async def test__send_sasl_token(self):
        # Before Kafka 1.0.0 SASL was performed on the wire without
        # KAFKA_HEADER in the protocol. So we needed another private
        # function to send `raw` data with only length prefixed

        # setup connection with mocked transport and protocol
        conn = AIOKafkaConnection(host="", port=9999)
        conn.close = mock.MagicMock()
        conn._writer = mock.MagicMock()
        out_buffer = []
        conn._writer.write = mock.Mock(side_effect=out_buffer.append)
        conn._reader = mock.MagicMock()
        self.assertEqual(len(conn._requests), 0)

        # Successful send
        fut = conn._send_sasl_token(b"Super data")
        self.assertEqual(b"".join(out_buffer), b"\x00\x00\x00\nSuper data")
        self.assertEqual(len(conn._requests), 1)
        out_buffer.clear()

        # Resolve the request
        conn._requests[0][2].set_result(None)
        conn._requests.clear()
        await fut

        # Broken pipe error
        conn._writer.write.side_effect = OSError
        with self.assertRaises(KafkaConnectionError):
            conn._send_sasl_token(b"Super data")
        self.assertEqual(out_buffer, [])
        self.assertEqual(len(conn._requests), 0)
        self.assertEqual(conn.close.call_count, 1)

        conn._writer = None
        with self.assertRaises(KafkaConnectionError):
            conn._send_sasl_token(b"Super data")
        # We don't need to close 2ce
        self.assertEqual(conn.close.call_count, 1)
