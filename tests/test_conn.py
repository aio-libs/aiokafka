import asyncio
import unittest
import functools
from unittest import mock
from kafka.common import MetadataResponse, ProduceRequest, ConnectionError
from kafka.protocol import KafkaProtocol, create_message_set

from aiokafka.conn import AIOKafkaConnection, create_conn
from ._testutil import BaseTest, run_until_complete


class ConnTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.conn = AIOKafkaConnection('localhost', 1234, loop=self.loop)

    def tearDown(self):
        self.loop.close()

    def test_ctor(self):
        conn = AIOKafkaConnection('localhost', 1234, loop=self.loop)
        self.assertEqual('localhost', conn.host)
        self.assertEqual(1234, conn.port)
        self.assertTrue('KafkaConnection' in conn.__repr__())
        self.assertIsNone(conn._reader)
        self.assertIsNone(conn._writer)


class ConnIntegrationTest(BaseTest):

    @run_until_complete
    def test_global_loop_for_create_conn(self):
        asyncio.set_event_loop(self.loop)
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port)
        self.assertIs(conn._loop, self.loop)
        conn.close()
        # make sure second closing does nothing and we have full coverage
        # of *if self._reader:* condition
        conn.close()

    @run_until_complete
    def test_basic_connection_load_meta(self):
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port, loop=self.loop)

        encoder = KafkaProtocol.encode_metadata_request
        decoder = KafkaProtocol.decode_metadata_response

        request_id = 1
        client_id = b"aiokafka-python"
        payloads = ()
        request = encoder(client_id=client_id, correlation_id=request_id,
                          payloads=payloads)
        fut = conn.send(request)
        raw_response = yield from fut
        response = decoder(raw_response)
        conn.close()
        self.assertIsInstance(response, MetadataResponse)

    @run_until_complete
    def test_send_without_response(self):
        """Imitate producer without acknowledge, in this case client produces
        messages and kafka does not send response, and we make sure that
        futures do not stuck in queue forever"""

        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port, loop=self.loop)

        # prepare message
        msgs = create_message_set([b'foo'], 0, None)
        req = ProduceRequest(b'bar', 0, msgs)

        encoder = functools.partial(
            KafkaProtocol.encode_produce_request,
            acks=0, timeout=int(10*1000))

        request_id = 1
        client_id = b"aiokafka-python"
        request = encoder(client_id=client_id, correlation_id=request_id,
                          payloads=[req])
        # produce messages without acknowledge
        for i in range(100):
            conn.send(request, no_ack=True)
        # make sure futures no stuck in queue
        self.assertEqual(len(conn._requests), 0)

    @run_until_complete
    def test_send_cancelled(self):
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port, loop=self.loop)

        encoder = KafkaProtocol.encode_metadata_request
        decoder = KafkaProtocol.decode_metadata_response

        request_id = 1
        client_id = b"aiokafka-python"
        payloads = ()
        request = encoder(client_id=client_id, correlation_id=request_id,
                          payloads=payloads)
        fut = conn.send(request)
        fut.cancel()
        asyncio.sleep(0.1, loop=self.loop)
        self.assertTrue(fut.cancelled())

        # make sure that connections still working
        raw_response = yield from conn.send(request)
        response = decoder(raw_response)
        self.assertIsInstance(response, MetadataResponse)
        conn.close()

    @run_until_complete
    def test_pending_futures(self):
        host, port = self.kafka_host, self.kafka_port
        conn = yield from create_conn(host, port, loop=self.loop)

        encoder = KafkaProtocol.encode_metadata_request

        request_id = 1
        client_id = b"aiokafka-python"
        payloads = ()
        request = encoder(client_id=client_id, correlation_id=request_id,
                          payloads=payloads)
        fut1 = conn.send(request)
        fut2 = conn.send(request)
        fut3 = conn.send(request)
        conn.close()

        self.assertTrue(fut1.cancelled())
        self.assertTrue(fut2.cancelled())
        self.assertTrue(fut3.cancelled())

    @run_until_complete
    def test_osserror_in_reader_task(self):
        host, port = self.kafka_host, self.kafka_port

        @asyncio.coroutine
        def invoke_osserror(*a, **kw):
            yield from asyncio.sleep(0.1, loop=self.loop)
            raise OSError

        encoder = KafkaProtocol.encode_metadata_request
        request = encoder(client_id=b"aiokafka-python", correlation_id=1,
                          payloads=())

        # setup connection with mocked reader and writer
        conn = AIOKafkaConnection(host=host, port=port, loop=self.loop)

        # setup reader
        reader = mock.MagicMock()
        reader.readexactly.return_value = invoke_osserror()
        writer = mock.MagicMock()

        conn._reader = reader
        conn._writer = writer
        # invoke reader task
        conn._read_task = asyncio.async(conn._read(), loop=self.loop)

        with self.assertRaises(ConnectionError):
            yield from conn.send(request)
