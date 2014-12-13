import asyncio
import unittest
from unittest import mock
from kafka.common import MetadataResponse
from kafka.protocol import KafkaProtocol

from aiokafka.conn import AIOKafkaConnection, create_conn
from .fixtures import ZookeeperFixture, KafkaFixture


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

    def xtest_send(self):
        conn = AIOKafkaConnection('localhost', 1234, loop=self.loop)
        conn._writer = mock.Mock()
        self.loop.run_until_complete(conn.send(b'data'))
        conn._writer.write.assert_called_with(b'data')

    def xtest_recv(self):
        conn = AIOKafkaConnection('localhost', 1234, loop=self.loop)
        conn._reader = mock.Mock()

        rets = [(4, AIOKafkaConnection.HEADER.pack(6)),
                (6, b'dataok')]
        idx = 0

        @asyncio.coroutine
        def readexactly(n):
            nonlocal idx
            self.assertEqual(rets[idx][0], n)
            ret = rets[idx][1]
            idx += 1
            return ret

        conn._reader.readexactly.side_effect = readexactly

        self.loop.run_until_complete(conn.recv(123))

        self.assertEqual(2, idx)


class ConnIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    @classmethod
    def setUpClass(cls):
        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)

    @classmethod
    def tearDownClass(cls):
        cls.server.close()
        cls.zk.close()

    def test_basic_connection_load_meta(self):
        host, port = self.server.host, self.server.port
        conn = self.loop.run_until_complete(
            create_conn(host, port, loop=self.loop))

        encoder = KafkaProtocol.encode_metadata_request
        decoder = KafkaProtocol.decode_metadata_response

        request_id = 1
        client_id = b"aiokafka-python"
        payloads = ()
        request = encoder(client_id=client_id, correlation_id=request_id,
                          payloads=payloads)
        response = None

        @asyncio.coroutine
        def get_metadata():
            fut = conn.send(request)
            raw_response = yield from fut
            nonlocal response
            response = decoder(raw_response)
            conn.close()

        self.loop.run_until_complete(get_metadata())
        self.assertIsInstance(response, MetadataResponse)
