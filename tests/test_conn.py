import asyncio
import unittest
from unittest import mock


from aiokafka.conn import AIOKafkaConnection


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
        self.assertIsNone(conn._reader)
        self.assertIsNone(conn._writer)

    def test_send(self):
        conn = AIOKafkaConnection('localhost', 1234, loop=self.loop)
        conn._writer = mock.Mock()
        self.loop.run_until_complete(conn.send(123, b'data'))
        conn._writer.write.assert_called_with(b'data')

    def test_recv(self):
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
