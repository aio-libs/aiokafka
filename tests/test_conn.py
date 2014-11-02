import asyncio
import unittest
from unittest import mock


from aiokafka.conn import KafkaConnection


class ConnTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.conn = KafkaConnection('localhost', 1234)

    def tearDown(self):
        self.loop.close()

    def test_ctor(self):
        conn = KafkaConnection('localhost', 1234)
        self.assertEqual('localhost', conn.host)
        self.assertEqual(1234, conn.port)
        self.assertIsNone(conn._reader)
        self.assertIsNone(conn._writer)

    def test_send(self):
        conn = KafkaConnection('localhost', 1234)
        conn._writer = mock.Mock()
        self.loop.run_until_complete(conn.send(123, b'data'))
        conn._writer.write.assert_called_with(b'data')

    def test_recv(self):
        conn = KafkaConnection('localhost', 1234)
        conn._reader = mock.Mock()

        rets = [(4, KafkaConnection.HEADER.pack(6)),
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
