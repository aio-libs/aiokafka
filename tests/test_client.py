import asyncio
import unittest
from unittest import mock
from aiokafka.client import AIOKafkaClient
from kafka.common import KafkaUnavailableError


class TestAIOKafkaClinet(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_init_with_list(self):
        client = AIOKafkaClient(
            ['kafka01:9092', 'kafka02:9092', 'kafka03:9092'], loop=self.loop)

        self.assertEqual(sorted({'kafka01': 9092,
                                 'kafka02': 9092,
                                 'kafka03': 9092}.items()),
                         sorted(client.hosts))

    def test_init_with_csv(self):
        client = AIOKafkaClient('kafka01:9092,kafka02:9092,kafka03:9092',
                                loop=self.loop)

        self.assertEqual(sorted({'kafka01': 9092,
                                 'kafka02': 9092,
                                 'kafka03': 9092}.items()),
                         sorted(client.hosts))

    def test_send_broker_unaware_request_fail(self):
        'Tests that call fails when all hosts are unavailable'

        mocked_conns = {
            ('kafka01', 9092): mock.MagicMock(),
            ('kafka02', 9092): mock.MagicMock()
        }

        # inject KafkaConnection side effects
        mocked_conns[('kafka01', 9092)].send.side_effect = RuntimeError(
            "kafka01 went away (unittest)")
        mocked_conns[('kafka02', 9092)].send.side_effect = RuntimeError(
            "Kafka02 went away (unittest)")

        client = AIOKafkaClient(['kafka01:9092', 'kafka02:9092'],
                                loop=self.loop)
        client._conns = mocked_conns

        @asyncio.coroutine
        def go():
            with self.assertRaises(KafkaUnavailableError):
                yield from client._send_broker_unaware_request(
                    payloads=['fake request'],
                    encoder_fn=mock.MagicMock(
                        return_value=b'fake encoded message'),
                    decoder_fn=lambda x: x)

            for key, conn in mocked_conns.items():
                conn.send.assert_called_with(mock.ANY,
                                             b'fake encoded message')

        self.loop.run_until_complete(go())

    def test_send_broker_unaware_request(self):
        'Tests that call works when at least one of the host is available'

        mocked_conns = {
            ('kafka01', 9092): mock.MagicMock(),
            ('kafka02', 9092): mock.MagicMock(),
            ('kafka03', 9092): mock.MagicMock()
        }
        # inject KafkaConnection side effects
        mocked_conns[('kafka01', 9092)].send.side_effect = RuntimeError(
            "kafka01 went away (unittest)")

        @asyncio.coroutine
        def recv(request_id):
            return b'valid response'

        mocked_conns[('kafka02', 9092)].recv.side_effect = recv
        mocked_conns[('kafka03', 9092)].send.side_effect = RuntimeError(
            "kafka03 went away (unittest)")

        client = AIOKafkaClient('kafka01:9092,kafka02:9092', loop=self.loop)
        client._conns = mocked_conns

        resp = self.loop.run_until_complete(
            client._send_broker_unaware_request(payloads=[b'fake request'],
                                                encoder_fn=mock.MagicMock(),
                                                decoder_fn=lambda x: x))

        self.assertEqual(b'valid response', resp)
        mocked_conns[('kafka02', 9092)].recv.assert_called_with(0)
