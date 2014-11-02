import asyncio
import unittest
from unittest import mock
from aiokafka.client import AIOKafkaClient
from kafka.common import (KafkaUnavailableError, BrokerMetadata, TopicMetadata,
                          PartitionMetadata, TopicAndPartition,
                          LeaderNotAvailableError,
                          UnknownTopicOrPartitionError,
                          MetadataResponse, KafkaTimeoutError)


NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
NO_LEADER = 5


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
        self.assertTrue(mocked_conns[('kafka02', 9092)].recv.called)

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_load_metadata(self, protocol):

        @asyncio.coroutine
        def recv(request_id):
            return b'response'

        mocked_conns = {('broker_1', 4567): mock.MagicMock()}
        mocked_conns[('broker_1', 4567)].recv.side_effect = recv
        client = AIOKafkaClient(['broker_1:4567'], loop=self.loop)
        client._conns = mocked_conns

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_1', NO_ERROR, [
                PartitionMetadata('topic_1', 0, 1, [1, 2], [1, 2], NO_ERROR)
            ]),
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader', 0, -1, [], [],
                                  NO_LEADER),
                PartitionMetadata('topic_noleader', 1, -1, [], [],
                                  NO_LEADER),
            ]),
            TopicMetadata('topic_no_partitions', NO_LEADER, []),
            TopicMetadata('topic_unknown', UNKNOWN_TOPIC_OR_PARTITION, []),
            TopicMetadata('topic_3', NO_ERROR, [
                PartitionMetadata('topic_3', 0, 0, [0, 1], [0, 1], NO_ERROR),
                PartitionMetadata('topic_3', 1, 1, [1, 0], [1, 0], NO_ERROR),
                PartitionMetadata('topic_3', 2, 0, [0, 1], [0, 1], NO_ERROR)
            ])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())
        self.assertDictEqual({
            TopicAndPartition('topic_1', 0): brokers[1],
            TopicAndPartition('topic_noleader', 0): None,
            TopicAndPartition('topic_noleader', 1): None,
            TopicAndPartition('topic_3', 0): brokers[0],
            TopicAndPartition('topic_3', 1): brokers[1],
            TopicAndPartition('topic_3', 2): brokers[0]},
            client._topics_to_brokers)

        # if we ask for metadata explicitly, it should raise errors
        with self.assertRaises(LeaderNotAvailableError):
            self.loop.run_until_complete(
                client.load_metadata_for_topics('topic_no_partitions'))

        with self.assertRaises(UnknownTopicOrPartitionError):
            self.loop.run_until_complete(
                client.load_metadata_for_topics('topic_unknown'))

        # This should not raise
        self.loop.run_until_complete(
            client.load_metadata_for_topics('topic_no_leader'))

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_has_metadata_for_topic(self, protocol):

        @asyncio.coroutine
        def recv(request_id):
            return b'response'

        mocked_conns = {('broker_1', 4567): mock.MagicMock()}
        mocked_conns[('broker_1', 4567)].recv.side_effect = recv
        client = AIOKafkaClient(['broker_1:4567'], loop=self.loop)
        client._conns = mocked_conns

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_still_creating', NO_LEADER, []),
            TopicMetadata('topic_doesnt_exist',
                          UNKNOWN_TOPIC_OR_PARTITION, []),
            TopicMetadata('topic_noleaders', NO_ERROR, [
                PartitionMetadata('topic_noleaders', 0, -1, [], [], NO_LEADER),
                PartitionMetadata('topic_noleaders', 1, -1, [], [], NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())

        # Topics with no partitions return False
        self.assertFalse(client.has_metadata_for_topic('topic_still_creating'))
        self.assertFalse(client.has_metadata_for_topic('topic_doesnt_exist'))

        # Topic with partition metadata, but no leaders return True
        self.assertTrue(client.has_metadata_for_topic('topic_noleaders'))

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_ensure_topic_exists(self, protocol):

        @asyncio.coroutine
        def recv(request_id):
            return b'response'

        mocked_conns = {('broker_1', 4567): mock.MagicMock()}
        mocked_conns[('broker_1', 4567)].recv.side_effect = recv
        client = AIOKafkaClient(['broker_1:4567'], loop=self.loop)
        client._conns = mocked_conns

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_still_creating', NO_LEADER, []),
            TopicMetadata('topic_doesnt_exist',
                          UNKNOWN_TOPIC_OR_PARTITION, []),
            TopicMetadata('topic_noleaders', NO_ERROR, [
                PartitionMetadata('topic_noleaders', 0, -1, [], [], NO_LEADER),
                PartitionMetadata('topic_noleaders', 1, -1, [], [], NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())

        with self.assertRaises(UnknownTopicOrPartitionError):
            self.loop.run_until_complete(
                client.ensure_topic_exists('topic_doesnt_exist', timeout=1))

        with self.assertRaises(KafkaTimeoutError):
            self.loop.run_until_complete(
                client.ensure_topic_exists('topic_still_creating', timeout=1))

        # This should not raise
        self.loop.run_until_complete(
            client.ensure_topic_exists('topic_noleaders', timeout=1))

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_get_leader_for_partitions_reloads_metadata(self, protocol):
        "Get leader for partitions reload metadata if it is not available"

        @asyncio.coroutine
        def recv(request_id):
            return b'response'

        mocked_conns = {('broker_1', 4567): mock.MagicMock()}
        mocked_conns[('broker_1', 4567)].recv.side_effect = recv
        client = AIOKafkaClient(['broker_1:4567'], loop=self.loop)
        client._conns = mocked_conns

        brokers = [
            BrokerMetadata(0, 'broker_1', 4567),
            BrokerMetadata(1, 'broker_2', 5678)
        ]

        topics = [
            TopicMetadata('topic_no_partitions', NO_LEADER, [])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())

        # topic metadata is loaded but empty
        self.assertDictEqual({}, client._topics_to_brokers)

        topics = [
            TopicMetadata('topic_one_partition', NO_ERROR, [
                PartitionMetadata('topic_no_partition',
                                  0, 0, [0, 1], [0, 1], NO_ERROR)
            ])
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        # calling _get_leader_for_partition (from any broker aware request)
        # will try loading metadata again for the same topic
        leader = self.loop.run_until_complete(client._get_leader_for_partition(
            'topic_one_partition', 0))

        self.assertEqual(brokers[0], leader)
        self.assertDictEqual({
            TopicAndPartition('topic_one_partition', 0): brokers[0]},
            client._topics_to_brokers)
