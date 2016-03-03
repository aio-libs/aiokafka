import asyncio
import pytest
import unittest
from unittest import mock
from kafka.common import (KafkaUnavailableError, BrokerMetadata, TopicMetadata,
                          PartitionMetadata, TopicAndPartition,
                          LeaderNotAvailableError,
                          UnknownTopicOrPartitionError,
                          MetadataResponse, ProduceRequest, FetchRequest,
                          OffsetCommitRequest, OffsetFetchRequest,
                          KafkaTimeoutError)

from kafka.protocol import create_message

from aiokafka.client import AIOKafkaClient, connect
from ._testutil import KafkaIntegrationTestCase, run_until_complete


NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
NO_LEADER = 5
REPLICA_NOT_AVAILABLE = 9


@pytest.mark.usefixtures('setup_test_class')
class TestAIOKafkaClient(unittest.TestCase):

    def test_init_with_list(self):
        client = AIOKafkaClient(
            ['kafka01:9092', 'kafka02:9092', 'kafka03:9092'], loop=self.loop)
        self.assertTrue('KafkaClient' in client.__repr__())
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
        fut1 = asyncio.Future(loop=self.loop)
        fut1.set_exception(RuntimeError("kafka01 went away (unittest)"))
        mocked_conns[('kafka01', 9092)].send.return_value = fut1

        fut2 = asyncio.Future(loop=self.loop)
        fut2.set_exception(RuntimeError("kafka02 went away (unittest)"))
        mocked_conns[('kafka02', 9092)].send.return_value = fut2

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
                conn.send.assert_called_with(b'fake encoded message')

        self.loop.run_until_complete(go())

    def test_send_broker_unaware_request(self):
        'Tests that call works when at least one of the host is available'

        mocked_conns = {
            ('kafka01', 9092): mock.MagicMock(),
            ('kafka02', 9092): mock.MagicMock(),
            ('kafka03', 9092): mock.MagicMock()
        }
        # inject KafkaConnection side effects
        fut = asyncio.Future(loop=self.loop)
        fut.set_exception(RuntimeError("kafka01 went away (unittest)"))
        mocked_conns[('kafka01', 9092)].send.return_value = fut

        fut2 = asyncio.Future(loop=self.loop)
        fut2.set_result(b'valid response')
        mocked_conns[('kafka02', 9092)].send.return_value = fut2

        fut3 = asyncio.Future(loop=self.loop)
        fut3.set_exception(RuntimeError("kafka03 went away (unittest)"))
        mocked_conns[('kafka03', 9092)].send.return_value = fut3

        client = AIOKafkaClient('kafka01:9092,kafka02:9092', loop=self.loop)
        client._conns = mocked_conns

        resp = self.loop.run_until_complete(
            client._send_broker_unaware_request(payloads=[b'fake request'],
                                                encoder_fn=mock.MagicMock(),
                                                decoder_fn=lambda x: x))

        self.assertEqual(b'valid response', resp)

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
            ]),
            TopicMetadata('topic_4', NO_ERROR, [
                PartitionMetadata('topic_4', 0, 0, [0, 1], [0, 1], NO_ERROR),
                PartitionMetadata('topic_4', 1, 1, [1, 0], [1, 0],
                                  REPLICA_NOT_AVAILABLE),
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
            TopicAndPartition('topic_3', 2): brokers[0],
            TopicAndPartition('topic_4', 0): brokers[0],
            TopicAndPartition('topic_4', 1): brokers[1]},
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

        # This should not raise ReplicaNotAvailableError
        self.loop.run_until_complete(
            client.load_metadata_for_topics('topic_4'))

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

        mocked_conns = {('broker_1', 4567): mock.MagicMock()}
        fut = asyncio.Future(loop=self.loop)
        fut.set_result(b'response')
        mocked_conns[('broker_1', 4567)].send.return_value = fut
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

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_get_leader_for_unassigned_partitions(self, protocol):

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
            TopicMetadata('topic_no_partitions', NO_LEADER, []),
            TopicMetadata('topic_unknown', UNKNOWN_TOPIC_OR_PARTITION, []),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())

        self.assertDictEqual({}, client._topics_to_brokers)

        with self.assertRaises(LeaderNotAvailableError):
            self.loop.run_until_complete(
                client._get_leader_for_partition('topic_no_partitions', 0))

        with self.assertRaises(UnknownTopicOrPartitionError):
            self.loop.run_until_complete(
                client._get_leader_for_partition('topic_unknown', 0))

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_get_leader_exceptions_when_noleader(self, protocol):

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
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader', 0, -1, [], [],
                                  NO_LEADER),
                PartitionMetadata('topic_noleader', 1, -1, [], [],
                                  NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())
        self.assertDictEqual(
            {
                TopicAndPartition('topic_noleader', 0): None,
                TopicAndPartition('topic_noleader', 1): None
            },
            client._topics_to_brokers)

        # No leader partitions -- raise LeaderNotAvailableError
        with self.assertRaises(LeaderNotAvailableError):
            self.assertIsNone(
                self.loop.run_until_complete(
                    client._get_leader_for_partition('topic_noleader', 0)))
        with self.assertRaises(LeaderNotAvailableError):
            self.assertIsNone(
                self.loop.run_until_complete(
                    client._get_leader_for_partition('topic_noleader', 1)))

        # Unknown partitions -- raise UnknownTopicOrPartitionError
        with self.assertRaises(UnknownTopicOrPartitionError):
            self.assertIsNone(
                self.loop.run_until_complete(
                    client._get_leader_for_partition('topic_noleader', 2)))

        topics = [
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader',
                                  0, 0, [0, 1], [0, 1], NO_ERROR),
                PartitionMetadata('topic_noleader',
                                  1, 1, [1, 0], [1, 0], NO_ERROR)
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)
        self.assertEqual(brokers[0], self.loop.run_until_complete(
            client._get_leader_for_partition('topic_noleader', 0)))
        self.assertEqual(brokers[1], self.loop.run_until_complete(
            client._get_leader_for_partition('topic_noleader', 1)))

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_send_produce_request_raises_when_noleader(self, protocol):
        """Send producer request raises LeaderNotAvailableError
           if leader is not available"""

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
            TopicMetadata('topic_noleader', NO_ERROR, [
                PartitionMetadata('topic_noleader', 0, -1, [], [],
                                  NO_LEADER),
                PartitionMetadata('topic_noleader', 1, -1, [], [],
                                  NO_LEADER),
            ]),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())

        requests = [ProduceRequest(
            "topic_noleader", 0,
            [create_message("a"), create_message("b")])]

        with self.assertRaises(LeaderNotAvailableError):
            self.loop.run_until_complete(client.send_produce_request(requests))

    @mock.patch('aiokafka.client.KafkaProtocol')
    def test_send_produce_request_raises_when_topic_unknown(self, protocol):

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
            TopicMetadata('topic_doesnt_exist',
                          UNKNOWN_TOPIC_OR_PARTITION, []),
        ]
        protocol.decode_metadata_response.return_value = MetadataResponse(
            brokers, topics)

        self.loop.run_until_complete(client.load_metadata_for_topics())

        requests = [ProduceRequest(
            "topic_doesnt_exist", 0,
            [create_message("a"), create_message("b")])]

        with self.assertRaises(UnknownTopicOrPartitionError):
            self.loop.run_until_complete(client.send_produce_request(requests))

    def test__next_id(self):
        client = AIOKafkaClient(['broker_1:4567'], loop=self.loop)
        self.assertEqual(0, client._request_id)
        self.assertEqual(1, client._next_id())
        self.assertEqual(1, client._request_id)

    def test__next_id_overflow(self):
        client = AIOKafkaClient(['broker_1:4567'], loop=self.loop)
        client._request_id = 0x7fffffffff
        self.assertEqual(0, client._next_id())
        self.assertEqual(0, client._request_id)

    def test_close(self):
        client = AIOKafkaClient(['broker_1:4567'], loop=self.loop)
        m1 = mock.Mock()
        m2 = mock.Mock()
        client._conns = {('host1', 4567): m1, ('host2', 5678): m2}
        client.close()
        self.assertEqual({}, client._conns)
        m1.close.assert_raises_with()
        m2.close.assert_raises_with()

    # def test_timeout(self):
    #     def _timeout(*args, **kwargs):
    #         timeout = args[1]
    #         sleep(timeout)
    #         raise socket.timeout

    #     with patch.object(socket, "create_connection", side_effect=_timeout):

    #         with Timer() as t:
    #             with self.assertRaises(ConnectionError):
    #                 KafkaConnection("nowhere", 1234, 1.0)
    #         self.assertGreaterEqual(t.interval, 1.0)


class TestKafkaClientIntegration(KafkaIntegrationTestCase):

    @run_until_complete
    def test_connect_with_global_loop(self):
        asyncio.set_event_loop(self.loop)
        client = yield from connect(self.hosts)
        self.assertIs(client._loop, self.loop)
        client.close()

    @run_until_complete
    def test_clear_metadata(self):
        # make sure that mapping topic/partition exists
        for partition in self.client._topic_partitions[self.topic].values():
            self.assertIsInstance(partition, PartitionMetadata)

        # make sure that mapping TopicAndPartition to broker exists
        key = TopicAndPartition(self.topic, 0)
        self.assertIsInstance(self.client._topics_to_brokers[key],
                              BrokerMetadata)

        self.client.reset_all_metadata()

        self.assertEqual(self.client._topics_to_brokers, {})
        self.assertEqual(self.client._topic_partitions, {})

    @run_until_complete
    def test_consume_none(self):
        fetch = FetchRequest(self.topic, 0, 0, 1024)
        (fetch_resp,) = yield from self.client.send_fetch_request([fetch])
        self.assertEquals(fetch_resp.error, 0)
        self.assertEquals(fetch_resp.topic, self.topic)
        self.assertEquals(fetch_resp.partition, 0)

        messages = list(fetch_resp.messages)
        self.assertEquals(len(messages), 0)

    @run_until_complete
    def test_ensure_topic_exists(self):

        # assume that self.topic was created by setUp
        # if so, this should succeed
        yield from self.client.ensure_topic_exists(self.topic, timeout=1)

        # ensure_topic_exists should fail with KafkaTimeoutError
        with self.assertRaises(KafkaTimeoutError):
            yield from self.client.ensure_topic_exists(
                b"this_topic_doesnt_exist", timeout=0)

    @run_until_complete
    def test_commit_fetch_offsets(self):

        req = OffsetCommitRequest(self.topic, 0, 42, b"metadata")
        (resp,) = yield from self.client.send_offset_commit_request(b"group",
                                                                    [req])
        self.assertEquals(resp.error, 0)

        req = OffsetFetchRequest(self.topic, 0)
        (resp,) = yield from self.client.send_offset_fetch_request(b"group",
                                                                   [req])
        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.offset, 42)
        self.assertEquals(resp.metadata, b"")  # Metadata isn't stored for now
