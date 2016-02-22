import asyncio
import pytest
import unittest
from unittest import mock

from kafka.common import (KafkaError, ConnectionError,
                          NodeNotReadyError, UnrecognizedBrokerVersion)
from kafka.protocol.metadata import MetadataRequest, MetadataResponse

from aiokafka.client import AIOKafkaClient
from aiokafka.conn import AIOKafkaConnection
from ._testutil import KafkaIntegrationTestCase, run_until_complete


NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
NO_LEADER = 5
REPLICA_NOT_AVAILABLE = 9


@pytest.mark.usefixtures('setup_test_class')
class TestAIOKafkaClient(unittest.TestCase):

    def test_init_with_list(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=['kafka01:9092', 'kafka02:9092', 'kafka03:9092'])
        self.assertEqual(
            '<AIOKafkaClient client_id=aiokafka-0.0.1>', client.__repr__())
        self.assertEqual(sorted({'kafka01': 9092,
                                 'kafka02': 9092,
                                 'kafka03': 9092}.items()),
                         sorted(client.hosts))

        node = client.get_random_node()
        self.assertEqual(node, None)  # unknown cluster metadata

    def test_init_with_csv(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers='kafka01:9092,kafka02:9092,kafka03:9092')

        self.assertEqual(sorted({'kafka01': 9092,
                                 'kafka02': 9092,
                                 'kafka03': 9092}.items()),
                         sorted(client.hosts))

    def test_load_metadata(self):
        brokers = [
            (0, 'broker_1', 4567),
            (1, 'broker_2', 5678)
        ]

        topics = [
            (NO_ERROR, 'topic_1', [
                (NO_ERROR, 0, 1, [1, 2], [1, 2])
            ]),
            (NO_ERROR, 'topic_2', [
                (NO_LEADER, 0, -1, [], []),
                (NO_LEADER, 1, 1, [], []),
            ]),
            (NO_LEADER, 'topic_no_partitions', []),
            (UNKNOWN_TOPIC_OR_PARTITION, 'topic_unknown', []),
            (NO_ERROR, 'topic_3', [
                (NO_ERROR, 0, 0, [0, 1], [0, 1]),
                (NO_ERROR, 1, 1, [1, 0], [1, 0]),
                (NO_ERROR, 2, 0, [0, 1], [0, 1])
            ]),
            (NO_ERROR, 'topic_4', [
                (NO_ERROR, 0, 0, [0, 1], [0, 1]),
                (REPLICA_NOT_AVAILABLE, 1, 1, [1, 0], [1, 0]),
            ])
        ]

        @asyncio.coroutine
        def send(request_id):
            return MetadataResponse(brokers, topics)

        mocked_conns = {0: mock.MagicMock()}
        mocked_conns[0].send.side_effect = send
        client = AIOKafkaClient(loop=self.loop,
                                bootstrap_servers=['broker_1:4567'])
        client._conns = mocked_conns
        client.cluster.update_metadata(MetadataResponse(brokers[:1], []))

        self.loop.run_until_complete(client.force_metadata_update())

        md = client.cluster
        c_brokers = md.brokers()
        self.assertEqual(len(c_brokers), 2)
        self.assertEqual(sorted(brokers), sorted(list(c_brokers)))
        c_topics = md.topics()
        self.assertEqual(len(c_topics), 4)
        self.assertEqual(md.partitions_for_topic('topic_1'), set([0]))
        self.assertEqual(md.partitions_for_topic('topic_2'), set([0, 1]))
        self.assertEqual(md.partitions_for_topic('topic_3'), set([0, 1, 2]))
        self.assertEqual(md.partitions_for_topic('topic_4'), set([0, 1]))
        self.assertEqual(
            md.available_partitions_for_topic('topic_2'), set([1]))

        mocked_conns[0].connected.return_value = False
        is_ready = self.loop.run_until_complete(client.ready(0))
        self.assertEqual(is_ready, False)
        is_ready = self.loop.run_until_complete(client.ready(1))
        self.assertEqual(is_ready, False)
        self.assertEqual(mocked_conns, {})

        with self.assertRaises(NodeNotReadyError):
            self.loop.run_until_complete(client.send(0, None))


class TestKafkaClientIntegration(KafkaIntegrationTestCase):

    @run_until_complete
    def test_bootstrap(self):
        client = AIOKafkaClient(loop=self.loop,
                                bootstrap_servers='127.0.0.2:22')
        with self.assertRaises(ConnectionError):
            yield from client.bootstrap()

        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        yield from self.wait_topic(client, 'test_topic')

        metadata = yield from client.fetch_all_metadata()
        self.assertTrue('test_topic' in metadata.topics())

        client.set_topics(['t2', 't3'])
        client.set_topics(['t2', 't3'])  # should be ignored
        client.add_topic('t2')  # shold be ignored
        # bootstrap again -- no error expected
        yield from client.bootstrap()
        yield from client.close()

    @run_until_complete
    def test_failed_bootstrap(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        with mock.patch.object(AIOKafkaConnection, 'send') as mock_send:
            mock_send.side_effect = KafkaError('some kafka error')
            with self.assertRaises(ConnectionError):
                yield from client.bootstrap()

    @run_until_complete
    def test_send_request(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        node_id = client.get_random_node()
        resp = yield from client.send(node_id, MetadataRequest([]))
        self.assertTrue(isinstance(resp, MetadataResponse))

    @run_until_complete
    def test_check_version(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        ver = yield from client.check_version()
        self.assertTrue('0.' in ver)
        yield from self.wait_topic(client, 'some_test_topic')
        ver2 = yield from client.check_version()
        self.assertEqual(ver, ver2)
        ver2 = yield from client.check_version(client.get_random_node())
        self.assertEqual(ver, ver2)

        with mock.patch.object(
                AIOKafkaConnection, 'send') as mocked:
            mocked.side_effect = KafkaError('mocked exception')
            with self.assertRaises(UnrecognizedBrokerVersion):
                yield from client.check_version(client.get_random_node())

    @run_until_complete
    def test_metadata_synchronizer(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=100)

        with mock.patch.object(
                AIOKafkaClient, 'force_metadata_update') as mocked:
            @asyncio.coroutine
            def dummy():
                client.cluster.failed_update(None)
            mocked.side_effect = dummy

            yield from client.bootstrap()
            yield from asyncio.sleep(0.15, loop=self.loop)
            yield from client.close()

            self.assertNotEqual(
                len(client.force_metadata_update.mock_calls), 0)

    @run_until_complete
    def test_metadata_update_fail(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()

        with mock.patch.object(
                AIOKafkaConnection, 'send') as mocked:
            mocked.side_effect = KafkaError('mocked exception')

            updated = yield from client.force_metadata_update()

            self.assertEqual(updated, False)

            with self.assertRaises(KafkaError):
                yield from client.fetch_all_metadata()
