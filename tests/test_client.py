import asyncio
import pytest
import unittest
import socket
import types
from unittest import mock

from kafka.common import (KafkaError, ConnectionError, RequestTimedOutError,
                          NodeNotReadyError, UnrecognizedBrokerVersion)
from kafka.protocol.metadata import (
    MetadataRequest_v0 as MetadataRequest,
    MetadataResponse_v0 as MetadataResponse)
from kafka.protocol.fetch import FetchRequest_v0

from aiokafka.client import AIOKafkaClient, ConnectionGroup, CoordinationType
from aiokafka.conn import AIOKafkaConnection, CloseReason
from aiokafka.util import ensure_future
from ._testutil import KafkaIntegrationTestCase, run_until_complete


NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
NO_LEADER = 5
REPLICA_NOT_AVAILABLE = 9
INVALID_TOPIC = 17
UNKNOWN_ERROR = -1
TOPIC_AUTHORIZATION_FAILED = 29


@pytest.mark.usefixtures('setup_test_class')
class TestAIOKafkaClient(unittest.TestCase):

    def test_init_with_list(self):
        client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=[
                '127.0.0.1:9092', '127.0.0.2:9092', '127.0.0.3:9092'])
        self.assertEqual(
            '<AIOKafkaClient client_id=aiokafka-0.5.0>',
            client.__repr__())
        self.assertEqual(
            sorted([('127.0.0.1', 9092, socket.AF_INET),
                    ('127.0.0.2', 9092, socket.AF_INET),
                    ('127.0.0.3', 9092, socket.AF_INET)]),
            sorted(client.hosts))

        node = client.get_random_node()
        self.assertEqual(node, None)  # unknown cluster metadata

    def test_init_with_csv(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers='127.0.0.1:9092,127.0.0.2:9092,127.0.0.3:9092')

        self.assertEqual(
            sorted([('127.0.0.1', 9092, socket.AF_INET),
                    ('127.0.0.2', 9092, socket.AF_INET),
                    ('127.0.0.3', 9092, socket.AF_INET)]),
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
            ]),
            (INVALID_TOPIC, 'topic_5', []),  # Just ignored
            (UNKNOWN_ERROR, 'topic_6', []),  # Just ignored
            (TOPIC_AUTHORIZATION_FAILED, 'topic_auth_error', []),

        ]

        @asyncio.coroutine
        def send(request_id):
            return MetadataResponse(brokers, topics)

        mocked_conns = {(0, 0): mock.MagicMock()}
        mocked_conns[(0, 0)].send.side_effect = send
        client = AIOKafkaClient(loop=self.loop,
                                bootstrap_servers=['broker_1:4567'])
        task = ensure_future(client._md_synchronizer(), loop=self.loop)
        client._conns = mocked_conns
        client.cluster.update_metadata(MetadataResponse(brokers[:1], []))

        self.loop.run_until_complete(client.force_metadata_update())
        task.cancel()

        md = client.cluster
        c_brokers = md.brokers()
        self.assertEqual(len(c_brokers), 2)
        expected_brokers = [
            (0, 'broker_1', 4567, None),
            (1, 'broker_2', 5678, None)
        ]
        self.assertEqual(sorted(expected_brokers), sorted(list(c_brokers)))
        c_topics = md.topics()
        self.assertEqual(len(c_topics), 4)
        self.assertEqual(md.partitions_for_topic('topic_1'), set([0]))
        self.assertEqual(md.partitions_for_topic('topic_2'), set([0, 1]))
        self.assertEqual(md.partitions_for_topic('topic_3'), set([0, 1, 2]))
        self.assertEqual(md.partitions_for_topic('topic_4'), set([0, 1]))
        self.assertEqual(
            md.available_partitions_for_topic('topic_2'), set([1]))

        mocked_conns[(0, 0)].connected.return_value = False
        is_ready = self.loop.run_until_complete(client.ready(0))
        self.assertEqual(is_ready, False)
        is_ready = self.loop.run_until_complete(client.ready(1))
        self.assertEqual(is_ready, False)
        self.assertEqual(mocked_conns, {})

        with self.assertRaises(NodeNotReadyError):
            self.loop.run_until_complete(client.send(0, None))

        self.assertEqual(md.unauthorized_topics, {'topic_auth_error'})

    @run_until_complete
    def test_send_timeout_deletes_connection(self):
        correct_response = MetadataResponse([], [])

        @asyncio.coroutine
        def send_exception(*args, **kwargs):
            raise asyncio.TimeoutError()

        @asyncio.coroutine
        def send(*args, **kwargs):
            return correct_response

        @asyncio.coroutine
        def get_conn(self, node_id, *, group=0):
            conn_id = (node_id, group)
            if conn_id in self._conns:
                conn = self._conns[conn_id]
                if not conn.connected():
                    del self._conns[conn_id]
                else:
                    return conn

            conn = mock.MagicMock()
            conn.send.side_effect = send
            self._conns[conn_id] = conn
            return conn

        node_id = 0
        conn = mock.MagicMock()
        conn.send.side_effect = send_exception
        conn.connected.return_value = True
        mocked_conns = {(node_id, 0): conn}
        client = AIOKafkaClient(loop=self.loop,
                                bootstrap_servers=['broker_1:4567'])
        client._conns = mocked_conns
        client._get_conn = types.MethodType(get_conn, client)

        # first send timeouts
        with self.assertRaises(RequestTimedOutError):
            yield from client.send(0, MetadataRequest([]))

        conn.close.assert_called_once_with(
            reason=CloseReason.CONNECTION_TIMEOUT)
        # this happens because conn was closed
        conn.connected.return_value = False

        # second send gets new connection and obtains result
        response = yield from client.send(0, MetadataRequest([]))
        self.assertEqual(response, correct_response)
        self.assertNotEqual(conn, client._conns[(node_id, 0)])

    @run_until_complete
    def test_client_receive_zero_brokers(self):
        brokers = [
            (0, 'broker_1', 4567),
            (1, 'broker_2', 5678)
        ]
        correct_meta = MetadataResponse(brokers, [])
        bad_response = MetadataResponse([], [])

        @asyncio.coroutine
        def send(*args, **kwargs):
            return bad_response

        client = AIOKafkaClient(loop=self.loop,
                                bootstrap_servers=['broker_1:4567'],
                                api_version="0.10")
        conn = mock.Mock()
        client._conns = [mock.Mock()]
        client._get_conn = mock.Mock()
        client._get_conn.side_effect = asyncio.coroutine(lambda x: conn)
        conn.send = mock.Mock()
        conn.send.side_effect = send
        client.cluster.update_metadata(correct_meta)
        brokers_before = client.cluster.brokers()

        yield from client._metadata_update(client.cluster, [])

        # There broker list should not be purged
        self.assertNotEqual(client.cluster.brokers(), set([]))
        self.assertEqual(client.cluster.brokers(), brokers_before)


class TestKafkaClientIntegration(KafkaIntegrationTestCase):

    @run_until_complete
    def test_bootstrap(self):
        client = AIOKafkaClient(loop=self.loop,
                                bootstrap_servers='0.42.42.42:444')
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
        yield from client.close()

    @run_until_complete
    def test_check_version(self):
        kafka_version = tuple(int(x) for x in self.kafka_version.split("."))

        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        ver = yield from client.check_version()
        self.assertEqual(kafka_version[:2], ver[:2])
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

        client._get_conn = asyncio.coroutine(lambda _, **kw: None)
        with self.assertRaises(ConnectionError):
            yield from client.check_version()
        yield from client.close()

    @run_until_complete
    def test_metadata_synchronizer(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            api_version="0.9",
            metadata_max_age_ms=10)

        with mock.patch.object(
                AIOKafkaClient, '_metadata_update') as mocked:
            @asyncio.coroutine
            def dummy(*d, **kw):
                client.cluster.failed_update(None)
            mocked.side_effect = dummy

            yield from client.bootstrap()
            # wait synchronizer task timeout
            yield from asyncio.sleep(0.1, loop=self.loop)

            self.assertNotEqual(
                len(client._metadata_update.mock_calls), 0)
        yield from client.close()

    @run_until_complete
    def test_metadata_update_fail(self):
        client = AIOKafkaClient(loop=self.loop, bootstrap_servers=self.hosts)
        yield from client.bootstrap()
        # Make sure the connection is initialize before mock to avoid crashing
        # api_version routine
        yield from client.force_metadata_update()

        with mock.patch.object(
                AIOKafkaConnection, 'send') as mocked:
            mocked.side_effect = KafkaError('mocked exception')

            updated = yield from client.force_metadata_update()

            self.assertEqual(updated, False)

            with self.assertRaises(KafkaError):
                yield from client.fetch_all_metadata()
        yield from client.close()

    @run_until_complete
    def test_force_metadata_update_multiple_times(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=10000)
        yield from client.bootstrap()
        self.add_cleanup(client.close)

        orig = client._metadata_update
        with mock.patch.object(client, '_metadata_update') as mocked:
            @asyncio.coroutine
            def new(*args, **kw):
                yield from asyncio.sleep(0.2, loop=self.loop)
                return (yield from orig(*args, **kw))
            mocked.side_effect = new

            client.force_metadata_update()
            yield from asyncio.sleep(0.01, loop=self.loop)
            self.assertEqual(
                len(client._metadata_update.mock_calls), 1)
            client.force_metadata_update()
            yield from asyncio.sleep(0.01, loop=self.loop)
            self.assertEqual(
                len(client._metadata_update.mock_calls), 1)
            client.force_metadata_update()
            yield from asyncio.sleep(0.5, loop=self.loop)
            self.assertEqual(
                len(client._metadata_update.mock_calls), 1)

    @run_until_complete
    def test_set_topics_trigger_metadata_update(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=10000)
        yield from client.bootstrap()
        self.add_cleanup(client.close)

        orig = client._metadata_update
        with mock.patch.object(client, '_metadata_update') as mocked:
            @asyncio.coroutine
            def new(*args, **kw):
                yield from asyncio.sleep(0.01, loop=self.loop)
                return (yield from orig(*args, **kw))
            mocked.side_effect = new

            yield from client.set_topics(["topic1"])
            self.assertEqual(
                len(client._metadata_update.mock_calls), 1)
            # Same topics list should not trigger update
            yield from client.set_topics(["topic1"])
            self.assertEqual(
                len(client._metadata_update.mock_calls), 1)

            yield from client.set_topics(["topic1", "topic2"])
            self.assertEqual(
                len(client._metadata_update.mock_calls), 2)
            # Less topics should not update too
            yield from client.set_topics(["topic2"])
            self.assertEqual(
                len(client._metadata_update.mock_calls), 2)

            # Setting [] should force update as it meens all topics
            yield from client.set_topics([])
            self.assertEqual(
                len(client._metadata_update.mock_calls), 3)

            # Changing topics during refresh should trigger 2 refreshes
            client.set_topics(["topic3"])
            yield from asyncio.sleep(0.001, loop=self.loop)
            self.assertEqual(
                len(client._metadata_update.mock_calls), 4)
            yield from client.set_topics(["topic3", "topics4"])
            self.assertEqual(
                len(client._metadata_update.mock_calls), 5)

    @run_until_complete
    def test_metadata_updated_on_socket_disconnect(self):
        # Related to issue 176. A disconnect means that either we lost
        # connection to the node, or we have a node failure. In both cases
        # there's a high probability that Leader distribution will also change.
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=10000)
        yield from client.bootstrap()
        self.add_cleanup(client.close)

        # Init a clonnection
        node_id = client.get_random_node()
        assert node_id is not None
        req = MetadataRequest([])
        yield from client.send(node_id, req)

        # No metadata update pending atm
        self.assertFalse(client._md_update_waiter.done())

        # Connection disconnect should trigger an update
        conn = yield from client._get_conn(node_id)
        conn.close(reason=CloseReason.CONNECTION_BROKEN)
        self.assertTrue(client._md_update_waiter.done())

    @run_until_complete
    def test_no_concurrent_send_on_connection(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=10000)
        yield from client.bootstrap()
        self.add_cleanup(client.close)

        yield from self.wait_topic(client, self.topic)

        node_id = client.get_random_node()
        wait_request = FetchRequest_v0(
            -1,  # replica_id
            500,  # max_wait_ms
            1024 * 1024,  # min_bytes
            [(self.topic, [(0, 0, 1024)]
              )])
        vanila_request = MetadataRequest([])

        send_time = self.loop.time()
        long_task = self.loop.create_task(
            client.send(node_id, wait_request)
        )
        yield from asyncio.sleep(0.0001, loop=self.loop)
        self.assertFalse(long_task.done())

        yield from client.send(node_id, vanila_request)
        resp_time = self.loop.time()
        fetch_resp = yield from long_task
        # Check error code like resp->topics[0]->partitions[0]->error_code
        self.assertEqual(fetch_resp.topics[0][1][0][1], 0)

        # Check that vanila request actually executed after wait request
        self.assertGreaterEqual(resp_time - send_time, 0.5)

    @run_until_complete
    def test_different_connections_in_conn_groups(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=10000)
        yield from client.bootstrap()
        self.add_cleanup(client.close)

        node_id = client.get_random_node()
        broker = client.cluster.broker_metadata(node_id)
        client.cluster.add_coordinator(
            node_id, broker.host, broker.port, rack=None,
            purpose=(CoordinationType.GROUP, ""))

        conn1 = yield from client._get_conn(node_id)
        conn2 = yield from client._get_conn(
            node_id, group=ConnectionGroup.COORDINATION)

        self.assertTrue(conn1 is not conn2)
        self.assertEqual((conn1.host, conn1.port), (conn2.host, conn2.port))

    @run_until_complete
    def test_concurrent_send_on_different_connection_groups(self):
        client = AIOKafkaClient(
            loop=self.loop,
            bootstrap_servers=self.hosts,
            metadata_max_age_ms=10000)
        yield from client.bootstrap()
        self.add_cleanup(client.close)

        yield from self.wait_topic(client, self.topic)

        node_id = client.get_random_node()
        broker = client.cluster.broker_metadata(node_id)
        client.cluster.add_coordinator(
            node_id, broker.host, broker.port, rack=None,
            purpose=(CoordinationType.GROUP, ""))

        wait_request = FetchRequest_v0(
            -1,  # replica_id
            500,  # max_wait_ms
            1024 * 1024,  # min_bytes
            [(self.topic, [(0, 0, 1024)]
              )])
        vanila_request = MetadataRequest([])

        send_time = self.loop.time()
        long_task = self.loop.create_task(
            client.send(node_id, wait_request)
        )
        yield from asyncio.sleep(0.0001, loop=self.loop)
        self.assertFalse(long_task.done())

        yield from client.send(
            node_id, vanila_request, group=ConnectionGroup.COORDINATION)
        resp_time = self.loop.time()
        self.assertFalse(long_task.done())

        fetch_resp = yield from long_task
        # Check error code like resp->topics[0]->partitions[0]->error_code
        self.assertEqual(fetch_resp.topics[0][1][0][1], 0)

        # Check that vanila request actually executed after wait request
        self.assertLess(resp_time - send_time, 0.5)
