import asyncio
import socket
import types
from typing import Any
from unittest import mock

from aiokafka import __version__
from aiokafka.client import AIOKafkaClient, ConnectionGroup
from aiokafka.conn import AIOKafkaConnection, CloseReason
from aiokafka.errors import (
    KafkaConnectionError,
    KafkaError,
    NodeNotReadyError,
    RequestTimedOutError,
)
from aiokafka.protocol.fetch import FetchRequest
from aiokafka.protocol.metadata import MetadataRequest
from aiokafka.protocol.metadata import MetadataResponse_v0 as MetadataResponse
from aiokafka.util import create_task, get_running_loop

from ._testutil import KafkaIntegrationTestCase, run_until_complete

NO_ERROR = 0
UNKNOWN_TOPIC_OR_PARTITION = 3
NO_LEADER = 5
REPLICA_NOT_AVAILABLE = 9
INVALID_TOPIC = 17
UNKNOWN_ERROR = -1
TOPIC_AUTHORIZATION_FAILED = 29


class TestKafkaClientIntegration(KafkaIntegrationTestCase):
    @run_until_complete
    async def test_init_with_list(self):
        client = AIOKafkaClient(
            bootstrap_servers=["127.0.0.1:9092", "127.0.0.2:9092", "127.0.0.3:9092"]
        )
        self.assertEqual(
            f"<AIOKafkaClient client_id=aiokafka-{__version__}>", client.__repr__()
        )
        self.assertEqual(
            sorted(
                [
                    ("127.0.0.1", 9092, socket.AF_INET),
                    ("127.0.0.2", 9092, socket.AF_INET),
                    ("127.0.0.3", 9092, socket.AF_INET),
                ]
            ),
            sorted(client.hosts),
        )

        node = client.get_random_node()
        self.assertEqual(node, None)  # unknown cluster metadata

    @run_until_complete
    async def test_init_with_csv(self):
        client = AIOKafkaClient(
            bootstrap_servers="127.0.0.1:9092,127.0.0.2:9092,127.0.0.3:9092"
        )

        self.assertEqual(
            sorted(
                [
                    ("127.0.0.1", 9092, socket.AF_INET),
                    ("127.0.0.2", 9092, socket.AF_INET),
                    ("127.0.0.3", 9092, socket.AF_INET),
                ]
            ),
            sorted(client.hosts),
        )

    @run_until_complete
    async def test_load_metadata(self):
        brokers = [(0, "broker_1", 4567), (1, "broker_2", 5678)]

        topics = [
            (NO_ERROR, "topic_1", [(NO_ERROR, 0, 1, [1, 2], [1, 2])]),
            (
                NO_ERROR,
                "topic_2",
                [
                    (NO_LEADER, 0, -1, [], []),
                    (NO_LEADER, 1, 1, [], []),
                ],
            ),
            (NO_LEADER, "topic_no_partitions", []),
            (UNKNOWN_TOPIC_OR_PARTITION, "topic_unknown", []),
            (
                NO_ERROR,
                "topic_3",
                [
                    (NO_ERROR, 0, 0, [0, 1], [0, 1]),
                    (NO_ERROR, 1, 1, [1, 0], [1, 0]),
                    (NO_ERROR, 2, 0, [0, 1], [0, 1]),
                ],
            ),
            (
                NO_ERROR,
                "topic_4",
                [
                    (NO_ERROR, 0, 0, [0, 1], [0, 1]),
                    (REPLICA_NOT_AVAILABLE, 1, 1, [1, 0], [1, 0]),
                ],
            ),
            (INVALID_TOPIC, "topic_5", []),  # Just ignored
            (UNKNOWN_ERROR, "topic_6", []),  # Just ignored
            (TOPIC_AUTHORIZATION_FAILED, "topic_auth_error", []),
        ]

        async def send(request_id):
            return MetadataResponse(brokers, topics)

        mocked_conns = {(0, 0): mock.MagicMock()}
        mocked_conns[(0, 0)].send.side_effect = send
        client = AIOKafkaClient(bootstrap_servers=["broker_1:4567"])
        task = create_task(client._md_synchronizer())
        client._conns = mocked_conns
        client.cluster.update_metadata(MetadataResponse(brokers[:1], []))

        await client.force_metadata_update()
        task.cancel()

        md = client.cluster
        c_brokers = md.brokers()
        self.assertEqual(len(c_brokers), 2)
        expected_brokers = [(0, "broker_1", 4567, None), (1, "broker_2", 5678, None)]
        self.assertEqual(sorted(expected_brokers), sorted(c_brokers))
        c_topics = md.topics()
        self.assertEqual(len(c_topics), 4)
        self.assertEqual(md.partitions_for_topic("topic_1"), {0})
        self.assertEqual(md.partitions_for_topic("topic_2"), {0, 1})
        self.assertEqual(md.partitions_for_topic("topic_3"), {0, 1, 2})
        self.assertEqual(md.partitions_for_topic("topic_4"), {0, 1})
        self.assertEqual(md.available_partitions_for_topic("topic_2"), {1})

        mocked_conns[(0, 0)].connected.return_value = False
        is_ready = await client.ready(0)
        self.assertEqual(is_ready, False)
        is_ready = await client.ready(1)
        self.assertEqual(is_ready, False)
        self.assertEqual(mocked_conns, {})

        with self.assertRaises(NodeNotReadyError):
            await client.send(0, None)

        with self.assertRaises(NodeNotReadyError):
            await client.send(0, None, group=ConnectionGroup.COORDINATION)

        self.assertEqual(md.unauthorized_topics, {"topic_auth_error"})

    @run_until_complete
    async def test_send_timeout_deletes_connection(self):
        correct_response = MetadataResponse([], [])

        async def send_exception(*args, **kwargs):
            raise asyncio.TimeoutError()

        async def send(*args, **kwargs):
            return correct_response

        async def get_conn(self, node_id, *, group=0):
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
        client = AIOKafkaClient(bootstrap_servers=["broker_1:4567"])
        client._conns = mocked_conns
        client._get_conn = types.MethodType(get_conn, client)

        # first send timeouts
        with self.assertRaises(RequestTimedOutError):
            await client.send(0, MetadataRequest([]))

        conn.close.assert_called_once_with(reason=CloseReason.CONNECTION_TIMEOUT)
        # this happens because conn was closed
        conn.connected.return_value = False

        # second send gets new connection and obtains result
        response = await client.send(0, MetadataRequest([]))
        self.assertEqual(response, correct_response)
        self.assertNotEqual(conn, client._conns[(node_id, 0)])

    @run_until_complete
    async def test_client_receive_zero_brokers(self):
        brokers = [(0, "broker_1", 4567), (1, "broker_2", 5678)]
        correct_meta = MetadataResponse(brokers, [])
        bad_response = MetadataResponse([], [])

        async def send(*args, **kwargs):
            return bad_response

        client = AIOKafkaClient(bootstrap_servers=["broker_1:4567"])
        conn = mock.Mock()
        client._conns = [mock.Mock()]

        async def _get_conn(*args: Any, **kwargs: Any):
            return conn

        client._get_conn = mock.Mock()
        client._get_conn.side_effect = _get_conn
        conn.send = mock.Mock()
        conn.send.side_effect = send
        client.cluster.update_metadata(correct_meta)
        brokers_before = client.cluster.brokers()

        await client._metadata_update(client.cluster, [])

        # There broker list should not be purged
        self.assertNotEqual(client.cluster.brokers(), set())
        self.assertEqual(client.cluster.brokers(), brokers_before)

    @run_until_complete
    async def test_client_receive_zero_brokers_timeout_on_send(self):
        brokers = [(0, "broker_1", 4567), (1, "broker_2", 5678)]
        correct_meta = MetadataResponse(brokers, [])

        async def send(*args, **kwargs):
            raise asyncio.TimeoutError()

        client = AIOKafkaClient(bootstrap_servers=["broker_1:4567"])
        conn = mock.Mock()
        client._conns = [mock.Mock()]

        async def _get_conn(*args: Any, **kwargs: Any):
            return conn

        client._get_conn = mock.Mock()
        client._get_conn.side_effect = _get_conn
        conn.send = mock.Mock()
        conn.send.side_effect = send
        client.cluster.update_metadata(correct_meta)
        brokers_before = client.cluster.brokers()

        await client._metadata_update(client.cluster, [])

        # There broker list should not be purged
        self.assertNotEqual(client.cluster.brokers(), set())
        self.assertEqual(client.cluster.brokers(), brokers_before)

    @run_until_complete
    async def test_bootstrap(self):
        client = AIOKafkaClient(bootstrap_servers="0.42.42.42:444")
        with self.assertRaises(KafkaConnectionError):
            await client.bootstrap()

        client = AIOKafkaClient(bootstrap_servers=self.hosts)
        await client.bootstrap()
        await self.wait_topic(client, "test_topic")

        metadata = await client.fetch_all_metadata()
        self.assertTrue("test_topic" in metadata.topics())

        client.set_topics(["t2", "t3"])
        client.set_topics(["t2", "t3"])  # should be ignored
        client.add_topic("t2")  # should be ignored
        # bootstrap again -- no error expected
        await client.bootstrap()
        await client.close()

    @run_until_complete
    async def test_failed_bootstrap(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts)
        with mock.patch.object(AIOKafkaConnection, "send") as mock_send:
            mock_send.side_effect = KafkaError("some kafka error")
            with self.assertRaises(KafkaConnectionError):
                await client.bootstrap()

    @run_until_complete
    async def test_failed_bootstrap_timeout(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts)
        with mock.patch.object(AIOKafkaConnection, "send") as mock_send:
            mock_send.side_effect = asyncio.TimeoutError("Timeout error")
            with self.assertRaises(KafkaConnectionError):
                await client.bootstrap()

    @run_until_complete
    async def test_send_request(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts)
        await client.bootstrap()
        node_id = client.get_random_node()
        resp = await client.send(node_id, MetadataRequest([]))
        self.assertEqual(resp.API_KEY, MetadataRequest.API_KEY)
        await client.close()

    @run_until_complete
    async def test_metadata_synchronizer(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts, metadata_max_age_ms=10)

        with mock.patch.object(AIOKafkaClient, "_metadata_update") as mocked:

            async def dummy(*d, **kw):
                client.cluster.failed_update(None)

            mocked.side_effect = dummy

            await client.bootstrap()
            # wait synchronizer task timeout
            await asyncio.sleep(0.1)

            self.assertNotEqual(len(client._metadata_update.mock_calls), 0)
        await client.close()

    @run_until_complete
    async def test_metadata_update_fail(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts)
        await client.bootstrap()
        # Make sure the connection is initialize before mock to avoid crashing
        # api_version routine
        await client.force_metadata_update()

        with mock.patch.object(AIOKafkaConnection, "send") as mocked:
            mocked.side_effect = KafkaError("mocked exception")

            updated = await client.force_metadata_update()

            self.assertEqual(updated, False)

            with self.assertRaises(KafkaError):
                await client.fetch_all_metadata()
        await client.close()

    @run_until_complete
    async def test_force_metadata_update_multiple_times(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts, metadata_max_age_ms=10000)
        await client.bootstrap()
        self.add_cleanup(client.close)

        orig = client._metadata_update
        with mock.patch.object(client, "_metadata_update") as mocked:

            async def new(*args, **kw):
                await asyncio.sleep(0.2)
                return await orig(*args, **kw)

            mocked.side_effect = new

            client.force_metadata_update()
            await asyncio.sleep(0.01)
            self.assertEqual(len(client._metadata_update.mock_calls), 1)
            client.force_metadata_update()
            await asyncio.sleep(0.01)
            self.assertEqual(len(client._metadata_update.mock_calls), 1)
            client.force_metadata_update()
            await asyncio.sleep(0.5)
            self.assertEqual(len(client._metadata_update.mock_calls), 1)

    @run_until_complete
    async def test_set_topics_trigger_metadata_update(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts, metadata_max_age_ms=10000)
        await client.bootstrap()
        self.add_cleanup(client.close)

        orig = client._metadata_update
        with mock.patch.object(client, "_metadata_update") as mocked:

            async def new(*args, **kw):
                await asyncio.sleep(0.01)
                return await orig(*args, **kw)

            mocked.side_effect = new

            await client.set_topics(["topic1"])
            self.assertEqual(len(client._metadata_update.mock_calls), 1)
            # Same topics list should not trigger update
            await client.set_topics(["topic1"])
            self.assertEqual(len(client._metadata_update.mock_calls), 1)

            await client.set_topics(["topic1", "topic2"])
            self.assertEqual(len(client._metadata_update.mock_calls), 2)
            # Less topics should not update too
            await client.set_topics(["topic2"])
            self.assertEqual(len(client._metadata_update.mock_calls), 2)

            # Setting [] should force update as it means all topics
            await client.set_topics([])
            self.assertEqual(len(client._metadata_update.mock_calls), 3)

            # Changing topics during refresh should trigger 2 refreshes
            client.set_topics(["topic3"])
            await asyncio.sleep(0.001)
            self.assertEqual(len(client._metadata_update.mock_calls), 4)
            await client.set_topics(["topic3", "topics4"])
            self.assertEqual(len(client._metadata_update.mock_calls), 5)

    @run_until_complete
    async def test_metadata_updated_on_socket_disconnect(self):
        # Related to issue 176. A disconnect means that either we lost
        # connection to the node, or we have a node failure. In both cases
        # there's a high probability that Leader distribution will also change.
        client = AIOKafkaClient(bootstrap_servers=self.hosts, metadata_max_age_ms=10000)
        await client.bootstrap()
        self.add_cleanup(client.close)

        # Init a clonnection
        node_id = client.get_random_node()
        assert node_id is not None
        req = MetadataRequest([])
        await client.send(node_id, req)

        # No metadata update pending atm
        self.assertFalse(client._md_update_waiter.done())

        # Connection disconnect should trigger an update
        conn = await client._get_conn(node_id)
        conn.close(reason=CloseReason.CONNECTION_BROKEN)
        self.assertTrue(client._md_update_waiter.done())

    @run_until_complete
    async def test_no_concurrent_send_on_connection(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts, metadata_max_age_ms=10000)
        await client.bootstrap()
        self.add_cleanup(client.close)

        await self.wait_topic(client, self.topic)

        node_id = client.get_random_node()
        wait_request = FetchRequest(
            max_wait_time=500,
            min_bytes=1024 * 1024,
            max_bytes=52428800,
            isolation_level=0,
            topics=[(self.topic, [(0, 0, 1024)])],
        )
        vanila_request = MetadataRequest([])

        loop = get_running_loop()
        send_time = loop.time()
        long_task = create_task(client.send(node_id, wait_request))
        await asyncio.sleep(0.0001)
        self.assertFalse(long_task.done())

        await client.send(node_id, vanila_request)
        resp_time = loop.time()
        fetch_resp = await long_task
        # Check error code like resp->topics[0]->partitions[0]->error_code
        self.assertEqual(fetch_resp.topics[0][1][0][1], 0)

        # Check that vanila request actually executed after wait request
        self.assertGreaterEqual(resp_time - send_time, 0.5)

    @run_until_complete
    async def test_different_connections_in_conn_groups(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts, metadata_max_age_ms=10000)
        await client.bootstrap()
        self.add_cleanup(client.close)

        node_id = client.get_random_node()

        conn1 = await client._get_conn(node_id)
        conn2 = await client._get_conn(node_id, group=ConnectionGroup.COORDINATION)

        self.assertTrue(conn1 is not conn2)
        self.assertEqual((conn1.host, conn1.port), (conn2.host, conn2.port))

    @run_until_complete
    async def test_concurrent_send_on_different_connection_groups(self):
        client = AIOKafkaClient(bootstrap_servers=self.hosts, metadata_max_age_ms=10000)
        await client.bootstrap()
        self.add_cleanup(client.close)

        await self.wait_topic(client, self.topic)

        node_id = client.get_random_node()

        wait_request = FetchRequest(
            max_wait_time=500,
            min_bytes=1024 * 1024,
            max_bytes=52428800,
            isolation_level=0,
            topics=[(self.topic, [(0, 0, 1024)])],
        )
        vanila_request = MetadataRequest([])

        loop = get_running_loop()
        send_time = loop.time()
        long_task = create_task(client.send(node_id, wait_request))
        await asyncio.sleep(0.0001)
        self.assertFalse(long_task.done())

        await client.send(node_id, vanila_request, group=ConnectionGroup.COORDINATION)
        resp_time = loop.time()
        self.assertFalse(long_task.done())

        fetch_resp = await long_task
        # Check error code like resp->topics[0]->partitions[0]->error_code
        self.assertEqual(fetch_resp.topics[0][1][0][1], 0)

        # Check that vanila request actually executed after wait request
        self.assertLess(resp_time - send_time, 0.5)
