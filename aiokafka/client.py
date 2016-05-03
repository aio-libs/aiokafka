import asyncio
import logging
import random

from kafka.conn import collect_hosts
from kafka.common import (KafkaError,
                          ConnectionError,
                          NodeNotReadyError,
                          KafkaTimeoutError,
                          UnrecognizedBrokerVersion)
from kafka.cluster import ClusterMetadata
from kafka.protocol.metadata import MetadataRequest_v0 as MetadataRequest
from kafka.protocol.produce import ProduceRequest

from aiokafka.conn import create_conn
from aiokafka import ensure_future, __version__


__all__ = ['AIOKafkaClient']


log = logging.getLogger('aiokafka')


class AIOKafkaClient:
    """This class implements interface for interact with Kafka cluster"""

    def __init__(self, *, loop, bootstrap_servers='localhost',
                 client_id='aiokafka-'+__version__, metadata_max_age_ms=300000,
                 request_timeout_ms=40000):
        """Initialize an asynchronous kafka client

        Keyword Arguments:
            bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
                strings) that the consumer should contact to bootstrap initial
                cluster metadata. This does not have to be the full node list.
                It just needs to have at least one broker that will respond to
                Metadata API Request. Default port is 9092. If no servers are
                specified, will default to localhost:9092.
            client_id (str): a name for this client. This string is passed in
                each request to servers and can be used to identify specific
                server-side log entries that correspond to this client. Also
                submitted to GroupCoordinator for logging with respect to
                consumer group administration. Default: 'aiokafka-{ver}'
            request_timeout_ms (int): Client request timeout in milliseconds.
                Default: 40000.
            metadata_max_age_ms (int): The period of time in milliseconds after
                which we force a refresh of metadata even if we haven't seen
                any partition leadership changes to proactively discover any
                new brokers or partitions. Default: 300000
        """
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._metadata_max_age_ms = metadata_max_age_ms
        self._request_timeout_ms = request_timeout_ms

        self.cluster = ClusterMetadata(metadata_max_age_ms=metadata_max_age_ms)
        self._topics = set()  # empty set will fetch all topic metadata
        self._conns = {}
        self._loop = loop
        self._sync_task = None

        self._md_update_fut = asyncio.Future(loop=self._loop)
        self._md_update_waiter = asyncio.Future(loop=self._loop)
        self._get_conn_lock = asyncio.Lock(loop=loop)

    def __repr__(self):
        return '<AIOKafkaClient client_id=%s>' % self._client_id

    @property
    def hosts(self):
        return collect_hosts(self._bootstrap_servers)

    @asyncio.coroutine
    def close(self):
        if self._sync_task:
            self._sync_task.cancel()
            try:
                yield from self._sync_task
            except asyncio.CancelledError:
                pass
            self._sync_task = None
        for conn in self._conns.values():
            conn.close()

    @asyncio.coroutine
    def bootstrap(self):
        """Try to to bootstrap initial cluster metadata"""
        metadata_request = MetadataRequest([])
        for host, port, _ in self.hosts:
            log.debug("Attempting to bootstrap via node at %s:%s", host, port)

            try:
                bootstrap_conn = yield from create_conn(
                    host, port, loop=self._loop, client_id=self._client_id,
                    request_timeout_ms=self._request_timeout_ms)
            except (OSError, asyncio.TimeoutError) as err:
                log.error('Unable connect to "%s:%s": %s', host, port, err)
                continue

            try:
                metadata = yield from bootstrap_conn.send(metadata_request)
            except KafkaError as err:
                log.warning('Unable to request metadata from "%s:%s": %s',
                            host, port, err)
                bootstrap_conn.close()
                continue

            self.cluster.update_metadata(metadata)

            # A cluster with no topics can return no broker metadata
            # in that case, we should keep the bootstrap connection
            if not len(self.cluster.brokers()):
                self._conns['bootstrap'] = bootstrap_conn
            else:
                bootstrap_conn.close()

            log.debug('Received cluster metadata: %s', self.cluster)
            break
        else:
            raise ConnectionError(
                'Unable to bootstrap from {}'.format(self.hosts))

        if self._sync_task is None:
            # starting metadata synchronizer task
            self._sync_task = ensure_future(
                self._md_synchronizer(), loop=self._loop)

    @asyncio.coroutine
    def _md_synchronizer(self):
        """routine (async task) for synchronize cluster metadata every
        `metadata_max_age_ms` milliseconds"""
        while True:
            yield from asyncio.wait(
                [self._md_update_waiter],
                timeout=self._metadata_max_age_ms / 1000,
                loop=self._loop)

            self._md_update_waiter = asyncio.Future(loop=self._loop)
            ret = yield from self._metadata_update(self.cluster, self._topics)
            self._md_update_fut.set_result(ret)
            self._md_update_fut = asyncio.Future(loop=self._loop)

    def get_random_node(self):
        """choice random node from known cluster brokers

        Returns:
            nodeId - identifier of broker
        """
        nodeids = [b.nodeId for b in self.cluster.brokers()]
        if not nodeids:
            return None
        return random.choice(nodeids)

    @asyncio.coroutine
    def _metadata_update(self, cluster_metadata, topics):
        assert isinstance(cluster_metadata, ClusterMetadata)
        metadata_request = MetadataRequest(list(topics))
        nodeids = [b.nodeId for b in self.cluster.brokers()]
        if 'bootstrap' in self._conns:
            nodeids.append('bootstrap')
        random.shuffle(nodeids)
        for node_id in nodeids:
            conn = yield from self._get_conn(node_id)

            if conn is None:
                continue
            log.debug("Sending metadata request %s to %s",
                      metadata_request, node_id)

            try:
                metadata = yield from conn.send(metadata_request)
            except KafkaError as err:
                log.error(
                    'Unable to request metadata from node with id %s: %s',
                    node_id, err)
                continue

            cluster_metadata.update_metadata(metadata)
            break
        else:
            log.error('Unable to update metadata from %s', nodeids)
            cluster_metadata.failed_update(None)
            return False
        return True

    def force_metadata_update(self):
        """Update cluster metadata

        Returns:
            True/False - metadata updated or not
        """
        # Wake up the `_md_synchronizer` task
        if not self._md_update_waiter.done():
            self._md_update_waiter.set_result(None)
        # Metadata will be updated in the background by syncronizer
        return self._md_update_fut

    @asyncio.coroutine
    def fetch_all_metadata(self):
        cluster_md = ClusterMetadata(
            metadata_max_age_ms=self._metadata_max_age_ms)
        updated = yield from self._metadata_update(cluster_md, [])
        if not updated:
            raise KafkaError(
                'Unable to get cluster metadata over all known brokers')
        return cluster_md

    def add_topic(self, topic):
        """Add a topic to the list of topics tracked via metadata.

        Arguments:
            topic (str): topic to track
        """
        if topic in self._topics:
            return
        self._topics.add(topic)

    def set_topics(self, topics):
        """Set specific topics to track for metadata.

        Arguments:
            topics (list of str): topics to track
        """
        if set(topics).difference(self._topics):
            self._topics = set(topics)
            # update metadata in async manner
            self.force_metadata_update()

    @asyncio.coroutine
    def _get_conn(self, node_id):
        "Get or create a connection to a broker using host and port"
        if node_id in self._conns:
            conn = self._conns[node_id]
            if not conn.connected():
                del self._conns[node_id]
            else:
                return conn

        try:
            broker = self.cluster.broker_metadata(node_id)
            assert broker, 'Broker id %s not in current metadata' % node_id
            log.debug("Initiating connection to node %s at %s:%s",
                      node_id, broker.host, broker.port)

            with (yield from self._get_conn_lock):
                if node_id in self._conns:
                    return self._conns[node_id]
                self._conns[node_id] = yield from create_conn(
                    broker.host, broker.port, loop=self._loop,
                    client_id=self._client_id,
                    request_timeout_ms=self._request_timeout_ms)
        except (OSError, asyncio.TimeoutError) as err:
            log.error('Unable connect to node with id %s: %s', node_id, err)
            return None
        else:
            return self._conns[node_id]

    @asyncio.coroutine
    def ready(self, node_id):
        conn = yield from self._get_conn(node_id)
        if conn is None:
            return False
        return True

    @asyncio.coroutine
    def send(self, node_id, request):
        """Send a request to a specific node.

        Arguments:
            node_id (int): destination node
            request (Struct): request object (not-encoded)

        Raises:
            kafka.common.KafkaTimeoutError
            kafka.common.NodeNotReadyError
            kafka.commom.ConnectionError
            kafka.common.CorrelationIdError

        Returns:
            Future: resolves to Response struct
        """
        if not (yield from self.ready(node_id)):
            raise NodeNotReadyError(
                "Attempt to send a request to node"
                " which is not ready (node id {}).".format(node_id))

        # Every request gets a response, except one special case:
        expect_response = True
        if isinstance(request, tuple(ProduceRequest)) and \
                request.required_acks == 0:
            expect_response = False

        future = self._conns[node_id].send(
            request, expect_response=expect_response)
        try:
            result = yield from future
        except asyncio.TimeoutError:
            raise KafkaTimeoutError()
        else:
            return result

    @asyncio.coroutine
    def check_version(self, node_id=None):
        """Attempt to guess the broker version"""
        if node_id is None:
            if self._conns:
                node_id = list(self._conns.keys())[0]
            else:
                assert self.cluster.brokers(), 'no brokers in metadata'
                node_id = list(self.cluster.brokers())[0].nodeId

        from kafka.protocol.admin import ListGroupsRequest_v0
        from kafka.protocol.commit import (
            OffsetFetchRequest_v0, GroupCoordinatorRequest_v0)
        from kafka.protocol.metadata import MetadataRequest_v0
        test_cases = [
            ('0.9', ListGroupsRequest_v0()),
            ('0.8.2', GroupCoordinatorRequest_v0('aiokafka-default-group')),
            ('0.8.1', OffsetFetchRequest_v0('aiokafka-default-group', [])),
            ('0.8.0', MetadataRequest_v0([])),
        ]

        # kafka kills the connection when it doesnt recognize an API request
        # so we can send a test request and then follow immediately with a
        # vanilla MetadataRequest. If the server did not recognize the first
        # request, both will be failed with a ConnectionError that wraps
        # socket.error (32, 54, or 104)
        conn = yield from self._get_conn(node_id)
        if conn is None:
            raise ConnectionError(
                "No connection to node with id {}".format(node_id))
        for version, request in test_cases:
            try:
                if not conn.connected():
                    yield from conn.connect()
                assert conn, 'no connection to node with id {}'.format(node_id)
                yield from conn.send(request)
            except KafkaError:
                continue
            else:
                return version

        raise UnrecognizedBrokerVersion()
