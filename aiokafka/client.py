import asyncio
import logging
import random

from kafka.conn import collect_hosts
from kafka.cluster import ClusterMetadata
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.produce import ProduceRequest

from aiokafka.conn import create_conn, CloseReason
from aiokafka.errors import (
    KafkaError,
    ConnectionError,
    NodeNotReadyError,
    KafkaTimeoutError,
    UnknownTopicOrPartitionError,
    UnrecognizedBrokerVersion)
from aiokafka import ensure_future, __version__


__all__ = ['AIOKafkaClient']


log = logging.getLogger('aiokafka')


class AIOKafkaClient:
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
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        api_version (str): specify which kafka API version to use.
            AIOKafka supports Kafka API versions >=0.9 only.
            If set to 'auto', will attempt to infer the broker version by
            probing various APIs. Default: auto
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. For more information see :ref:`ssl_auth`.
            Default: None.
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks. Default: 540000 (9hours).
    """

    def __init__(self, *, loop, bootstrap_servers='localhost',
                 client_id='aiokafka-' + __version__,
                 metadata_max_age_ms=300000,
                 request_timeout_ms=40000,
                 retry_backoff_ms=100,
                 ssl_context=None,
                 security_protocol='PLAINTEXT',
                 api_version='auto',
                 connections_max_idle_ms=540000):
        if security_protocol not in ('SSL', 'PLAINTEXT'):
            raise ValueError("`security_protocol` should be SSL or PLAINTEXT")
        if security_protocol == "SSL" and ssl_context is None:
            raise ValueError(
                "`ssl_context` is mandatory if security_protocol=='SSL'")
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._metadata_max_age_ms = metadata_max_age_ms
        self._request_timeout_ms = request_timeout_ms
        self._api_version = api_version
        self._security_protocol = security_protocol
        self._ssl_context = ssl_context
        self._retry_backoff = retry_backoff_ms / 1000
        self._connections_max_idle_ms = connections_max_idle_ms

        self.cluster = ClusterMetadata(metadata_max_age_ms=metadata_max_age_ms)
        self._topics = set()  # empty set will fetch all topic metadata
        self._conns = {}
        self._loop = loop
        self._sync_task = None

        self._md_update_fut = None
        self._md_update_waiter = asyncio.Future(loop=self._loop)
        self._get_conn_lock = asyncio.Lock(loop=loop)

    def __repr__(self):
        return '<AIOKafkaClient client_id=%s>' % self._client_id

    @property
    def api_version(self):
        if type(self._api_version) is tuple:
            return self._api_version
        # unknown api version, return minimal supported version
        return (0, 9)

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
        # Be careful to wait for graceful closure of all connections, so we
        # process all pending buffers.
        futs = []
        for conn in self._conns.values():
            futs.append(conn.close(reason=CloseReason.SHUTDOWN))
        if futs:
            yield from asyncio.gather(*futs, loop=self._loop)

    @asyncio.coroutine
    def bootstrap(self):
        """Try to to bootstrap initial cluster metadata"""
        # using request v0 for bootstap (bcs api version is not detected yet)
        metadata_request = MetadataRequest[0]([])
        for host, port, _ in self.hosts:
            log.debug("Attempting to bootstrap via node at %s:%s", host, port)

            try:
                bootstrap_conn = yield from create_conn(
                    host, port, loop=self._loop, client_id=self._client_id,
                    request_timeout_ms=self._request_timeout_ms,
                    ssl_context=self._ssl_context,
                    security_protocol=self._security_protocol,
                    max_idle_ms=self._connections_max_idle_ms)
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

        # detect api version if need
        if self._api_version == 'auto':
            self._api_version = yield from self.check_version()
        if type(self._api_version) is not tuple:
            self._api_version = tuple(
                map(int, self._api_version.split('.')))

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

            topics = self._topics
            if self._md_update_fut is None:
                self._md_update_fut = asyncio.Future(loop=self._loop)
            ret = yield from self._metadata_update(self.cluster, topics)
            # If list of topics changed during metadata update we must update
            # it again right away.
            if topics != self._topics:
                continue
            # Earlier this waiter was set before sending metadata_request,
            # but that was to avoid topic list changes being unnoticed, which
            # is handled explicitly now.
            self._md_update_waiter = asyncio.Future(loop=self._loop)

            self._md_update_fut.set_result(ret)
            self._md_update_fut = None

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
        topics = list(topics)
        version_id = 0 if self.api_version < (0, 10) else 1
        if version_id == 1 and not topics:
            topics = None
        metadata_request = MetadataRequest[version_id](topics)
        nodeids = [b.nodeId for b in self.cluster.brokers()]
        if 'bootstrap' in self._conns:
            nodeids.append('bootstrap')
        random.shuffle(nodeids)
        for node_id in nodeids:
            conn = yield from self._get_conn(node_id)

            if conn is None:
                continue
            log.debug("Sending metadata request %s to node %s",
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
        if self._md_update_fut is None:
            # Wake up the `_md_synchronizer` task
            if not self._md_update_waiter.done():
                self._md_update_waiter.set_result(None)
            self._md_update_fut = asyncio.Future(loop=self._loop)
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
            res = asyncio.Future(loop=self._loop)
            res.set_result(True)
        else:
            res = self.force_metadata_update()
        self._topics.add(topic)
        return res

    def set_topics(self, topics):
        """Set specific topics to track for metadata.

        Arguments:
            topics (list of str): topics to track
        """
        assert not isinstance(topics, str)
        if not topics or set(topics).difference(self._topics):
            res = self.force_metadata_update()
        else:
            res = asyncio.Future(loop=self._loop)
            res.set_result(True)
        self._topics = set(topics)
        return res

    def _on_connection_closed(self, conn, reason):
        """ Callback called when connection is closed
        """
        # Connection failures imply that our metadata is stale, so let's
        # refresh
        if reason == CloseReason.CONNECTION_BROKEN or \
                reason == CloseReason.CONNECTION_TIMEOUT:
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
                    request_timeout_ms=self._request_timeout_ms,
                    ssl_context=self._ssl_context,
                    security_protocol=self._security_protocol,
                    on_close=self._on_connection_closed)
        except (OSError, asyncio.TimeoutError) as err:
            log.error('Unable connect to node with id %s: %s', node_id, err)
            # Connection failures imply that our metadata is stale, so let's
            # refresh
            self.force_metadata_update()
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
            kafka.common.ConnectionError
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
            # close connection so it is renewed in next request
            self._conns[node_id].close(reason=CloseReason.CONNECTION_TIMEOUT)
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

        from kafka.protocol.admin import (
            ListGroupsRequest_v0, ApiVersionRequest_v0)
        from kafka.protocol.commit import (
            OffsetFetchRequest_v0, GroupCoordinatorRequest_v0)
        from kafka.protocol.metadata import MetadataRequest_v0
        test_cases = [
            ('0.10', ApiVersionRequest_v0()),
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
                # request can be ignored by Kafka broker,
                # so we send metadata request and wait response
                task = self._loop.create_task(conn.send(request))
                yield from asyncio.wait([task], timeout=0.1, loop=self._loop)
                try:
                    yield from conn.send(MetadataRequest_v0([]))
                except KafkaError:
                    # metadata request can be cancelled in case
                    # of invalid correlationIds order
                    pass
                yield from task
            except KafkaError:
                continue
            else:
                return version

        raise UnrecognizedBrokerVersion()

    @asyncio.coroutine
    def _wait_on_metadata(self, topic):
        """
        Wait for cluster metadata including partitions for the given topic to
        be available.

        Arguments:
            topic (str): topic we want metadata for

        Returns:
            set: partition ids for the topic

        Raises:
            UnknownTopicOrPartitionError: if no topic or partitions found
                in cluster metadata
        """
        if topic in self.cluster.topics():
            return self.cluster.partitions_for_topic(topic)

        # add topic to metadata topic list if it is not there already.
        self.add_topic(topic)

        t0 = self._loop.time()
        while True:
            yield from self.force_metadata_update()
            if topic in self.cluster.topics():
                break
            if (self._loop.time() - t0) > (self._request_timeout_ms / 1000):
                raise UnknownTopicOrPartitionError()
            yield from asyncio.sleep(self._retry_backoff, loop=self._loop)

        return self.cluster.partitions_for_topic(topic)

    @asyncio.coroutine
    def _maybe_wait_metadata(self):
        if self._md_update_fut is not None:
            yield from self._md_update_fut
