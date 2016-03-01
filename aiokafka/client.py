import asyncio
import binascii
import collections
import functools
import logging
import time

from kafka.conn import collect_hosts
from kafka.common import (TopicAndPartition, BrokerMetadata,
                          ConnectionError, FailedPayloadsError,
                          KafkaTimeoutError, KafkaUnavailableError,
                          LeaderNotAvailableError,
                          UnknownError,
                          UnknownTopicOrPartitionError,
                          NotLeaderForPartitionError,
                          ReplicaNotAvailableError, check_error)
from kafka.protocol import KafkaProtocol

from .conn import create_conn


__all__ = ['connect', 'AIOKafkaClient']


log = logging.getLogger('aiokafka')


@asyncio.coroutine
def connect(hosts, *, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    client = AIOKafkaClient(hosts, loop=loop)
    yield from client.load_metadata_for_topics()
    return client


class AIOKafkaClient:

    CLIENT_ID = b"aiokafka-python"

    def __init__(self, hosts, *, client_id=CLIENT_ID, loop):
        self._client_id = client_id
        self._hosts = collect_hosts(hosts)

        self._conns = {}
        self._brokers = {}  # broker_id -> BrokerMetadata
        self._topics_to_brokers = {}  # TopicAndPartition -> BrokerMetadata
        self._topic_partitions = {}  # topic -> partition -> PartitionMetadata

        self._loop = loop
        self._request_id = 0

    @property
    def hosts(self):
        return self._hosts

    @property
    def loop(self):
        return self._loop

    @asyncio.coroutine
    def _get_conn(self, host, port):
        "Get or create a connection to a broker using host and port"
        host_key = (host, port)
        if host_key not in self._conns:
            self._conns[host_key] = yield from create_conn(
                host, port, loop=self._loop)
        return self._conns[host_key]

    def _drop_conn(self, conn):
        conn.close()
        del self._conns[(conn.host, conn.port)]

    @asyncio.coroutine
    def _get_leader_for_partition(self, topic, partition):
        """
        Returns the leader for a partition or None if the partition exists
        but has no leader.

        UnknownTopicOrPartitionError will be raised if the topic or partition
        is not part of the metadata.

        LeaderNotAvailableError is raised if server has metadata, but there is
        no current leader
        """

        key = TopicAndPartition(topic, partition)

        # Use cached metadata if it is there
        if self._topics_to_brokers.get(key) is not None:
            return self._topics_to_brokers[key]

        # Otherwise refresh metadata

        # If topic does not already exist, this will raise
        # UnknownTopicOrPartitionError if not auto-creating
        # LeaderNotAvailableError otherwise until partitions are created
        yield from self.load_metadata_for_topics(topic)

        # If the partition doesn't actually exist, raise
        if partition not in self._topic_partitions[topic]:
            raise UnknownTopicOrPartitionError(key)

        # If there's no leader for the partition, raise
        meta = self._topic_partitions[topic][partition]
        if meta.leader == -1:
            raise LeaderNotAvailableError(meta)

        # Otherwise return the BrokerMetadata
        return self._brokers[meta.leader]

    def _next_id(self):
        """
        Generate a new correlation id
        """
        self._request_id += 1
        if self._request_id > 0x7fffffff:
            self._request_id = 0
        return self._request_id

    @asyncio.coroutine
    def _send_broker_unaware_request(self, payloads, encoder_fn, decoder_fn):
        """
        Attempt to send a broker-agnostic request to one of the available
        brokers. Keep trying until you succeed.
        """
        for (host, port) in self._hosts:
            request_id = self._next_id()
            try:
                conn = yield from self._get_conn(host, port)
                request = encoder_fn(client_id=self._client_id,
                                     correlation_id=request_id,
                                     payloads=payloads)

                try:
                    fut = conn.send(request)
                    response = yield from fut
                except Exception:
                    self._drop_conn(conn)
                    raise
                return decoder_fn(response)

            except Exception as e:
                log.warning("Could not send request [%r] to server %s:%i, "
                            "trying next server: %s",
                            request_id, host, port, e)

        raise KafkaUnavailableError("All servers failed to process request")

    @asyncio.coroutine
    def _send_broker_aware_request(self, payloads, encoder_fn, decoder_fn):
        """
        Group a list of request payloads by topic+partition and send them to
        the leader broker for that partition using the supplied encode/decode
        functions

        Params
        ======
        payloads: list of object-like entities with a topic (str) and
                  partition (int) attribute
        encode_fn: a method to encode the list of payloads to a request body,
                   must accept client_id, correlation_id, and payloads as
                   keyword arguments
        decode_fn: a method to decode a response body into response objects.
                   The response objects must be object-like and have topic
                   and partition attributes

        Return
        ======
        List of response objects in the same order as the supplied payloads
        """

        # Group the requests by topic+partition
        original_keys = []
        payloads_by_broker = collections.defaultdict(list)

        for payload in payloads:
            leader = yield from self._get_leader_for_partition(
                payload.topic, payload.partition)

            payloads_by_broker[leader].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # keep a list of payloads that were failed to be sent to brokers
        failed_payloads = []

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = yield from self._get_conn(
                broker.host.decode('utf-8'), broker.port)
            request_id = self._next_id()
            request = encoder_fn(client_id=self._client_id,
                                 correlation_id=request_id, payloads=payloads)

            failed = False
            # Send the request, recv the response
            try:
                try:
                    no_ack = decoder_fn is None
                    fut = conn.send(request, no_ack=no_ack)
                    if decoder_fn is None:
                        continue
                    try:
                        response = yield from fut
                    except ConnectionError as e:
                        log.warning(
                            "Could not receive response to request [%s] "
                            "from server %s: %s",
                            binascii.b2a_hex(request), conn, e)
                        failed = True
                        self._drop_conn(conn)
                except Exception:
                    self._drop_conn(conn)
                    raise
            except ConnectionError as e:
                log.warning("Could not send request [%s] to server %s: %s",
                            binascii.b2a_hex(request), conn, e)
                failed = True

            if failed:
                failed_payloads += payloads
                self.reset_all_metadata()
                continue

            for response in decoder_fn(response):
                acc[(response.topic, response.partition)] = response

        if failed_payloads:
            raise FailedPayloadsError(failed_payloads)

        # Order the accumulated responses by the original key order
        return (acc[k] for k in original_keys) if acc else ()

    def __repr__(self):
        return '<KafkaClient client_id=%s>' % self._client_id

    def _raise_on_response_error(self, resp):
        try:
            check_error(resp)
        except (UnknownTopicOrPartitionError, NotLeaderForPartitionError):
            self.reset_topic_metadata(resp.topic)
            raise

    def close(self):
        for conn in self._conns.values():
            conn.close()
        self._conns = {}

    def reset_topic_metadata(self, *topics):
        for topic in topics:
            try:
                partitions = self._topic_partitions[topic]
            except KeyError:
                continue

            for partition in partitions:
                self._topics_to_brokers.pop(
                    TopicAndPartition(topic, partition),
                    None)

            del self._topic_partitions[topic]

    def reset_all_metadata(self):
        self._topics_to_brokers.clear()
        self._topic_partitions.clear()

    def has_metadata_for_topic(self, topic):
        return (topic in self._topic_partitions and
                len(self._topic_partitions[topic]) > 0)

    def get_partition_ids_for_topic(self, topic):
        if topic not in self._topic_partitions:
            return None

        return list(self._topic_partitions[topic])

    @asyncio.coroutine
    def ensure_topic_exists(self, topic, *, timeout=30):
        start_time = time.monotonic()

        while not self.has_metadata_for_topic(topic):
            if time.monotonic() > start_time + timeout:
                raise KafkaTimeoutError(
                    "Unable to create topic {0}".format(topic))
            try:
                yield from self.load_metadata_for_topics(topic)
            # Catching UnknownError since there is a strange behavior on
            # Kafka <= 0.8.2.x when service already accepts a client connection
            # after cold start, but send_metadata_request raises UnknownError.
            except (LeaderNotAvailableError, UnknownError):
                pass
            except UnknownTopicOrPartitionError:
                # Server is not configured to auto-create
                # retrying in this case will not help
                raise
            yield from asyncio.sleep(0.5, loop=self._loop)

    @asyncio.coroutine
    def load_metadata_for_topics(self, *topics):
        """
        Fetch broker and topic-partition metadata from the server,
        and update internal data:
        broker list, topic/partition list, and topic/parition -> broker map

        This method should be called after receiving any error

        @param: *topics (optional)
        If a list of topics is provided, the metadata refresh will be limited
        to the specified topics only.

        Exceptions:
        ----------
        If the broker is configured to not auto-create topics,
        expect UnknownTopicOrPartitionError for topics that don't exist

        If the broker is configured to auto-create topics,
        expect LeaderNotAvailableError for new topics
        until partitions have been initialized.

        Exceptions *will not* be raised in a full refresh (i.e. no topic list)
        In this case, error codes will be logged as errors

        Partition-level errors will also not be raised here
        (a single partition w/o a leader, for example)
        """
        resp = yield from self.send_metadata_request(topics)

        log.debug("Broker metadata: %s", resp.brokers)
        log.debug("Topic metadata: %s", resp.topics)

        self._brokers = dict([(broker.nodeId, broker)
                              for broker in resp.brokers])

        for topic_metadata in resp.topics:
            topic = topic_metadata.topic
            partitions = topic_metadata.partitions

            self.reset_topic_metadata(topic)

            # Errors expected for new topics
            try:
                check_error(topic_metadata)
            except (UnknownTopicOrPartitionError,
                    LeaderNotAvailableError) as e:

                # Raise if the topic was passed in explicitly
                if topic in topics:
                    raise

                # Otherwise, just log a warning
                log.error("Error loading topic metadata for %s: %s",
                          topic, type(e))
                continue

            self._topic_partitions[topic] = {}
            for partition_metadata in partitions:
                partition = partition_metadata.partition
                leader = partition_metadata.leader

                self._topic_partitions[topic][partition] = partition_metadata

                # Populate topics_to_brokers dict
                topic_part = TopicAndPartition(topic, partition)

                # Check for partition errors
                try:
                    check_error(partition_metadata)

                # If No Leader, topics_to_brokers topic_partition -> None
                except LeaderNotAvailableError:
                    log.error('No leader for topic %s partition %d',
                              topic, partition)
                    self._topics_to_brokers[topic_part] = None
                    continue
                # If one of the replicas is unavailable -- ignore
                # this error code is provided for admin purposes only
                # we never talk to replicas, only the leader
                except ReplicaNotAvailableError:
                    log.warning('Some (non-leader) replicas not available '
                                'for topic %s partition %d', topic, partition)
                # If Known Broker, topic_partition -> BrokerMetadata
                if leader in self._brokers:
                    self._topics_to_brokers[topic_part] = self._brokers[leader]

                # If Unknown Broker, fake BrokerMetadata so we dont lose the id
                # (not sure how this could happen.
                # server could be in bad state)
                else:
                    self._topics_to_brokers[topic_part] = BrokerMetadata(
                        leader, None, None
                    )

    @asyncio.coroutine
    def send_metadata_request(self, payloads=()):

        encoder = KafkaProtocol.encode_metadata_request
        decoder = KafkaProtocol.decode_metadata_response

        return (yield from self._send_broker_unaware_request(
            payloads, encoder, decoder))

    def _build_resp(self, resps):
        out = []
        for resp in resps:
            self._raise_on_response_error(resp)
            out.append(resp)
        return out

    @asyncio.coroutine
    def send_produce_request(self, payloads=(), *, acks=1, ack_timeout=1.0):
        """
        Encode and send some ProduceRequests

        ProduceRequests will be grouped by (topic, partition) and then
        sent to a specific broker. Output is a list of responses in the
        same order as the list of payloads specified

        Params
        ======
        payloads: list of ProduceRequest

        Return
        ======
        list of ProduceResponse, in the order of input payloads
        """

        encoder = functools.partial(
            KafkaProtocol.encode_produce_request,
            acks=acks,
            timeout=int(ack_timeout*1000))

        if acks == 0:
            decoder = None
        else:
            decoder = KafkaProtocol.decode_produce_response

        resps = yield from self._send_broker_aware_request(
            payloads, encoder, decoder)

        return self._build_resp(resps)

    @asyncio.coroutine
    def send_fetch_request(self, payloads=(),
                           *, max_wait_time=0.1, min_bytes=4096):
        """
        Encode and send a FetchRequest

        Payloads are grouped by topic and partition so they can be pipelined
        to the same brokers.
        """

        encoder = functools.partial(KafkaProtocol.encode_fetch_request,
                                    max_wait_time=int(max_wait_time*1000),
                                    min_bytes=min_bytes)

        resps = yield from self._send_broker_aware_request(
            payloads, encoder,
            KafkaProtocol.decode_fetch_response)

        return self._build_resp(resps)

    @asyncio.coroutine
    def send_offset_request(self, payloads=()):
        resps = yield from self._send_broker_aware_request(
            payloads,
            KafkaProtocol.encode_offset_request,
            KafkaProtocol.decode_offset_response)

        return self._build_resp(resps)

    @asyncio.coroutine
    def send_offset_commit_request(self, group, payloads=()):
        encoder = functools.partial(KafkaProtocol.encode_offset_commit_request,
                                    group=group)
        decoder = KafkaProtocol.decode_offset_commit_response
        resps = yield from self._send_broker_aware_request(
            payloads, encoder, decoder)

        return self._build_resp(resps)

    @asyncio.coroutine
    def send_offset_fetch_request(self, group, payloads=()):

        encoder = functools.partial(KafkaProtocol.encode_offset_fetch_request,
                                    group=group)
        decoder = KafkaProtocol.decode_offset_fetch_response
        resps = yield from self._send_broker_aware_request(
            payloads, encoder, decoder)

        return self._build_resp(resps)
