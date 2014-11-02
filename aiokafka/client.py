import asyncio
import collections
import itertools
import logging
from kafka.conn import DEFAULT_SOCKET_TIMEOUT_SECONDS, collect_hosts
from kafka.common import (TopicAndPartition, BrokerMetadata,
                          ConnectionError, FailedPayloadsError,
                          KafkaTimeoutError, KafkaUnavailableError,
                          LeaderNotAvailableError,
                          UnknownTopicOrPartitionError,
                          NotLeaderForPartitionError)

from .conn import KafkaConnection

log = logging.getLogger('aiokafka')


class KafkaClient:

    CLIENT_ID = b"aiokafka-python"
    ID_GEN = itertools.count()

    def __init__(self, hosts, client_id=CLIENT_ID,
                 timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS):
        self._client_id = client_id
        self._timeout = timeout
        self._hosts = collect_hosts(hosts)

        self._conns = {}
        self._brokers = {}  # broker_id -> BrokerMetadata
        self._topics_to_brokers = {}  # TopicAndPartition -> BrokerMetadata
        self._topic_partitions = {}  # topic -> partition -> PartitionMetadata

        # self.load_metadata_for_topics()

    @asyncio.coroutine
    def _get_conn(self, host, port):
        "Get or create a connection to a broker using host and port"
        host_key = (host, port)
        if host_key not in self._conns:
            self._conns[host_key] = KafkaConnection(
                host, port)
        return self._conns[host_key]

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
        return next(self.ID_GEN)

    @asyncio.coroutine
    def _send_broker_unaware_request(self, payloads, encoder_fn, decoder_fn):
        """
        Attempt to send a broker-agnostic request to one of the available
        brokers. Keep trying until you succeed.
        """
        for (host, port) in self._hosts:
            request_id = self._next_id()
            try:
                conn = self._get_conn(host, port)
                request = encoder_fn(client_id=self._client_id,
                                     correlation_id=request_id,
                                     payloads=payloads)

                yield from conn.send(request_id, request)
                response = yield from conn.recv(request_id)
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
            leader = self._get_leader_for_partition(payload.topic,
                                                    payload.partition)

            payloads_by_broker[leader].append(payload)
            original_keys.append((payload.topic, payload.partition))

        # Accumulate the responses in a dictionary
        acc = {}

        # keep a list of payloads that were failed to be sent to brokers
        failed_payloads = []

        # For each broker, send the list of request payloads
        for broker, payloads in payloads_by_broker.items():
            conn = self._get_conn(broker.host.decode('utf-8'), broker.port)
            requestId = self._next_id()
            request = encoder_fn(client_id=self.client_id,
                                 correlation_id=requestId, payloads=payloads)

            failed = False
            # Send the request, recv the response
            try:
                conn.send(requestId, request)
                if decoder_fn is None:
                    continue
                try:
                    response = conn.recv(requestId)
                except ConnectionError as e:
                    log.warning("Could not receive response to request [%s] "
                                "from server %s: %s", binascii.b2a_hex(request), conn, e)
                    failed = True
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
        return '<KafkaClient client_id=%s>' % (self.client_id)

    def _raise_on_response_error(self, resp):
        try:
            kafka.common.check_error(resp)
        except (UnknownTopicOrPartitionError, NotLeaderForPartitionError):
            self.reset_topic_metadata(resp.topic)
            raise

