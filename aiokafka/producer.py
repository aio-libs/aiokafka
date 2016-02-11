import asyncio
import copy
import logging
import io

from aiokafka.client import AIOKafkaClient
from kafka.common import (TopicPartition,
                          NotLeaderForPartitionError,
                          LeaderNotAvailableError,
                          MessageSizeTooLargeError)

from kafka.producer.buffer import MessageSetBuffer
from kafka.partitioner.default import DefaultPartitioner
from kafka.protocol.message import Message, MessageSet
from kafka.protocol.produce import ProduceRequest


log = logging.getLogger(__name__)


class AIOKafkaProducer(object):
    """A Kafka client that publishes records to the Kafka cluster.

    The producer consists of a pool of buffer space that holds records that
    haven't yet been transmitted to the server as well as a background I/O
    thread that is responsible for turning these records into requests and
    transmitting them to the cluster.

    The send() method is asynchronous. When called it adds the record to a
    buffer of pending record sends and immediately returns. This allows the
    producer to batch together individual records for efficiency.

    The 'acks' config controls the criteria under which requests are considered
    complete. The "all" setting will result in blocking on the full commit of
    the record, the slowest but most durable setting.

    If the request fails, the producer can automatically retry, unless
    'retries' is configured to 0. Enabling retries also opens up the
    possibility of duplicates (see the documentation on message
    delivery semantics for details:
    http://kafka.apache.org/documentation.html#semantics
    ).

    The producer maintains buffers of unsent records for each partition. These
    buffers are of a size specified by the 'batch_size' config. Making this
    larger can result in more batching, but requires more memory (since we will
    generally have one of these buffers for each active partition).

    By default a buffer is available to send immediately even if there is
    additional unused space in the buffer. However if you want to reduce the
    number of requests you can set 'linger_ms' to something greater than 0.
    This will instruct the producer to wait up to that number of milliseconds
    before sending a request in hope that more records will arrive to fill up
    the same batch. This is analogous to Nagle's algorithm in TCP. Note that
    records that arrive close together in time will generally batch together
    even with linger_ms=0 so under heavy load batching will occur regardless of
    the linger configuration; however setting this to something larger than 0
    can lead to fewer, more efficient requests when not under maximal load at
    the cost of a small amount of latency.

    The key_serializer and value_serializer instruct how to turn the key and
    value objects the user provides into bytes.

    Keyword Arguments:
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the producer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client.
            Default: 'kafka-python-producer-#' (appended with a unique number
            per instance)
        key_serializer (callable): used to convert user-supplied keys to bytes
            If not None, called as f(key), should return bytes. Default: None.
        value_serializer (callable): used to convert user-supplied message
            values to bytes. If not None, called as f(value), should return
            bytes. Default: None.
        acks (0, 1, 'all'): The number of acknowledgments the producer requires
            the leader to have received before considering a request complete.
            This controls the durability of records that are sent. The
            following settings are common:
            0:  Producer will not wait for any acknowledgment from the server
                at all. The message will immediately be added to the socket
                buffer and considered sent. No guarantee can be made that the
                server has received the record in this case, and the retries
                configuration will not take effect (as the client won't
                generally know of any failures). The offset given back for each
                record will always be set to -1.
            1: The broker leader will write the record to its local log but
                will respond without awaiting full acknowledgement from all
                followers. In this case should the leader fail immediately
                after acknowledging the record but before the followers have
                replicated it then the record will be lost.
            all: The broker leader will wait for the full set of in-sync
                replicas to acknowledge the record. This guarantees that the
                record will not be lost as long as at least one in-sync replica
                remains alive. This is the strongest available guarantee.
            If unset, defaults to acks=1.
        compression_type (str): The compression type for all data generated by
            the producer. Valid values are 'gzip', 'snappy', 'lz4', or None.
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression). Default: None.
        retries (int): Setting a value greater than zero will cause the client
            to resend any record whose send fails with a potentially transient
            error. Note that this retry is no different than if the client
            resent the record upon receiving the error. Allowing retries will
            potentially change the ordering of records because if two records
            are sent to a single partition, and the first fails and is retried
            but the second succeeds, then the second record may appear first.
            Default: 0.
        batch_size (int): Requests sent to brokers will contain multiple
            batches, one for each partition with data available to be sent.
            A small batch size will make batching less common and may reduce
            throughput (a batch size of zero will disable batching entirely).
            Default: 16384
        linger_ms (int): The producer groups together any records that arrive
            in between request transmissions into a single batched request.
            Normally this occurs only under load when records arrive faster
            than they can be sent out. However in some circumstances the client
            may want to reduce the number of requests even under moderate load.
            This setting accomplishes this by adding a small amount of
            artificial delay; that is, rather than immediately sending out a
            record the producer will wait for up to the given delay to allow
            other records to be sent so that the sends can be batched together.
            This can be thought of as analogous to Nagle's algorithm in TCP.
            This setting gives the upper bound on the delay for batching: once
            we get batch_size worth of records for a partition it will be sent
            immediately regardless of this setting, however if we have fewer
            than this many bytes accumulated for this partition we will
            'linger' for the specified time waiting for more records to show
            up. This setting defaults to 0 (i.e. no delay). Setting linger_ms=5
            would have the effect of reducing the number of requests sent but
            would add up to 5ms of latency to records sent in the absense of
            load. Default: 0.
        partitioner (callable): Callable used to determine which partition
            each message is assigned to. Called (after key serialization):
            partitioner(key_bytes, all_partitions, available_partitions).
            The default partitioner implementation hashes each non-None key
            using the same murmur2 algorithm as the java client so that
            messages with the same key are assigned to the same partition.
            When a key is None, the message is delivered to a random partition
            (filtered to partitions with available leaders only, if possible).
        max_block_ms (int): Number of milliseconds to block during send()
            when attempting to allocate additional memory before raising an
            exception. Default: 60000.
        max_request_size (int): The maximum size of a request. This is also
            effectively a cap on the maximum record size. Note that the server
            has its own cap on record size which may be different from this.
            This setting will limit the number of record batches the producer
            will send in a single request to avoid sending huge requests.
            Default: 1048576.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 30000.
        receive_buffer_bytes (int): The size of the TCP receive buffer
            (SO_RCVBUF) to use when reading data. Default: 32768
        send_buffer_bytes (int): The size of the TCP send buffer
            (SO_SNDBUF) to use when sending data. Default: 131072
        reconnect_backoff_ms (int): The amount of time in milliseconds to
            wait before attempting to reconnect to a given host.
            Default: 50.
        max_in_flight_requests_per_connection (int): Requests are pipelined
            to kafka brokers up to this number of maximum requests per
            broker connection. Default: 5.
        api_version (str): specify which kafka API version to use.
            If set to 'auto', will attempt to infer the broker version by
            probing various APIs. Default: auto

    Note:
        Configuration parameters are described in more detail at
        https://kafka.apache.org/090/configuration.html#producerconfigs
    """
    _DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': None,
        'key_serializer': None,
        'value_serializer': None,
        'acks': 1,
        'compression_type': None,
        'retries': 0,
        'batch_size': 16384,
        'linger_ms': 0,
        'partitioner': DefaultPartitioner(),
        'connections_max_idle_ms': 600000, # not implemented yet
        'max_block_ms': 60000,
        'max_request_size': 1048576,
        'metadata_max_age_ms': 300000,
        'retry_backoff_ms': 100,
        'request_timeout_ms': 30000,
        'receive_buffer_bytes': 32768,
        'send_buffer_bytes': 131072,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'api_version': 'auto',
    }

    PRODUCER_CLIENT_ID_SEQUENCE = 0

    def __init__(self, *, loop, **configs):
        log.debug("Starting the Kafka producer") # trace
        self.config = copy.copy(self._DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        # Only check for extra config keys in top-level class
        assert not configs, 'Unrecognized configs: %s' % configs

        self.PRODUCER_CLIENT_ID_SEQUENCE += 1
        if self.config['client_id'] is None:
            self.config['client_id'] = 'kafka-python-producer-%s' % \
                self.PRODUCER_CLIENT_ID_SEQUENCE

        if self.config['acks'] == 'all':
            self.config['acks'] = -1

        self.client = AIOKafkaClient(loop=loop, **self.config)
        self._metadata = self.client.cluster

        self._closed = False
        log.debug("Kafka producer started")

    @asyncio.coroutine
    def start(self):
        yield from self.client.bootstrap()

        # Check Broker Version if not set explicitly
        if self.config['api_version'] == 'auto':
            self.config['api_version'] = yield from self.client.check_version()
        assert self.config['api_version'] in ('0.9', '0.8.2', '0.8.1', '0.8.0')

        # Convert api_version config to tuple for easy comparisons
        self.config['api_version'] = tuple(
            map(int, self.config['api_version'].split('.')))

        if self.config['compression_type'] == 'lz4':
            assert self.config['api_version'] >= (0, 8, 2), \
                'LZ4 Requires >= Kafka 0.8.2 Brokers'

    @asyncio.coroutine
    def stop(self):
        if self._closed:
            return

        self.client.close()

        try:
            self.config['key_serializer'].close()
        except AttributeError:
            pass
        try:
            self.config['value_serializer'].close()
        except AttributeError:
            pass
        self._closed = True
        log.debug("The Kafka producer has closed.")

    @asyncio.coroutine
    def partitions_for(self, topic):
        """Returns set of all known partitions for the topic."""
        return (yield from self._wait_on_metadata(topic))

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
        # add topic to metadata topic list if it is not there already.
        yield from self.client.add_topic(topic)
        return self._metadata.partitions_for_topic(topic)

    def send(self, topic, value=None, key=None, partition=None):
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (optional): message value. Must be type bytes, or be
                serializable to bytes via configured value_serializer. If value
                is None, key is required and message acts as a 'delete'.
                See kafka compaction documentation for more details:
                http://kafka.apache.org/documentation.html#compaction
                (compaction requires kafka >= 0.8.1)
            partition (int, optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                'partitioner'.
            key (optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is None (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is None, partition is chosen randomly).
                Must be type bytes, or be serializable to bytes via configured
                key_serializer.

        Returns:
            response from Kafka
        """
        assert value is not None or self.config['api_version'] >= (0, 8, 1), (
            'Null messages require kafka >= 0.8.1')
        assert not (value is None and key is None), \
            'Need at least one: key or value'

        # first make sure the metadata for the topic is available
        yield from self._wait_on_metadata(topic)

        key_bytes, value_bytes = self._serialize(topic, key, value)
        partition = self._partition(topic, partition, key, value,
                                    key_bytes, value_bytes)

        tp = TopicPartition(topic, partition)
        leader = self._metadata.leader_for_partition(tp)
        if leader is None:
            raise NotLeaderForPartitionError()
        elif leader == -1:
            raise LeaderNotAvailableError()

        log.debug("Sending (key=%s value=%s) to %s", key, value, tp)
        msg = Message(value_bytes, key=key_bytes)
        buf = MessageSetBuffer(io.BytesIO(),
                         self.config['batch_size'],
                         self.config['compression_type'])
        buf.append(0, msg)
        buf.close()
        request = ProduceRequest(
            required_acks=self.config['acks'],
            timeout=self.config['request_timeout_ms'],
            topics=[(topic, [(partition, buf.buffer())])])

        response = yield from self.client.send(leader, request)
        return response

    def _serialize(self, topic, key, value):
        if self.config['key_serializer']:
            serialized_key = self.config['key_serializer'](key)
        else:
            serialized_key = key
        if self.config['value_serializer']:
            serialized_value = self.config['value_serializer'](value)
        else:
            serialized_value = value

        message_size = MessageSet.HEADER_SIZE + Message.HEADER_SIZE
        if serialized_key is not None:
            message_size += len(serialized_key)
        if serialized_value is not None:
            message_size += len(serialized_value)
        if message_size > self.config['max_request_size']:
            raise MessageSizeTooLargeError(
                "The message is %d bytes when serialized which is larger than"
                " the maximum request size you have configured with the"
                " max_request_size configuration" % message_size)

        return serialized_key, serialized_value

    def _partition(self, topic, partition, key, value,
                   serialized_key, serialized_value):
        if partition is not None:
            assert partition >= 0
            assert partition in self._metadata.partitions_for_topic(topic), \
                'Unrecognized partition'
            return partition

        all_partitions = list(self._metadata.partitions_for_topic(topic))
        available = list(self._metadata.available_partitions_for_topic(topic))
        return self.config['partitioner'](serialized_key,
                                          all_partitions,
                                          available)
