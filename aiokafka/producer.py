import asyncio
import logging
import random
from itertools import cycle

from kafka.protocol import (CODEC_NONE, CODEC_SNAPPY, CODEC_GZIP, ALL_CODECS,
                            create_message_set)
from kafka import HashedPartitioner
from kafka.common import UnsupportedCodecError, ProduceRequest


__all__ = ['AIOProducer', 'SimpleAIOProducer', 'KeyedAIOProducer']


log = logging.getLogger("aiokafka.producer")

CODEC_SNAPPY, CODEC_GZIP  # For pyflakes check


class AIOProducer:

    ACK_NOT_REQUIRED = 0            # No ack is required
    ACK_AFTER_LOCAL_WRITE = 1       # Send response after it is written to log
    ACK_AFTER_CLUSTER_COMMIT = -1   # Send response after data is committed

    def __init__(self, client, *, req_acks, ack_timeout, codec=None):
        self._client = client
        self._req_acks = req_acks
        self._ack_timeout = ack_timeout

        if codec is None:
            codec = CODEC_NONE
        elif codec not in ALL_CODECS:
            raise UnsupportedCodecError("Codec 0x%02x unsupported" % codec)
        self._codec = codec

    @asyncio.coroutine
    def _send(self, topic, partition, *msgs, key=None):

        if any(not isinstance(m, (bytes, bytearray, memoryview))
               for m in msgs):
            raise TypeError("all produce message payloads must be byte-ish")

        if key is not None and not isinstance(key,
                                              (bytes, bytearray, memoryview)):
            raise TypeError("the key must be byte-ish")

        messages = create_message_set(msgs, self._codec, key)
        req = ProduceRequest(topic, partition, messages)
        try:
            resp = yield from self._client.send_produce_request(
                [req], acks=self._req_acks, timeout=self._ack_timeout)
        except Exception:
            log.exception("Unable to send messages")
            raise
        return resp


class SimpleAIOProducer(AIOProducer):
    """A simple, round-robin producer. Each message goes to exactly one
    partition

    client: the aiokafka client instance to use
    req_acks: a value indicating the acknowledgements that
        the server must receive before responding to the request
    ack_timeout: ``int``, value (in milliseconds) indicating a timeout
        for waiting for an acknowledgement
    codec: a valued indicating message compression codec, by default no
        compression used.
    random_start: ``bool``, if true, randomize the initial partition
        which the the first message block will be published to, otherwise
        if false, the first message block will always publish  to partition
        0 before cycling through each partition
    """

    def __init__(self, client, *,
                 req_acks=AIOProducer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=1.0,
                 codec=None,
                 random_start=False):

        self._partition_cycles = {}
        self._random_start = random_start
        super().__init__(client, req_acks=req_acks, ack_timeout=ack_timeout,
                         codec=codec)

    @asyncio.coroutine
    def _next_partition(self, topic):

        if topic not in self._partition_cycles:
            if not self._client.has_metadata_for_topic(topic):
                yield from self._client.load_metadata_for_topics(topic)

            partition_ids = self._client.get_partition_ids_for_topic(topic)
            self._partition_cycles[topic] = cycle(partition_ids)

            # Randomize the initial partition that is returned
            if self._random_start:
                for _ in range(random.randint(0, len(partition_ids)-1)):
                    next(self._partition_cycles[topic])
        return next(self._partition_cycles[topic])

    @asyncio.coroutine
    def send(self, topic, *msgs):
        partition = yield from self._next_partition(topic)
        resp = yield from self._send(topic, partition, *msgs)
        return resp

    def __repr__(self):
        return '<SimpleAIOProducer req_acks={}>'.format(self._req_acks)


class KeyedAIOProducer(AIOProducer):
    """A producer which distributes messages to partitions based on the key

    client: the aiokafka client instance to use
    partitioner: partitioner class that will be used to get the
        partition to send the message to. Must be derived from ``Partitioner``
    req_acks: a value indicating the acknowledgements that
        the server must receive before responding to the request
    ack_timeout: ``int``, value (in milliseconds) indicating a timeout
        for waiting for an acknowledgement
    codec: a valued indicating message compression codec, by default no
        compression used.
    random_start: ``bool``, if true, randomize the initial partition
        which the the first message block will be published to, otherwise
        if false, the first message block will always publish  to partition
        0 before cycling through each partition
    """

    def __init__(self, client, *, partitioner=None,
                 req_acks=AIOProducer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=1.0, codec=None):

        self._partitioner_class = partitioner or HashedPartitioner
        self._partitioners = {}
        super().__init__(client, req_acks=req_acks, ack_timeout=ack_timeout,
                         codec=codec)

    @asyncio.coroutine
    def _next_partition(self, topic, key):

        if topic not in self._partitioners:
            if not self._client.has_metadata_for_topic(topic):
                yield from self._client.load_metadata_for_topics(topic)
            partition_ids = self._client.get_partition_ids_for_topic(topic)
            self._partitioners[topic] = self._partitioner_class(partition_ids)

        partition_ids = self._client.get_partition_ids_for_topic(topic)
        partitioner = self._partitioners[topic]
        return partitioner.partition(key, partition_ids)

    @asyncio.coroutine
    def send(self, topic, *msgs, key):
        partition = yield from self._next_partition(topic, key)
        return (yield from self._send(topic, partition, *msgs, key=key))

    def __repr__(self):
        return '<KeyedAIOProducer req_acks={} partitioner={!r}>'.format(
            self._req_acks, self._partitioner_class)
