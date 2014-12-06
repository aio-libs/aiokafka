import asyncio
import logging
import random

from itertools import cycle
from kafka import HashedPartitioner
from kafka.protocol import CODEC_NONE, ALL_CODECS, create_message_set
from kafka.common import UnsupportedCodecError, ProduceRequest


__all__ = ['AIOProducer', 'SimpleAIOProducer', 'KeyedAIOProducer']


log = logging.getLogger("aiokafka.producer")


class AIOProducer:

    ACK_NOT_REQUIRED = 0            # No ack is required
    ACK_AFTER_LOCAL_WRITE = 1       # Send response after it is written to log
    ACK_AFTER_CLUSTER_COMMIT = -1   # Send response after data is committed

    DEFAULT_ACK_TIMEOUT = 1000

    def __init__(self, client, req_acks=ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=DEFAULT_ACK_TIMEOUT, codec=None):
        self._client = client
        self._req_acks = req_acks
        self._ack_timeout = ack_timeout

        if codec is None:
            codec = CODEC_NONE
        elif codec not in ALL_CODECS:
            raise UnsupportedCodecError("Codec 0x%02x unsupported" % codec)
        self._codec = codec

    @asyncio.coroutine
    def _send_messages(self, topic, partition, *msg, **kwargs):
        key = kwargs.pop('key', None)

        if not isinstance(msg, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        if any(not isinstance(m, bytes) for m in msg):
            raise TypeError("all produce message payloads must be type bytes")

        if key is not None and not isinstance(key, bytes):
            raise TypeError("the key must be type bytes")

        messages = create_message_set(msg, self._codec, key)
        req = ProduceRequest(topic, partition, messages)
        try:
            resp = yield from self._client.send_produce_request(
                [req], acks=self._req_acks, timeout=self._ack_timeout)
        except Exception:
            log.exception("Unable to send messages")
            raise
        return resp


class SimpleAIOProducer(AIOProducer):

    def __init__(self, client,
                 req_acks=AIOProducer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=AIOProducer.DEFAULT_ACK_TIMEOUT,
                 codec=None,
                 random_start=False):

        self.partition_cycles = {}
        self.random_start = random_start
        super().__init__(client, req_acks, ack_timeout, codec)

    @asyncio.coroutine
    def _next_partition(self, topic):

        if topic not in self.partition_cycles:
            if not self._client.has_metadata_for_topic(topic):
                yield from self._client.load_metadata_for_topics(topic)

            partition_ids = self._client.get_partition_ids_for_topic(topic)
            self.partition_cycles[topic] = cycle(partition_ids)

            # Randomize the initial partition that is returned
            if self.random_start:
                for _ in range(random.randint(0, len(partition_ids)-1)):
                    next(self.partition_cycles[topic])
        return next(self.partition_cycles[topic])

    @asyncio.coroutine
    def send_messages(self, topic, msg, *msgs):
        partition = yield from self._next_partition(topic)
        resp = yield from self._send_messages(topic, partition, msg, *msgs)
        return resp

    def __repr__(self):
        return '<SimpleProducer>'


class KeyedAIOProducer(AIOProducer):

    def __init__(self, client, partitioner=None,
                 req_acks=AIOProducer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=AIOProducer.DEFAULT_ACK_TIMEOUT,
                 codec=None):

        self._partitioner_class = partitioner or HashedPartitioner
        self._partitioners = {}
        super().__init__(client, req_acks, ack_timeout, codec)

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
    def send(self, topic, key, msg):
        partition = yield from self._next_partition(topic, key)
        return (yield from self._send_messages(topic, partition, msg, key=key))

    def __repr__(self):
        return '<KeyedProducer>'
