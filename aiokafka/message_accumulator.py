import io
import asyncio
import collections

from kafka.common import (KafkaError,
                          KafkaTimeoutError,
                          NotLeaderForPartitionError,
                          LeaderNotAvailableError)
from kafka.producer.buffer import MessageSetBuffer
from kafka.protocol.message import Message

RecordMetadata = collections.namedtuple(
    'RecordMetadata', ['topic', 'partition', 'topic_partition', 'offset'])


class ProducerClosed(KafkaError):
    pass


class MessageBatch:
    """This class incapsulate operations with batch of produce messages"""
    def __init__(self, tp, records, ttl, loop):
        self._tp = tp
        self._records = records
        self._relative_offset = 0
        self._loop = loop
        self._ttl = ttl
        self._ctime = loop.time()

        # Waiters
        # Set when messages are delivered to Kafka based on ACK setting
        self._msg_futures = []
        # Set when sender takes this batch
        self._drain_waiter = asyncio.Future(loop=loop)

    def append(self, key, value):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when message is delivered
        """
        if not self._records.has_room_for(key, value):
            return None
        self._records.append(self._relative_offset, Message(value, key=key))
        future = asyncio.Future(loop=self._loop)
        self._msg_futures.append(future)
        self._relative_offset += 1
        return future

    def done(self, base_offset=None, exception=None):
        """Resolve all pending futures"""
        for relative_offset, future in enumerate(self._msg_futures):
            if exception is not None:
                future.set_exception(exception)
            elif base_offset is None:
                future.set_result(None)
            else:
                res = RecordMetadata(self._tp.topic, self._tp.partition,
                                     self._tp, base_offset+relative_offset)
                future.set_result(res)

    def wait_deliver(self):
        """Wait until all message from this batch is processed"""
        return asyncio.wait(self._msg_futures, loop=self._loop)

    def wait_drain(self):
        """Wait until all message from this batch is processed"""
        return self._drain_waiter

    def expired(self):
        """Check that batch is expired or not"""
        return (self._loop.time() - self._ctime) > self._ttl

    def drain_ready(self):
        """Compress batch to be ready for send"""
        self._records.close()
        self._drain_waiter.set_result(None)

    def data(self):
        return self._records.buffer()


class MessageAccumulator:
    """Accumulator of messages batches by topic-partition

    Producer add messages to this accumulator and background send task
    gets batches per nodes for process it.
    """
    def __init__(self, cluster, batch_size, compression_type, batch_ttl, loop):
        self._batches = {}
        self._cluster = cluster
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._batch_ttl = batch_ttl
        self._loop = loop
        self._wait_data_future = asyncio.Future(loop=loop)
        self._closed = False

    @asyncio.coroutine
    def close(self):
        self._closed = True
        for batch in list(self._batches.values()):
            yield from batch.wait_deliver()

    @asyncio.coroutine
    def add_message(self, tp, key, value, timeout):
        """Add message to batch by topic-partition
        If batch is already full this method waits (`ttl` seconds maximum)
        until batch is drained by send task
        """
        if self._closed:
            # this can happen when producer is closing but try to send some
            # messages in async task
            raise ProducerClosed()

        batch = self._batches.get(tp)
        if not batch:
            message_set_buffer = MessageSetBuffer(
                io.BytesIO(), self._batch_size, self._compression_type)
            batch = MessageBatch(
                tp, message_set_buffer, self._batch_ttl, self._loop)
            self._batches[tp] = batch

            if not self._wait_data_future.done():
                # Wakeup sender task if it waits for data
                self._wait_data_future.set_result(None)

        future = batch.append(key, value)
        if future is None:
            # Batch is full, can't append data atm,
            # waiting until batch per topic-partition is drained
            start = self._loop.time()
            yield from asyncio.wait(
                [batch.wait_drain()], timeout=timeout, loop=self._loop)
            timeout -= self._loop.time() - start
            if timeout <= 0:
                raise KafkaTimeoutError()
            return (yield from self.add_message(tp, key, value, timeout))
        return future

    @asyncio.coroutine
    def wait_data(self):
        """wait until there are some data for send"""
        if self._wait_data_future.done():
            return
        yield from self._wait_data_future

    def _pop_batch(self, tp):
        batch = self._batches.pop(tp)
        batch.drain_ready()
        return batch

    def drain_by_nodes(self, ignore_nodes):
        """return batches by nodes"""
        nodes = collections.defaultdict(dict)
        unknown_leaders_exist = False
        for tp in list(self._batches.keys()):
            leader = self._cluster.leader_for_partition(tp)
            if leader is None or leader == -1:
                if self._batches[tp].expired():
                    # batch is for partition is expired and still no leader,
                    # so set exception for batch and pop it
                    batch = self._pop_batch(tp)
                    if leader is None:
                        err = NotLeaderForPartitionError()
                    else:
                        err = LeaderNotAvailableError()
                    batch.done(exception=err)
                unknown_leaders_exist = True
                continue
            elif ignore_nodes and leader in ignore_nodes:
                continue

            batch = self._pop_batch(tp)
            nodes[leader][tp] = batch

        # all batches are drained from accumulator
        # so create "wait data" future again for waiting new data in send
        # task
        self._wait_data_future = asyncio.Future(loop=self._loop)

        return nodes, unknown_leaders_exist
