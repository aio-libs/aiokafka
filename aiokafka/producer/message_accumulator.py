import asyncio
import collections
import io
import copy

from kafka.protocol.types import Int32

from aiokafka.errors import (KafkaTimeoutError,
                             NotLeaderForPartitionError,
                             LeaderNotAvailableError,
                             ProducerClosed)
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder
from aiokafka.structs import RecordMetadata
from aiokafka.util import create_future


class BatchBuilder:
    def __init__(self, magic, batch_size, compression_type):
        self._builder = LegacyRecordBatchBuilder(
            magic, compression_type, batch_size)
        self._relative_offset = 0
        self._buffer = None
        self._closed = False

    def append(self, *, timestamp, key, value):
        """Add a message to the batch.

        Arguments:
            timestamp (float or None): epoch timestamp in seconds. If None,
                the timestamp will be set to the current time. If submitting to
                an 0.8.x or 0.9.x broker, the timestamp will be ignored.
            key (bytes or None): the message key. `key` and `value` may not
                both be None.
            value (bytes or None): the message value. `key` and `value` may not
                both be None.

        Returns:
            If the message was successfully added, returns a metadata object
            with crc, offset, size, and timestamp fields. If the batch is full
            or closed, returns None.
        """
        if self._closed:
            return None

        metadata = self._builder.append(
            self._relative_offset, timestamp, key, value)

        # Check if we could add the message
        if metadata is None:
            return None

        self._relative_offset += 1
        return metadata

    def close(self):
        """Close the batch to further updates.

        Closing the batch before submitting to the producer ensures that no
        messages are added via the ``producer.send()`` interface. To gracefully
        support both the batch and individual message interfaces, leave the
        batch open. For complete control over the batch's contents, close
        before submission. Closing a batch has no effect on when it's sent to
        the broker.

        A batch may not be reopened after it's closed.
        """
        if self._closed:
            return
        self._closed = True
        data = self._builder.build()
        self._buffer = io.BytesIO(Int32.encode(len(data)) + data)
        del self._builder

    def _build(self):
        if not self._closed:
            self.close()
        return self._buffer

    def size(self):
        """Get the size of batch in bytes."""
        if self._buffer:
            return self._buffer.getbuffer().nbytes
        else:
            return self._builder.size()

    def record_count(self):
        """Get the number of records in the batch."""
        return self._relative_offset


class MessageBatch:
    """This class incapsulate operations with batch of produce messages"""

    def __init__(self, tp, builder, ttl, loop):
        self._builder = builder
        self._tp = tp
        self._loop = loop
        self._ttl = ttl
        self._ctime = loop.time()

        # Waiters
        # Set when messages are delivered to Kafka based on ACK setting
        self.future = create_future(loop)
        self._msg_futures = []
        # Set when sender takes this batch
        self._drain_waiter = create_future(loop=loop)

    def append(self, key, value, timestamp_ms, _create_future=create_future):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when message is delivered
        """
        metadata = self._builder.append(
            timestamp=timestamp_ms, key=key, value=value)
        if metadata is None:
            return None

        future = _create_future(loop=self._loop)
        self._msg_futures.append((future, metadata))
        return future

    def done(self, base_offset, timestamp=None,
             _record_metadata_class=RecordMetadata):
        """Resolve all pending futures"""
        tp = self._tp
        topic = tp.topic
        partition = tp.partition
        if timestamp == -1:
            timestamp_type = 0
        else:
            timestamp_type = 1

        # Set main batch future
        if not self.future.done():
            self.future.set_result(_record_metadata_class(
                topic, partition, tp, base_offset, timestamp, timestamp_type))

        # Set message futures
        for future, metadata in self._msg_futures:
            if future.done():
                continue
            # If timestamp returned by broker is -1 it means we need to take
            # the timestamp sent by user.
            if timestamp == -1:
                timestamp = metadata.timestamp
            offset = base_offset + metadata.offset
            future.set_result(_record_metadata_class(
                topic, partition, tp, offset, timestamp, timestamp_type))

    def done_noack(self):
        """ Resolve all pending futures to None """
        # Faster resolve for base_offset=None case.
        if not self.future.done():
            self.future.set_result(None)
        for future, _ in self._msg_futures:
            if future.done():
                continue
            future.set_result(None)

    def failure(self, exception):
        if not self.future.done():
            self.future.set_exception(exception)
        for future, _ in self._msg_futures:
            if future.done():
                continue
            # we need to copy exception so traceback is not multiplied
            # https://github.com/aio-libs/aiokafka/issues/246
            future.set_exception(copy.copy(exception))

    def wait_deliver(self, timeout=None):
        """Wait until all message from this batch is processed"""
        return asyncio.wait([self.future], timeout=timeout, loop=self._loop)

    def wait_drain(self, timeout=None):
        """Wait until all message from this batch is processed"""
        return asyncio.wait(
            [self._drain_waiter], timeout=timeout, loop=self._loop)

    def expired(self):
        """Check that batch is expired or not"""
        return (self._loop.time() - self._ctime) > self._ttl

    def drain_ready(self):
        """Compress batch to be ready for send"""
        if not self._drain_waiter.done():
            self._drain_waiter.set_result(None)
            self._buffer = self._builder._build()

    def get_data_buffer(self):
        self._buffer.seek(0)
        return self._buffer


class MessageAccumulator:
    """Accumulator of messages batched by topic-partition

    Producer adds messages to this accumulator and a background send task
    gets batches per nodes to process it.
    """
    def __init__(self, cluster, batch_size, compression_type, batch_ttl, loop):
        self._batches = collections.defaultdict(collections.deque)
        self._cluster = cluster
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._batch_ttl = batch_ttl
        self._loop = loop
        self._wait_data_future = create_future(loop=loop)
        self._closed = False
        self._api_version = (0, 9)

    def set_api_version(self, api_version):
        self._api_version = api_version

    @asyncio.coroutine
    def flush(self):
        # NOTE: we copy to avoid mutation during `yield from` below
        for batches in list(self._batches.values()):
            for batch in list(batches):
                yield from batch.wait_deliver()

    @asyncio.coroutine
    def close(self):
        self._closed = True
        yield from self.flush()

    @asyncio.coroutine
    def add_message(self, tp, key, value, timeout, timestamp_ms=None):
        """ Add message to batch by topic-partition
        If batch is already full this method waits (`timeout` seconds maximum)
        until batch is drained by send task
        """
        if self._closed:
            # this can happen when producer is closing but try to send some
            # messages in async task
            raise ProducerClosed()

        pending_batches = self._batches.get(tp)
        if not pending_batches:
            builder = self.create_builder()
            batch = self._append_batch(builder, tp)
        else:
            batch = pending_batches[-1]

        future = batch.append(key, value, timestamp_ms)
        if future is None:
            # Batch is full, can't append data atm,
            # waiting until batch per topic-partition is drained
            start = self._loop.time()
            yield from batch.wait_drain(timeout)
            timeout -= self._loop.time() - start
            if timeout <= 0:
                raise KafkaTimeoutError()
            return (yield from self.add_message(
                tp, key, value, timeout, timestamp_ms))
        return future

    def data_waiter(self):
        """ Return waiter future that will be resolved when accumulator contain
        some data for drain
        """
        return self._wait_data_future

    def _pop_batch(self, tp):
        batch = self._batches[tp].popleft()
        batch.drain_ready()
        if len(self._batches[tp]) == 0:
            del self._batches[tp]
        return batch

    def reenqueue(self, batch):
        tp = batch._tp
        self._batches[tp].appendleft(batch)

    def drain_by_nodes(self, ignore_nodes):
        """ Group batches by leader to partiton nodes. """
        nodes = collections.defaultdict(dict)
        unknown_leaders_exist = False
        for tp in list(self._batches.keys()):
            leader = self._cluster.leader_for_partition(tp)
            if leader is None or leader == -1:
                if self._batches[tp][0].expired():
                    # batch is for partition is expired and still no leader,
                    # so set exception for batch and pop it
                    batch = self._pop_batch(tp)
                    if leader is None:
                        err = NotLeaderForPartitionError()
                    else:
                        err = LeaderNotAvailableError()
                    batch.failure(exception=err)
                unknown_leaders_exist = True
                continue
            elif ignore_nodes and leader in ignore_nodes:
                continue

            batch = self._pop_batch(tp)
            nodes[leader][tp] = batch

        # all batches are drained from accumulator
        # so create "wait data" future again for waiting new data in send
        # task
        if not self._wait_data_future.done():
            self._wait_data_future.set_result(None)
        self._wait_data_future = create_future(loop=self._loop)

        return nodes, unknown_leaders_exist

    def create_builder(self):
        magic = 0 if self._api_version < (0, 10) else 1
        return BatchBuilder(magic, self._batch_size, self._compression_type)

    def _append_batch(self, builder, tp):
        batch = MessageBatch(tp, builder, self._batch_ttl, self._loop)
        self._batches[tp].append(batch)
        if not self._wait_data_future.done():
            self._wait_data_future.set_result(None)
        return batch

    @asyncio.coroutine
    def add_batch(self, builder, tp, timeout):
        """Add BatchBuilder to queue by topic-partition.

        Arguments:
            builder (BatchBuilder): batch object to enqueue.
            tp (TopicPartition): topic and partition to enqueue this batch for.
            timeout (int): time in seconds to wait for a free slot in the batch
                queue.

        Returns:
            MessageBatch: delivery wrapper around the BatchBuilder object.

        Raises:
            aiokafka.errors.ProducerClosed: the accumulator has already been
                closed and flushed.
            aiokafka.errors.KafkaTimeoutError: the batch could not be added
                within the specified timeout.
        """
        if self._closed:
            raise ProducerClosed()

        start = self._loop.time()
        while timeout > 0:
            pending = self._batches.get(tp)
            if pending:
                yield from pending[-1].wait_drain(timeout=timeout)
                timeout -= self._loop.time() - start
            else:
                batch = self._append_batch(builder, tp)
                return asyncio.shield(batch.future, loop=self._loop)
        raise KafkaTimeoutError()
