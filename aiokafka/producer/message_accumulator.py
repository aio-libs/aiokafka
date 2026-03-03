import asyncio
import collections
import contextlib
import copy
import time
from collections.abc import Sequence

from aiokafka.errors import (
    KafkaTimeoutError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    ProducerClosed,
)
from aiokafka.record.default_records import DefaultRecordBatchBuilder
from aiokafka.structs import RecordMetadata
from aiokafka.util import create_future, get_running_loop


class BatchBuilder:
    def __init__(
        self,
        batch_size,
        compression_type,
        *,
        is_transactional=0,
        key_serializer=None,
        value_serializer=None,
    ):
        self._builder = DefaultRecordBatchBuilder(
            2,
            compression_type,
            is_transactional=is_transactional,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=0,
            batch_size=batch_size,
        )
        self._relative_offset = 0
        self._buffer = None
        self._closed = False
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

    def _serialize(self, key, value):
        if self._key_serializer is None:
            serialized_key = key
        else:
            serialized_key = self._key_serializer(key)
        if self._value_serializer is None:
            serialized_value = value
        else:
            serialized_value = self._value_serializer(value)

        return serialized_key, serialized_value

    def append(self, *, timestamp, key, value, headers: Sequence = []):
        """Add a message to the batch.

        Arguments:
            timestamp (float or None): epoch timestamp in seconds. If None,
                the timestamp will be set to the current time.
            key (bytes or None): the message key. `key` and `value` may not
                both be None.
            value (bytes or None): the message value. `key` and `value` may not
                both be None.

        Returns:
            If the message was successfully added, returns a metadata object
            with crc, offset, size, and timestamp fields. If the batch is full
            or closed, returns None.
        """
        if headers is None:
            headers = []
        if self._closed:
            return None

        key_bytes, value_bytes = self._serialize(key, value)
        metadata = self._builder.append(
            self._relative_offset,
            timestamp,
            key=key_bytes,
            value=value_bytes,
            headers=headers,
        )

        # Check if we could add the message
        if metadata is None:
            self.close()
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

    def _set_producer_state(self, producer_id, producer_epoch, base_sequence):
        assert type(self._builder) is DefaultRecordBatchBuilder
        self._builder.set_producer_state(producer_id, producer_epoch, base_sequence)

    def _build(self):
        self.close()
        if self._buffer is None:
            self._buffer = self._builder.build()
            del self._builder  # We may only call self._builder.build() once!
        return self._buffer

    def size(self):
        """Get the size of batch in bytes."""
        if self._buffer is not None:
            return len(self._buffer)
        else:
            return self._builder.size()

    def record_count(self):
        """Get the number of records in the batch."""
        return self._relative_offset

    def closed(self):
        """Indicates if the builder is already closed"""
        return self._closed


class MessageBatch:
    """This class encapsulate operations with batch of produce messages"""

    def __init__(self, tp, builder, ttl, linger_time):
        self._builder = builder
        self._tp = tp
        self._ttl = ttl
        self._linger_time = linger_time
        self._ctime = time.monotonic()

        # Waiters
        # Set when messages are delivered to Kafka based on ACK setting
        self.future = create_future()
        self._msg_futures = []
        # Set when sender takes this batch
        self._drain_waiter = create_future()
        self._retry_count = 0

    @property
    def tp(self):
        return self._tp

    @property
    def record_count(self):
        return self._builder.record_count()

    def append(
        self,
        key,
        value,
        timestamp_ms,
        _create_future=create_future,
        headers: Sequence = [],
    ):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when message is delivered
        """
        metadata = self._builder.append(
            timestamp=timestamp_ms, key=key, value=value, headers=headers
        )
        if metadata is None:
            return None

        future = _create_future()
        self._msg_futures.append((future, metadata))
        return future

    def done(
        self,
        base_offset,
        timestamp=None,
        log_start_offset=None,
        _record_metadata_class=RecordMetadata,
    ):
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
            self.future.set_result(
                _record_metadata_class(
                    topic,
                    partition,
                    tp,
                    base_offset,
                    timestamp,
                    timestamp_type,
                    log_start_offset,
                )
            )

        # Set message futures
        for future, metadata in self._msg_futures:
            if future.done():
                continue
            # If timestamp returned by broker is -1 it means we need to take
            # the timestamp sent by user.
            if timestamp == -1:
                timestamp = metadata.timestamp
            offset = base_offset + metadata.offset
            future.set_result(
                _record_metadata_class(
                    topic,
                    partition,
                    tp,
                    offset,
                    timestamp,
                    timestamp_type,
                    log_start_offset,
                )
            )

    def done_noack(self):
        """Resolve all pending futures to None"""
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

        # Consume exception to avoid warnings. We delegate this consumption
        # to user only in case of explicit batch API.
        if self._msg_futures:
            self.future.exception()

        # In case where sender fails and closes batches all waiters have to be
        # reset also.
        if not self._drain_waiter.done():
            self._drain_waiter.set_exception(exception)

    async def wait_drain(self, timeout=None):
        """Wait until all message from this batch is processed"""
        waiter = self._drain_waiter
        await asyncio.wait([waiter], timeout=timeout)
        if waiter.done():
            waiter.result()  # Check for exception

    def remaining_linger(self):
        """Return the eventual remaining linger time"""
        lifetime = time.monotonic() - self._ctime
        # Batch builders reject a message if they reach
        # their maximum capacity
        if self._builder.closed() or lifetime >= self._linger_time:
            return None
        return self._linger_time - lifetime

    def expired(self):
        """Check that batch is expired or not"""
        return (time.monotonic() - self._ctime) > self._ttl

    def drain_ready(self):
        """Compress batch to be ready for send"""
        if not self._drain_waiter.done():
            self._drain_waiter.set_result(None)
        self._retry_count += 1

    def reset_drain(self):
        """Reset drain waiter, until we will do another retry"""
        assert self._drain_waiter.done()
        self._drain_waiter = create_future()

    def set_producer_state(self, producer_id, producer_epoch, base_sequence):
        assert not self._drain_waiter.done()
        self._builder._set_producer_state(producer_id, producer_epoch, base_sequence)

    def get_data_buffer(self):
        return self._builder._build()

    def is_empty(self):
        return self._builder.record_count() == 0

    @property
    def retry_count(self):
        return self._retry_count


class MessageAccumulator:
    """Accumulator of messages batched by topic-partition

    Producer adds messages to this accumulator and a background send task
    gets batches per nodes to process it.
    """

    def __init__(
        self,
        cluster,
        batch_size,
        compression_type,
        batch_ttl,
        *,
        txn_manager=None,
        loop=None,
        linger_ms=0,
    ):
        if loop is None:
            loop = get_running_loop()
        self._loop = loop
        self._batches = collections.defaultdict(collections.deque)
        self._pending_batches = set()
        self._cluster = cluster
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._batch_ttl = batch_ttl
        self._waiter_future = loop.create_future()
        self._wakeup_task = None
        self._closed = False
        self._txn_manager = txn_manager
        self._linger_time = linger_ms / 1000

        self._exception = None  # Critical exception

    async def flush(self):
        waiters = [
            batch.future for batches in self._batches.values() for batch in batches
        ]
        waiters += [batch.future for batch in self._pending_batches]
        if waiters:
            await asyncio.wait(waiters)

    async def flush_for_commit(self):
        waiters = []
        for batches in self._batches.values():
            for batch in batches:
                # We force all buffers to close to finalize the transaction
                # scope. We should not add anything to this transaction.
                batch._builder.close()
                waiters.append(batch.future)
        # We wake up eventually the sender waiting for lingering batches
        if not self._waiter_future.done():
            self._waiter_future.set_result(None)
        waiters += [batch.future for batch in self._pending_batches]
        # Wait for all waiters to finish. We only wait for the scope we defined
        # above, other batches should not be delivered as part of this
        # transaction
        if waiters:
            await asyncio.wait(waiters)

    def fail_all(self, exception):
        # Close all batches with this exception
        for batches in self._batches.values():
            for batch in batches:
                batch.failure(exception)
        for batch in self._pending_batches:
            batch.failure(exception)
        self._exception = exception

    async def close(self):
        self._closed = True
        await self.flush()

    async def add_message(
        self,
        tp,
        key,
        value,
        timeout,
        timestamp_ms=None,
        headers: Sequence = [],
    ):
        """Add message to batch by topic-partition
        If batch is already full this method waits (`timeout` seconds maximum)
        until batch is drained by send task
        """
        while True:
            if self._closed:
                # this can happen when producer is closing but try to send some
                # messages in async task
                raise ProducerClosed()
            if self._exception is not None:
                raise copy.copy(self._exception)

            pending_batches = self._batches.get(tp)
            if not pending_batches:
                builder = self.create_builder()
                batch = self._append_batch(builder, tp)
            else:
                batch = pending_batches[-1]

            future = batch.append(key, value, timestamp_ms, headers=headers)
            if future is not None:
                return future
            # Batch is full, can't append data atm,
            # wake up the sender loop
            # and wait until batch per topic-partition is drained
            if not self._waiter_future.done():
                self._waiter_future.set_result(None)

            start = time.monotonic()
            await batch.wait_drain(timeout)
            timeout -= time.monotonic() - start
            if timeout <= 0:
                raise KafkaTimeoutError()

    def waiter(self):
        """Return waiter future that will be resolved when accumulator contain
        some data for drain or a batch lingering is ready to be picked
        """
        return self._waiter_future

    def _pop_batch(self, tp):
        batch = self._batches[tp].popleft()
        not_retry = batch.retry_count == 0
        if self._txn_manager is not None and not_retry:
            assert self._txn_manager.has_pid(), (
                "We should have waited for it in sender routine"
            )
            seq = self._txn_manager.sequence_number(batch.tp)
            self._txn_manager.increment_sequence_number(batch.tp, batch.record_count)
            batch.set_producer_state(
                producer_id=self._txn_manager.producer_id,
                producer_epoch=self._txn_manager.producer_epoch,
                base_sequence=seq,
            )
        batch.drain_ready()
        if len(self._batches[tp]) == 0:
            del self._batches[tp]
        self._pending_batches.add(batch)

        if not_retry:

            def cb(fut, batch=batch, self=self):
                self._pending_batches.remove(batch)

            batch.future.add_done_callback(cb)
        return batch

    def reenqueue(self, batch):
        tp = batch.tp
        self._batches[tp].appendleft(batch)
        self._pending_batches.remove(batch)
        batch.reset_drain()

    async def drain_by_nodes(self, ignore_nodes, muted_partitions=frozenset()):
        """Group batches by leader to partition nodes."""
        nodes = collections.defaultdict(dict)
        unknown_leaders_exist = False
        remaining_linger_time = None
        for tp in list(self._batches.keys()):
            # Just ignoring by node is not enough, as leader can change during
            # the cycle
            if tp in muted_partitions:
                continue
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

            batch_remaining_linger = self._batches[tp][0].remaining_linger()
            if batch_remaining_linger:
                # Batch should linger more
                remaining_linger_time = (
                    min(remaining_linger_time, batch_remaining_linger)
                    if remaining_linger_time
                    else batch_remaining_linger
                )
                continue

            batch = self._pop_batch(tp)
            # We can get an empty batch here if all `append()` calls failed
            # with validation...
            if not batch.is_empty():
                nodes[leader][tp] = batch
            else:
                # XXX: use something more graceful. We just want to trigger
                # delivery future here, no message futures.
                batch.done_noack()

        # all batches are drained from accumulator
        # so create "wait" future again for waiting new data/linger expired in send
        # task
        if not self._waiter_future.done():
            self._waiter_future.set_result(None)
        self._waiter_future = self._loop.create_future()

        # cleaning up old wakeup task
        if self._wakeup_task:
            self._wakeup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._wakeup_task
            self._wakeup_task = None

        # schedule a new wakeup task
        if remaining_linger_time:
            self._wakeup_task = self._loop.create_task(
                self._wakeup(self._waiter_future, remaining_linger_time)
            )

        return nodes, unknown_leaders_exist

    @staticmethod
    async def _wakeup(fut, after):
        await asyncio.sleep(after)
        if not fut.done():
            fut.set_result(None)

    def create_builder(self, key_serializer=None, value_serializer=None):
        is_transactional = False
        if (
            self._txn_manager is not None
            and self._txn_manager.transactional_id is not None
        ):
            is_transactional = True
        return BatchBuilder(
            self._batch_size,
            self._compression_type,
            is_transactional=is_transactional,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

    def _append_batch(self, builder, tp):
        # We must do this before actual add takes place to check for errors.
        if self._txn_manager is not None:
            self._txn_manager.maybe_add_partition_to_txn(tp)

        batch = MessageBatch(tp, builder, self._batch_ttl, self._linger_time)
        self._batches[tp].append(batch)
        if not self._waiter_future.done():
            self._waiter_future.set_result(None)
        return batch

    async def add_batch(self, builder, tp, timeout):
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
        if self._exception is not None:
            raise copy.copy(self._exception)

        start = time.monotonic()
        while timeout > 0:
            pending = self._batches.get(tp)
            if pending:
                await pending[-1].wait_drain(timeout=timeout)
                timeout -= time.monotonic() - start
            else:
                batch = self._append_batch(builder, tp)
                return asyncio.shield(batch.future)
        raise KafkaTimeoutError()
