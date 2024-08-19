from __future__ import annotations

import asyncio
import collections
import copy
import time
from collections.abc import Sequence
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Callable,
    DefaultDict,
    Deque,
    Generic,
    Protocol,
    TypeVar,
)

from typing_extensions import Literal, TypeAlias

from aiokafka.cluster import ClusterMetadata
from aiokafka.errors import (
    BrokerResponseError,
    KafkaTimeoutError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    ProducerClosed,
)
from aiokafka.producer.transaction_manager import TransactionManager
from aiokafka.protocol.types import BrokerId
from aiokafka.record._protocols import (
    DefaultRecordBatchBuilderProtocol,
    DefaultRecordMetadataProtocol,
    LegacyRecordBatchBuilderProtocol,
    LegacyRecordMetadataProtocol,
)
from aiokafka.record.default_records import DefaultRecordBatchBuilder
from aiokafka.record.legacy_records import LegacyRecordBatchBuilder
from aiokafka.structs import RecordMetadata, TopicPartition
from aiokafka.util import create_future, get_running_loop

T = TypeVar("T")

BytesSerializer: TypeAlias = "Callable[[T | None], bytes | None]"

if TYPE_CHECKING:
    KT = TypeVar("KT", default=bytes)
    VT = TypeVar("VT", default=bytes)
else:
    KT = TypeVar("KT")
    VT = TypeVar("VT")

_Metadata: TypeAlias = "DefaultRecordMetadataProtocol | LegacyRecordMetadataProtocol"


class BatchBuilder(Generic[KT, VT]):
    def __init__(
        self,
        magic: Literal[0, 1, 2],
        batch_size: int,
        compression_type: Literal[0, 1, 2, 3],
        *,
        is_transactional: bool,
        key_serializer: BytesSerializer[KT] | None = None,
        value_serializer: BytesSerializer[VT] | None = None,
    ) -> None:
        self._builder: (
            LegacyRecordBatchBuilderProtocol | DefaultRecordBatchBuilderProtocol
        )
        if magic < 2:
            assert not is_transactional
            self._builder = LegacyRecordBatchBuilder(
                magic,  # type: ignore[arg-type]
                compression_type,
                batch_size,
            )
        else:
            self._builder = DefaultRecordBatchBuilder(
                magic,  # type: ignore[arg-type]
                compression_type,
                is_transactional=is_transactional,
                producer_id=-1,
                producer_epoch=-1,
                base_sequence=0,
                batch_size=batch_size,
            )
        self._relative_offset = 0
        self._buffer: bytearray | None = None
        self._closed = False
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

    def _serialize(
        self,
        key: KT | None,
        value: VT | None,
    ) -> tuple[bytes | None, bytes | None]:
        serialized_key: bytes | None
        if self._key_serializer is None:
            serialized_key = key  # type: ignore[assignment]
        else:
            serialized_key = self._key_serializer(key)

        serialized_value: bytes | None
        if self._value_serializer is None:
            serialized_value = value  # type: ignore[assignment]
        else:
            serialized_value = self._value_serializer(value)

        return serialized_key, serialized_value

    def append(
        self,
        *,
        timestamp: int | None,
        key: KT | None,
        value: VT | None,
        headers: Sequence[tuple[str, bytes]] = [],
    ) -> _Metadata | None:
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
            return None

        self._relative_offset += 1
        return metadata

    def close(self) -> None:
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

    def _set_producer_state(
        self,
        producer_id: int,
        producer_epoch: int,
        base_sequence: int,
    ) -> None:
        assert type(self._builder) is DefaultRecordBatchBuilder
        self._builder.set_producer_state(producer_id, producer_epoch, base_sequence)

    def _build(self) -> bytearray:
        self.close()
        if self._buffer is None:
            self._buffer = self._builder.build()
            del self._builder  # We may only call self._builder.build() once!
        return self._buffer

    def size(self) -> int:
        """Get the size of batch in bytes."""
        if self._buffer is not None:
            return len(self._buffer)
        else:
            return self._builder.size()

    def record_count(self) -> int:
        """Get the number of records in the batch."""
        return self._relative_offset


class _FutureCreator(Protocol):
    def __call__(
        self,
        loop: asyncio.AbstractEventLoop | None = ...,
    ) -> asyncio.Future[object]: ...


class MessageBatch:
    """This class incapsulate operations with batch of produce messages"""

    def __init__(self, tp: TopicPartition, builder: BatchBuilder, ttl: int) -> None:
        self._builder = builder
        self._tp = tp
        self._ttl = ttl
        self._ctime = time.monotonic()

        # Waiters
        # Set when messages are delivered to Kafka based on ACK setting
        self.future: asyncio.Future[object] = create_future()
        self._msg_futures: list[tuple[asyncio.Future[object], _Metadata]] = []
        # Set when sender takes this batch
        self._drain_waiter: asyncio.Future[object] = create_future()
        self._retry_count = 0

    @property
    def tp(self) -> TopicPartition:
        return self._tp

    @property
    def record_count(self) -> int:
        return self._builder.record_count()

    def append(
        self,
        key: bytes | None,
        value: bytes | None,
        timestamp_ms: int | None,
        _create_future: _FutureCreator = create_future,
        headers: Sequence[tuple[str, bytes]] = [],
    ) -> asyncio.Future[object] | None:
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
        base_offset: int,
        timestamp: int | None = None,
        log_start_offset: int | None = None,
        _record_metadata_class: type[RecordMetadata] = RecordMetadata,
    ) -> None:
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

    def done_noack(self) -> None:
        """Resolve all pending futures to None"""
        # Faster resolve for base_offset=None case.
        if not self.future.done():
            self.future.set_result(None)
        for future, _ in self._msg_futures:
            if future.done():
                continue
            future.set_result(None)

    def failure(self, exception: BaseException) -> None:
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

    async def wait_drain(self, timeout: float | None = None) -> None:
        """Wait until all message from this batch is processed"""
        waiter = self._drain_waiter
        await asyncio.wait([waiter], timeout=timeout)
        if waiter.done():
            waiter.result()  # Check for exception

    def expired(self) -> bool:
        """Check that batch is expired or not"""
        return (time.monotonic() - self._ctime) > self._ttl

    def drain_ready(self) -> None:
        """Compress batch to be ready for send"""
        if not self._drain_waiter.done():
            self._drain_waiter.set_result(None)
        self._retry_count += 1

    def reset_drain(self) -> None:
        """Reset drain waiter, until we will do another retry"""
        assert self._drain_waiter.done()
        self._drain_waiter = create_future()

    def set_producer_state(
        self,
        producer_id: int,
        producer_epoch: int,
        base_sequence: int,
    ) -> None:
        assert not self._drain_waiter.done()
        self._builder._set_producer_state(producer_id, producer_epoch, base_sequence)

    def get_data_buffer(self) -> bytearray:
        return self._builder._build()

    def is_empty(self) -> bool:
        return self._builder.record_count() == 0

    @property
    def retry_count(self) -> int:
        return self._retry_count


class MessageAccumulator:
    """Accumulator of messages batched by topic-partition

    Producer adds messages to this accumulator and a background send task
    gets batches per nodes to process it.
    """

    def __init__(
        self,
        cluster: ClusterMetadata,
        batch_size: int,
        compression_type: Literal[0, 1, 2, 3],
        batch_ttl: int,
        *,
        txn_manager: TransactionManager | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        if loop is None:
            loop = get_running_loop()
        self._loop = loop
        self._batches: DefaultDict[TopicPartition, Deque[MessageBatch]] = (
            collections.defaultdict(collections.deque)
        )
        self._pending_batches: set[MessageBatch] = set()
        self._cluster = cluster
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._batch_ttl = batch_ttl
        self._wait_data_future = loop.create_future()
        self._closed = False
        self._api_version: tuple[int, int, int] | tuple[int, int] = (0, 9)
        self._txn_manager = txn_manager

        self._exception: BaseException | None = None  # Critical exception

    def set_api_version(self, api_version: tuple[int, int, int]) -> None:
        self._api_version = api_version

    async def flush(self) -> None:
        waiters = [
            batch.future for batches in self._batches.values() for batch in batches
        ]
        waiters += [batch.future for batch in self._pending_batches]
        if waiters:
            await asyncio.wait(waiters)

    async def flush_for_commit(self) -> None:
        waiters = []
        for batches in self._batches.values():
            for batch in batches:
                # We force all buffers to close to finalize the transaction
                # scope. We should not add anything to this transaction.
                batch._builder.close()
                waiters.append(batch.future)
        waiters += [batch.future for batch in self._pending_batches]
        # Wait for all waiters to finish. We only wait for the scope we defined
        # above, other batches should not be delivered as part of this
        # transaction
        if waiters:
            await asyncio.wait(waiters)

    def fail_all(self, exception: BaseException) -> None:
        # Close all batches with this exception
        for batches in self._batches.values():
            for batch in batches:
                batch.failure(exception)
        for batch in self._pending_batches:
            batch.failure(exception)
        self._exception = exception

    async def close(self) -> None:
        self._closed = True
        await self.flush()

    async def add_message(
        self,
        tp: TopicPartition,
        key: bytes | None,
        value: bytes | None,
        timeout: float,
        timestamp_ms: int | None = None,
        headers: Sequence[tuple[str, bytes]] = [],
    ) -> asyncio.Future[object] | None:
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
            # waiting until batch per topic-partition is drained
            start = time.monotonic()
            await batch.wait_drain(timeout)
            timeout -= time.monotonic() - start
            if timeout <= 0:
                raise KafkaTimeoutError()

    def data_waiter(self) -> asyncio.Future[None]:
        """Return waiter future that will be resolved when accumulator contain
        some data for drain
        """
        return self._wait_data_future

    def _pop_batch(self, tp: TopicPartition) -> MessageBatch:
        batch = self._batches[tp].popleft()
        not_retry = batch.retry_count == 0
        if self._txn_manager is not None and not_retry:
            assert (
                self._txn_manager.has_pid()
            ), "We should have waited for it in sender routine"
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

            def cb(
                fut: asyncio.Future[object],
                batch: MessageBatch = batch,
                self: MessageAccumulator = self,
            ) -> None:
                self._pending_batches.remove(batch)

            batch.future.add_done_callback(cb)
        return batch

    def reenqueue(self, batch: MessageBatch) -> None:
        tp = batch.tp
        self._batches[tp].appendleft(batch)
        self._pending_batches.remove(batch)
        batch.reset_drain()

    def drain_by_nodes(
        self,
        ignore_nodes: AbstractSet[BrokerId],
        muted_partitions: AbstractSet[TopicPartition] = frozenset(),
    ) -> tuple[
        dict[BrokerId, dict[TopicPartition, MessageBatch]],
        bool,
    ]:
        """Group batches by leader to partition nodes."""
        nodes: DefaultDict[BrokerId, dict[TopicPartition, MessageBatch]] = (
            collections.defaultdict(dict)
        )
        unknown_leaders_exist = False
        err: BrokerResponseError
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
        # so create "wait data" future again for waiting new data in send
        # task
        if not self._wait_data_future.done():
            self._wait_data_future.set_result(None)
        self._wait_data_future = self._loop.create_future()

        return nodes, unknown_leaders_exist

    def create_builder(
        self,
        key_serializer: BytesSerializer[KT] | None = None,
        value_serializer: BytesSerializer[VT] | None = None,
    ) -> BatchBuilder[KT, VT]:
        magic: Literal[0, 1, 2]

        if self._api_version >= (0, 11):
            magic = 2
        elif self._api_version >= (0, 10):
            magic = 1
        else:
            magic = 0

        is_transactional = False
        if (
            self._txn_manager is not None
            and self._txn_manager.transactional_id is not None
        ):
            is_transactional = True
        return BatchBuilder(
            magic,
            self._batch_size,
            self._compression_type,
            is_transactional=is_transactional,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

    def _append_batch(self, builder: BatchBuilder, tp: TopicPartition) -> MessageBatch:
        # We must do this before actual add takes place to check for errors.
        if self._txn_manager is not None:
            self._txn_manager.maybe_add_partition_to_txn(tp)

        batch = MessageBatch(tp, builder, self._batch_ttl)
        self._batches[tp].append(batch)
        if not self._wait_data_future.done():
            self._wait_data_future.set_result(None)
        return batch

    async def add_batch(
        self,
        builder: BatchBuilder,
        tp: TopicPartition,
        timeout: int | float,
    ) -> asyncio.Future[object]:
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
