import io
import asyncio
import collections

from kafka.protocol.message import Message
from kafka.protocol.types import Int32, Int64
from kafka.codec import (has_gzip, has_snappy, has_lz4,
                         gzip_encode, snappy_encode,
                         lz4_encode, lz4_encode_old_kafka)

from aiokafka.errors import (KafkaTimeoutError,
                             NotLeaderForPartitionError,
                             LeaderNotAvailableError,
                             ProducerClosed)

from aiokafka.structs import RecordMetadata
from aiokafka.util import create_future


class BatchBuilder:

    _COMPRESSORS = {
        'gzip': (has_gzip, gzip_encode, Message.CODEC_GZIP),
        'snappy': (has_snappy, snappy_encode, Message.CODEC_SNAPPY),
        'lz4': (has_lz4, lz4_encode, Message.CODEC_LZ4),
        'lz4-old-kafka': (has_lz4, lz4_encode_old_kafka, Message.CODEC_LZ4),
    }

    LOG_OVERHEAD = 12  # offset + message_size

    # crc(4), magic(1), attributes(1), key+value size(4*2)
    HEADER_SIZE_V0 = 14
    # crc(4), magic(1), attributes(1), timestamp(8), key+value size(4*2)
    HEADER_SIZE_V1 = 22

    def __init__(self, magic, batch_size, compression_type):
        if compression_type == 'lz4' and magic == 0:
            compression_type = 'lz4-old-kafka'
        if compression_type:
            checker, _, _ = self._COMPRESSORS[compression_type]
            assert checker(), 'Compression Libraries Not Found'

        self._magic = magic
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._relative_offset = 0
        self._buffer = io.BytesIO()
        self._buffer.write(Int32.encode(0))  # first 4 bytes for batch size
        self._closed = False

    def append(self, *, timestamp, key, value):
        if not self._has_room_for(key, value):
            return 0

        # `.encode()` is a weak method for some reason, so we need to save
        # reference before calling it.
        if self._magic == 0:
            msg_inst = Message(value, key=key, magic=self._magic)
        else:
            msg_inst = Message(value, key=key, magic=self._magic,
                               timestamp=timestamp)

        encoded = msg_inst.encode()
        msg = Int64.encode(self._relative_offset) + Int32.encode(len(encoded))
        msg += encoded
        actual_size = self._buffer.write(msg)
        self._relative_offset += 1
        return actual_size

    def _has_room_for(self, key, value):
        """return True if batch does not have free capacity for append message
        """
        if self._is_full():
            return False

        # We always allow at least one record to be appended
        if self._relative_offset == 0:
            return True

        record_size = self._size_in_bytes(key, value)

        return self._buffer.tell() + record_size < self._batch_size

    def _is_full(self):
        if self._closed:
            return True
        return self._buffer.tell() >= self._batch_size

    def _size_in_bytes(self, key, value):
        if self._magic == 0:
            needed_bytes = self.LOG_OVERHEAD + self.HEADER_SIZE_V0
        else:
            needed_bytes = self.LOG_OVERHEAD + self.HEADER_SIZE_V1

        if key is not None:
            needed_bytes += len(key)
        if value is not None:
            needed_bytes += len(value)
        return needed_bytes

    def _build(self):
        if self._closed:
            self._buffer.seek(0)
            return self._buffer

        self._closed = True
        memview = self._buffer.getbuffer()
        if self._compression_type:
            _, compressor, attrs = self._COMPRESSORS[self._compression_type]
            msg = Message(compressor(memview[4:].tobytes()), attributes=attrs,
                          magic=self._magic)
            encoded = msg.encode()
            # if compressed message is longer than original
            # we should send it as is (not compressed)
            header_size = 16   # 4(all size) + 8(offset) + 4(compressed size)
            if len(encoded) + header_size < len(memview):
                # write compressed message set (with header) to buffer
                # using memory view (for avoid memory copying)
                memview[:4] = Int32.encode(len(encoded) + 12)
                memview[4:12] = Int64.encode(0)  # offset 0
                memview[12:16] = Int32.encode(len(encoded))
                memview[16:16 + len(encoded)] = encoded

                memview.release()
                self._buffer.seek(16 + len(encoded))
                self._buffer.truncate()
                self._buffer.seek(0)
                return self._buffer

        # update batch size (first 4 bytes of buffer)
        memview[:4] = Int32.encode(self._buffer.tell() - 4)
        self._buffer.seek(0)
        return self._buffer


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
        self._msg_futures = []
        # Set when sender takes this batch
        self._drain_waiter = create_future(loop=loop)

    def append(self, key, value, timestamp_ms):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when message is delivered
        """
        size = self._builder.append(
            timestamp=timestamp_ms, key=key, value=value)
        if size == 0:
            return None

        future = create_future(loop=self._loop)
        self._msg_futures.append(future)
        return future

    def done(self, base_offset=None, exception=None):
        """Resolve all pending futures"""
        for relative_offset, future in enumerate(self._msg_futures):
            if future.done():
                continue
            if exception is not None:
                future.set_exception(exception)
            elif base_offset is None:
                future.set_result(None)
            else:
                res = RecordMetadata(self._tp.topic, self._tp.partition,
                                     self._tp, base_offset + relative_offset)
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
        if not self._drain_waiter.done():
            self._drain_waiter.set_result(None)

    def get_data_buffer(self):
        return self._builder._build()


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
            magic = 0 if self._api_version < (0, 10) else 1
            builder = BatchBuilder(
                magic, self._batch_size, self._compression_type
            )
            batch = MessageBatch(
                tp, builder, self._batch_ttl, self._loop)
            self._batches[tp].append(batch)

            if not self._wait_data_future.done():
                # Wakeup sender task if it waits for data
                self._wait_data_future.set_result(None)
        else:
            batch = pending_batches[-1]

        future = batch.append(key, value, timestamp_ms)
        if future is None:
            # Batch is full, can't append data atm,
            # waiting until batch per topic-partition is drained
            start = self._loop.time()
            yield from asyncio.wait(
                [batch.wait_drain()], timeout=timeout, loop=self._loop)
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
        if not self._wait_data_future.done():
            self._wait_data_future.set_result(None)
        self._wait_data_future = create_future(loop=self._loop)

        return nodes, unknown_leaders_exist
