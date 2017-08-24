import io
import asyncio
import collections

from kafka.protocol.message import Message, MessageSet
from kafka.protocol.types import Int32, Int64
from kafka.codec import (has_gzip, has_snappy, has_lz4,
                         gzip_encode, snappy_encode,
                         lz4_encode, lz4_encode_old_kafka)

from aiokafka.errors import (KafkaTimeoutError,
                             NotLeaderForPartitionError,
                             LeaderNotAvailableError,
                             ProducerClosed)

from aiokafka.structs import RecordMetadata


class MessageBatch:
    """This class incapsulate operations with batch of produce messages"""
    _COMPRESSORS = {
        'gzip': (has_gzip, gzip_encode, Message.CODEC_GZIP),
        'snappy': (has_snappy, snappy_encode, Message.CODEC_SNAPPY),
        'lz4': (has_lz4, lz4_encode, Message.CODEC_LZ4),
        'lz4-old-kafka': (has_lz4, lz4_encode_old_kafka, Message.CODEC_LZ4),
    }

    def __init__(self, tp, batch_size, compression_type,
                 ttl, api_version, loop):
        # Kafka 0.8/0.9 had a quirky lz4...
        version_id = 0 if api_version < (0, 10) else 1
        if compression_type == 'lz4' and version_id == 0:
            compression_type = 'lz4-old-kafka'

        if compression_type:
            checker, _, _ = self._COMPRESSORS[compression_type]
            assert checker(), 'Compression Libraries Not Found'

        self._tp = tp
        self._batch_size = batch_size
        self._compression_type = compression_type
        self._buffer = io.BytesIO()
        self._buffer.write(Int32.encode(0))  # first 4 bytes for batch size
        self._relative_offset = 0
        self._loop = loop
        self._ttl = ttl
        self._ctime = loop.time()
        self._version_id = version_id

        # Waiters
        # Set when messages are delivered to Kafka based on ACK setting
        self._msg_futures = []
        # Set when sender takes this batch
        self._drain_waiter = asyncio.Future(loop=loop)
        self._drained = False

    def _is_full(self, key, value):
        """return True if batch does not have free capacity for append message
        """
        if self._relative_offset == 0:
            # batch must contain al least one message
            return False
        needed_bytes = MessageSet.HEADER_SIZE + Message.HEADER_SIZE
        if key is not None:
            needed_bytes += len(key)
        if value is not None:
            needed_bytes += len(value)
        return self._buffer.tell() + needed_bytes > self._batch_size

    def append(self, key, value, timestamp_ms):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when message is delivered
        """
        if self._is_full(key, value):
            return None

        # `.encode()` is a weak method for some reason, so we need to save
        # reference before calling it.
        if self._version_id == 0:
            msg_inst = Message(value, key=key, magic=self._version_id)
        else:
            msg_inst = Message(value, key=key, magic=self._version_id,
                               timestamp=timestamp_ms)

        encoded = msg_inst.encode()
        msg = Int64.encode(self._relative_offset) + Int32.encode(len(encoded))
        msg += encoded
        self._buffer.write(msg)

        future = asyncio.Future(loop=self._loop)
        self._msg_futures.append(future)
        self._relative_offset += 1
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
        # Check is we are retrying, if so no need to do anything
        if self._drained:
            return

        self._drained = True
        memview = self._buffer.getbuffer()
        self._drain_waiter.set_result(None)
        if self._compression_type:
            _, compressor, attrs = self._COMPRESSORS[self._compression_type]
            msg = Message(compressor(memview[4:].tobytes()), attributes=attrs,
                          magic=self._version_id)
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
                self._buffer.seek(0)
                return

        # update batch size (first 4 bytes of buffer)
        memview[:4] = Int32.encode(self._buffer.tell() - 4)
        self._buffer.seek(0)

    def get_data_buffer(self):
        # ProduceRequest will read the buffer on encoding, leaving it in
        # incorrect state, so to avoid that we reset it here.
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
        self._wait_data_future = asyncio.Future(loop=loop)
        self._closed = False
        self._api_version = (0, 9)

    def set_api_version(self, api_version):
        self._api_version = api_version

    @asyncio.coroutine
    def close(self):
        self._closed = True
        # NOTE: we copy to avoid mutation during `yield from` below
        for batches in list(self._batches.values()):
            for batch in list(batches):
                yield from batch.wait_deliver()

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
            batch = MessageBatch(
                tp, self._batch_size, self._compression_type,
                self._batch_ttl, self._api_version, self._loop)
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
        self._wait_data_future = asyncio.Future(loop=self._loop)

        return nodes, unknown_leaders_exist
