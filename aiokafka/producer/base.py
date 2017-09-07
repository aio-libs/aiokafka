import asyncio
import collections
import io
import logging

from kafka.codec import (
    gzip_encode,
    has_gzip,
    has_lz4,
    has_snappy,
    lz4_encode,
    lz4_encode_old_kafka,
    snappy_encode,
)
from kafka.protocol.message import Message, MessageSet
from kafka.protocol.produce import ProduceRequest
from kafka.protocol.types import Int32, Int64

import aiokafka.errors as Errors
from aiokafka import ensure_future
from aiokafka.client import AIOKafkaClient
from aiokafka.errors import (
    KafkaError,
    KafkaTimeoutError,
    LeaderNotAvailableError,
    MessageSizeTooLargeError,
    NotLeaderForPartitionError,
    ProducerClosed,
    UnknownTopicOrPartitionError,
)
from aiokafka.structs import RecordMetadata, TopicPartition

log = logging.getLogger(__name__)


class AIOKafkaBaseProducer(object):
    """A Kafka producer that acts on message batches.

    For most client applications, AIOKafkaProducer is the recommended producer
    as it offers message-oriented sending methods with graceful serialization
    and partitioning.
    """

    API_VERSIONS = ('auto', '0.10', '0.9', '0.8.2', '0.8.1', '0.8.0')

    def __init__(self, *, loop, bootstrap_servers='localhost',
                 client_id='aiokafka-producer',
                 metadata_max_age_ms=300000, request_timeout_ms=40000,
                 api_version='auto', acks=1,
                 compression_type=None, max_batch_size=16384,
                 max_request_size=1048576,
                 linger_ms=0, send_backoff_ms=100,
                 retry_backoff_ms=100, security_protocol="PLAINTEXT",
                 ssl_context=None):
        if acks not in (0, 1, -1, 'all'):
            raise ValueError("Invalid ACKS parameter")
        if compression_type not in ('gzip', 'snappy', 'lz4', None):
            raise ValueError("Invalid compression type!")
        if api_version not in self.API_VERSIONS:
            raise ValueError("Unsupported Kafka version")

        if acks == 'all':
            acks = -1
        self._acks = acks
        self._batch_size = max_batch_size
        self._compression_type = compression_type
        self._max_request_size = max_request_size
        self._request_timeout_ms = request_timeout_ms

        self.client = AIOKafkaClient(
            loop=loop, bootstrap_servers=bootstrap_servers,
            client_id=client_id, metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            api_version=api_version, security_protocol=security_protocol,
            ssl_context=ssl_context)
        self._metadata = self.client.cluster
        self._sender_task = None
        self._batches = collections.defaultdict(collections.deque)
        self._in_flight = set()
        self._closed = False
        self._loop = loop
        self._data_waiter = asyncio.Future(loop=self._loop)
        self._batch_ttl = self._request_timeout_ms / 1000
        self._retry_backoff = retry_backoff_ms / 1000
        self._linger_time = linger_ms / 1000

    @property
    def api_version(self):
        return self.client.api_version

    @asyncio.coroutine
    def start(self):
        """Connect to Kafka cluster and check server version"""
        log.debug("Starting the Kafka producer")

        yield from self.client.bootstrap()

        if self._compression_type == 'lz4' and self.api_version < (0, 8, 2):
            raise ValueError("LZ4 Requires >= Kafka 0.8.2 Brokers")

        self._sender_task = ensure_future(
            self._sender_routine(), loop=self._loop)
        log.debug("Kafka producer started")

    @asyncio.coroutine
    def stop(self):
        """Flush all pending data and close all connections to kafka cluster"""
        if self._closed:
            return

        self._closed = True
        yield from self.flush()

        if self._sender_task:
            self._sender_task.cancel()
            yield from self._sender_task

        yield from self.client.close()
        log.debug("The Kafka producer has closed.")

    @asyncio.coroutine
    def flush(self):
        """Wait until all batches are delivered and futures resolved"""
        # use list() to aviod mutation during `yield from` below
        for batches in list(self._batches.values()):
            for batch in list(batches):
                yield from batch.wait_deliver()

    @asyncio.coroutine
    def partitions_for(self, topic):
        """Returns set of all known partitions for the topic."""
        return (yield from self.client._wait_on_metadata(topic))

    def create_batch(self, tp):
        """Create a MessageBatch object. Does not queue for sending."""
        if self._closed:
            raise ProducerClosed()
        return MessageBatch(
            tp, self._batch_size, self._compression_type,
            self._batch_ttl, self.api_version, self._loop)

    def get_batch(self, tp):
        """Return a queued MessageBatch, creating it if necessary.

        If the batch is created, the sender routine will also be notified that
        new data is available.
        """
        if self._closed:
            raise ProducerClosed()
        if self._batches[tp]:
            return self._batches[tp][-1]
        else:
            batch = self.create_batch(tp)
            self._batches[tp].append(batch)
            if not self._data_waiter.done():
                self._data_waiter.set_result(None)
            return batch

    @asyncio.coroutine
    def send_batch(self, batch, timeout=None):
        """Publish a message batch.

        Arguments:
            batch: the MessageBatch object to publish.
            timeout: time to wait for this batch to be sent. Defaults to the
                `request_timeout_ms` configuration parameter.

        Returns:
            asyncio.Future: object that will be set when message is processed.

        Raises:
            kafka.KafkaTimeoutError: if the batch can't be scheduled in the
                given timeout (i.e., the pending buffer is full).
        """
        if self._closed:
            raise ProducerClosed()
        if timeout is None:
            timeout = self._request_timeout_ms / 1000

        tp = batch.tp
        log.debug("Sending batch to %s", tp)
        while timeout > 0:
            if self._closed:
                raise ProducerClosed()

            # queue is empty, so just append and wake the sender
            if not self._batches[tp]:
                self._batches[tp].append(batch)
                if not self._data_waiter.done():
                    self._data_waiter.set_result(None)
                return batch.future

            # this batch is already queued, nothing to do
            elif self._batches[tp][0] is batch:
                return batch.future

            # busy with another batch, wait for it to drain
            else:
                start = self._loop.time()
                yield from self._batches[tp][0].wait_drain(timeout=timeout)
                timeout -= self._loop.time() - start

        raise KafkaTimeoutError()

    @asyncio.coroutine
    def send_batch_and_wait(self, batch, timeout=None):
        """Publish a message batch and wait for the result."""
        future = yield from self.send_batch(batch, timeout=timeout)
        return (yield from future)

    @asyncio.coroutine
    def _sender_routine(self):
        """ Background task, that sends pending batches to leader nodes for
        batch's partition. This incapsulates same logic as Java's `Sender`
        background thread. Because we use asyncio this is more event based
        loop, rather than counting timeout till next possible even like in
        Java.

            The procedure:
            * Group pending batches by partition leaders (write nodes)
            * Ignore not ready (disconnected) and nodes, that already have a
              pending request.
            * If we have unknown leaders for partitions, we request a metadata
              update.
            * Wait for any event, that can change the above procedure, like
              new metadata or pending send is finished and a new one can be
              done.
        """
        tasks = set()
        try:
            while True:
                batches, unknown_leaders_exist = self._drain_by_nodes(
                    ignore_nodes=self._in_flight)

                # create produce task for every batch
                for node_id, batches in batches.items():
                    task = ensure_future(
                        self._send_produce_req(node_id, batches),
                        loop=self._loop)
                    self._in_flight.add(node_id)
                    tasks.add(task)

                if unknown_leaders_exist:
                    # we have at least one unknown partition's leader,
                    # try to update cluster metadata and wait backoff time
                    fut = self.client.force_metadata_update()
                    waiters = tasks.union([fut])
                else:
                    waiters = tasks.union([self._data_waiter])

                # wait when:
                # * At least one of produce task is finished
                # * Data for new partition arrived
                # * Metadata update if partition leader unknown
                done, _ = yield from asyncio.wait(
                    waiters,
                    return_when=asyncio.FIRST_COMPLETED,
                    loop=self._loop)

                # if a done task produces an error, it's a bug
                for task in done:
                    task.result()

                tasks -= done

        except asyncio.CancelledError:
            pass
        except Exception:  # pragma: no cover
            log.error("Unexpected error in sender routine", exc_info=True)

    @asyncio.coroutine
    def _send_produce_req(self, node_id, batches):
        """ Create produce request to node
        If producer configured with `retries`>0 and produce response contain
        "failed" partitions produce request for this partition will try
        resend to broker `retries` times with `retry_timeout_ms` timeouts.

        Arguments:
            node_id (int): kafka broker identifier
            batches (dict): dictionary of {TopicPartition: MessageBatch}
        """
        t0 = self._loop.time()

        topics = collections.defaultdict(list)
        for tp, batch in batches.items():
            topics[tp.topic].append((tp.partition, batch.get_data_buffer()))

        if self.api_version >= (0, 10):
            version = 2
        elif self.api_version == (0, 9):
            version = 1
        else:
            version = 0

        request = ProduceRequest[version](
            required_acks=self._acks,
            timeout=self._request_timeout_ms,
            topics=list(topics.items()))

        reenqueue = []

        try:
            response = yield from self.client.send(node_id, request)
        except KafkaError as err:
            log.warning("Got error produce response: %s", err)
            if getattr(err, "invalid_metadata", False):
                self.client.force_metadata_update()

            for batch in batches.values():
                if not self._can_retry(err, batch):
                    batch.done(exception=err)
                else:
                    reenqueue.append(batch)
        else:
            # noacks, just mark batches as "done"
            if request.required_acks == 0:
                for batch in batches.values():
                    batch.done()

            for topic, partitions in response.topics:
                for partition_info in partitions:
                    if response.API_VERSION < 2:
                        partition, error_code, offset = partition_info
                    else:
                        partition, error_code, offset, _ = partition_info
                    tp = TopicPartition(topic, partition)
                    error = Errors.for_code(error_code)
                    batch = batches.pop(tp, None)
                    if batch is None:
                        continue

                    if error is Errors.NoError:
                        batch.done(offset)
                    elif not self._can_retry(error(), batch):
                        batch.done(exception=error())
                    else:
                        log.warning(
                            "Got error produce response on topic-partition"
                            " %s, retrying. Error: %s", tp, error)
                        # Ok, we can retry this batch
                        if getattr(error, "invalid_metadata", False):
                            self.client.force_metadata_update()
                        reenqueue.append(batch)

        if reenqueue:
            # Wait backoff before reequeue
            yield from asyncio.sleep(self._retry_backoff, loop=self._loop)

            for batch in reenqueue:
                self._batches[batch.tp].appendleft(batch)
            # If some error started metadata refresh we have to wait before
            # trying again
            yield from self.client._maybe_wait_metadata()

        # if batches for node is processed in less than a linger seconds
        # then waiting for the remaining time
        sleep_time = self._linger_time - (self._loop.time() - t0)
        if sleep_time > 0:
            yield from asyncio.sleep(sleep_time, loop=self._loop)

        self._in_flight.remove(node_id)

    def _can_retry(self, error, batch):
        if batch.expired():
            return False
        # XXX: remove unknown topic check as we fix
        #      https://github.com/dpkp/kafka-python/issues/1155
        if error.retriable or isinstance(error, UnknownTopicOrPartitionError)\
                or error is UnknownTopicOrPartitionError:
            return True
        return False

    def _pop_batch(self, tp):
        batch = self._batches[tp].popleft()
        batch.drain_ready()
        if len(self._batches[tp]) == 0:
            del self._batches[tp]
        return batch

    def _drain_by_nodes(self, ignore_nodes):
        """ Group batches by leader to partiton nodes. """
        nodes = collections.defaultdict(dict)
        unknown_leaders_exist = False
        for tp in list(self._batches.keys()):
            leader = self._metadata.leader_for_partition(tp)
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

        # all batches are drained, so create a new data waiter for send task
        if not self._data_waiter.done():
            self._data_waiter.set_result(None)
        self._data_waiter = asyncio.Future(loop=self._loop)

        return nodes, unknown_leaders_exist


class MessageBatch:
    __slots__ = (
        "_api_version",
        "_buffer",
        "_compression_type",
        "_ctime",
        "_drain_waiter",
        "_drained",
        "_loop",
        "_msg_futures",
        "_relative_offset",
        "_size",
        "_ttl",
        "_version_id",
        "future",
        "tp",
    )

    _COMPRESSORS = {
        'gzip': (has_gzip, gzip_encode, Message.CODEC_GZIP),
        'snappy': (has_snappy, snappy_encode, Message.CODEC_SNAPPY),
        'lz4': (has_lz4, lz4_encode, Message.CODEC_LZ4),
        'lz4-old-kafka': (has_lz4, lz4_encode_old_kafka, Message.CODEC_LZ4),
    }

    def __init__(self, tp, size, compression_type, ttl, api_version, loop):
        # Kafka 0.8/0.9 had a quirky lz4...
        version_id = 0 if api_version < (0, 10) else 1
        if compression_type == 'lz4' and version_id == 0:
            compression_type = 'lz4-old-kafka'

        if compression_type:
            checker, _, _ = self._COMPRESSORS[compression_type]
            if not checker():
                raise ValueError('Compression libraries not found')

        self.tp = tp
        self._size = size
        self._compression_type = compression_type
        self._buffer = io.BytesIO()
        self._buffer.write(Int32.encode(0))  # first 4 bytes for batch size
        self._relative_offset = 0
        self._loop = loop
        self._ttl = ttl
        self._ctime = loop.time()
        self._api_version = api_version
        self._version_id = version_id

        # Set when batch is delivered to Kafka based on ACK setting
        self.future = asyncio.Future(loop=self._loop)
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
        needed_bytes += len(key or '')
        needed_bytes += len(value or '')
        if needed_bytes > self._size:
            raise MessageSizeTooLargeError(
                "Message is %d bytes which is larger than the maximum batch"
                " size configured with max_batch_size" % needed_bytes)
        return self._buffer.tell() + needed_bytes > self._size

    def append(self, key, value, timestamp_ms=None, future=False):
        """Append message (key and value) to batch

        Returns:
            None if batch is full
              or
            asyncio.Future that will resolved when batch is delivered
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

        self._relative_offset += 1
        if future:
            fut = asyncio.Future(loop=self._loop)
            self._msg_futures.append(fut)
            return fut
        return self.future

    def done(self, base_offset=None, exception=None):
        """Resolve delivery futures."""
        for relative_offset, future in enumerate(self._msg_futures):
            self._set_future(future, base_offset, relative_offset, exception)
        self._set_future(
            self.future, base_offset, self._relative_offset, exception)

    def _set_future(self, future, base_offset, relative_offset, exception):
        if future.done():
            return
        if exception is not None:
            future.set_exception(exception)
        elif base_offset is None:
            future.set_result(None)
        else:
            res = RecordMetadata(self.tp.topic, self.tp.partition,
                                 self.tp, base_offset + relative_offset)
            future.set_result(res)

    def wait_deliver(self, timeout=None):
        """Wait until all message from this batch are delivered."""
        return asyncio.wait([self.future], timeout=timeout, loop=self._loop)

    def wait_drain(self, timeout=None):
        """Wait until all message from this batch are drained."""
        return asyncio.wait(
            [self._drain_waiter], timeout=timeout, loop=self._loop)

    def expired(self):
        """Check whether batch has expired."""
        return (self._loop.time() - self._ctime) > self._ttl

    def drain_ready(self):
        """Compress batch to be ready for send"""
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
