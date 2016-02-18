import asyncio
from collections import deque

import logging
import numbers
from itertools import zip_longest, repeat

from kafka.common import (
    OffsetRequest, OffsetCommitRequest, OffsetFetchRequest,
    UnknownTopicOrPartitionError, FetchRequest,
    ConsumerFetchSizeTooSmall, ConsumerNoMoreData,
    check_error
)

__all__ = ['AIOConsumer', 'SimpleAIOConsumer']


log = logging.getLogger("aiokafka")


class AIOConsumer(object):

    AUTO_COMMIT_MSG_COUNT = 100
    AUTO_COMMIT_INTERVAL = 5000
    FETCH_DEFAULT_BLOCK_TIMEOUT = 1
    FETCH_BUFFER_SIZE_BYTES = 4096
    MAX_FETCH_BUFFER_SIZE_BYTES = FETCH_BUFFER_SIZE_BYTES * 8

    def __init__(self, client, group,
                 topic,
                 partitions=None,
                 auto_commit=True,
                 auto_commit_every_n=AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AUTO_COMMIT_INTERVAL):

        self._client = client
        self._loop = self._client.loop

        self._topic = topic
        self._group = group
        self._offsets = {}

        # Variables for handling offset commits
        self._commit_lock = asyncio.Lock(loop=self._loop)
        self._commit_timer = None
        self._count_since_commit = 0
        self._auto_commit = auto_commit
        self._auto_commit_every_n = auto_commit_every_n
        self._auto_commit_every_t = auto_commit_every_t

        self._partitions = partitions
        self._commit_task = None

    @asyncio.coroutine
    def _connect(self):
        yield from self._client.load_metadata_for_topics(self._topic)

        if not self._partitions:
            self._partitions = self._client.get_partition_ids_for_topic(
                self._topic)
        else:
            assert all(isinstance(x, numbers.Integral)
                       for x in self._partitions)

        # Set up the auto-commit timer
        if self._auto_commit is True and self._auto_commit_every_t is not None:
            self._commit_task = asyncio.Task(self._auto_committer(),
                                             loop=self._loop)

        if self._auto_commit:
            yield from self.fetch_last_known_offsets(self._partitions)
        else:
            for partition in self._partitions:
                self._offsets[partition] = 0
        self._is_working = True

    @asyncio.coroutine
    def _auto_committer(self):

        while True:
            yield from asyncio.sleep(self._auto_commit_every_t,
                                     loop=self._loop)
            yield from self.commit()

    @asyncio.coroutine
    def fetch_last_known_offsets(self, partitions=None):
        yield from self._client.load_metadata_for_topics(self._topic)

        # if not partitions:
        partitions = self._client.get_partition_ids_for_topic(self._topic)
        for partition in partitions:
            req = OffsetFetchRequest(self._topic, partition)
            try:
                (resp,) = yield from self._client.send_offset_fetch_request(
                    self._group, [req])
                partition_offset = resp.offset
            except UnknownTopicOrPartitionError:
                partition_offset = 0

            self._offsets[partition] = partition_offset

        self._fetch_offsets = self._offsets.copy()

    @asyncio.coroutine
    def commit(self, partitions=None):
        """XXX"""

        # short circuit if nothing happened. This check is kept outside
        # to prevent un-necessarily acquiring a lock for checking the state
        if self._count_since_commit == 0:
            return

        with (yield from self._commit_lock):
            # Do this check again, just in case the state has changed
            # during the lock acquiring timeout
            if self._count_since_commit == 0:
                return

            reqs = []
            if not partitions:  # commit all partitions
                partitions = self._offsets.keys()

            for partition in partitions:
                offset = self._offsets[partition]
                log.debug("Commit offset %d in SimpleConsumer: "
                          "group=%s, topic=%s, partition=%s" %
                          (offset, self._group, self._topic, partition))

                reqs.append(OffsetCommitRequest(self._topic, partition,
                                                offset, None))

            resps = yield from self._client.send_offset_commit_request(
                self._group, reqs)

            for resp in resps:
                check_error(resp)
            self._count_since_commit = 0

    @asyncio.coroutine
    def _is_time_to_commit(self):
        """
        Check if we have to commit based on number of messages and commit
        """

        # Check if we are supposed to do an auto-commit
        if not self._auto_commit or self._auto_commit_every_n is None:
            return

        if self._count_since_commit >= self._auto_commit_every_n:
            yield from self.commit()

    @asyncio.coroutine
    def stop(self):
        if self._commit_task is not None:
            self._commit_task.cancel()
            yield from self.commit()

    @asyncio.coroutine
    def pending(self, partitions=None):
        """
        Gets the pending message count

        partitions: list of partitions to check for, default is to check all
        """
        if not partitions:
            partitions = self._offsets.keys()

        total = 0
        reqs = []

        for partition in partitions:
            reqs.append(OffsetRequest(self._topic, partition, -1, 1))

        resps = yield from self._client.send_offset_request(reqs)
        for resp in resps:
            partition = resp.partition
            pending = resp.offsets[0]
            offset = self._offsets[partition]
            total += pending - offset

        return total


class SimpleAIOConsumer(AIOConsumer):

    def __init__(self, client,
                 group, topic,
                 auto_commit=True,
                 partitions=None,
                 auto_commit_every_n=AIOConsumer.AUTO_COMMIT_MSG_COUNT,
                 auto_commit_every_t=AIOConsumer.AUTO_COMMIT_INTERVAL,
                 fetch_size_bytes=1024,
                 buffer_size=AIOConsumer.FETCH_BUFFER_SIZE_BYTES,
                 max_buffer_size=AIOConsumer.MAX_FETCH_BUFFER_SIZE_BYTES):

        super().__init__(
            client, group, topic,
            partitions=partitions,
            auto_commit=auto_commit,
            auto_commit_every_n=auto_commit_every_n,
            auto_commit_every_t=auto_commit_every_t)

        if max_buffer_size is not None and buffer_size > max_buffer_size:
            raise ValueError("buffer_size (%d) is greater than "
                             "max_buffer_size (%d)" %
                             (buffer_size, max_buffer_size))

        self._buffer_size = buffer_size
        self._max_buffer_size = max_buffer_size
        self._partition_info = False     # Do not return partition info in msgs
        self._fetch_offsets = self._offsets.copy()
        self._queue = deque()

        self._is_working = False

    def __repr__(self):
        return '<SimpleConsumer group=%s, topic=%s, partitions=%s>' % \
            (self._group, self._topic, str(self._offsets.keys()))

    def provide_partition_info(self):
        """
        Indicates that partition info must be returned by the consumer_factory
        """
        self._partition_info = True

    @asyncio.coroutine
    def seek(self, offset, whence):
        """
        Alter the current offset in the consumer_factory, similar to fseek

        offset: how much to modify the offset
        whence: where to modify it from
                0 is relative to the earliest available offset (head)
                1 is relative to the current offset
                2 is relative to the latest known offset (tail)
        """

        if whence == 1:  # relative to current position
            for partition, _offset in self._offsets.items():
                self._offsets[partition] = _offset + offset
        elif whence in (0, 2):  # relative to beginning or end
            # divide the request offset by number of partitions,
            # distribute the remained evenly
            (delta, rem) = divmod(offset, len(self._offsets))
            deltas = {}
            for partition, r in zip_longest(
                    self._offsets.keys(), repeat(1, rem), fillvalue=0):
                deltas[partition] = delta + r

            reqs = []
            for partition in self._offsets.keys():
                if whence == 0:
                    reqs.append(OffsetRequest(self._topic, partition, -2, 1))
                elif whence == 2:
                    reqs.append(OffsetRequest(self._topic, partition, -1, 1))
                else:
                    pass

            resps = yield from self._client.send_offset_request(reqs)
            for resp in resps:
                self._offsets[resp.partition] = \
                    resp.offsets[0] + deltas[resp.partition]
        else:
            raise ValueError("Unexpected value for `whence`, %d" % whence)

        # Reset queue and fetch offsets since they are invalid
        self._fetch_offsets = self._offsets.copy()
        if self._auto_commit:
            self._count_since_commit += 1
            yield from self.commit()

        self._queue = deque()

    @asyncio.coroutine
    def get_messages(self, count=1):
        messages = []
        new_offsets = {}
        while count > 0:
            result = yield from self._get_message(
                get_partition_info=True, update_offset=False)

            if not result:
                continue
            partition, message = result
            if self._partition_info:
                messages.append(result)
            else:
                messages.append(message)
            new_offsets[partition] = message.offset + 1
            count -= 1

        self._offsets.update(new_offsets)
        self._count_since_commit += len(messages)
        yield from self._is_time_to_commit()
        return messages

    @asyncio.coroutine
    def _get_message(self, get_partition_info=None, update_offset=True):
        """
        If no messages can be fetched, returns None.
        If get_partition_info is None, it defaults to self.partition_info
        If get_partition_info is True, returns (partition, message)
        If get_partition_info is False, returns message
        """
        if len(self._queue) == 0:
            # TODO: FIX THIS
            yield from self._fetch(1, self.FETCH_DEFAULT_BLOCK_TIMEOUT)

        if len(self._queue) == 0:
            return None
        partition, message = self._queue.popleft()

        if update_offset:
            # Update partition offset
            self._offsets[partition] = message.offset + 1

            # Count, check and commit messages if necessary
            self._count_since_commit += 1
            yield from self._is_time_to_commit()

        if get_partition_info is None:
            get_partition_info = self._partition_info
        if get_partition_info:
            return partition, message
        else:
            return message

    @asyncio.coroutine
    def _fetch(self, min_bytes, wait_time):
        # Create fetch request payloads for all the partitions
        partitions = {p: self._buffer_size for p in self._fetch_offsets}
        while partitions:
            requests = []

            for partition, buffer_size in partitions.items():
                req = FetchRequest(self._topic, partition,
                                   self._fetch_offsets[partition], buffer_size)
                requests.append(req)

            # Send request
            responses = yield from self._client.send_fetch_request(
                requests,
                max_wait_time=wait_time,
                min_bytes=min_bytes)

            retry_partitions = {}
            for resp in responses:

                partition = resp.partition
                buffer_size = partitions[partition]
                try:
                    for message in resp.messages:
                        # Put the message in our queue
                        self._queue.append((partition, message))
                        self._fetch_offsets[partition] = message.offset + 1
                except ConsumerFetchSizeTooSmall:
                    if (self._max_buffer_size is not None and
                            buffer_size == self._max_buffer_size):
                        log.error("Max fetch size %d too small",
                                  self._max_buffer_size)
                        raise
                    if self._max_buffer_size is None:
                        buffer_size *= 2
                    else:
                        buffer_size = min(buffer_size * 2,
                                          self._max_buffer_size)
                    log.warn("Fetch size too small, increase to %d (2x) "
                             "and retry", buffer_size)
                    retry_partitions[partition] = buffer_size
                except ConsumerNoMoreData as e:
                    log.debug("Iteration was ended by %r", e)
                except StopIteration:
                    # Stop iterating through this partition
                    log.debug("Done iterating over partition %s" % partition)
            partitions = retry_partitions
