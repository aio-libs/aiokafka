import asyncio
import random
import time
import logging
import numbers
from collections import deque, namedtuple
from itertools import zip_longest, repeat

from kafka.common import (
    OffsetRequest, OffsetCommitRequest, OffsetFetchRequest,
    UnknownTopicOrPartitionError, FetchRequest,
    ConsumerFetchSizeTooSmall, ConsumerNoMoreData, KafkaMessage,
    # errors
    KafkaUnavailableError, OffsetOutOfRangeError,
    NotLeaderForPartitionError, RequestTimedOutError,
    FailedPayloadsError, ConsumerTimeout,
    KafkaConfigurationError, check_error)

__all__ = ['AIOConsumer', 'SimpleAIOConsumer']


log = logging.getLogger('aiokafka')


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
                log.debug('Commit offset %d in SimpleConsumer: '
                          'group=%s, topic=%s, partition=%s' %
                          (offset, self._group, self._topic, partition))

                reqs.append(OffsetCommitRequest(self._topic, partition,
                                                offset, None))

            yield from self._client.send_offset_commit_request(
                self._group, reqs)
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


OffsetsStruct = namedtuple("OffsetsStruct",
                           ["fetch", "highwater", "commit", "task_done"])


class KafkaConsumer:
    def __init__(self, client,
                 group_id=None,
                 fetch_message_max_bytes=1024 * 1024,
                 fetch_min_bytes=1,
                 fetch_wait_max_ms=100,
                 refresh_leader_backoff_ms=200,
                 socket_timeout_ms=30 * 1000,
                 auto_offset_reset='largest',
                 auto_commit_enable=False,
                 auto_commit_interval_ms=60 * 1000,
                 auto_commit_interval_messages=None,
                 consumer_timeout_ms=-1):

        self._client = client
        self._loop = client.loop
        self._group_id = group_id
        self._fetch_message_max_bytes = fetch_message_max_bytes
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_wait_max_ms = fetch_wait_max_ms
        self._refresh_leader_backoff_ms = refresh_leader_backoff_ms
        self._socket_timeout_ms = socket_timeout_ms
        self._auto_offset_reset = auto_offset_reset
        self._auto_commit_enable = auto_commit_enable
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._auto_commit_interval_messages = auto_commit_interval_messages
        self._consumer_timeout_ms = consumer_timeout_ms

        self._topics = None
        self._offsets = OffsetsStruct(fetch={}, commit={}, highwater={},
                                      task_done={})

    @asyncio.coroutine
    def subscribe(self, *topics):
        yield from self.set_topic_partitions(*topics)

    @asyncio.coroutine
    def set_topic_partitions(self, *topics):
        # Handle different topic types
        # (topic, (partition, offset))
        self._topics = []
        yield from self._client.load_metadata_for_topics()

        for arg in topics:

            # Topic name str -- all partitions
            if isinstance(arg, bytes):
                topic = arg

                partition_ids = self._client.get_partition_ids_for_topic(topic)
                for partition in partition_ids:
                    self._consume_topic_partition(topic, partition)

            # (topic, partition [, offset]) tuple
            elif isinstance(arg, tuple):
                topic = arg[0]
                partition = arg[1]
                if len(arg) == 3:
                    offset = arg[2]
                    self._offsets.fetch[(topic, partition)] = offset
                self._consume_topic_partition(topic, partition)

            else:
                raise KafkaConfigurationError('Unknown topic type (%s)'
                                              % type(arg))

        # If we have a consumer group, try to fetch stored offsets
        if self._group_id:
            yield from self._get_commit_offsets()

        # Update missing fetch/commit offsets
        for topic, partition in self._topics:

            # Commit offsets default is None
            if (topic, partition) not in self._offsets.commit:
                self._offsets.commit[(topic, partition)] = None

            # Skip if we already have a fetch offset from user args
            if (topic, partition) not in self._offsets.fetch:

                # Fetch offsets default is (1) commit
                if self._offsets.commit[(topic, partition)] is not None:
                    self._offsets.fetch[(topic, partition)] = \
                        self._offsets.commit[(topic, partition)]

                # or (2) auto reset
                else:
                    self._offsets.fetch[(topic, partition)] = yield from \
                        self._reset_partition_offset(topic, partition)

        # highwater marks (received from server on fetch response)
        # and task_done (set locally by user)
        # should always get initialized to None
        self._reset_highwater_offsets()
        self._reset_task_done_offsets()

    @asyncio.coroutine
    def fetch_messages(self):
        """
        Sends FetchRequests for all topic/partitions set for consumption
        Returns a generator that yields KafkaMessage structs
        after deserializing with the configured `deserializer_class`

        Refreshes metadata on errors, and resets fetch offset on
        OffsetOutOfRange, per the configured `auto_offset_reset` policy
        """
        max_bytes = self._fetch_message_max_bytes
        max_wait_time = self._fetch_wait_max_ms
        min_bytes = self._fetch_min_bytes
        # Get current fetch offsets
        offsets = self._offsets.fetch

        if not offsets:
            if not self._topics:
                raise KafkaConfigurationError('No topics or partitions '
                                              'configured')
            raise KafkaConfigurationError('No fetch offsets found when '
                                          'calling fetch_messages')

        fetches = []
        for topic_partition, offset in offsets.items():
            topic, partition = topic_partition
            fetches.append(FetchRequest(topic, partition, offset, max_bytes))

        # client.send_fetch_request will collect topic/partition requests
        # by leader
        # and send each group as a single FetchRequest to the correct broker
        try:
            responses = yield from self._client.send_fetch_request(
                fetches,
                max_wait_time=max_wait_time,
                min_bytes=min_bytes,
                fail_on_error=False)

        except FailedPayloadsError:
            log.warning('FailedPayloadsError attempting to fetch data '
                        'from kafka')
            yield from self._refresh_metadata_on_error()
            raise
        msgs = []
        for resp in responses:
            topic, partition = resp.topic, resp.partition
            try:
                check_error(resp)
            except OffsetOutOfRangeError:
                log.warning('OffsetOutOfRange: topic %s, partition %d, '
                            'offset %d (Highwatermark: %d)',
                            resp.topic, resp.partition,
                            offsets[(topic, partition)], resp.highwaterMark)
                # Reset offset
                self._offsets.fetch[(topic, partition)] = yield from \
                    self._reset_partition_offset(topic, partition)
                continue

            except NotLeaderForPartitionError:
                log.warning("NotLeaderForPartitionError for %s - %d. "
                            "Metadata may be out of date",
                            resp.topic, resp.partition)
                yield from self._refresh_metadata_on_error()
                continue

            except RequestTimedOutError:
                log.warning("RequestTimedOutError for %s - %d",
                            resp.topic, resp.partition)
                continue

            # Track server highwater mark
            self._offsets.highwater[(topic, partition)] = resp.highwaterMark

            # Yield each message
            # Kafka-python could raise an exception during iteration
            # we are not catching -- user will need to address
            for (offset, message) in resp.messages:

                # deserializer_class could raise an exception here
                msg = KafkaMessage(resp.topic, resp.partition, offset,
                                   message.key, message.value)
                # Only increment fetch offset if we safely got the message and
                #  deserialized
                self._offsets.fetch[(topic, partition)] = offset + 1

                msgs.append(msg)
        return msgs

    @asyncio.coroutine
    def get_partition_offsets(self, topic, partition, request_time_ms,
                              max_num_offsets):
        """Request available fetch offsets for a single topic/partition

        :param topic: ``bytes``
        :param partition: ``int``
        :param request_time_ms: ``int``Used to ask for all messages before a
            certain time (ms). There are two special values. Specify -1 to
            receive the latest offset (i.e. the offset of the next coming
            message) and -2 to receive the earliest available offset. Note
            that because offsets are pulled in descending order, asking for
            the earliest offset will always return you a single element.
        :param max_num_offsets: ``int``
        :return offsets: ``list`` of ``int``
        """
        reqs = [OffsetRequest(topic, partition, request_time_ms,
                              max_num_offsets)]
        (resp,) = yield from self._client.send_offset_request(reqs)
        return resp.offsets

    def task_done(self, message):
        """ Mark a fetched message as consumed.

        Offsets for messages marked as "task_done" will be stored back to
        the kafka cluster for this consumer group on commit()

        :param message: ``KafkaMessage`` structure
        """
        topic, partition = (message.topic, message.partition)
        offset = message.offset

        # Warn on non-contiguous offsets
        prev_done_offset = self._offsets.task_done[(topic, partition)]

        if prev_done_offset is not None and offset != (prev_done_offset + 1):
            log.warning('Marking task_done on a non-continuous offset:'
                        ' %d != %d + 1', offset, prev_done_offset)

        # Warn on smaller offsets than previous commit
        # "commit" offsets are actually the offset of the next message
        # to fetch.
        prev_commit_offset = self._offsets.commit[(topic, partition)]
        if (prev_commit_offset is not None and
                ((offset + 1) <= prev_commit_offset)):
            log.warning('Marking task_done on a previously committed '
                        'offset?: %d (+1) <= %d', offset, prev_commit_offset)

        self._offsets.task_done[(topic, partition)] = offset

        # Check for auto-commit
        if self._does_auto_commit_messages():
            self._incr_auto_commit_message_count()

        if self._should_auto_commit():
            self.commit()

    @asyncio.coroutine
    def commit(self):
        """
        Store consumed message offsets (marked via task_done())
        to kafka cluster for this consumer_group.

        Note -- this functionality requires server version >=0.8.1.1
        see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+
        The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
        """
        # API supports storing metadata with each commit
        # but for now it is unused
        metadata = b''

        offsets = self._offsets.task_done
        commits = []
        for topic_partition, task_done_offset in offsets.items():

            # Skip if None
            if task_done_offset is None:
                continue

            # Commit offsets as the next offset to fetch
            # which is consistent with the Java Client
            # task_done is marked by messages consumed,
            # so add one to mark the next message for fetching
            commit_offset = (task_done_offset + 1)

            # Skip if no change from previous committed
            if commit_offset == self._offsets.commit[topic_partition]:
                continue

            commits.append(OffsetCommitRequest(topic_partition[0],
                                               topic_partition[1],
                                               commit_offset,
                                               metadata))

        if commits:
            log.info('committing consumer offsets to group %s',
                     self._group_id)
            resps = yield from self._client.send_offset_commit_request(
                self._group_id, commits)

            for r in resps:
                topic_partition = (r.topic, r.partition)
                task_done = self._offsets.task_done[topic_partition]
                self._offsets.commit[topic_partition] = (task_done + 1)

            if self._auto_commit_enable:
                self._reset_auto_commit()
            return True

        else:
            log.info('No new offsets found to commit in group %s',
                     self._config['group_id'])
            return False

    #
    # Topic/partition management private methods
    #

    def _consume_topic_partition(self, topic, partition):
        if topic not in self._client._topic_partitions:
            raise UnknownTopicOrPartitionError('Topic %s not found in broker '
                                               'metadata' % topic)
        if partition not in self._client.get_partition_ids_for_topic(topic):
            raise UnknownTopicOrPartitionError(
                'Partition %d not found in Topic %s '
                'in broker metadata' % (partition, topic))
        self._topics.append((topic, partition))

    @asyncio.coroutine
    def _refresh_metadata_on_error(self):
        refresh_ms = self._refresh_leader_backoff_ms
        jitter_pct = 0.20
        sleep_ms = random.randint(
            int((1.0 - 0.5 * jitter_pct) * refresh_ms),
            int((1.0 + 0.5 * jitter_pct) * refresh_ms)
        )
        while True:
            log.info('Sleeping for refresh_leader_backoff_ms: %d', sleep_ms)
            yield from asyncio.sleep(sleep_ms / 1000.0, loop=self._loop)

            try:
                yield from self._client.load_metadata_for_topics()
            except KafkaUnavailableError:
                log.warning('Unable to refresh topic metadata... '
                            'cluster unavailable')
            else:
                log.info('Topic metadata refreshed')
                return

    @asyncio.coroutine
    def _get_commit_offsets(self):
        # offset management
        log.info('Consumer fetching stored offsets')
        for topic_part in self._topics:
            topic, partition = topic_part
            payload = [OffsetFetchRequest(topic, partition)]

            (resp,) = yield from self._client.send_offset_fetch_request(
                self._group_id, payload, fail_on_error=False)

            # API spec says server wont set an error here
            # but 0.8.1.1 does actually...
            try:
                check_error(resp)
            except UnknownTopicOrPartitionError:
                pass

            # -1 offset signals no commit is currently stored
            if resp.offset == -1:
                self._offsets.commit[topic_part] = None
            # Otherwise we committed the stored offset
            # and need to fetch the next one
            else:
                self._offsets.commit[topic_part] = resp.offset

    def _reset_highwater_offsets(self):
        for topic_partition in self._topics:
            self._offsets.highwater[topic_partition] = None

    def _reset_task_done_offsets(self):
        for topic_partition in self._topics:
            self._offsets.task_done[topic_partition] = None

    @asyncio.coroutine
    def _reset_partition_offset(self, topic, partition):
        LATEST = -1
        EARLIEST = -2

        request_time_ms = None
        if self._auto_offset_reset == 'largest':
            request_time_ms = LATEST
        elif self._auto_offset_reset == 'smallest':
            request_time_ms = EARLIEST

        (offset, ) = yield from self.get_partition_offsets(
            topic, partition, request_time_ms, max_num_offsets=1)
        return offset

    # Consumer Timeout private methods
    def _set_consumer_timeout_start(self):
        self._consumer_timeout = False
        if self._consumer_timeout_ms >= 0:
            consumer_sec = self._consumer_timeout_ms / 1000.0
            self._consumer_timeout = time.time() + consumer_sec

    def _check_consumer_timeout(self):
        if self._consumer_timeout and time.time() > self._consumer_timeout:
            raise ConsumerTimeout('Consumer timed out after %d ms' % +
                                  self._consumer_timeout_ms)

    def _should_auto_commit(self):
        if self._does_auto_commit_ms():
            if time.time() >= self._next_commit_time:
                return True

        if self._does_auto_commit_messages():
            if self._uncommitted_message_count >= \
                    self._auto_commit_interval_messages:
                return True

        return False

    def _reset_auto_commit(self):
        self._uncommitted_message_count = 0
        self._next_commit_time = None
        if self._does_auto_commit_ms():
            auto_commit_sec = self._auto_commit_interval_ms / 1000.0
            self._next_commit_time = time.time() + auto_commit_sec

    def _incr_auto_commit_message_count(self, n=1):
        self._uncommitted_message_count += n

    def _does_auto_commit_ms(self):
        if not self._auto_commit_enable:
            return False

        conf = self._auto_commit_interval_ms
        if conf is not None and conf > 0:
            return True
        return False

    def _does_auto_commit_messages(self):
        if not self._auto_commit_enable:
            return False

        conf = self._auto_commit_interval_messages
        if conf is not None and conf > 0:
            return True
        return False

    def __repr__(self):
        topics = ['{:s}-{:p}'.format(topic_partition)
                  for topic_partition in self._topics]
        return '<KafkaConsumer topics=(%s)>' % ', '.join(topics)
