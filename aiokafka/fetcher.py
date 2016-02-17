import asyncio
import collections
import copy
import logging

import kafka.common as Errors
from kafka.common import TopicPartition
from kafka.protocol.fetch import FetchRequest
from kafka.protocol.message import PartialMessage
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy

log = logging.getLogger(__name__)


ConsumerRecord = collections.namedtuple(
    "ConsumerRecord", ["topic", "partition", "offset", "key", "value"])


class NoOffsetForPartitionError(Errors.KafkaError):
    pass


class RecordTooLargeError(Errors.KafkaError):
    pass


class Fetcher:
    DEFAULT_CONFIG = {
        'key_deserializer': None,
        'value_deserializer': None,
        'fetch_min_bytes': 1,
        'fetch_max_wait_ms': 500,
        'max_partition_fetch_bytes': 1048576,
        'check_crcs': True,
    }

    def __init__(self, client, subscriptions, *, loop, **configs):
        """Initialize a Kafka Message Fetcher.

        Keyword Arguments:
            key_deserializer (callable): Any callable that takes a
                raw message key and returns a deserialized key.
            value_deserializer (callable, optional): Any callable that takes a
                raw message value and returns a deserialized value.
            fetch_min_bytes (int): Minimum amount of data the server should
                return for a fetch request, otherwise wait up to
                fetch_max_wait_ms for more data to accumulate. Default: 1.
            fetch_max_wait_ms (int): The maximum amount of time in milliseconds
                the server will block before answering the fetch request if
                there isn't sufficient data to immediately satisfy the
                requirement given by fetch_min_bytes. Default: 500.
            max_partition_fetch_bytes (int): The maximum amount of data
                per-partition the server will return. The maximum total memory
                used for a request = #partitions * max_partition_fetch_bytes.
                This size must be at least as large as the maximum message size
                the server allows or else it is possible for the producer to
                send messages larger than the consumer can fetch. If that
                happens, the consumer can get stuck trying to fetch a large
                message on a certain partition. Default: 1048576.
            check_crcs (bool): Automatically check the CRC32 of the records
                consumed. This ensures no on-the-wire or on-disk corruption to
                the messages occurred. This check adds some overhead, so it may
                be disabled in cases seeking extreme performance. Default: True
        """
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self._client = client
        self._loop = loop
        self._subscriptions = subscriptions
        self._records = asyncio.Queue(loop=loop)
        self._unauthorized_topics = set()
        self._offset_out_of_range_partitions = dict()  # {partition: offset}
        self._record_too_large_partitions = dict()  # {partition: offset}
        self._fetch_task = None
        self._iterator = None

    def initialized(self):
        return self._fetch_task is not None

    def close(self):
        if self._fetch_task:
            self._fetch_task.cancel()
            self._fetch_task = None

    @asyncio.coroutine
    def init_fetches(self):
        """Send FetchRequests asynchronously for all assigned partitions.
        """
        if not self._fetch_task:
            self._fetch_task = asyncio.ensure_future(
                self._fetch_requests_routine())

    @asyncio.coroutine
    def _fetch_requests_routine(self):
        while True:
            futures = []
            requests = self._create_fetch_requests()
            for node_id, request in requests.items():
                if (yield from self._client.ready(node_id)):
                    log.debug("Sending FetchRequest to node %s", node_id)
                    future = asyncio.ensure_future(
                        self._proc_fetch_request(node_id, request))
                    futures.append(future)
            if not futures:
                # no ready partitions for fetch
                yield from asyncio.sleep(0.01, loop=self._loop)
                continue
            yield from asyncio.wait(
                futures, return_when=asyncio.ALL_COMPLETED)
            yield from self._records.join()

    @asyncio.coroutine
    def _proc_fetch_request(self, node_id, request):
        try:
            response = yield from self._client.send(node_id, request)
        except Errors.KafkaError as err:
            log.error("Failed fetch messages from %s: %s", node_id, err)
            return

        fetch_offsets = {}
        for topic, partitions in request.topics:
            for partition, offset, _ in partitions:
                fetch_offsets[TopicPartition(topic, partition)] = offset

        for topic, partitions in response.topics:
            for partition, error_code, highwater, messages in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if not self._subscriptions.is_fetchable(tp):
                    # this can happen when a rebalance happened or a partition
                    # consumption paused while fetch is still in-flight
                    log.debug("Ignoring fetched records for partition %s"
                              " since it is no longer fetchable", tp)

                elif error_type is Errors.NoError:
                    self._subscriptions.assignment[tp].highwater = highwater

                    # we are interested in this fetch only if the beginning
                    # offset matches the current consumed position
                    fetch_offset = fetch_offsets[tp]
                    position = self._subscriptions.assignment[tp].position
                    if position is None or position != fetch_offset:
                        log.debug("Discarding fetch response for partition %s"
                                  " since its offset %d does not match the"
                                  " expected offset %d", tp, fetch_offset,
                                  position)
                        continue

                    partial = None
                    if messages and \
                            isinstance(messages[-1][-1], PartialMessage):
                        partial = messages.pop()

                    if messages:
                        log.debug(
                            "Adding fetched record for partition %s with"
                            " offset %d to buffered record list", tp, position)
                        self._records.put_nowait((fetch_offset, tp, messages))
                    elif partial:
                        # we did not read a single message from a non-empty
                        # buffer because that message's size is larger than
                        # fetch size, in this case record this exception
                        self._record_too_large_partitions[tp] = fetch_offset

                elif error_type in (Errors.NotLeaderForPartitionError,
                                    Errors.UnknownTopicOrPartitionError):
                    yield from self._client.force_metadata_update()
                elif error_type is Errors.OffsetOutOfRangeError:
                    fetch_offset = fetch_offsets[tp]
                    if self._subscriptions.has_default_offset_reset_policy():
                        self._subscriptions.need_offset_reset(tp)
                    else:
                        self._offset_out_of_range_partitions[tp] = fetch_offset
                    log.info(
                        "Fetch offset %s is out of range, resetting offset",
                        fetch_offset)
                elif error_type is Errors.TopicAuthorizationFailedError:
                    log.warn(
                        "Not authorized to read from topic %s.", tp.topic)
                    self._unauthorized_topics.add(tp.topic)
                elif error_type is Errors.UnknownError:
                    log.warn(
                        "Unknown error fetching data for partition %s", tp)
                else:
                    log.warn('Unexpected error while fetching data')

    @asyncio.coroutine
    def update_fetch_positions(self, partitions):
        """Update the fetch positions for the provided partitions.

        Arguments:
            partitions (list of TopicPartitions): partitions to update

        Raises:
            NoOffsetForPartitionError: if no offset is stored for a given
                partition and no reset policy is available
        """
        futures = []
        # reset the fetch position to the committed position
        for tp in partitions:
            if not self._subscriptions.is_assigned(tp):
                log.warning("partition %s is not assigned - skipping offset"
                            " update", tp)
                continue
            elif self._subscriptions.is_fetchable(tp):
                log.warning(
                    "partition %s is still fetchable -- skipping offset"
                    " update", tp)
                continue

            if self._subscriptions.is_offset_reset_needed(tp):
                futures.append(self._reset_offset(tp))
            elif self._subscriptions.assignment[tp].committed is None:
                # there's no committed position, so we need to reset with the
                # default strategy
                self._subscriptions.need_offset_reset(tp)
                futures.append(self._reset_offset(tp))
            else:
                committed = self._subscriptions.assignment[tp].committed
                log.debug("Resetting offset for partition %s to the committed"
                          " offset %s", tp, committed)
                self._subscriptions.seek(tp, committed)

        if futures:
            yield from asyncio.wait(
                futures, return_when=asyncio.ALL_COMPLETED)

    @asyncio.coroutine
    def _reset_offset(self, partition):
        """Reset offsets for the given partition using
        the offset reset strategy.

        Arguments:
            partition (TopicPartition): the partition that needs reset offset

        Raises:
            NoOffsetForPartitionError: if no offset reset strategy is defined
        """
        timestamp = self._subscriptions.assignment[partition].reset_strategy
        if timestamp is OffsetResetStrategy.EARLIEST:
            strategy = 'earliest'
        elif timestamp is OffsetResetStrategy.LATEST:
            strategy = 'latest'
        else:
            raise NoOffsetForPartitionError(partition)

        log.debug("Resetting offset for partition %s to %s offset.",
                  partition, strategy)
        offset = yield from self._offset(partition, timestamp)

        # we might lose the assignment while fetching the offset,
        # so check it is still active
        if self._subscriptions.is_assigned(partition):
            self._subscriptions.seek(partition, offset)

    @asyncio.coroutine
    def _offset(self, partition, timestamp):
        """Fetch a single offset before the given timestamp for the partition.

        Blocks until offset is obtained or a non-retriable exception is raised

        Arguments:
            partition The partition that needs fetching offset.
            timestamp (int): timestamp for fetching offset. -1 for the latest
                available, -2 for the earliest available. Otherwise timestamp
                is treated as epoch seconds.

        Returns:
            int: message offset
        """
        while True:
            try:
                offset = yield from self._proc_offset_request(
                    partition, timestamp)
            except Errors.KafkaError as error:
                if not error.retriable:
                    raise error
                if error.invalid_metadata:
                    yield from self._client.force_metadata_update()
            else:
                return offset

    @asyncio.coroutine
    def _proc_offset_request(self, partition, timestamp):
        """Fetch a single offset before the given timestamp for the partition.

        Arguments:
            partition (TopicPartition): partition that needs fetching offset
            timestamp (int): timestamp for fetching offset

        Returns:
            Future: resolves to the corresponding offset
        """
        node_id = self._client.cluster.leader_for_partition(partition)
        if node_id is None:
            log.debug("Partition %s is unknown for fetching offset,"
                      " wait for metadata refresh", partition)
            raise Errors.StaleMetadata(partition)
        elif node_id == -1:
            log.debug(
                "Leader for partition %s unavailable for fetching offset,"
                " wait for metadata refresh", partition)
            raise Errors.LeaderNotAvailableError(partition)

        request = OffsetRequest(
            -1, [(partition.topic, [(partition.partition, timestamp, 1)])]
        )

        if not (yield from self._client.ready(node_id)):
            raise Errors.NodeNotReadyError(node_id)

        response = yield from self._client.send(node_id, request)

        topic, partition_info = response.topics[0]
        assert len(response.topics) == 1 and len(partition_info) == 1, (
            'OffsetResponse should only be for a single topic-partition')

        part, error_code, offsets = partition_info[0]
        assert topic == partition.topic and part == partition.partition, (
            'OffsetResponse partition does not match OffsetRequest partition')

        error_type = Errors.for_code(error_code)
        if error_type is Errors.NoError:
            assert len(offsets) == 1, 'Expected OffsetResponse with one offset'
            offset = offsets[0]
            log.debug("Fetched offset %d for partition %s", offset, partition)
            return offset
        elif error_type in (Errors.NotLeaderForPartitionError,
                            Errors.UnknownTopicOrPartitionError):
            log.warning("Attempt to fetch offsets for partition %s failed due"
                        " to obsolete leadership information, retrying.",
                        partition)
            raise error_type(partition)
        else:
            log.error(
                "Attempt to fetch offsets for partition %s failed due to:"
                " %s", partition, error_type)
            raise error_type(partition)

    def _raise_if_offset_out_of_range(self):
        """Check FetchResponses for offset out of range.

        Raises:
            OffsetOutOfRangeError: if any partition from previous FetchResponse
                contains OffsetOutOfRangeError and the default_reset_policy is
                None
        """
        if not self._offset_out_of_range_partitions:
            return

        current_out_of_range_partitions = {}

        # filter only the fetchable partitions
        for partition, offset in self._offset_out_of_range_partitions:
            if not self._subscriptions.is_fetchable(partition):
                log.debug("Ignoring fetched records for %s since it is no"
                          " longer fetchable", partition)
                continue
            position = self._subscriptions.assignment[partition].position
            # ignore partition if the current position != offset
            # in FetchResponse e.g. after seek()
            if position is not None and offset == position:
                current_out_of_range_partitions[partition] = position

        self._offset_out_of_range_partitions.clear()
        if current_out_of_range_partitions:
            raise Errors.OffsetOutOfRangeError(
                current_out_of_range_partitions)

    def _raise_if_unauthorized_topics(self):
        """Check FetchResponses for topic authorization failures.

        Raises:
            TopicAuthorizationFailedError
        """
        if self._unauthorized_topics:
            topics = set(self._unauthorized_topics)
            self._unauthorized_topics.clear()
            raise Errors.TopicAuthorizationFailedError(topics)

    def _raise_if_record_too_large(self):
        """Check FetchResponses for messages larger than the max per partition

        Raises:
            RecordTooLargeError: if there is a message larger than fetch size
        """
        if not self._record_too_large_partitions:
            return

        copied_record_too_large_partitions = dict(
            self._record_too_large_partitions)
        self._record_too_large_partitions.clear()

        raise RecordTooLargeError(
            "There are some messages at [Partition=Offset]: %s "
            " whose size is larger than the fetch size %s"
            " and hence cannot be ever returned."
            " Increase the fetch size, or decrease the maximum message"
            " size the broker will allow.",
            copied_record_too_large_partitions,
            self.config['max_partition_fetch_bytes'])

    def _unpack_message_set(self, tp, messages):
        for offset, size, msg in messages:
            if self.config['check_crcs'] and not msg.validate_crc():
                raise Errors.InvalidMessageError(msg)
            elif msg.is_compressed():
                for record in self._unpack_message_set(tp, msg.decompress()):
                    yield record
            else:
                key, value = self._deserialize(msg)
                yield ConsumerRecord(
                    tp.topic, tp.partition, offset, key, value)

    def _message_generator(self):
        """Iterate over fetched_records"""
        while True:
            # Check on each iteration since this is a generator
            self._raise_if_offset_out_of_range()
            self._raise_if_unauthorized_topics()
            self._raise_if_record_too_large()

            try:
                (fetch_offset, tp, messages) = self._records.get_nowait()
                self._records.task_done()
            except asyncio.QueueEmpty:
                break

            if not self._subscriptions.is_assigned(tp):
                # this can happen when a rebalance happened before
                # fetched records are returned
                log.debug("Not returning fetched records for partition %s"
                          " since it is no longer assigned", tp)
                continue

            # note that the consumed position should always be available
            # as long as the partition is still assigned
            position = self._subscriptions.assignment[tp].position
            if not self._subscriptions.is_fetchable(tp):
                # this can happen when a partition consumption paused before
                # fetched records are returned
                log.debug(
                    "Not returning fetched records for assigned partition"
                    " %s since it is no longer fetchable", tp)

            elif fetch_offset == position:
                log.debug(
                    "Returning fetched records at offset %d for assigned"
                    " partition %s", position, tp)
                for msg in self._unpack_message_set(tp, messages):
                    # Because we are in a generator, it is possible for
                    # subscription state to change between yield calls
                    # so we need to re-check on each loop
                    # this should catch assignment changes, pauses
                    # and resets via seek_to_beginning / seek_to_end
                    position = self._subscriptions.assignment[tp].position
                    if not self._subscriptions.is_fetchable(tp):
                        log.debug(
                            "Not returning fetched records for partition %s"
                            " since it is no longer fetchable", tp)
                        break

                    if self._subscriptions.needs_partition_assignment:
                        log.debug("Not returning fetched records"
                                  " bcs partitions are unassigned")
                        break

                    # Compressed messagesets may include earlier messages
                    # It is also possible that the user called seek()
                    elif msg.offset != position:
                        continue

                    self._subscriptions.assignment[tp].position += 1
                    yield msg
            else:
                # these records aren't next in line based on the last consumed
                # position, ignore them they must be from an obsolete request
                log.debug("Ignoring fetched records for %s at offset %s",
                          tp, fetch_offset)

    def fetched_records(self):
        """Returns previously fetched records and updates consumed offsets.

        Incompatible with iterator interface - use one or the other, not both.

        Raises:
            OffsetOutOfRangeError: if no subscription offset_reset_strategy
            InvalidMessageError: if message crc validation fails (check_crcs
                must be set to True)
            RecordTooLargeError: if a message is larger than the currently
                configured max_partition_fetch_bytes
            TopicAuthorizationError: if consumer is not authorized to fetch
                messages from the topic
            AssertionError: if used with iterator (incompatible)

        Returns:
            dict: {TopicPartition: [messages]}
        """
        fetched = collections.defaultdict(list)
        for message in self._message_generator():
            tp = TopicPartition(message.topic, message.partition)
            fetched[tp].append(message)
        return fetched

    @asyncio.coroutine
    def next_record(self):
        while True:
            while self._iterator is None and self._records.empty():
                return None

            if not self._iterator:
                self._iterator = self._message_generator()
            try:
                return next(self._iterator)
            except StopIteration:
                self._iterator = None
                continue

    def _deserialize(self, msg):
        if self.config['key_deserializer']:
            key = self.config['key_deserializer'](msg.key)
            # pylint: disable-msg=not-callable
        else:
            key = msg.key
        if self.config['value_deserializer']:
            value = self.config['value_deserializer'](msg.value)
        else:
            value = msg.value
        return key, value

    def _create_fetch_requests(self):
        """Create fetch requests for all assigned partitions, grouped by node.

        FetchRequests skipped if no leader, or node has requests in flight

        Returns:
            dict: {node_id: FetchRequest, ...}
        """
        if self._subscriptions.needs_partition_assignment:
            return {}

        # create the fetch info as a dict of lists of partition info tuples
        # which can be passed to FetchRequest() via .items()
        fetchable = collections.defaultdict(
            lambda: collections.defaultdict(list))

        for partition in self._subscriptions.fetchable_partitions():
            node_id = self._client.cluster.leader_for_partition(partition)
            if node_id is None or node_id == -1:
                log.debug("No leader found for partition %s."
                          " Requesting metadata update", partition)
            else:
                # fetch if there is a leader and no in-flight requests
                position = self._subscriptions.assignment[partition].position
                partition_info = (
                    partition.partition,
                    position,
                    self.config['max_partition_fetch_bytes']
                )
                fetchable[node_id][partition.topic].append(partition_info)
                log.debug(
                    "Adding fetch request for partition %s at offset %d",
                    partition, position)

        requests = {}
        for node_id, partition_data in fetchable.items():
            requests[node_id] = FetchRequest(
                -1,  # replica_id
                self.config['fetch_max_wait_ms'],
                self.config['fetch_min_bytes'],
                partition_data.items())
        return requests
