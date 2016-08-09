import asyncio
import collections
import logging
from itertools import chain

import kafka.common as Errors
from kafka.common import TopicPartition
from kafka.protocol.fetch import FetchRequest_v0 as FetchRequest
from kafka.protocol.message import PartialMessage
from kafka.protocol.offset import (
    OffsetRequest_v0 as OffsetRequest, OffsetResetStrategy)

from aiokafka import ensure_future

log = logging.getLogger(__name__)


ConsumerRecord = collections.namedtuple(
    "ConsumerRecord", ["topic", "partition", "offset", "key", "value"])


class NoOffsetForPartitionError(Errors.KafkaError):
    pass


class RecordTooLargeError(Errors.KafkaError):
    pass


class FetchResult:
    def __init__(self, tp, *, subscriptions, loop, messages, backoff):
        self._topic_partition = tp
        self._subscriptions = subscriptions
        self._messages = messages
        self._created = loop.time()
        self._backoff = backoff
        self._loop = loop

    def calculate_backoff(self):
        lifetime = self._loop.time() - self._created
        if lifetime < self._backoff:
            return self._backoff - lifetime
        return 0

    def _check_assignment(self, tp):
        if self._subscriptions.needs_partition_assignment or \
                not self._subscriptions.is_fetchable(tp):
            # this can happen when a rebalance happened before
            # fetched records are returned
            log.debug("Not returning fetched records for partition %s"
                      " since it is no fetchable (unassigned or paused)", tp)
            self._messages.clear()
            return False
        return True

    def getone(self):
        tp = self._topic_partition
        if not self._check_assignment(tp):
            return

        while True:
            if not self._messages:
                return

            msg = self._messages.popleft()
            if msg.offset == self._subscriptions.assignment[tp].position:
                # Compressed messagesets may include earlier messages
                # It is also possible that the user called seek()
                self._subscriptions.assignment[tp].position += 1
                return msg

    def getall(self):
        tp = self._topic_partition
        if not self._check_assignment(tp):
            return []

        ret_list = []
        while True:
            if not self._messages:
                return ret_list

            msg = self._messages.popleft()
            if msg.offset == self._subscriptions.assignment[tp].position:
                # Compressed messagesets may include earlier messages
                # It is also possible that the user called seek()
                self._subscriptions.assignment[tp].position += 1
                ret_list.append(msg)


class FetchError:
    def __init__(self, *, loop, error, backoff):
        self._error = error
        self._created = loop.time()
        self._backoff = backoff
        self._loop = loop

    def calculate_backoff(self):
        lifetime = self._loop.time() - self._created
        if lifetime < self._backoff:
            return self._backoff - lifetime
        return 0

    def check_raise(self):
        # TODO: Do we need to raise error if partition not assigned anymore
        raise self._error


class Fetcher:
    def __init__(self, client, subscriptions, *, loop,
                 key_deserializer=None,
                 value_deserializer=None,
                 fetch_min_bytes=1,
                 fetch_max_wait_ms=500,
                 max_partition_fetch_bytes=1048576,
                 check_crcs=True,
                 fetcher_timeout=0.2,
                 prefetch_backoff=0.1):
        """Initialize a Kafka Message Fetcher.

        Parameters:
            client (AIOKafkaClient): kafka client
            subscription (SubscriptionState): instance of SubscriptionState
                located in kafka.consumer.subscription_state
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
            fetcher_timeout (float): Maximum polling interval in the background
                fetching routine. Default: 0.2
            prefetch_backoff (float): number of seconds to wait until
                consumption of partition is paused. Paused partitions will not
                request new data from Kafka server (will not be included in
                next poll request).
        """
        self._client = client
        self._loop = loop
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._check_crcs = check_crcs
        self._fetcher_timeout = fetcher_timeout
        self._prefetch_backoff = prefetch_backoff
        self._subscriptions = subscriptions

        self._records = collections.OrderedDict()
        self._in_flight = set()
        self._fetch_tasks = set()

        self._wait_consume_future = None
        self._wait_empty_future = None

        self._fetch_task = ensure_future(
            self._fetch_requests_routine(), loop=loop)

    @asyncio.coroutine
    def close(self):
        self._fetch_task.cancel()
        try:
            yield from self._fetch_task
        except asyncio.CancelledError:
            pass

        for x in self._fetch_tasks:
            x.cancel()
            try:
                yield from x
            except asyncio.CancelledError:
                pass

    @asyncio.coroutine
    def _fetch_requests_routine(self):
        """ Background task, that always prefetches next result page.

        The algorithm:
        * Group partitions per node, which is the leader for it.
        * If all partitions for this node need prefetch - do it right alway
        * If any partition has some data (in `self._records`) wait up till
          `prefetch_backoff` so application can consume data from it.
        * If data in `self._records` is not consumed up to
          `prefetch_backoff` just request data for other partitions from this
          node.

        We request data in such manner cause Kafka blocks the connection if
        we perform a FetchRequest and we don't have enough data. This means
        we must perform a FetchRequest to as many partitions as we can in a
        node.

        Original Java Kafka client processes data differently, as it only
        prefetches data if all messages were given to application (i.e. if
        `self._records` are empty). We don't use this method, cause we allow
        to process partitions separately (by passing `partitions` list to
        `getall()` call of the consumer), which can end up in a long wait
        if some partitions (or topics) are processed slower, than others.

        """
        try:
            while True:
                # Reset consuming signal future.
                self._wait_consume_future = asyncio.Future(loop=self._loop)
                # Create and send fetch requests
                requests, timeout = self._create_fetch_requests()
                for node_id, request in requests:
                    node_ready = yield from self._client.ready(node_id)
                    if not node_ready:
                        # We will request it on next routine
                        continue
                    log.debug("Sending FetchRequest to node %s", node_id)
                    task = ensure_future(
                        self._proc_fetch_request(node_id, request),
                        loop=self._loop)
                    self._fetch_tasks.add(task)
                    self._in_flight.add(node_id)

                done_set, _ = yield from asyncio.wait(
                    chain(self._fetch_tasks, [self._wait_consume_future]),
                    loop=self._loop,
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED)

                # Process fetch tasks results if any
                done_fetches = self._fetch_tasks.intersection(done_set)
                if done_fetches:
                    has_new_data = any(fut.result() for fut in done_fetches)
                    if has_new_data:
                        # we added some messages to self._records,
                        # wake up getters
                        self._notify(self._wait_empty_future)
                    self._fetch_tasks -= done_fetches
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa
            log.error("Unexpected error in fetcher routine", exc_info=True)

    def _notify(self, future):
        if future is not None and not future.done():
            future.set_result(None)

    def _create_fetch_requests(self):
        """Create fetch requests for all assigned partitions, grouped by node.

        FetchRequests skipped if:
        * no leader, or node has already fetches in flight
        * we have data for this partition
        * we have data for other partitions on this node

        Returns:
            dict: {node_id: FetchRequest, ...}
        """
        if self._subscriptions.needs_partition_assignment:
            return {}, self._fetcher_timeout

        # create the fetch info as a dict of lists of partition info tuples
        # which can be passed to FetchRequest() via .items()
        fetchable = collections.defaultdict(
            lambda: collections.defaultdict(list))
        backoff_by_nodes = collections.defaultdict(list)

        fetchable_partitions = self._subscriptions.fetchable_partitions()
        for tp in fetchable_partitions:
            node_id = self._client.cluster.leader_for_partition(tp)
            if tp in self._records:
                record = self._records[tp]
                # Calculate backoff for this node if data is only recently
                # fetched. If data is consumed before backoff we will
                # include this partition in this fetch request
                backoff = record.calculate_backoff()
                if backoff:
                    backoff_by_nodes[node_id].append(backoff)
                # We have some prefetched data for this partition already
                continue
            if node_id in self._in_flight:
                # We have in-flight fetches to this node
                continue
            if node_id is None or node_id == -1:
                log.debug("No leader found for partition %s."
                          " Waiting metadata update", tp)
            else:
                # fetch if there is a leader and no in-flight requests
                position = self._subscriptions.assignment[tp].position
                partition_info = (
                    tp.partition,
                    position,
                    self._max_partition_fetch_bytes)
                fetchable[node_id][tp.topic].append(partition_info)
                log.debug(
                    "Adding fetch request for partition %s at offset %d",
                    tp, position)

        requests = []
        for node_id, partition_data in fetchable.items():
            if node_id in backoff_by_nodes:
                # At least one partition is still waiting to be consumed
                continue
            req = FetchRequest(
                -1,  # replica_id
                self._fetch_max_wait_ms,
                self._fetch_min_bytes,
                partition_data.items())
            requests.append((node_id, req))
        if backoff_by_nodes:
            # Return min time til any node will be ready to send event
            # (max of it's backoffs)
            backoff = min(map(max, backoff_by_nodes.values()))
        else:
            backoff = self._fetcher_timeout
        return requests, backoff

    @asyncio.coroutine
    def _proc_fetch_request(self, node_id, request):
        needs_wakeup = False
        try:
            response = yield from self._client.send(node_id, request)
        except Errors.KafkaError as err:
            log.error("Failed fetch messages from %s: %s", node_id, err)
            return False
        finally:
            self._in_flight.remove(node_id)

        fetch_offsets = {}
        for topic, partitions in request.topics:
            for partition, offset, _ in partitions:
                fetch_offsets[TopicPartition(topic, partition)] = offset

        for topic, partitions in response.topics:
            for partition, error_code, highwater, messages in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if not self._subscriptions.is_fetchable(tp):
                    # this can happen when a rebalance happened
                    log.debug("Ignoring fetched records for partition %s"
                              " since it is no longer fetchable", tp)

                elif error_type is Errors.NoError:
                    self._subscriptions.assignment[tp].highwater = highwater

                    # we are interested in this fetch only if the beginning
                    # offset matches the current consumed position
                    fetch_offset = fetch_offsets[tp]
                    partial = None
                    if messages and \
                            isinstance(messages[-1][-1], PartialMessage):
                        partial = messages.pop()

                    if messages:
                        log.debug(
                            "Adding fetched record for partition %s with"
                            " offset %d to buffered record list",
                            tp, fetch_offset)
                        try:
                            messages = collections.deque(
                                self._unpack_message_set(tp, messages))
                        except Errors.InvalidMessageError as err:
                            self._set_error(tp, err)
                            continue

                        self._records[tp] = FetchResult(
                            tp, messages=messages,
                            subscriptions=self._subscriptions,
                            backoff=self._prefetch_backoff,
                            loop=self._loop)
                        # We added at least 1 successful record
                        needs_wakeup = True
                    elif partial:
                        # we did not read a single message from a non-empty
                        # buffer because that message's size is larger than
                        # fetch size, in this case record this exception
                        err = RecordTooLargeError(
                            "There are some messages at [Partition=Offset]: "
                            "%s=%s whose size is larger than the fetch size %s"
                            " and hence cannot be ever returned. "
                            "Increase the fetch size, or decrease the maximum "
                            "message size the broker will allow.",
                            tp, fetch_offset, self._max_partition_fetch_bytes)
                        self._set_error(tp, err)
                        needs_wakeup = True
                        self._subscriptions.assignment[tp].position += 1

                elif error_type in (Errors.NotLeaderForPartitionError,
                                    Errors.UnknownTopicOrPartitionError):
                    self._client.force_metadata_update()
                elif error_type is Errors.OffsetOutOfRangeError:
                    fetch_offset = fetch_offsets[tp]
                    if self._subscriptions.has_default_offset_reset_policy():
                        self._subscriptions.need_offset_reset(tp)
                    else:
                        err = Errors.OffsetOutOfRangeError({tp: fetch_offset})
                        self._set_error(tp, err)
                        needs_wakeup = True
                    log.info(
                        "Fetch offset %s is out of range, resetting offset",
                        fetch_offset)
                elif error_type is Errors.TopicAuthorizationFailedError:
                    log.warn("Not authorized to read from topic %s.", tp.topic)
                    err = Errors.TopicAuthorizationFailedError(tp.topic)
                    self._set_error(tp, err)
                    needs_wakeup = True
                else:
                    log.warn('Unexpected error while fetching data: %s',
                             error_type.__name__)
        return needs_wakeup

    def _set_error(self, tp, error):
        assert tp not in self._records
        self._records[tp] = FetchError(
            error=error, backoff=self._prefetch_backoff, loop=self._loop)

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
            done, _ = yield from asyncio.wait(
                futures, return_when=asyncio.ALL_COMPLETED, loop=self._loop)
            # retrieve task result, can raise exception
            [x.result() for x in done]

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
        else:
            strategy = 'latest'

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
            if not offsets:
                return -1
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

    @asyncio.coroutine
    def next_record(self, partitions):
        """ Return one fetched records

        This method will contain a little overhead as we will do more work this
        way:
            * Notify prefetch routine per every consumed partition
            * Assure message marked for autocommit

        """
        for tp in list(self._records.keys()):
            if partitions and tp not in partitions:
                continue
            res_or_error = self._records[tp]
            if type(res_or_error) == FetchResult:
                message = res_or_error.getone()
                if message is None:
                    # We already processed all messages, request new ones
                    del self._records[tp]
                    self._notify(self._wait_consume_future)
                else:
                    return message
            else:
                # Remove error, so we can fetch on partition again
                del self._records[tp]
                self._notify(self._wait_consume_future)
                res_or_error.check_raise()
        # No messages ready. Wait for some to arrive
        if self._wait_empty_future is None or self._wait_empty_future.done():
            self._wait_empty_future = asyncio.Future(loop=self._loop)
        yield from asyncio.shield(self._wait_empty_future, loop=self._loop)
        return (yield from self.next_record(partitions))

    @asyncio.coroutine
    def fetched_records(self, partitions, timeout=0):
        """ Returns previously fetched records and updates consumed offsets.
        """
        drained = {}
        for tp in list(self._records.keys()):
            if partitions and tp not in partitions:
                continue
            res_or_error = self._records[tp]
            if type(res_or_error) == FetchResult:
                drained[tp] = res_or_error.getall()
                # We processed all messages - request new ones
                del self._records[tp]
                self._notify(self._wait_consume_future)
            else:
                # We already got some of messages from other partition -
                # return them. We will raise this error on next call
                if drained:
                    return drained
                else:
                    # Remove error, so we can fetch on partition again
                    del self._records[tp]
                    self._notify(self._wait_consume_future)
                    res_or_error.check_raise()

        if drained or not timeout:
            return drained

        if self._wait_empty_future is None or self._wait_empty_future.done():
            self._wait_empty_future = asyncio.Future(loop=self._loop)
        done, _ = yield from asyncio.wait(
            [self._wait_empty_future], timeout=timeout, loop=self._loop)

        if done:
            return (yield from self.fetched_records(partitions, 0))
        return {}

    def _unpack_message_set(self, tp, messages):
        for offset, size, msg in messages:
            if self._check_crcs and not msg.validate_crc():
                raise Errors.InvalidMessageError(msg)
            elif msg.is_compressed():
                yield from self._unpack_message_set(tp, msg.decompress())
            else:
                key, value = self._deserialize(msg)
                yield ConsumerRecord(
                    tp.topic, tp.partition, offset, key, value)

    def _deserialize(self, msg):
        if self._key_deserializer:
            key = self._key_deserializer(msg.key)
        else:
            key = msg.key
        if self._value_deserializer:
            value = self._value_deserializer(msg.value)
        else:
            value = msg.value
        return key, value
