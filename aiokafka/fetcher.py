import asyncio
import collections
import logging
import random
from itertools import chain

from kafka.protocol.fetch import FetchRequest
from kafka.protocol.message import PartialMessage
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy

import aiokafka.errors as Errors
from aiokafka import ensure_future
from aiokafka.errors import (
    ConsumerStoppedError, RecordTooLargeError, KafkaTimeoutError)
from aiokafka.structs import OffsetAndTimestamp, TopicPartition, ConsumerRecord

log = logging.getLogger(__name__)

UNKNOWN_OFFSET = -1


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

    def check_assignment(self, tp):
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
        if not self.check_assignment(tp):
            return

        tp_assignment = self._subscriptions.assignment[tp]
        while True:
            if not self._messages:
                return

            msg = self._messages.popleft()
            if msg.offset < tp_assignment.position:
                # Probably just a compressed messageset, it's ok.
                continue
            elif (msg.offset > tp_assignment.position and
                    tp_assignment.drop_pending_message_set):
                # We seeked to a different position between `get*` calls,
                # so we can't verify if the message is correct.
                self._messages.clear()
                return

            tp_assignment.position = msg.offset + 1
            return msg

    def getall(self, max_records=None):
        tp = self._topic_partition
        if not self.check_assignment(tp) or not self._messages:
            return []

        if max_records is None:
            max_records = len(self._messages)

        tp_assignment = self._subscriptions.assignment[tp]
        if self._messages[0].offset > tp_assignment.position:
            if tp_assignment.drop_pending_message_set:
                # We seeked to a different position between `get*` calls,
                # so we can't verify if the message is correct.
                self._messages.clear()
                return []

        ret_list = []
        while True:
            if not self._messages or len(ret_list) == max_records:
                return ret_list

            msg = self._messages.popleft()
            if msg.offset < tp_assignment.position:
                # Probably just a compressed messageset, it's ok.
                continue
            tp_assignment.position = msg.offset + 1
            ret_list.append(msg)

    def has_more(self):
        return bool(self._messages)


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
                 prefetch_backoff=0.1,
                 retry_backoff_ms=100):
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
        self._retry_backoff = retry_backoff_ms / 1000
        self._subscriptions = subscriptions

        self._records = collections.OrderedDict()
        self._in_flight = set()
        self._fetch_tasks = set()

        self._wait_consume_future = None
        self._wait_empty_future = None

        req_version = 2 if client.api_version >= (0, 10) else 1
        self._fetch_request_class = FetchRequest[req_version]

        self._fetch_task = ensure_future(
            self._fetch_requests_routine(), loop=loop)

    @asyncio.coroutine
    def close(self):
        self._fetch_task.cancel()
        try:
            yield from self._fetch_task
        except asyncio.CancelledError:
            pass

        # Fail all pending fetchone/fetchall calls
        if self._wait_empty_future is not None and \
                not self._wait_empty_future.done():
            self._wait_empty_future.set_exception(ConsumerStoppedError())

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
        * If all partitions for this node need prefetch - do it right away
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
        except Exception:  # pragma: no cover
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
            # Shuffle partition data to help get more equal consumption
            partition_data = list(partition_data.items())
            random.shuffle(partition_data)  # shuffle topics
            for _, partition in partition_data:
                random.shuffle(partition)  # shuffle partitions
            if node_id in backoff_by_nodes:
                # At least one partition is still waiting to be consumed
                continue
            req = self._fetch_request_class(
                -1,  # replica_id
                self._fetch_max_wait_ms,
                self._fetch_min_bytes,
                partition_data)
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
                    tp_assignment = self._subscriptions.assignment[tp]
                    tp_assignment.highwater = highwater

                    # `drop_pending_message_set` is set after a seek to another
                    # position. If we request the *new* position we have to
                    # drop this flag, so we catch future seek's.
                    fetch_offset = fetch_offsets[tp]
                    if fetch_offset == tp_assignment.position:
                        tp_assignment.drop_pending_message_set = False
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

            assignment = self._subscriptions.assignment[tp]
            if self._subscriptions.is_offset_reset_needed(tp):
                futures.append(self._reset_offset(tp, assignment))
            elif self._subscriptions.assignment[tp].committed is None:
                # there's no committed position, so we need to reset with the
                # default strategy
                self._subscriptions.need_offset_reset(tp)
                futures.append(self._reset_offset(tp, assignment))
            else:
                committed = self._subscriptions.assignment[tp].committed
                log.debug("Resetting offset for partition %s to the committed"
                          " offset %s", tp, committed)
                self._subscriptions.seek(tp, committed)

        if futures:
            yield from asyncio.gather(*futures, loop=self._loop)

    @asyncio.coroutine
    def get_offsets_by_times(self, timestamps, timeout_ms):
        offsets = yield from self._retrieve_offsets(timestamps, timeout_ms)
        for tp in timestamps:
            if tp not in offsets:
                offsets[tp] = None
            else:
                offset, timestamp = offsets[tp]
                if offset == UNKNOWN_OFFSET:
                    offsets[tp] = None
                else:
                    offsets[tp] = OffsetAndTimestamp(offset, timestamp)
        return offsets

    @asyncio.coroutine
    def beginning_offsets(self, partitions, timeout_ms):
        timestamps = {tp: OffsetResetStrategy.EARLIEST for tp in partitions}
        offsets = yield from self._retrieve_offsets(timestamps, timeout_ms)
        return {
            tp: offset for (tp, (offset, ts)) in offsets.items()
        }

    @asyncio.coroutine
    def end_offsets(self, partitions, timeout_ms):
        timestamps = {tp: OffsetResetStrategy.LATEST for tp in partitions}
        offsets = yield from self._retrieve_offsets(timestamps, timeout_ms)
        return {
            tp: offset for (tp, (offset, ts)) in offsets.items()
        }

    @asyncio.coroutine
    def _reset_offset(self, partition, assignment):
        """Reset offsets for the given partition using
        the offset reset strategy.

        Arguments:
            partition (TopicPartition): the partition that needs reset offset

        Raises:
            NoOffsetForPartitionError: if no offset reset strategy is defined
        """
        timestamp = assignment.reset_strategy
        assert timestamp is not None
        if timestamp is OffsetResetStrategy.EARLIEST:
            strategy = 'earliest'
        else:
            strategy = 'latest'

        log.debug("Resetting offset for partition %s to %s offset.",
                  partition, strategy)
        offsets = yield from self._retrieve_offsets({partition: timestamp})
        assert partition in offsets
        offset = offsets[partition][0]

        # we might lose the assignment while fetching the offset,
        # so check it is still active
        if self._subscriptions.is_assigned(partition) and \
                self._subscriptions.assignment[partition] is assignment:
            self._subscriptions.seek(partition, offset)

    @asyncio.coroutine
    def _retrieve_offsets(self, timestamps, timeout_ms=float("inf")):
        """ Fetch offset for each partition passed in ``timestamps`` map.

        Blocks until offsets are obtained, a non-retriable exception is raised
        or ``timeout_ms`` passed.

        Arguments:
            timestamps: {TopicPartition: int} dict with timestamps to fetch
                offsets by. -1 for the latest available, -2 for the earliest
                available. Otherwise timestamp is treated as epoch miliseconds.

        Returns:
            {TopicPartition: (int, int)}: Mapping of partition to
                retrieved offset and timestamp. If offset does not exist for
                the provided timestamp, that partition will be missing from
                this mapping.
        """
        if not timestamps:
            return {}

        timeout = timeout_ms / 1000
        start_time = self._loop.time()
        remaining = timeout
        while True:
            try:
                offsets = yield from asyncio.wait_for(
                    self._proc_offset_requests(timestamps),
                    timeout=None if remaining == float("inf") else remaining,
                    loop=self._loop
                )
            except asyncio.TimeoutError:
                break
            except Errors.KafkaError as error:
                if not error.retriable:
                    raise error
                if error.invalid_metadata:
                    self._client.force_metadata_update()
                elapsed = self._loop.time() - start_time
                remaining = max(0, remaining - elapsed)
                if remaining < self._retry_backoff:
                    break
                yield from asyncio.sleep(self._retry_backoff, loop=self._loop)
            else:
                return offsets
        raise KafkaTimeoutError(
            "Failed to get offsets by times in %s ms" % timeout_ms)

    @asyncio.coroutine
    def _proc_offset_requests(self, timestamps):
        """ Fetch offsets for each partition in timestamps dict. This may send
        request to multiple nodes, based on who is Leader for partition.

        Arguments:
            timestamps (dict): {TopicPartition: int} mapping of fetching
                timestamps.

        Returns:
            Future: resolves to a mapping of retrieved offsets
        """
        # The could be several places where we triggered an update, so only
        # wait for it once here
        yield from self._client._maybe_wait_metadata()

        # Group it in hierarhy `node` -> `topic` -> `[(partition, offset)]`
        timestamps_by_node = collections.defaultdict(
            lambda: collections.defaultdict(list))

        for partition, timestamp in timestamps.items():
            node_id = self._client.cluster.leader_for_partition(partition)
            if node_id is None:
                # triggers a metadata update
                self._client.add_topic(partition.topic)
                log.debug("Partition %s is unknown for fetching offset,"
                          " wait for metadata refresh", partition)
                raise Errors.StaleMetadata(partition)
            elif node_id == -1:
                log.debug("Leader for partition %s unavailable for fetching "
                          "offset, wait for metadata refresh", partition)
                raise Errors.LeaderNotAvailableError(partition)
            else:
                timestamps_by_node[node_id][partition.topic].append(
                    (partition.partition, timestamp)
                )

        futs = []
        for node_id, topic_data in timestamps_by_node.items():
            futs.append(
                self._proc_offset_request(node_id, topic_data)
            )
        offsets = {}
        res = yield from asyncio.gather(*futs, loop=self._loop)
        for partial_offsets in res:
            offsets.update(partial_offsets)
        return offsets

    @asyncio.coroutine
    def _proc_offset_request(self, node_id, topic_data):
        if self._client.api_version < (0, 10, 1):
            version = 0
            # Version 0 had another field `max_offsets`, set it to `1`
            for topic, part_data in topic_data.items():
                topic_data[topic] = [(part, ts, 1) for part, ts in part_data]
        else:
            version = 1
        request = OffsetRequest[version](-1, list(topic_data.items()))

        if not (yield from self._client.ready(node_id)):
            raise Errors.NodeNotReadyError(node_id)

        response = yield from self._client.send(node_id, request)

        res_offsets = {}
        for topic, part_data in response.topics:
            for part, error_code, *partition_info in part_data:
                partition = TopicPartition(topic, part)
                error_type = Errors.for_code(error_code)
                if error_type is Errors.NoError:
                    if response.API_VERSION == 0:
                        offsets = partition_info[0]
                        assert len(offsets) <= 1, \
                            'Expected OffsetResponse with one offset'
                        if offsets:
                            offset = offsets[0]
                            log.debug(
                                "Handling v0 ListOffsetResponse response for "
                                "%s. Fetched offset %s", partition, offset)
                            res_offsets[partition] = (offset, None)
                        else:
                            res_offsets[partition] = (UNKNOWN_OFFSET, None)
                    else:
                        timestamp, offset = partition_info
                        log.debug(
                            "Handling ListOffsetResponse response for "
                            "%s. Fetched offset %s, timestamp %s",
                            partition, offset, timestamp)
                        res_offsets[partition] = (offset, timestamp)
                elif error_type is Errors.UnsupportedForMessageFormatError:
                    # The message format on the broker side is before 0.10.0,
                    # we will simply put None in the response.
                    log.debug("Cannot search by timestamp for partition %s "
                              "because the message format version is before "
                              "0.10.0", partition)
                elif error_type is Errors.NotLeaderForPartitionError:
                    log.debug(
                        "Attempt to fetch offsets for partition %s ""failed "
                        "due to obsolete leadership information, retrying.",
                        partition)
                    raise error_type(partition)
                elif error_type is Errors.UnknownTopicOrPartitionError:
                    log.warn(
                        "Received unknown topic or partition error in "
                        "ListOffset request for partition %s. The "
                        "topic/partition may not exist or the user may not "
                        "have Describe access to it.", partition)
                    raise error_type(partition)
                else:
                    log.warning(
                        "Attempt to fetch offsets for partition %s failed due "
                        "to: %s", partition, error_type)
                    raise error_type(partition)
        return res_offsets

    @asyncio.coroutine
    def next_record(self, partitions):
        """ Return one fetched records

        This method will contain a little overhead as we will do more work this
        way:
            * Notify prefetch routine per every consumed partition
            * Assure message marked for autocommit

        """
        while True:
            for tp in list(self._records.keys()):
                if partitions and tp not in partitions:
                    # Cleanup results for unassigned partitons
                    if not self._subscriptions.is_assigned(tp):
                        del self._records[tp]
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
            if self._wait_empty_future is None \
                    or self._wait_empty_future.done():
                self._wait_empty_future = asyncio.Future(loop=self._loop)
            yield from asyncio.shield(self._wait_empty_future, loop=self._loop)

    @asyncio.coroutine
    def fetched_records(self, partitions, timeout=0, max_records=None):
        """ Returns previously fetched records and updates consumed offsets.
        """
        while True:
            start_time = self._loop.time()
            drained = {}
            for tp in list(self._records.keys()):
                if partitions and tp not in partitions:
                    # Cleanup results for unassigned partitons
                    if not self._subscriptions.is_assigned(tp):
                        del self._records[tp]
                    continue
                res_or_error = self._records[tp]
                if type(res_or_error) == FetchResult:
                    records = res_or_error.getall(max_records)
                    if not res_or_error.has_more():
                        # We processed all messages - request new ones
                        del self._records[tp]
                        self._notify(self._wait_consume_future)
                    if not records:
                        continue
                    drained[tp] = records
                    if max_records is not None:
                        max_records -= len(drained[tp])
                        assert max_records >= 0  # Just in case
                        if max_records == 0:
                            break
                else:
                    # We already got some messages from another partition -
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

            if self._wait_empty_future is None \
                    or self._wait_empty_future.done():
                self._wait_empty_future = asyncio.Future(loop=self._loop)
            done, _ = yield from asyncio.wait(
                [self._wait_empty_future], timeout=timeout, loop=self._loop)

            # _wait_empty_future can be set an exception in `close()` call
            # to stop all long running tasks
            if not done or self._wait_empty_future.exception() is not None:
                return {}

            # Decrease timeout accordingly
            timeout = timeout - (self._loop.time() - start_time)
            timeout = max(0, timeout)

    def _unpack_message_set(self, tp, messages):
        for offset, size, msg in messages:
            if self._check_crcs and not msg.validate_crc():
                raise Errors.InvalidMessageError(msg)
            elif msg.is_compressed():
                # If relative offset is used, we need to decompress the entire
                # message first to compute the absolute offset.
                inner_mset = msg.decompress()
                if msg.magic > 0:
                    last_offset, _, _ = inner_mset[-1]
                    absolute_base_offset = offset - last_offset
                else:
                    absolute_base_offset = -1

                for inner_offset, inner_size, inner_msg in inner_mset:
                    if msg.magic > 0:
                        # When magic value is greater than 0, the timestamp
                        # of a compressed message depends on the
                        # typestamp type of the wrapper message:
                        if msg.timestamp_type == 0:  # CREATE_TIME (0)
                            inner_timestamp = inner_msg.timestamp
                        elif msg.timestamp_type == 1:  # LOG_APPEND_TIME (1)
                            inner_timestamp = msg.timestamp
                        else:
                            raise ValueError('Unknown timestamp type: {0}'
                                             .format(msg.timestamp_type))
                    else:
                        inner_timestamp = msg.timestamp

                    if absolute_base_offset >= 0:
                        inner_offset += absolute_base_offset

                    key, value = self._deserialize(inner_msg)
                    yield ConsumerRecord(
                        tp.topic, tp.partition, inner_offset,
                        inner_timestamp, msg.timestamp_type,
                        key, value, inner_msg.crc,
                        len(inner_msg.key)
                        if inner_msg.key is not None else -1,
                        len(inner_msg.value)
                        if inner_msg.value is not None else -1)
            else:
                key, value = self._deserialize(msg)
                yield ConsumerRecord(
                    tp.topic, tp.partition, offset,
                    msg.timestamp, msg.timestamp_type,
                    key, value, msg.crc,
                    len(msg.key) if msg.key is not None else -1,
                    len(msg.value) if msg.value is not None else -1)

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
