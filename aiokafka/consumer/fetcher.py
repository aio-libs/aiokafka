import asyncio
import collections
import logging
import random
from itertools import chain

from kafka.protocol.offset import OffsetRequest

from aiokafka.consumer.fetch import FetchRequest
import aiokafka.errors as Errors
from aiokafka.errors import (
    ConsumerStoppedError, RecordTooLargeError, KafkaTimeoutError)
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.structs import OffsetAndTimestamp, TopicPartition, ConsumerRecord
from aiokafka.util import ensure_future, create_future

log = logging.getLogger(__name__)

UNKNOWN_OFFSET = -1


class OffsetResetStrategy:
    LATEST = -1
    EARLIEST = -2
    NONE = 0

    @classmethod
    def from_str(cls, name):
        name = name.lower()
        if name == "latest":
            return cls.LATEST
        if name == "earliest":
            return cls.EARLIEST
        if name == "none":
            return cls.NONE
        else:
            log.warning(
                'Unrecognized ``auto_offset_reset`` config, using NONE')
            return cls.NONE

    @classmethod
    def to_str(cls, value):
        if value == cls.LATEST:
            return "latest"
        if value == cls.EARLIEST:
            return "earliest"
        if value == cls.NONE:
            return "none"
        else:
            return "timestamp({})".format(value)


class FetchResult:
    def __init__(
            self, tp, *, assignment, loop, message_iterator, backoff,
            fetch_offset):
        self._topic_partition = tp
        self._message_iter = message_iterator

        self._created = loop.time()
        self._backoff = backoff
        self._loop = loop

        self._assignment = assignment
        # There are cases where the returned offset from broker differs from
        # what was requested. This would not be much of an issue if the user
        # could not seek to a different position between yielding results,
        # so we can't reliably say witch case it is without a new veriable.
        self._expected_position = fetch_offset

    def calculate_backoff(self):
        lifetime = self._loop.time() - self._created
        if lifetime < self._backoff:
            return self._backoff - lifetime
        return 0

    def check_assignment(self, tp):
        assignment = self._assignment
        return_result = True
        if assignment.active:
            tp = self._topic_partition
            position = assignment.state_value(tp).position
            if position != self._expected_position:
                return_result = False
        else:
            return_result = False

        if not return_result:
            # this can happen when a rebalance happened before
            # fetched records are returned
            log.debug("Not returning fetched records for partition %s"
                      " since it is no fetchable (unassigned or paused)", tp)
            self._message_iter = None
            return False
        return True

    def _consume_offset(self, offset):
        position = self._expected_position = offset + 1
        state = self._assignment.state_value(self._topic_partition)
        state.consumed_to(position)

    def getone(self):
        tp = self._topic_partition
        if not self.check_assignment(tp) or not self.has_more():
            return

        next_batch_iter = self._message_iter
        while True:
            try:
                msg = next(next_batch_iter)
            except StopIteration:
                self._message_iter = None
                return

            if msg.offset < self._expected_position:
                # Probably just a compressed messageset, it's ok to skip.
                continue

            # It's not a problem if the offset is larger than expected
            # position. It may happen in compacted topics, some messages were
            # just removed.

            self._consume_offset(msg.offset)
            return msg

    def getall(self, max_records=None):
        tp = self._topic_partition
        if not self.check_assignment(tp) or not self.has_more():
            return []

        next_batch_iter = self._message_iter
        ret_list = []
        for msg in next_batch_iter:
            if msg.offset < self._expected_position:
                # Probably just a compressed messageset, it's ok.
                continue
            # As in the `getone` case it's ok if offset is larger than expected

            ret_list.append(msg)
            if max_records is not None and len(ret_list) >= max_records:
                break
        else:
            self._message_iter = None

        if ret_list:
            self._consume_offset(ret_list[-1].offset)

        return ret_list

    def has_more(self):
        return self._message_iter is not None

    def __repr__(self):
        return "<FetchResult position={!r}>".format(self._expected_position)


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

    def __repr__(self):
        return "<FetchError error={!r}>".format(self._error)


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
                 retry_backoff_ms=100,
                 auto_offset_reset='latest'):
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
            auto_offset_reset (str): A policy for resetting offsets on
                OffsetOutOfRange errors: 'earliest' will move to the oldest
                available message, 'latest' will move to the most recent. Any
                ofther value will raise the exception. Default: 'latest'.
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
        self._default_reset_strategy = OffsetResetStrategy.from_str(
            auto_offset_reset)

        self._records = collections.OrderedDict()
        self._in_flight = set()
        self._pending_tasks = set()

        self._wait_consume_future = None
        self._fetch_waiters = set()

        req_version = 2 if client.api_version >= (0, 10) else 1
        self._fetch_request_class = FetchRequest[req_version]

        self._fetch_task = ensure_future(
            self._fetch_requests_routine(), loop=loop)

        self._closed = False

    @asyncio.coroutine
    def close(self):
        self._closed = True

        self._fetch_task.cancel()
        try:
            yield from self._fetch_task
        except asyncio.CancelledError:
            pass

        # Fail all pending fetchone/fetchall calls
        for waiter in self._fetch_waiters:
            self._notify(waiter)

        for x in self._pending_tasks:
            x.cancel()
            yield from x

    def _notify(self, future):
        if future is not None and not future.done():
            future.set_result(None)

    def _create_fetch_waiter(self):
        fut = create_future(loop=self._loop)
        self._fetch_waiters.add(fut)
        fut.add_done_callback(
            lambda f, waiters=self._fetch_waiters: waiters.remove(f))
        return fut

    @asyncio.coroutine
    def _fetch_requests_routine(self):
        """ Implements a background task to populate internal fetch queue
        ``self._records`` with prefetched messages. This helps isolate the
        ``getall/getone`` calls from actual calls to broker. This way we don't
        need to think of what happens if user calls get in 2 tasks, etc.

            The loop is quite complicated due to a large set of events that
        can allow new fetches to be send. Those include previous fetches,
        offset resets, metadata updates to discover new leaders for partitions,
        data consumed for partition.

            Previously the offset reset was performed separately, but it did
        not perform too reliably. In ``kafka-python`` and Java client the reset
        is perform in ``poll()`` before each fetch, which works good for sync
        systems. But with ``aiokafka`` the user can actually break such
        behaviour quite easily by performing actions from different tasks.
        """
        try:
            assignment = None

            def start_pending_task(coro, node_id, self=self):
                task = ensure_future(coro, loop=self._loop)
                self._pending_tasks.add(task)
                self._in_flight.add(node_id)

                def on_done(fut, self=self):
                    self._in_flight.discard(node_id)
                task.add_done_callback(on_done)

            while True:
                # If we lose assignment we just cancel all current tasks,
                # wait for new assignment and restart the loop
                if assignment is None or not assignment.active:
                    for task in self._pending_tasks:
                        # Those tasks should have proper handling for
                        # cancellation
                        if not task.done():
                            task.cancel()
                        yield from task
                    self._pending_tasks.clear()
                    self._records.clear()

                    subscription = self._subscriptions.subscription
                    if subscription is None or \
                            subscription.assignment is None:
                        yield from self._subscriptions.wait_for_assignment()
                    assignment = self._subscriptions.subscription.assignment
                assert assignment is not None and assignment.active

                # Reset consuming signal future.
                self._wait_consume_future = create_future(loop=self._loop)
                # Determine what action to take per node
                fetch_requests, reset_requests, timeout, invalid_metadata = \
                    self._get_actions_per_node(assignment)
                # Start fetch tasks
                for node_id, request in fetch_requests:
                    start_pending_task(
                        self._proc_fetch_request(assignment, node_id, request),
                        node_id=node_id)
                # Start update position tasks
                for node_id, tps in reset_requests.items():
                    start_pending_task(
                        self._update_fetch_positions(assignment, node_id, tps),
                        node_id=node_id)
                # Apart from pending requests we also need to react to other
                # events to send new fetches as soon as possible
                other_futs = [self._wait_consume_future,
                              assignment.unassign_future]
                if invalid_metadata:
                    fut = self._client.force_metadata_update()
                    other_futs.append(fut)

                done_set, _ = yield from asyncio.wait(
                    chain(self._pending_tasks, other_futs),
                    loop=self._loop,
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED)

                # Process fetch tasks results if any
                done_pending = self._pending_tasks.intersection(done_set)
                if done_pending:
                    has_new_data = any(fut.result() for fut in done_pending)
                    if has_new_data:
                        for waiter in self._fetch_waiters:
                            # we added some messages to self._records,
                            # wake up waiters
                            self._notify(waiter)
                    self._pending_tasks -= done_pending
        except asyncio.CancelledError:
            pass
        except Exception:  # pragma: no cover
            log.error("Unexpected error in fetcher routine", exc_info=True)

    def _get_actions_per_node(self, assignment):
        """ For each assigned partition determine the action needed to be
        performed and group those by leader node id.
        """
        # create the fetch info as a dict of lists of partition info tuples
        # which can be passed to FetchRequest() via .items()
        fetchable = collections.defaultdict(list)
        awaiting_reset = collections.defaultdict(list)
        backoff_by_nodes = collections.defaultdict(list)
        invalid_metadata = False

        for tp in assignment.tps:
            tp_state = assignment.state_value(tp)
            node_id = self._client.cluster.leader_for_partition(tp)
            backoff = 0
            if tp in self._records:
                # We have data still not consumed by user. In this case we
                # usually wait for the user to finish consumption, but to avoid
                # blocking other partitions we have a timeout here.
                record = self._records[tp]
                backoff = record.calculate_backoff()
                if backoff:
                    backoff_by_nodes[node_id].append(backoff)
            elif node_id in self._in_flight:
                # We have in-flight fetches to this node
                continue
            elif node_id is None or node_id == -1:
                log.debug("No leader found for partition %s."
                          " Waiting metadata update", tp)
                invalid_metadata = True
            elif not tp_state.has_valid_position:
                awaiting_reset[node_id].append(tp)
            else:
                position = tp_state.position
                fetchable[node_id].append((tp, position))
                log.debug(
                    "Adding fetch request for partition %s at offset %d",
                    tp, position)

        fetch_requests = []
        for node_id, partition_data in fetchable.items():
            if node_id in backoff_by_nodes:
                # At least one partition is still waiting to be consumed
                continue
            if node_id in awaiting_reset:
                # First we need to reset offset for some partitions, then we
                # will fetch next page of results
                continue

            # Shuffle partition data to help get more equal consumption
            random.shuffle(partition_data)  # shuffle topics
            # Create fetch request
            by_topics = collections.defaultdict(list)
            for tp, position in partition_data:
                by_topics[tp.topic].append((
                    tp.partition,
                    position,
                    self._max_partition_fetch_bytes))
            req = self._fetch_request_class(
                -1,  # replica_id
                self._fetch_max_wait_ms,
                self._fetch_min_bytes,
                list(by_topics.items()))
            fetch_requests.append((node_id, req))

        if backoff_by_nodes:
            # Return min time till any node will be ready to send event
            # (max of it's backoffs)
            backoff = min(map(max, backoff_by_nodes.values()))
        else:
            backoff = self._fetcher_timeout
        return fetch_requests, awaiting_reset, backoff, invalid_metadata

    @asyncio.coroutine
    def _proc_fetch_request(self, assignment, node_id, request):
        needs_wakeup = False
        try:
            response = yield from self._client.send(node_id, request)
        except Errors.KafkaError as err:
            log.error("Failed fetch messages from %s: %s", node_id, err)
            return False
        except asyncio.CancelledError:
            # Either `close()` or partition unassigned. Either way the result
            # is no longer of interest.
            return False

        if not assignment.active:
            log.debug(
                "Discarding fetch response since the assignment changed during"
                " fetch")
            return False

        fetch_offsets = {}
        for topic, partitions in request.topics:
            for partition, offset, _ in partitions:
                fetch_offsets[TopicPartition(topic, partition)] = offset

        for topic, partitions in response.topics:
            for partition, error_code, highwater, raw_batch in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                fetch_offset = fetch_offsets[tp]
                tp_state = assignment.state_value(tp)
                if not tp_state.has_valid_position or \
                        tp_state.position != fetch_offset:
                    log.debug(
                        "Discarding fetch response for partition %s "
                        "since its offset %s does not match the current "
                        "position", tp, fetch_offset)
                    continue

                if error_type is Errors.NoError:
                    tp_state.highwater = highwater

                    records = MemoryRecords(raw_batch)
                    if records.has_next():
                        log.debug(
                            "Adding fetched record for partition %s with"
                            " offset %d to buffered record list",
                            tp, fetch_offset)

                        message_iterator = self._unpack_records(tp, records)
                        self._records[tp] = FetchResult(
                            tp, message_iterator=message_iterator,
                            assignment=assignment,
                            backoff=self._prefetch_backoff,
                            fetch_offset=fetch_offset,
                            loop=self._loop)

                        # We added at least 1 successful record
                        needs_wakeup = True
                    elif records.size_in_bytes() > 0:
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
                        tp_state.consumed_to(tp_state.position + 1)
                        needs_wakeup = True

                elif error_type in (Errors.NotLeaderForPartitionError,
                                    Errors.UnknownTopicOrPartitionError):
                    self._client.force_metadata_update()
                elif error_type is Errors.OffsetOutOfRangeError:
                    if self._default_reset_strategy != \
                            OffsetResetStrategy.NONE:
                        tp_state.await_reset(self._default_reset_strategy)
                    else:
                        err = Errors.OffsetOutOfRangeError({tp: fetch_offset})
                        self._set_error(tp, err)
                        needs_wakeup = True
                    log.info(
                        "Fetch offset %s is out of range for partition %s,"
                        " resetting offset", fetch_offset, tp)
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
        assert tp not in self._records, self._records[tp]
        self._records[tp] = FetchError(
            error=error, backoff=self._prefetch_backoff, loop=self._loop)

    @asyncio.coroutine
    def _update_fetch_positions(self, assignment, node_id, tps):
        """ This task will be called if there is no valid position for
        partition. It may be right after assignment, on seek_to_* calls of
        Consumer or if current position went out of range.
        """
        log.debug("Updating fetch positions for partitions %s", tps)
        needs_wakeup = False
        # The list of partitions provided can consist of paritions that are
        # awaiting reset already and freshly assigned partitions. The second
        # ones can yet have no commit point fetched, so we will check that.
        for tp in tps:
            tp_state = assignment.state_value(tp)
            if tp_state.has_valid_position or tp_state.awaiting_reset:
                continue

            committed = tp_state.committed
            # None means the Coordinator has yet to update the offset
            if committed is None:
                try:
                    yield from tp_state.wait_for_committed()
                except asyncio.CancelledError:
                    return needs_wakeup
                committed = tp_state.committed
            assert committed is not None

            # There could have been a seek() call of some sort while
            # waiting for committed point
            if tp_state.has_valid_position or tp_state.awaiting_reset:
                continue

            if committed.offset == UNKNOWN_OFFSET:
                # No offset stored in Kafka, need to reset
                if self._default_reset_strategy != OffsetResetStrategy.NONE:
                    tp_state.await_reset(self._default_reset_strategy)
                else:
                    err = Errors.NoOffsetForPartitionError(tp)
                    self._set_error(tp, err)
                    needs_wakeup = True
                log.debug(
                    "No committed offset found for %s", tp)
            else:
                log.debug("Resetting offset for partition %s to the "
                          "committed offset %s", tp, committed)
                tp_state.reset_to(committed.offset)

        topic_data = collections.defaultdict(list)
        needs_reset = []
        for tp in tps:
            tp_state = assignment.state_value(tp)
            if not tp_state.awaiting_reset:
                continue
            needs_reset.append(tp)

            strategy = tp_state.reset_strategy
            assert strategy is not None
            log.debug("Resetting offset for partition %s using %s strategy.",
                      tp, OffsetResetStrategy.to_str(strategy))
            topic_data[tp.topic].append((tp.partition, strategy))

        if not topic_data:
            return needs_wakeup

        try:
            try:
                offsets = yield from self._proc_offset_request(
                    node_id, topic_data)
            except Errors.KafkaError as err:
                log.error("Failed fetch offsets from %s: %s", node_id, err)
                yield from asyncio.sleep(self._retry_backoff, loop=self._loop)
                return needs_wakeup
        except asyncio.CancelledError:
            return needs_wakeup

        for tp in needs_reset:
            offset = offsets[tp][0]
            tp_state = assignment.state_value(tp)
            # There could have been some `seek` call while fetching offset
            if tp_state.awaiting_reset:
                tp_state.reset_to(offset)
        return needs_wakeup

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
            if self._closed:
                raise ConsumerStoppedError()

            # While the background routine will fetch new records up till new
            # assignment is finished, we don't want to return records, that may
            # not belong to this instance after rebalance.
            if self._subscriptions.reassignment_in_progress:
                yield from self._subscriptions.wait_for_assignment()

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
            waiter = self._create_fetch_waiter()
            yield from waiter

    @asyncio.coroutine
    def fetched_records(self, partitions, timeout=0, max_records=None):
        """ Returns previously fetched records and updates consumed offsets.
        """
        while True:
            # While the background routine will fetch new records up till new
            # assignment is finished, we don't want to return records, that may
            # not belong to this instance after rebalance.
            if self._subscriptions.reassignment_in_progress:
                yield from self._subscriptions.wait_for_assignment()

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

            waiter = self._create_fetch_waiter()
            done, _ = yield from asyncio.wait(
                [waiter], timeout=timeout, loop=self._loop)

            if not done or self._closed:
                return {}

            # Decrease timeout accordingly
            timeout = timeout - (self._loop.time() - start_time)
            timeout = max(0, timeout)

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
    def request_offset_reset(self, tps, strategy):
        """ Force a position reset. Called from Consumer of `seek_to_*` API's.
        """
        assignment = self._subscriptions.subscription.assignment
        assert assignment is not None

        waiters = []
        for tp in tps:
            tp_state = assignment.state_value(tp)
            tp_state.await_reset(strategy)
            waiters.append(tp_state.wait_for_position())
            # Invalidate previous fetch result
            if tp in self._records:
                del self._records[tp]

        # XXX: Maybe we should use a different future or rename? Not really
        # describing the purpose.
        self._notify(self._wait_consume_future)

        yield from asyncio.wait(
            [asyncio.gather(*waiters, loop=self._loop),
             assignment.unassign_future],
            loop=self._loop, return_when=asyncio.FIRST_COMPLETED
        )
        return assignment.active

    def seek_to(self, tp, offset):
        """ Force a position change to specific offset. Called from
        `Consumer.seek()` API.
        """
        self._subscriptions.seek(tp, offset)
        if tp in self._records:
            del self._records[tp]

        # XXX: Maybe we should use a different future or rename? Not really
        # describing the purpose.
        self._notify(self._wait_consume_future)

    def _unpack_records(self, tp, records):
        # NOTE: if the batch is not compressed it's equal to 1 record in
        #       v0 and v1.
        deserialize = self._deserialize
        check_crcs = self._check_crcs
        while records.has_next():
            next_batch = records.next_batch()
            if check_crcs and not next_batch.validate_crc():
                # This iterator will be closed after the exception, so we don't
                # try to drain other batches here. They will be refetched.
                raise Errors.CorruptRecordException("Invalid CRC")
            for record in next_batch:
                # Save encoded sizes
                key_size = len(record.key) if record.key is not None else -1
                value_size = \
                    len(record.value) if record.value is not None else -1
                key, value = deserialize(record)
                yield ConsumerRecord(
                    tp.topic, tp.partition, record.offset, record.timestamp,
                    record.timestamp_type, key, value, record.checksum,
                    key_size, value_size)

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
