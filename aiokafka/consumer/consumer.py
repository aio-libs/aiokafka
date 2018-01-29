import asyncio
import logging
import re

from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor

from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.client import AIOKafkaClient
from aiokafka.errors import (
    TopicAuthorizationFailedError, OffsetOutOfRangeError,
    ConsumerStoppedError, IllegalOperation, UnsupportedVersionError,
    IllegalStateError, NoOffsetForPartitionError, RecordTooLargeError,
    CorruptRecordException
)
from aiokafka.structs import TopicPartition, OffsetAndMetadata
from aiokafka.util import PY_35
from aiokafka import __version__

from .fetcher import Fetcher, OffsetResetStrategy
from .group_coordinator import GroupCoordinator, NoGroupCoordinator
from .subscription_state import SubscriptionState

log = logging.getLogger(__name__)


class AIOKafkaConsumer(object):
    """
    A client that consumes records from a Kafka cluster.

    The consumer will transparently handle the failure of servers in the Kafka
    cluster, and adapt as topic-partitions are created or migrate between
    brokers. It also interacts with the assigned kafka Group Coordinator node
    to allow multiple consumers to load balance consumption of topics (feature
    of kafka >= 0.9.0.0).

    .. _create_connection:
        https://docs.python.org/3/library/asyncio-eventloop.html\
        #creating-connections

    Arguments:
        *topics (str): optional list of topics to subscribe to. If not set,
            call subscribe() or assign() before consuming records. Passing
            topics directly is same as calling ``subscribe()`` API.
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the consumer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: 'aiokafka-{version}'
        group_id (str or None): name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            Default: None
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
        max_poll_records (int): The maximum number of records returned in a
            single call to ``getmany()``. Defaults ``None``, no limit.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 40000.
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        auto_offset_reset (str): A policy for resetting offsets on
            OffsetOutOfRange errors: 'earliest' will move to the oldest
            available message, 'latest' will move to the most recent. Any
            ofther value will raise the exception. Default: 'latest'.
        enable_auto_commit (bool): If true the consumer's offset will be
            periodically committed in the background. Default: True.
        auto_commit_interval_ms (int): milliseconds between automatic
            offset commits, if enable_auto_commit is True. Default: 5000.
        check_crcs (bool): Automatically check the CRC32 of the records
            consumed. This ensures no on-the-wire or on-disk corruption to
            the messages occurred. This check adds some overhead, so it may
            be disabled in cases seeking extreme performance. Default: True
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        partition_assignment_strategy (list): List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy. Default: [RoundRobinPartitionAssignor]
        heartbeat_interval_ms (int): The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than session_timeout_ms, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances. Default: 3000
        session_timeout_ms (int): The timeout used to detect failures when
            using Kafka's group managementment facilities. Default: 30000
        consumer_timeout_ms (int): maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions. Default: 200
        api_version (str): specify which kafka API version to use.
            AIOKafkaConsumer supports Kafka API versions >=0.9 only.
            If set to 'auto', will attempt to infer the broker version by
            probing various APIs. Default: auto
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. Directly passed into asyncio's
            `create_connection`_. For more information see :ref:`ssl_auth`.
            Default: None.
        exclude_internal_topics (bool): Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it. Requires 0.10+ Default: True
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks. Default: 540000 (9hours).

    Note:
        Many configuration parameters are taken from Java Client:
        https://kafka.apache.org/documentation.html#newconsumerconfigs

    """
    def __init__(self, *topics, loop,
                 bootstrap_servers='localhost',
                 client_id='aiokafka-' + __version__,
                 group_id=None,
                 key_deserializer=None, value_deserializer=None,
                 fetch_max_wait_ms=500,
                 fetch_min_bytes=1,
                 max_partition_fetch_bytes=1 * 1024 * 1024,
                 request_timeout_ms=40 * 1000,
                 retry_backoff_ms=100,
                 auto_offset_reset='latest',
                 enable_auto_commit=True,
                 auto_commit_interval_ms=5000,
                 check_crcs=True,
                 metadata_max_age_ms=5 * 60 * 1000,
                 partition_assignment_strategy=(RoundRobinPartitionAssignor,),
                 heartbeat_interval_ms=3000,
                 session_timeout_ms=30000,
                 consumer_timeout_ms=200,
                 max_poll_records=None,
                 ssl_context=None,
                 security_protocol='PLAINTEXT',
                 api_version='auto',
                 exclude_internal_topics=True,
                 connections_max_idle_ms=540000):
        if api_version not in ('auto', '0.9', '0.10'):
            raise ValueError("Unsupported Kafka API version")
        self._client = AIOKafkaClient(
            loop=loop, bootstrap_servers=bootstrap_servers,
            client_id=client_id, metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            api_version=api_version,
            ssl_context=ssl_context,
            security_protocol=security_protocol,
            connections_max_idle_ms=connections_max_idle_ms)

        if max_poll_records is not None and (
                not isinstance(max_poll_records, int) or max_poll_records < 1):
            raise ValueError("`max_poll_records` should be positive Integer")

        self._group_id = group_id
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._session_timeout_ms = session_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._auto_offset_reset = auto_offset_reset
        self._request_timeout_ms = request_timeout_ms
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._partition_assignment_strategy = partition_assignment_strategy
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._exclude_internal_topics = exclude_internal_topics
        self._max_poll_records = max_poll_records
        self._consumer_timeout = consumer_timeout_ms / 1000
        self._check_crcs = check_crcs
        self._subscription = SubscriptionState(loop=loop)
        self._fetcher = None
        self._coordinator = None
        self._closed = False
        self._loop = loop

        if topics:
            topics = self._validate_topics(topics)
            self._client.set_topics(topics)
            self._subscription.subscribe(topics=topics)

    @asyncio.coroutine
    def start(self):
        """ Connect to Kafka cluster. This will:

            * Load metadata for all cluster nodes and partition allocation
            * Wait for possible topic autocreation
            * Join group if ``group_id`` provided
        """
        yield from self._client.bootstrap()
        yield from self._wait_topics()

        if self._client.api_version < (0, 9):
            raise ValueError("Unsupported Kafka version: {}".format(
                self._client.api_version))

        self._fetcher = Fetcher(
            self._client, self._subscription, loop=self._loop,
            key_deserializer=self._key_deserializer,
            value_deserializer=self._value_deserializer,
            fetch_min_bytes=self._fetch_min_bytes,
            fetch_max_wait_ms=self._fetch_max_wait_ms,
            max_partition_fetch_bytes=self._max_partition_fetch_bytes,
            check_crcs=self._check_crcs,
            fetcher_timeout=self._consumer_timeout,
            retry_backoff_ms=self._retry_backoff_ms,
            auto_offset_reset=self._auto_offset_reset)

        if self._group_id is not None:
            # using group coordinator for automatic partitions assignment
            self._coordinator = GroupCoordinator(
                self._client, self._subscription, loop=self._loop,
                group_id=self._group_id,
                heartbeat_interval_ms=self._heartbeat_interval_ms,
                session_timeout_ms=self._session_timeout_ms,
                retry_backoff_ms=self._retry_backoff_ms,
                enable_auto_commit=self._enable_auto_commit,
                auto_commit_interval_ms=self._auto_commit_interval_ms,
                assignors=self._partition_assignment_strategy,
                exclude_internal_topics=self._exclude_internal_topics)
            # In case we provided topics to constructor we better wait for
            # initial group join
            if self._subscription.subscription is not None:
                yield from self._subscription.wait_for_assignment()
        else:
            # Using a simple assignment coordinator for reassignment on
            # metadata changes
            self._coordinator = NoGroupCoordinator(
                self._client, self._subscription, loop=self._loop,
                exclude_internal_topics=self._exclude_internal_topics)

            # If we passed `topics` to constructor.
            if self._subscription.subscription is not None:
                yield from self._client.force_metadata_update()
                self._coordinator.assign_all_partitions(check_unknown=True)

    @asyncio.coroutine
    def _wait_topics(self):
        if self._subscription.subscription is not None:
            for topic in self._subscription.subscription.topics:
                yield from self._client._wait_on_metadata(topic)

    def _validate_topics(self, topics):
        if not isinstance(topics, (tuple, set, list)):
            raise ValueError("Topics should be list of strings")
        return set(topics)

    def assign(self, partitions):
        """ Manually assign a list of TopicPartitions to this consumer.

        This interface does not support incremental assignment and will
        replace the previous assignment (if there was one).

        Arguments:
            partitions (list of TopicPartition): assignment for this instance.

        Raises:
            IllegalStateError: if consumer has already called subscribe()

        Warning:
            It is not possible to use both manual partition assignment with
            assign() and group assignment with subscribe().

        Note:
            Manual topic assignment through this method does not use the
            consumer's group management functionality. As such, there will be
            **no rebalance operation triggered** when group membership or
            cluster and topic metadata change.
        """
        self._subscription.assign_from_user(partitions)
        self._client.set_topics([tp.topic for tp in partitions])
        if self._group_id is not None:
            # refresh commit positions for all assigned partitions
            assignment = self._subscription.subscription.assignment
            self._coordinator.start_commit_offsets_refresh_task(assignment)

    def assignment(self):
        """ Get the set of partitions currently assigned to this consumer.

        If partitions were directly assigned using ``assign()``, then this will
        simply return the same partitions that were previously assigned.

        If topics were subscribed using ``subscribe()``, then this will give
        the set of topic partitions currently assigned to the consumer (which
        may be empty if the assignment hasn't happened yet or if the partitions
        are in the process of being reassigned).

        Returns:
            set: {TopicPartition, ...}
        """
        return self._subscription.assigned_partitions()

    @asyncio.coroutine
    def stop(self):
        """ Close the consumer, while waiting for finilizers:

            * Commit last consumed message if autocommit enabled
            * Leave group if used Consumer Groups
        """
        if self._closed:
            return
        log.debug("Closing the KafkaConsumer.")
        self._closed = True
        if self._coordinator:
            yield from self._coordinator.close()
        if self._fetcher:
            yield from self._fetcher.close()
        yield from self._client.close()
        log.debug("The KafkaConsumer has closed.")

    @asyncio.coroutine
    def commit(self, offsets=None):
        """ Commit offsets to Kafka.

        This commits offsets only to Kafka. The offsets committed using this
        API will be used on the first fetch after every rebalance and also on
        startup. As such, if you need to store offsets in anything other than
        Kafka, this API should not be used.

        Currently only supports kafka-topic offset storage (not zookeeper)

        When explicitly passing ``offsets`` use either offset of next record,
        or tuple of offset and metadata::

            tp = TopicPartition(msg.topic, msg.partition)
            metadata = "Some utf-8 metadata"
            # Either
            await consumer.commit({tp: msg.offset + 1})
            # Or position directly
            await consumer.commit({tp: (msg.offset + 1, metadata)})

        .. note:: If you want `fire and forget` commit, like ``commit_async()``
            in *kafka-python*, just run it in a task. Something like::

                fut = loop.create_task(consumer.commit())
                fut.add_done_callback(on_commit_done)

        Arguments:
            offsets (dict, optional): {TopicPartition: (offset, metadata)} dict
                to commit with the configured ``group_id``. Defaults to current
                consumed offsets for all subscribed partitions.
        Raises:
            IllegalOperation: If used with ``group_id == None``.
            IllegalStateError: If partitions not assigned.
            ValueError: If offsets is of wrong format.
            CommitFailedError: If membership already changed on broker.
            KafkaError: If commit failed on broker side. This could be due to
                invalid offset, too long metadata, authorization failure, etc.

        .. versionchanged:: 0.4.0

            Changed ``AssertionError`` to ``IllegalStateError`` in case of
            unassigned partition.

        .. versionchanged:: 0.4.0

            Will now raise ``CommitFailedError`` in case membership changed,
            as (posibly) this partition is handled by another consumer.
        """
        if self._group_id is None:
            raise IllegalOperation("Requires group_id")

        subscription = self._subscription.subscription
        if subscription is None:
            raise IllegalStateError("Not subscribed to any topics")
        assignment = subscription.assignment
        if assignment is None:
            raise IllegalStateError("No partitions assigned")

        if offsets is None:
            offsets = assignment.all_consumed_offsets()
        else:
            # validate `offsets` structure
            if not offsets or not isinstance(offsets, dict):
                raise ValueError(offsets)

            formatted_offsets = {}
            for tp, offset_and_metadata in offsets.items():
                if not isinstance(tp, TopicPartition):
                    raise ValueError("Key should be TopicPartition instance")

                if tp not in assignment.tps:
                    raise IllegalStateError(
                        "Partition {} is not assigned".format(tp))

                if isinstance(offset_and_metadata, int):
                    offset, metadata = offset_and_metadata, ""
                else:
                    try:
                        offset, metadata = offset_and_metadata
                    except Exception:
                        raise ValueError(offsets)

                    if not isinstance(metadata, str):
                        raise ValueError("Metadata should be a string")

                formatted_offsets[tp] = OffsetAndMetadata(offset, metadata)

            offsets = formatted_offsets

        yield from self._coordinator.commit_offsets(assignment, offsets)

    @asyncio.coroutine
    def committed(self, partition):
        """ Get the last committed offset for the given partition. (whether the
        commit happened by this process or another).

        This offset will be used as the position for the consumer
        in the event of a failure.

        This call may block to do a remote call if the partition in question
        isn't assigned to this consumer or if the consumer hasn't yet
        initialized its cache of committed offsets.

        Arguments:
            partition (TopicPartition): the partition to check

        Returns:
            The last committed offset, or None if there was no prior commit.

        Raises:
            IllegalOperation: If used with ``group_id == None``
        """
        if self._group_id is None:
            raise IllegalOperation("Requires group_id")

        if self._subscription.is_assigned(partition):
            assignment = self._subscription.subscription.assignment
            tp_state = assignment.state_value(partition)
            if tp_state.committed is None:
                yield from tp_state.wait_for_committed()
            committed = tp_state.committed.offset

        else:
            commit_map = yield from self._coordinator.fetch_committed_offsets(
                [partition])
            if partition in commit_map:
                committed = commit_map[partition].offset
            else:
                committed = None
        if committed == -1:
            return None
        return committed

    @asyncio.coroutine
    def topics(self):
        """ Get all topics the user is authorized to view.

        Returns:
            set: topics
        """
        cluster = yield from self._client.fetch_all_metadata()
        return cluster.topics()

    def partitions_for_topic(self, topic):
        """ Get metadata about the partitions for a given topic.

        This method will return `None` if Consumer does not already have
        metadata for this topic.

        Arguments:
            topic (str): topic to check

        Returns:
            set: partition ids
        """
        return self._client.cluster.partitions_for_topic(topic)

    @asyncio.coroutine
    def position(self, partition):
        """ Get the offset of the *next record* that will be fetched (if a
        record with that offset exists on broker).

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int: offset

        Raises:
            IllegalStateError: partition is not assigned

        .. versionchanged:: 0.4.0

            Changed ``AssertionError`` to ``IllegalStateError`` in case of
            unassigned partition
        """
        while True:
            if not self._subscription.is_assigned(partition):
                raise IllegalStateError(
                    'Partition {} is not assigned'.format(partition))
            assignment = self._subscription.subscription.assignment
            tp_state = assignment.state_value(partition)
            if not tp_state.has_valid_position:
                yield from asyncio.wait(
                    [tp_state.wait_for_position(),
                     assignment.unassign_future],
                    return_when=asyncio.FIRST_COMPLETED, loop=self._loop,
                )
                if not tp_state.has_valid_position:
                    if self._subscription.subscription is None:
                        raise IllegalStateError(
                            'Partition {} is not assigned'.format(partition))
                    if self._subscription.subscription.assignment is None:
                        yield from self._subscription.wait_for_assignment()
                    continue
            return tp_state.position

    def highwater(self, partition):
        """ Last known highwater offset for a partition.

        A highwater offset is the offset that will be assigned to the next
        message that is produced. It may be useful for calculating lag, by
        comparing with the reported position. Note that both position and
        highwater refer to the *next* offset – i.e., highwater offset is one
        greater than the newest available message.

        Highwater offsets are returned as part of ``FetchResponse``, so will
        not be available if messages for this partition were not requested yet.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: offset if available
        """
        assert self._subscription.is_assigned(partition), \
            'Partition is not assigned'
        assignment = self._subscription.subscription.assignment
        return assignment.state_value(partition).highwater

    def seek(self, partition, offset):
        """ Manually specify the fetch offset for a TopicPartition.

        Overrides the fetch offsets that the consumer will use on the next
        ``getmany()``/``getone()`` call. If this API is invoked for the same
        partition more than once, the latest offset will be used on the next
        fetch.

        Note:
            You may lose data if this API is arbitrarily used in the middle
            of consumption to reset the fetch offsets. Use it either on
            rebalance listeners or after all pending messages are processed.

        Arguments:
            partition (TopicPartition): partition for seek operation
            offset (int): message offset in partition

        Raises:
            ValueError: if offset is not a positive integer
            IllegalStateError: partition is not currently assigned

        .. versionchanged:: 0.4.0

            Changed ``AssertionError`` to ``IllegalStateError`` and
            ``ValueError`` in respective cases.
        """
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("Offset must be a positive integer")
        log.debug("Seeking to offset %s for partition %s", offset, partition)
        self._fetcher.seek_to(partition, offset)

    @asyncio.coroutine
    def seek_to_beginning(self, *partitions):
        """ Seek to the oldest available offset for partitions.

        Arguments:
            *partitions: Optionally provide specific TopicPartitions, otherwise
                default to all assigned partitions.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            TypeError: If partitions are not instances of TopicPartition

        .. versionadded:: 0.3.0

        """
        if not all([isinstance(p, TopicPartition) for p in partitions]):
            raise TypeError('partitions must be TopicPartition instances')

        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, 'No partitions are currently assigned'
        else:
            not_assigned = (
                set(partitions) - self._subscription.assigned_partitions()
            )
            if not_assigned:
                raise IllegalStateError(
                    "Partitions {} are not assigned".format(not_assigned))

        for tp in partitions:
            log.debug("Seeking to beginning of partition %s", tp)
        yield from self._fetcher.request_offset_reset(
            partitions, OffsetResetStrategy.EARLIEST)

    @asyncio.coroutine
    def seek_to_end(self, *partitions):
        """Seek to the most recent available offset for partitions.

        Arguments:
            *partitions: Optionally provide specific TopicPartitions, otherwise
                default to all assigned partitions.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            TypeError: If partitions are not instances of TopicPartition

        .. versionadded:: 0.3.0

        """
        if not all([isinstance(p, TopicPartition) for p in partitions]):
            raise TypeError('partitions must be TopicPartition instances')

        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, 'No partitions are currently assigned'
        else:
            not_assigned = (
                set(partitions) - self._subscription.assigned_partitions()
            )
            if not_assigned:
                raise IllegalStateError(
                    "Partitions {} are not assigned".format(not_assigned))

        for tp in partitions:
            log.debug("Seeking to end of partition %s", tp)
        yield from self._fetcher.request_offset_reset(
            partitions, OffsetResetStrategy.LATEST)

    @asyncio.coroutine
    def seek_to_committed(self, *partitions):
        """ Seek to the committed offset for partitions.

        Arguments:
            *partitions: Optionally provide specific TopicPartitions, otherwise
                default to all assigned partitions.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            IllegalOperation: If used with ``group_id == None``

        .. versionchanged:: 0.3.0

            Changed ``AssertionError`` to ``IllegalStateError`` in case of
            unassigned partition
        """
        if not all([isinstance(p, TopicPartition) for p in partitions]):
            raise TypeError('partitions must be TopicPartition instances')

        if not partitions:
            partitions = self._subscription.assigned_partitions()
            assert partitions, 'No partitions are currently assigned'
        else:
            not_assigned = (
                set(partitions) - self._subscription.assigned_partitions()
            )
            if not_assigned:
                raise IllegalStateError(
                    "Partitions {} are not assigned".format(not_assigned))

        for tp in partitions:
            offset = yield from self.committed(tp)
            log.debug("Seeking to committed of partition %s %s", tp, offset)
            if offset and offset > 0:
                self._fetcher.seek_to(tp, offset)

    @asyncio.coroutine
    def offsets_for_times(self, timestamps):
        """
        Look up the offsets for the given partitions by timestamp. The returned
        offset for each partition is the earliest offset whose timestamp is
        greater than or equal to the given timestamp in the corresponding
        partition.

        The consumer does not have to be assigned the partitions.

        If the message format version in a partition is before 0.10.0, i.e.
        the messages do not have timestamps, ``None`` will be returned for that
        partition.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            timestamps (dict): ``{TopicPartition: int}`` mapping from partition
                to the timestamp to look up. Unit should be milliseconds since
                beginning of the epoch (midnight Jan 1, 1970 (UTC))

        Returns:
            dict: ``{TopicPartition: OffsetAndTimestamp}`` mapping from
            partition to the timestamp and offset of the first message with
            timestamp greater than or equal to the target timestamp.

        Raises:
            ValueError: If the target timestamp is negative
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in request_timeout_ms

        .. versionadded:: 0.3.0

        """
        if self._client.api_version <= (0, 10, 0):
            raise UnsupportedVersionError(
                "offsets_for_times API not supported for cluster version {}"
                .format(self._client.api_version))
        for tp, ts in timestamps.items():
            timestamps[tp] = int(ts)
            if ts < 0:
                raise ValueError(
                    "The target time for partition {} is {}. The target time "
                    "cannot be negative.".format(tp, ts))
        offsets = yield from self._fetcher.get_offsets_by_times(
            timestamps, self._request_timeout_ms)
        return offsets

    @asyncio.coroutine
    def beginning_offsets(self, partitions):
        """ Get the first offset for the given partitions.

        This method does not change the current consumer position of the
        partitions.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            partitions (list): List of TopicPartition instances to fetch
                offsets for.

        Returns:
            dict: ``{TopicPartition: int}`` mapping of partition to  earliest
            available offset.

        Raises:
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in request_timeout_ms.

        .. versionadded:: 0.3.0

        """
        if self._client.api_version <= (0, 10, 0):
            raise UnsupportedVersionError(
                "offsets_for_times API not supported for cluster version {}"
                .format(self._client.api_version))
        offsets = yield from self._fetcher.beginning_offsets(
            partitions, self._request_timeout_ms)
        return offsets

    @asyncio.coroutine
    def end_offsets(self, partitions):
        """ Get the last offset for the given partitions. The last offset of a
        partition is the offset of the upcoming message, i.e. the offset of the
        last available message + 1.

        This method does not change the current consumer position of the
        partitions.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            partitions (list): List of TopicPartition instances to fetch
                offsets for.

        Returns:
            dict: ``{TopicPartition: int}`` mapping of partition to last
            available offset + 1.

        Raises:
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in request_timeout_ms

        .. versionadded:: 0.3.0

        """
        if self._client.api_version <= (0, 10, 0):
            raise UnsupportedVersionError(
                "offsets_for_times API not supported for cluster version {}"
                .format(self._client.api_version))
        offsets = yield from self._fetcher.end_offsets(
            partitions, self._request_timeout_ms)
        return offsets

    def subscribe(self, topics=(), pattern=None, listener=None):
        """ Subscribe to a list of topics, or a topic regex pattern.

        Partitions will be dynamically assigned via a group coordinator.
        Topic subscriptions are not incremental: this list will replace the
        current assignment (if there is one).

        This method is incompatible with ``assign()``.

        Arguments:
           topics (list): List of topics for subscription.
           pattern (str): Pattern to match available topics. You must provide
               either topics or pattern, but not both.
           listener (ConsumerRebalanceListener): Optionally include listener
               callback, which will be called before and after each rebalance
               operation.
               As part of group management, the consumer will keep track of
               the list of consumers that belong to a particular group and
               will trigger a rebalance operation if one of the following
               events trigger:

               * Number of partitions change for any of the subscribed topics
               * Topic is created or deleted
               * An existing member of the consumer group dies
               * A new member is added to the consumer group

               When any of these events are triggered, the provided listener
               will be invoked first to indicate that the consumer's
               assignment has been revoked, and then again when the new
               assignment has been received. Note that this listener will
               immediately override any listener set in a previous call
               to subscribe. It is guaranteed, however, that the partitions
               revoked/assigned
               through this interface are from topics subscribed in this call.
        Raises:
            IllegalStateError: if called after previously calling assign()
            ValueError: if neither topics or pattern is provided or both
               are provided
            TypeError: if listener is not a ConsumerRebalanceListener
        """
        if not (topics or pattern):
            raise ValueError(
                "You should provide either `topics` or `pattern`")
        if topics and pattern:
            raise ValueError(
                "You can't provide both `topics` and `pattern`")
        if listener is not None and \
                not isinstance(listener, ConsumerRebalanceListener):
            raise TypeError(
                "listener should be an instance of ConsumerRebalanceListener")
        if pattern is not None:
            try:
                pattern = re.compile(pattern)
            except re.error as err:
                raise ValueError(
                    "{!r} is not a valid pattern: {}".format(pattern, err))
            self._subscription.subscribe_pattern(
                pattern=pattern, listener=listener)
            # NOTE: set_topics will trigger a rebalance, so the coordinator
            # will get the initial subscription shortly by ``metadata_changed``
            # handler.
            self._client.set_topics([])
            log.info("Subscribed to topic pattern: %s", pattern)
        elif topics:
            topics = self._validate_topics(topics)
            self._subscription.subscribe(
                topics=topics, listener=listener)
            self._client.set_topics(self._subscription.subscription.topics)
            log.info("Subscribed to topic(s): %s", topics)

    def subscription(self):
        """ Get the current topic subscription.

        Returns:
            frozenset: {topic, ...}
        """
        return self._subscription.subscription.topics

    def unsubscribe(self):
        """ Unsubscribe from all topics and clear all assigned partitions. """
        self._subscription.unsubscribe()
        if self._group_id is not None:
            self._coordinator.maybe_leave_group()
        self._client.set_topics([])
        log.info(
            "Unsubscribed all topics or patterns and assigned partitions")

    @asyncio.coroutine
    def getone(self, *partitions):
        """
        Get one message from Kafka.
        If no new messages prefetched, this method will wait for it.

        Arguments:
            partitions (List[TopicPartition]): Optional list of partitions to
                return from. If no partitions specified then returned message
                will be from any partition, which consumer is subscribed to.

        Returns:
            ConsumerRecord

        Will return instance of

        .. code:: python

            collections.namedtuple(
                "ConsumerRecord",
                ["topic", "partition", "offset", "key", "value"])

        Example usage:


        .. code:: python

            while True:
                message = await consumer.getone()
                topic = message.topic
                partition = message.partition
                # Process message
                print(message.offset, message.key, message.value)

        """
        assert all(map(lambda k: isinstance(k, TopicPartition), partitions))
        if self._closed:
            raise ConsumerStoppedError()

        msg = yield from self._fetcher.next_record(partitions)
        return msg

    @asyncio.coroutine
    def getmany(self, *partitions, timeout_ms=0, max_records=None):
        """Get messages from assigned topics / partitions.

        Prefetched messages are returned in batches by topic-partition.
        If messages is not available in the prefetched buffer this method waits
        `timeout_ms` milliseconds.

        Arguments:
            partitions (List[TopicPartition]): The partitions that need
                fetching message. If no one partition specified then all
                subscribed partitions will be used
            timeout_ms (int, optional): milliseconds spent waiting if
                data is not available in the buffer. If 0, returns immediately
                with any records that are available currently in the buffer,
                else returns empty. Must not be negative. Default: 0
        Returns:
            dict: topic to list of records since the last fetch for the
                subscribed list of topics and partitions

        Example usage:


        .. code:: python

            data = await consumer.getmany()
            for tp, messages in data.items():
                topic = tp.topic
                partition = tp.partition
                for message in messages:
                    # Process message
                    print(message.offset, message.key, message.value)

        """
        assert all(map(lambda k: isinstance(k, TopicPartition), partitions))
        if self._closed:
            raise ConsumerStoppedError()

        if max_records is not None and (
                not isinstance(max_records, int) or max_records < 1):
            raise ValueError("`max_records` must be a positive Integer")

        timeout = timeout_ms / 1000
        records = yield from self._fetcher.fetched_records(
            partitions, timeout,
            max_records=max_records or self._max_poll_records)
        return records

    if PY_35:
        @asyncio.coroutine
        def __aiter__(self):
            if self._closed:
                raise ConsumerStoppedError()
            return self

        @asyncio.coroutine
        def __anext__(self):
            """Asyncio iterator interface for consumer

            Note:
                TopicAuthorizationFailedError and OffsetOutOfRangeError
                exceptions can be raised in iterator.
                All other KafkaError exceptions will be logged and not raised
            """
            while True:
                try:
                    return (yield from self.getone())
                except ConsumerStoppedError:
                    raise StopAsyncIteration  # noqa: F821
                except (TopicAuthorizationFailedError,
                        OffsetOutOfRangeError,
                        NoOffsetForPartitionError) as err:
                    raise err
                except (RecordTooLargeError,
                        CorruptRecordException):
                    log.exception("error in consumer iterator: %s")
