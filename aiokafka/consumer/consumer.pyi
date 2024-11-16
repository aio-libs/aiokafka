import asyncio
from ssl import SSLContext
from types import ModuleType, TracebackType
from typing import Callable, Dict, Generic, List, Literal, TypeVar
from aiokafka.abc import AbstractTokenProvider, ConsumerRebalanceListener
from aiokafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from aiokafka.structs import (
    ConsumerRecord,
    OffsetAndMetadata,
    OffsetAndTimestamp,
    TopicPartition,
)

log = ...
KT = TypeVar("KT", covariant=True)
VT = TypeVar("VT", covariant=True)
ET = TypeVar("ET", bound=BaseException)

class AIOKafkaConsumer(Generic[KT, VT]):
    """
    A client that consumes records from a Kafka cluster.

    The consumer will transparently handle the failure of servers in the Kafka
    cluster, and adapt as topic-partitions are created or migrate between
    brokers.

    It also interacts with the assigned Kafka Group Coordinator node to allow
    multiple consumers to load balance consumption of topics (feature of Kafka
    >= 0.9.0.0).

    .. _kip-62:
        https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread

    Arguments:
        *topics (tuple(str)): optional list of topics to subscribe to. If not set,
            call :meth:`.subscribe` or :meth:`.assign` before consuming records.
            Passing topics directly is same as calling :meth:`.subscribe` API.
        bootstrap_servers (str, list(str)): a ``host[:port]`` string (or list of
            ``host[:port]`` strings) that the consumer should contact to bootstrap
            initial cluster metadata.

            This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to ``localhost:9092``.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to :class:`~.consumer.group_coordinator.GroupCoordinator`
            for logging with respect to consumer group administration. Default:
            ``aiokafka-{version}``
        group_id (str or None): name of the consumer group to join for dynamic
            partition assignment (if enabled), and to use for fetching and
            committing offsets. If None, auto-partition assignment (via
            group coordinator) and offset commits are disabled.
            Default: None
        group_instance_id (str or None): name of the group instance ID used for
            static membership (KIP-345)
        key_deserializer (Callable): Any callable that takes a
            raw message key and returns a deserialized key.
        value_deserializer (Callable, Optional): Any callable that takes a
            raw message value and returns a deserialized value.
        fetch_min_bytes (int): Minimum amount of data the server should
            return for a fetch request, otherwise wait up to
            `fetch_max_wait_ms` for more data to accumulate. Default: 1.
        fetch_max_bytes (int): The maximum amount of data the server should
            return for a fetch request. This is not an absolute maximum, if
            the first message in the first non-empty partition of the fetch
            is larger than this value, the message will still be returned
            to ensure that the consumer can make progress. NOTE: consumer
            performs fetches to multiple brokers in parallel so memory
            usage will depend on the number of brokers containing
            partitions for the topic.
            Supported Kafka version >= 0.10.1.0. Default: 52428800 (50 Mb).
        fetch_max_wait_ms (int): The maximum amount of time in milliseconds
            the server will block before answering the fetch request if
            there isn't sufficient data to immediately satisfy the
            requirement given by fetch_min_bytes. Default: 500.
        max_partition_fetch_bytes (int): The maximum amount of data
            per-partition the server will return. The maximum total memory
            used for a request ``= #partitions * max_partition_fetch_bytes``.
            This size must be at least as large as the maximum message size
            the server allows or else it is possible for the producer to
            send messages larger than the consumer can fetch. If that
            happens, the consumer can get stuck trying to fetch a large
            message on a certain partition. Default: 1048576.
        max_poll_records (int or None): The maximum number of records
            returned in a single call to :meth:`.getmany`.
            Defaults ``None``, no limit.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 40000.
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        auto_offset_reset (str): A policy for resetting offsets on
            :exc:`.OffsetOutOfRangeError` errors: ``earliest`` will move to the oldest
            available message, ``latest`` will move to the most recent, and
            ``none`` will raise an exception so you can handle this case.
            Default: ``latest``.
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
        partition_assignment_strategy (list or tuple): List of objects to use to
            distribute partition ownership amongst consumer instances when
            group management is used. This preference is implicit in the order
            of the strategies in the list. When assignment strategy changes:
            to support a change to the assignment strategy, new versions must
            enable support both for the old assignment strategy and the new
            one. The coordinator will choose the old assignment strategy until
            all members have been updated. Then it will choose the new
            strategy. Default: [:class:`.RoundRobinPartitionAssignor`]

        max_poll_interval_ms (int): Maximum allowed time between calls to
            consume messages (e.g., :meth:`.getmany`). If this interval
            is exceeded the consumer is considered failed and the group will
            rebalance in order to reassign the partitions to another consumer
            group member. If API methods block waiting for messages, that time
            does not count against this timeout. See `KIP-62`_ for more
            information. Default 300000
        rebalance_timeout_ms (int): The maximum time server will wait for this
            consumer to rejoin the group in a case of rebalance. In Java client
            this behaviour is bound to `max.poll.interval.ms` configuration,
            but as ``aiokafka`` will rejoin the group in the background, we
            decouple this setting to allow finer tuning by users that use
            :class:`.ConsumerRebalanceListener` to delay rebalacing. Defaults
            to ``session_timeout_ms``
        session_timeout_ms (int): Client group session and failure detection
            timeout. The consumer sends periodic heartbeats
            (`heartbeat.interval.ms`) to indicate its liveness to the broker.
            If no hearts are received by the broker for a group member within
            the session timeout, the broker will remove the consumer from the
            group and trigger a rebalance. The allowed range is configured with
            the **broker** configuration properties
            `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
            Default: 10000
        heartbeat_interval_ms (int): The expected time in milliseconds
            between heartbeats to the consumer coordinator when using
            Kafka's group management feature. Heartbeats are used to ensure
            that the consumer's session stays active and to facilitate
            rebalancing when new consumers join or leave the group. The
            value must be set lower than `session_timeout_ms`, but typically
            should be set no higher than 1/3 of that value. It can be
            adjusted even lower to control the expected time for normal
            rebalances. Default: 3000

        consumer_timeout_ms (int): maximum wait timeout for background fetching
            routine. Mostly defines how fast the system will see rebalance and
            request new data for new partitions. Default: 200
        api_version (str): specify which kafka API version to use.
            :class:`AIOKafkaConsumer` supports Kafka API versions >=0.9 only.
            If set to ``auto``, will attempt to infer the broker version by
            probing various APIs. Default: ``auto``
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: ``PLAINTEXT``, ``SSL``, ``SASL_PLAINTEXT``,
            ``SASL_SSL``. Default: ``PLAINTEXT``.
        ssl_context (ssl.SSLContext): pre-configured :class:`~ssl.SSLContext`
            for wrapping socket connections. Directly passed into asyncio's
            :meth:`~asyncio.loop.create_connection`. For more information see
            :ref:`ssl_auth`. Default: None.
        exclude_internal_topics (bool): Whether records from internal topics
            (such as offsets) should be exposed to the consumer. If set to True
            the only way to receive records from an internal topic is
            subscribing to it. Requires 0.10+ Default: True
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks. Default: 540000 (9 minutes).
        isolation_level (str): Controls how to read messages written
            transactionally.

            If set to ``read_committed``, :meth:`.getmany` will only return
            transactional messages which have been committed.
            If set to ``read_uncommitted`` (the default), :meth:`.getmany` will
            return all messages, even transactional messages which have been
            aborted.

            Non-transactional messages will be returned unconditionally in
            either mode.

            Messages will always be returned in offset order. Hence, in
            `read_committed` mode, :meth:`.getmany` will only return
            messages up to the last stable offset (LSO), which is the one less
            than the offset of the first open transaction. In particular any
            messages appearing after messages belonging to ongoing transactions
            will be withheld until the relevant transaction has been completed.
            As a result, `read_committed` consumers will not be able to read up
            to the high watermark when there are in flight transactions.
            Further, when in `read_committed` the seek_to_end method will
            return the LSO. See method docs below. Default: ``read_uncommitted``

        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for ``SASL_PLAINTEXT`` or ``SASL_SSL``. Valid values are:
            ``PLAIN``, ``GSSAPI``, ``SCRAM-SHA-256``, ``SCRAM-SHA-512``,
            ``OAUTHBEARER``.
            Default: ``PLAIN``
        sasl_plain_username (str or None): username for SASL ``PLAIN`` authentication.
            Default: None
        sasl_plain_password (str or None): password for SASL ``PLAIN`` authentication.
            Default: None
        sasl_oauth_token_provider (~aiokafka.abc.AbstractTokenProvider or None):
            OAuthBearer token provider instance.
            Default: None

    Note:
        Many configuration parameters are taken from Java Client:
        https://kafka.apache.org/documentation.html#newconsumerconfigs

    """

    _closed = ...
    _source_traceback = ...
    def __init__(
        self,
        *topics: str,
        loop: asyncio.AbstractEventLoop | None = ...,
        bootstrap_servers: str | list[str] = ...,
        client_id: str = ...,
        group_id: str | None = ...,
        group_instance_id: str | None = ...,
        key_deserializer: Callable[[bytes], KT] = lambda x: x,
        value_deserializer: Callable[[bytes], VT] = lambda x: x,
        fetch_max_wait_ms: int = ...,
        fetch_max_bytes: int = ...,
        fetch_min_bytes: int = ...,
        max_partition_fetch_bytes: int = ...,
        request_timeout_ms: int = ...,
        retry_backoff_ms: int = ...,
        auto_offset_reset: (
            Literal["earliest"] | Literal["latest"] | Literal["none"]
        ) = ...,
        enable_auto_commit: bool = ...,
        auto_commit_interval_ms: int = ...,
        check_crcs: bool = ...,
        metadata_max_age_ms: int = ...,
        partition_assignment_strategy: tuple[
            type[AbstractPartitionAssignor], ...
        ] = ...,
        max_poll_interval_ms: int = ...,
        rebalance_timeout_ms: int | None = ...,
        session_timeout_ms: int = ...,
        heartbeat_interval_ms: int = ...,
        consumer_timeout_ms: int = ...,
        max_poll_records: int | None = ...,
        ssl_context: SSLContext | None = ...,
        security_protocol: (
            Literal["PLAINTEXT"]
            | Literal["SSL"]
            | Literal["SASL_PLAINTEXT"]
            | Literal["SASL_SSL"]
        ) = ...,
        api_version: str = ...,
        exclude_internal_topics: bool = ...,
        connections_max_idle_ms: int = ...,
        isolation_level: Literal["read_committed"] | Literal["read_uncommitted"] = ...,
        sasl_mechanism: (
            Literal["PLAIN"]
            | Literal["GSSAPI"]
            | Literal["SCRAM-SHA-256"]
            | Literal["SCRAM-SHA-512"]
            | Literal["OAUTHBEARER"]
        ) = ...,
        sasl_plain_password: str | None = ...,
        sasl_plain_username: str | None = ...,
        sasl_kerberos_service_name: str = ...,
        sasl_kerberos_domain_name: str | None = ...,
        sasl_oauth_token_provider: AbstractTokenProvider | None = ...,
    ) -> None: ...
    def __del__(self, _warnings: ModuleType = ...) -> None: ...
    async def start(self) -> None:
        """Connect to Kafka cluster. This will:

        * Load metadata for all cluster nodes and partition allocation
        * Wait for possible topic autocreation
        * Join group if ``group_id`` provided
        """
        ...

    def assign(self, partitions: list[TopicPartition]) -> None:
        """Manually assign a list of :class:`.TopicPartition` to this consumer.

        This interface does not support incremental assignment and will
        replace the previous assignment (if there was one).

        Arguments:
            partitions (list(TopicPartition)): assignment for this instance.

        Raises:
            IllegalStateError: if consumer has already called :meth:`subscribe`

        Warning:
            It is not possible to use both manual partition assignment with
            :meth:`assign` and group assignment with :meth:`subscribe`.

        Note:
            Manual topic assignment through this method does not use the
            consumer's group management functionality. As such, there will be
            **no rebalance operation triggered** when group membership or
            cluster and topic metadata change.
        """
        ...

    def assignment(self) -> set[TopicPartition]:
        """Get the set of partitions currently assigned to this consumer.

        If partitions were directly assigned using :meth:`assign`, then this will
        simply return the same partitions that were previously assigned.

        If topics were subscribed using :meth:`subscribe`, then this will give
        the set of topic partitions currently assigned to the consumer (which
        may be empty if the assignment hasn't happened yet or if the partitions
        are in the process of being reassigned).

        Returns:
            set(TopicPartition): the set of partitions currently assigned to
            this consumer
        """
        ...

    async def stop(self) -> None:
        """Close the consumer, while waiting for finalizers:

        * Commit last consumed message if autocommit enabled
        * Leave group if used Consumer Groups
        """
        ...

    async def commit(
        self,
        offsets: (
            dict[TopicPartition, int | tuple[int, str] | OffsetAndMetadata] | None
        ) = ...,
    ) -> None:
        """Commit offsets to Kafka.

        This commits offsets only to Kafka. The offsets committed using this
        API will be used on the first fetch after every rebalance and also on
        startup. As such, if you need to store offsets in anything other than
        Kafka, this API should not be used.

        Currently only supports kafka-topic offset storage (not Zookeeper)

        When explicitly passing `offsets` use either offset of next record,
        or tuple of offset and metadata::

            tp = TopicPartition(msg.topic, msg.partition)
            metadata = "Some utf-8 metadata"
            # Either
            await consumer.commit({tp: msg.offset + 1})
            # Or position directly
            await consumer.commit({tp: (msg.offset + 1, metadata)})

        .. note:: If you want *fire and forget* commit, like
            :meth:`~kafka.KafkaConsumer.commit_async` in `kafka-python`_, just
            run it in a task. Something like::

                fut = loop.create_task(consumer.commit())
                fut.add_done_callback(on_commit_done)

        Arguments:
            offsets (dict, Optional): A mapping from :class:`.TopicPartition` to
              ``(offset, metadata)`` to commit with the configured ``group_id``.
              Defaults to current consumed offsets for all subscribed partitions.
        Raises:
            ~aiokafka.errors.CommitFailedError: If membership already changed on broker.
            ~aiokafka.errors.IllegalOperation: If used with ``group_id == None``.
            ~aiokafka.errors.IllegalStateError: If partitions not assigned.
            ~aiokafka.errors.KafkaError: If commit failed on broker side. This
                could be due to invalid offset, too long metadata, authorization
                failure, etc.
            ValueError: If offsets is of wrong format.

        .. versionchanged:: 0.4.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` in case of unassigned
            partition.

        .. versionchanged:: 0.4.0

            Will now raise :exc:`~aiokafka.errors.CommitFailedError` in case
            membership changed, as (possibly) this partition is handled by
            another consumer.

        .. _kafka-python: https://github.com/dpkp/kafka-python
        """
        ...

    async def committed(self, partition: TopicPartition) -> int | None:
        """Get the last committed offset for the given partition. (whether the
        commit happened by this process or another).

        This offset will be used as the position for the consumer in the event
        of a failure.

        This call will block to do a remote call to get the latest offset, as
        those are not cached by consumer (Transactional Producer can change
        them without Consumer knowledge as of Kafka 0.11.0)

        Arguments:
            partition (TopicPartition): the partition to check

        Returns:
            The last committed offset, or None if there was no prior commit.

        Raises:
            IllegalOperation: If used with ``group_id == None``
        """
        ...

    async def topics(self) -> set[str]:
        """Get all topics the user is authorized to view.

        Returns:
            set: topics
        """
        ...

    def partitions_for_topic(self, topic: str) -> set[int] | None:
        """Get metadata about the partitions for a given topic.

        This method will return `None` if Consumer does not already have
        metadata for this topic.

        Arguments:
            topic (str): topic to check

        Returns:
            set: partition ids
        """
        ...

    async def position(self, partition: TopicPartition) -> int:
        """Get the offset of the *next record* that will be fetched (if a
        record with that offset exists on broker).

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int: offset

        Raises:
            IllegalStateError: partition is not assigned

        .. versionchanged:: 0.4.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` in case of unassigned
            partition
        """
        ...

    def highwater(self, partition: TopicPartition) -> int | None:
        """Last known highwater offset for a partition.

        A highwater offset is the offset that will be assigned to the next
        message that is produced. It may be useful for calculating lag, by
        comparing with the reported position. Note that both position and
        highwater refer to the *next* offset - i.e., highwater offset is one
        greater than the newest available message.

        Highwater offsets are returned as part of ``FetchResponse``, so will
        not be available if messages for this partition were not requested yet.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: offset if available
        """
        ...

    def last_stable_offset(self, partition: TopicPartition) -> int | None:
        """Returns the Last Stable Offset of a topic. It will be the last
        offset up to which point all transactions were completed. Only
        available in with isolation_level `read_committed`, in
        `read_uncommitted` will always return -1. Will return None for older
        Brokers.

        As with :meth:`highwater` will not be available until some messages are
        consumed.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: offset if available
        """
        ...

    def last_poll_timestamp(self, partition: TopicPartition) -> int | None:
        """Returns the timestamp of the last poll of this partition (in ms).
        It is the last time :meth:`highwater` and :meth:`last_stable_offset` were
        updated. However it does not mean that new messages were received.

        As with :meth:`highwater` will not be available until some messages are
        consumed.

        Arguments:
            partition (TopicPartition): partition to check

        Returns:
            int or None: timestamp if available
        """
        ...

    def seek(self, partition: TopicPartition, offset: int) -> None:
        """Manually specify the fetch offset for a :class:`.TopicPartition`.

        Overrides the fetch offsets that the consumer will use on the next
        :meth:`getmany`/:meth:`getone` call. If this API is invoked for the same
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

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` and :exc:`ValueError` in
            respective cases.
        """
        ...

    async def seek_to_beginning(self, *partitions: TopicPartition) -> bool:
        """Seek to the oldest available offset for partitions.

        Arguments:
            *partitions: Optionally provide specific :class:`.TopicPartition`,
                otherwise default to all assigned partitions.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            TypeError: If partitions are not instances of :class:`.TopicPartition`

        .. versionadded:: 0.3.0

        """
        ...

    async def seek_to_end(self, *partitions: TopicPartition) -> bool:
        """Seek to the most recent available offset for partitions.

        Arguments:
            *partitions: Optionally provide specific :class:`.TopicPartition`,
                otherwise default to all assigned partitions.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            TypeError: If partitions are not instances of :class:`.TopicPartition`

        .. versionadded:: 0.3.0

        """
        ...

    async def seek_to_committed(
        self, *partitions: TopicPartition
    ) -> dict[TopicPartition, int | None]:
        """Seek to the committed offset for partitions.

        Arguments:
            *partitions: Optionally provide specific :class:`.TopicPartition`,
                otherwise default to all assigned partitions.

        Returns:
            dict(TopicPartition, int): mapping
            of the currently committed offsets.

        Raises:
            IllegalStateError: If any partition is not currently assigned
            IllegalOperation: If used with ``group_id == None``

        .. versionchanged:: 0.3.0

            Changed :exc:`AssertionError` to
            :exc:`~aiokafka.errors.IllegalStateError` in case of unassigned
            partition
        """
        ...

    async def offsets_for_times(
        self, timestamps: dict[TopicPartition, int]
    ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
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
            timestamps (dict(TopicPartition, int)): mapping from partition
                to the timestamp to look up. Unit should be milliseconds since
                beginning of the epoch (midnight Jan 1, 1970 (UTC))

        Returns:
            dict(TopicPartition, OffsetAndTimestamp or None): mapping from
            partition to the timestamp and offset of the first message with
            timestamp greater than or equal to the target timestamp.

        Raises:
            ValueError: If the target timestamp is negative
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in `request_timeout_ms`

        .. versionadded:: 0.3.0

        """
        ...

    async def beginning_offsets(
        self, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, int]:
        """Get the first offset for the given partitions.

        This method does not change the current consumer position of the
        partitions.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            partitions (list[TopicPartition]): List of :class:`.TopicPartition`
                instances to fetch offsets for.

        Returns:
            dict [TopicPartition, int]: mapping of partition to  earliest
            available offset.

        Raises:
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in `request_timeout_ms`.

        .. versionadded:: 0.3.0

        """
        ...

    async def end_offsets(
        self, partitions: list[TopicPartition]
    ) -> dict[TopicPartition, int]:
        """Get the last offset for the given partitions. The last offset of a
        partition is the offset of the upcoming message, i.e. the offset of the
        last available message + 1.

        This method does not change the current consumer position of the
        partitions.

        Note:
            This method may block indefinitely if the partition does not exist.

        Arguments:
            partitions (list[TopicPartition]): List of :class:`.TopicPartition`
                instances to fetch offsets for.

        Returns:
            dict [TopicPartition, int]: mapping of partition to last
            available offset + 1.

        Raises:
            UnsupportedVersionError: If the broker does not support looking
                up the offsets by timestamp.
            KafkaTimeoutError: If fetch failed in ``request_timeout_ms``

        .. versionadded:: 0.3.0

        """
        ...

    def subscribe(
        self,
        topics: list[str] | tuple[str, ...] = ...,
        pattern: str | None = ...,
        listener: ConsumerRebalanceListener | None = ...,
    ) -> None:
        """Subscribe to a list of topics, or a topic regex pattern.

        Partitions will be dynamically assigned via a group coordinator.
        Topic subscriptions are not incremental: this list will replace the
        current assignment (if there is one).

        This method is incompatible with :meth:`assign`.

        Arguments:
           topics (list or tuple): List of topics for subscription.
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
            IllegalStateError: if called after previously calling :meth:`assign`
            ValueError: if neither topics or pattern is provided or both
               are provided
            TypeError: if listener is not a :class:`.ConsumerRebalanceListener`
        """
        ...

    def subscription(self) -> set[str]:
        """Get the current topics subscription.

        Returns:
            set(str): a set of topics
        """
        ...

    def unsubscribe(self) -> None:
        """Unsubscribe from all topics and clear all assigned partitions."""
        ...

    async def getone(self, *partitions: TopicPartition) -> ConsumerRecord[KT, VT]:
        """
        Get one message from Kafka.
        If no new messages prefetched, this method will wait for it.

        Arguments:
            partitions (list(TopicPartition)): Optional list of partitions to
                return from. If no partitions specified then returned message
                will be from any partition, which consumer is subscribed to.

        Returns:
            ~aiokafka.structs.ConsumerRecord: the message

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
        ...

    async def getmany(
        self,
        *partitions: TopicPartition,
        timeout_ms: int = ...,
        max_records: int | None = ...,
    ) -> Dict[TopicPartition, List[ConsumerRecord[KT, VT]]]:
        """Get messages from assigned topics / partitions.

        Prefetched messages are returned in batches by topic-partition.
        If messages is not available in the prefetched buffer this method waits
        `timeout_ms` milliseconds.

        Arguments:
            partitions (list[TopicPartition]): The partitions that need
                fetching message. If no one partition specified then all
                subscribed partitions will be used
            timeout_ms (int, Optional): milliseconds spent waiting if
                data is not available in the buffer. If 0, returns immediately
                with any records that are available currently in the buffer,
                else returns empty. Must not be negative. Default: 0
        Returns:
            dict(TopicPartition, list[ConsumerRecord]): topic to list of
            records since the last fetch for the subscribed list of topics and
            partitions

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
        ...

    def pause(self, *partitions: TopicPartition) -> None:
        """Suspend fetching from the requested partitions.

        Future calls to :meth:`.getmany` will not return any records from these
        partitions until they have been resumed using :meth:`.resume`.

        Note: This method does not affect partition subscription.
        In particular, it does not cause a group rebalance when automatic
        assignment is used.

        Arguments:
            *partitions (list[TopicPartition]): Partitions to pause.
        """
        ...

    def paused(self) -> set[TopicPartition]:
        """Get the partitions that were previously paused using
        :meth:`.pause`.

        Returns:
            set[TopicPartition]: partitions
        """
        ...

    def resume(self, *partitions: TopicPartition) -> None:
        """Resume fetching from the specified (paused) partitions.

        Arguments:
            *partitions (tuple[TopicPartition,...]): Partitions to resume.
        """
        ...

    def __aiter__(self) -> AIOKafkaConsumer[KT, VT]: ...
    async def __anext__(self) -> ConsumerRecord[KT, VT]:
        """Asyncio iterator interface for consumer

        Note:
            TopicAuthorizationFailedError and OffsetOutOfRangeError
            exceptions can be raised in iterator.
            All other KafkaError exceptions will be logged and not raised
        """
        ...

    async def __aenter__(self) -> AIOKafkaConsumer[KT, VT]: ...
    async def __aexit__(
        self, exc_type: type[ET] | None, exc: ET | None, tb: TracebackType | None
    ) -> None: ...
