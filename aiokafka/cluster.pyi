from concurrent.futures import Future
from typing import Any, Callable, Optional, Sequence, Set, TypedDict, Union
from aiokafka.client import CoordinationType
from aiokafka.protocol.commit import (
    GroupCoordinatorResponse_v0,
    GroupCoordinatorResponse_v1,
)
from aiokafka.protocol.metadata import (
    MetadataResponse_v0,
    MetadataResponse_v1,
    MetadataResponse_v2,
    MetadataResponse_v3,
    MetadataResponse_v4,
    MetadataResponse_v5,
)
from aiokafka.structs import BrokerMetadata, PartitionMetadata, TopicPartition

log = ...
MetadataResponse = Union[
    MetadataResponse_v0,
    MetadataResponse_v1,
    MetadataResponse_v2,
    MetadataResponse_v3,
    MetadataResponse_v4,
    MetadataResponse_v5,
]
GroupCoordinatorResponse = Union[
    GroupCoordinatorResponse_v0, GroupCoordinatorResponse_v1
]

class ClusterConfig(TypedDict):
    retry_backoff_ms: int
    metadata_max_age_ms: int
    bootstrap_servers: str | list[str]
    ...

class ClusterMetadata:
    """
    A class to manage kafka cluster metadata.

    This class does not perform any IO. It simply updates internal state
    given API responses (MetadataResponse, GroupCoordinatorResponse).

    Keyword Arguments:
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the client should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
    """

    DEFAULT_CONFIG: ClusterConfig = ...
    def __init__(self, **configs: int | str | list[str]) -> None: ...
    def is_bootstrap(self, node_id: str) -> bool: ...
    def brokers(self) -> set[BrokerMetadata]:
        """Get all BrokerMetadata

        Returns:
            set: {BrokerMetadata, ...}
        """
        ...

    def broker_metadata(self, broker_id: str) -> BrokerMetadata | None:
        """Get BrokerMetadata

        Arguments:
            broker_id (str): node_id for a broker to check

        Returns:
            BrokerMetadata or None if not found
        """
        ...

    def partitions_for_topic(self, topic: str) -> Optional[Set[int]]:
        """Return set of all partitions for topic (whether available or not)

        Arguments:
            topic (str): topic to check for partitions

        Returns:
            set: {partition (int), ...}
        """
        ...

    def available_partitions_for_topic(self, topic: str) -> Optional[Set[int]]:
        """Return set of partitions with known leaders

        Arguments:
            topic (str): topic to check for partitions

        Returns:
            set: {partition (int), ...}
            None if topic not found.
        """
        ...

    def leader_for_partition(self, partition: PartitionMetadata) -> int | None:
        """Return node_id of leader, -1 unavailable, None if unknown."""
        ...

    def partitions_for_broker(self, broker_id: int | str) -> set[TopicPartition] | None:
        """Return TopicPartitions for which the broker is a leader.

        Arguments:
            broker_id (int): node id for a broker

        Returns:
            set: {TopicPartition, ...}
            None if the broker either has no partitions or does not exist.
        """
        ...

    def coordinator_for_group(self, group: str) -> int | str | None:
        """Return node_id of group coordinator.

        Arguments:
            group (str): name of consumer group

        Returns:
            int: node_id for group coordinator
            None if the group does not exist.
        """
        ...

    def request_update(self) -> Future[ClusterMetadata]:
        """Flags metadata for update, return Future()

        Actual update must be handled separately. This method will only
        change the reported ttl()

        Returns:
            Future (value will be the cluster object after update)
        """
        ...

    def topics(self, exclude_internal_topics: bool = ...) -> set[str]:
        """Get set of known topics.

        Arguments:
            exclude_internal_topics (bool): Whether records from internal topics
                (such as offsets) should be exposed to the consumer. If set to
                True the only way to receive records from an internal topic is
                subscribing to it. Default True

        Returns:
            set: {topic (str), ...}
        """
        ...

    def failed_update(self, exception: BaseException) -> None:
        """Update cluster state given a failed MetadataRequest."""
        ...

    def update_metadata(self, metadata: MetadataResponse) -> None:
        """Update cluster state given a MetadataResponse.

        Arguments:
            metadata (MetadataResponse): broker response to a metadata request

        Returns: None
        """
        ...

    def add_listener(self, listener: Callable[[ClusterMetadata], Any]) -> None:
        """Add a callback function to be called on each metadata update"""
        ...

    def remove_listener(self, listener: Callable[[ClusterMetadata], Any]) -> None:
        """Remove a previously added listener callback"""
        ...

    def add_group_coordinator(
        self, group: str, response: GroupCoordinatorResponse
    ) -> str | None:
        """Update with metadata for a group coordinator

        Arguments:
            group (str): name of group from GroupCoordinatorRequest
            response (GroupCoordinatorResponse): broker response

        Returns:
            string: coordinator node_id if metadata is updated, None on error
        """
        ...

    def with_partitions(
        self, partitions_to_add: Sequence[PartitionMetadata]
    ) -> ClusterMetadata:
        """Returns a copy of cluster metadata with partitions added"""
        ...

    def coordinator_metadata(self, node_id: int | str) -> BrokerMetadata | None: ...
    def add_coordinator(
        self,
        node_id: int | str,
        host: str,
        port: int,
        rack: str | None = ...,
        *,
        purpose: tuple[CoordinationType, str],
    ) -> None:
        """Keep track of all coordinator nodes separately and remove them if
        a new one was elected for the same purpose (For example group
        coordinator for group X).
        """
        ...

    def __str__(self) -> str: ...
