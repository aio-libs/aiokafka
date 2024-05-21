import contextlib
import logging
from collections import defaultdict
from copy import deepcopy
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Sized,
    Tuple,
)

from aiokafka.cluster import ClusterMetadata
from aiokafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from aiokafka.coordinator.assignors.sticky.partition_movements import PartitionMovements
from aiokafka.coordinator.assignors.sticky.sorted_set import SortedSet
from aiokafka.coordinator.protocol import (
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
    Schema,
)
from aiokafka.protocol.struct import Struct
from aiokafka.protocol.types import Array, Int32, String
from aiokafka.structs import TopicPartition

log = logging.getLogger(__name__)


class ConsumerGenerationPair(NamedTuple):
    consumer: str
    generation: int


class ConsumerSubscription(NamedTuple):
    consumer: str
    partitions: Sequence[TopicPartition]


def has_identical_list_elements(list_: Sequence[List[Any]]) -> bool:
    """Checks if all lists in the collection have the same members

    Arguments:
      list_: collection of lists

    Returns:
      true if all lists in the collection have the same members; false otherwise
    """
    if not list_:
        return True
    return all(list_[i] == list_[i - 1] for i in range(1, len(list_)))


def subscriptions_comparator_key(element: Tuple[str, Sized]) -> Tuple[int, str]:
    return len(element[1]), element[0]


def partitions_comparator_key(
    element: Tuple[TopicPartition, Sized],
) -> Tuple[int, str, int]:
    return len(element[1]), element[0].topic, element[0].partition


def remove_if_present(collection: MutableSequence[Any], element: Any) -> None:
    with contextlib.suppress(ValueError, KeyError):
        collection.remove(element)


class StickyAssignorMemberMetadataV1(NamedTuple):
    subscription: List[str]
    partitions: List[TopicPartition]
    generation: int


class StickyAssignorUserDataV1(Struct):
    """
    Used for preserving consumer's previously assigned partitions
    list and sending it as user data to the leader during a rebalance
    """

    class PreviousAssignment(NamedTuple):
        topic: str
        partitions: List[int]

    previous_assignment: List[PreviousAssignment]
    generation: int

    SCHEMA = Schema(
        (
            "previous_assignment",
            Array(("topic", String("utf-8")), ("partitions", Array(Int32))),
        ),
        ("generation", Int32),
    )


class StickyAssignmentExecutor:
    def __init__(
        self,
        cluster: ClusterMetadata,
        members: Dict[str, StickyAssignorMemberMetadataV1],
    ) -> None:
        self.members = members
        # a mapping between consumers and their assigned partitions that is updated
        # during assignment procedure
        self.current_assignment: Dict[str, List[TopicPartition]] = defaultdict(list)
        # an assignment from a previous generation
        self.previous_assignment: Dict[TopicPartition, ConsumerGenerationPair] = {}
        # a mapping between partitions and their assigned consumers
        self.current_partition_consumer: Dict[TopicPartition, str] = {}
        # a flag indicating that there were no previous assignments performed ever
        self.is_fresh_assignment = False
        # a mapping of all topic partitions to all consumers that can be assigned to
        # them
        self.partition_to_all_potential_consumers: Dict[TopicPartition, List[str]] = {}
        # a mapping of all consumers to all potential topic partitions that can be
        # assigned to them
        self.consumer_to_all_potential_partitions: Dict[str, List[TopicPartition]] = {}
        # an ascending sorted set of consumers based on how many topic partitions are
        # already assigned to them
        self.sorted_current_subscriptions: SortedSet[ConsumerSubscription] = SortedSet()
        # an ascending sorted list of topic partitions based on how many consumers can
        # potentially use them
        self.sorted_partitions: List[TopicPartition] = []
        # all partitions that need to be assigned
        self.unassigned_partitions: List[TopicPartition] = []
        # a flag indicating that a certain partition cannot remain assigned to its
        # current consumer because the consumer is no longer subscribed to its topic
        self.revocation_required = False

        self.partition_movements = PartitionMovements()
        self._initialize(cluster)

    def perform_initial_assignment(self) -> None:
        self._populate_sorted_partitions()
        self._populate_partitions_to_reassign()

    def balance(self) -> None:
        self._initialize_current_subscriptions()
        initializing = (
            len(self.current_assignment[self._get_consumer_with_most_subscriptions()])
            == 0
        )

        # assign all unassigned partitions
        for partition in self.unassigned_partitions:
            # skip if there is no potential consumer for the partition
            if not self.partition_to_all_potential_consumers[partition]:
                continue
            self._assign_partition(partition)

        # narrow down the reassignment scope to only those partitions that can actually
        # be reassigned
        fixed_partitions = set()
        for partition in self.partition_to_all_potential_consumers:
            if not self._can_partition_participate_in_reassignment(partition):
                fixed_partitions.add(partition)
        for fixed_partition in fixed_partitions:
            remove_if_present(self.sorted_partitions, fixed_partition)
            remove_if_present(self.unassigned_partitions, fixed_partition)

        # narrow down the reassignment scope to only those consumers that are subject to
        # reassignment
        fixed_assignments: Dict[str, List[TopicPartition]] = {}
        for consumer in self.consumer_to_all_potential_partitions:
            if not self._can_consumer_participate_in_reassignment(consumer):
                self._remove_consumer_from_current_subscriptions_and_maintain_order(
                    consumer
                )
                fixed_assignments[consumer] = self.current_assignment[consumer]
                del self.current_assignment[consumer]

        # create a deep copy of the current assignment so we can revert to it
        # if we do not get a more balanced assignment later
        prebalance_assignment = deepcopy(self.current_assignment)
        prebalance_partition_consumers = deepcopy(self.current_partition_consumer)

        # if we don't already need to revoke something due to subscription changes,
        # first try to balance by only moving newly added partitions
        if not self.revocation_required:
            self._perform_reassignments(self.unassigned_partitions)
        reassignment_performed = self._perform_reassignments(self.sorted_partitions)

        # if we are not preserving existing assignments and we have made changes to the
        # current assignment make sure we are getting a more balanced assignment;
        # otherwise, revert to previous assignment
        if (
            not initializing
            and reassignment_performed
            and self._get_balance_score(self.current_assignment)
            >= self._get_balance_score(prebalance_assignment)
        ):
            self.current_assignment = prebalance_assignment
            self.current_partition_consumer.clear()
            self.current_partition_consumer.update(prebalance_partition_consumers)

        # add the fixed assignments (those that could not change) back
        for consumer, partitions in fixed_assignments.items():
            self.current_assignment[consumer] = partitions
            self._add_consumer_to_current_subscriptions_and_maintain_order(consumer)

    def get_final_assignment(self, member_id: str) -> Collection[Tuple[str, List[int]]]:
        assignment: Dict[str, List[int]] = defaultdict(list)
        for topic_partition in self.current_assignment[member_id]:
            assignment[topic_partition.topic].append(topic_partition.partition)
        assignment = {k: sorted(v) for k, v in assignment.items()}
        return assignment.items()

    def _initialize(self, cluster: ClusterMetadata) -> None:
        self._init_current_assignments(self.members)

        for topic in cluster.topics():
            partitions = cluster.partitions_for_topic(topic)
            if partitions is None:
                log.warning("No partition metadata for topic %s", topic)
                continue
            for p in partitions:
                partition = TopicPartition(topic=topic, partition=p)
                self.partition_to_all_potential_consumers[partition] = []
        for consumer_id, member_metadata in self.members.items():
            self.consumer_to_all_potential_partitions[consumer_id] = []
            for topic in member_metadata.subscription:
                partitions_for_topic = cluster.partitions_for_topic(topic)
                if partitions_for_topic is None:
                    log.warning("No partition metadata for topic %r", topic)
                    continue
                for p in partitions_for_topic:
                    partition = TopicPartition(topic=topic, partition=p)
                    self.consumer_to_all_potential_partitions[consumer_id].append(
                        partition
                    )
                    self.partition_to_all_potential_consumers[partition].append(
                        consumer_id
                    )
            if consumer_id not in self.current_assignment:
                self.current_assignment[consumer_id] = []

    def _init_current_assignments(
        self, members: Dict[str, StickyAssignorMemberMetadataV1]
    ) -> None:
        # we need to process subscriptions' user data with each consumer's reported
        # generation in mind higher generations overwrite lower generations in case of
        # a conflict note that a conflict could exists only if user data is for
        # different generations

        # for each partition we create a map of its consumers by generation
        sorted_partition_consumers_by_generation: Dict[
            TopicPartition, Dict[int, str]
        ] = {}
        for consumer, member_metadata in members.items():
            for partition in member_metadata.partitions:
                if partition in sorted_partition_consumers_by_generation:
                    consumers = sorted_partition_consumers_by_generation[partition]
                    if (
                        member_metadata.generation
                        and member_metadata.generation in consumers
                    ):
                        # same partition is assigned to two consumers during the same
                        # rebalance. log a warning and skip this record
                        log.warning(
                            "Partition %r is assigned to multiple consumers "
                            "following sticky assignment generation %d",
                            partition,
                            member_metadata.generation,
                        )
                    else:
                        consumers[member_metadata.generation] = consumer
                else:
                    sorted_consumers = {member_metadata.generation: consumer}
                    sorted_partition_consumers_by_generation[partition] = (
                        sorted_consumers
                    )

        # previous_assignment holds the prior ConsumerGenerationPair (before current) of
        # each partition current and previous consumers are the last two consumers of
        # each partition in the above sorted map
        for partition, consumers in sorted_partition_consumers_by_generation.items():
            generations = sorted(consumers.keys(), reverse=True)
            self.current_assignment[consumers[generations[0]]].append(partition)
            # now update previous assignment if any
            if len(generations) > 1:
                self.previous_assignment[partition] = ConsumerGenerationPair(
                    consumer=consumers[generations[1]], generation=generations[1]
                )

        self.is_fresh_assignment = len(self.current_assignment) == 0

        for consumer_id, partitions in self.current_assignment.items():
            for partition in partitions:
                self.current_partition_consumer[partition] = consumer_id

    def _are_subscriptions_identical(self) -> bool:
        """
        Returns:
            true, if both potential consumers of partitions and potential partitions
            that consumers can consume are the same
        """
        if not has_identical_list_elements(
            list(self.partition_to_all_potential_consumers.values())
        ):
            return False
        return has_identical_list_elements(
            list(self.consumer_to_all_potential_partitions.values())
        )

    def _populate_sorted_partitions(self) -> None:
        # set of topic partitions with their respective potential consumers
        all_partitions = {
            (tp, tuple(consumers))
            for tp, consumers in self.partition_to_all_potential_consumers.items()
        }
        partitions_sorted_by_num_of_potential_consumers = sorted(
            all_partitions, key=partitions_comparator_key
        )

        self.sorted_partitions = []
        if not self.is_fresh_assignment and self._are_subscriptions_identical():
            # if this is a reassignment and the subscriptions are identical (all
            # consumers can consumer from all topics) then we just need to simply list
            # partitions in a round robin fashion (from consumers with most assigned
            # partitions to those with least)
            assignments = deepcopy(self.current_assignment)
            for partitions in assignments.values():
                partitions[:] = [
                    partition
                    for partition in partitions
                    if partition in self.partition_to_all_potential_consumers
                ]

            sorted_consumers = SortedSet(
                iterable=[
                    (consumer, tuple(partitions))
                    for consumer, partitions in assignments.items()
                ],
                key=subscriptions_comparator_key,
            )
            # at this point, sorted_consumers contains an ascending-sorted list of
            # consumers based on how many valid partitions are currently assigned to
            # them

            while sorted_consumers:
                # take the consumer with the most partitions
                consumer, _ = sorted_consumers.pop_last()
                # currently assigned partitions to this consumer
                remaining_partitions = assignments[consumer]
                # from partitions that had a different consumer before,
                # keep only those that are assigned to this consumer now
                previous_partitions = set(self.previous_assignment.keys()).intersection(
                    set(remaining_partitions)
                )
                if previous_partitions:
                    # if there is a partition of this consumer that was assigned to
                    # another consumer before mark it as good options for reassignment
                    partition = previous_partitions.pop()
                    remaining_partitions.remove(partition)
                    self.sorted_partitions.append(partition)
                    sorted_consumers.add((consumer, tuple(assignments[consumer])))
                elif remaining_partitions:
                    # otherwise, mark any other one of the current partitions as a
                    # reassignment candidate
                    self.sorted_partitions.append(remaining_partitions.pop())
                    sorted_consumers.add((consumer, tuple(assignments[consumer])))

            while partitions_sorted_by_num_of_potential_consumers:
                partition = partitions_sorted_by_num_of_potential_consumers.pop(0)[0]
                if partition not in self.sorted_partitions:
                    self.sorted_partitions.append(partition)
        else:
            while partitions_sorted_by_num_of_potential_consumers:
                self.sorted_partitions.append(
                    partitions_sorted_by_num_of_potential_consumers.pop(0)[0]
                )

    def _populate_partitions_to_reassign(self) -> None:
        self.unassigned_partitions = deepcopy(self.sorted_partitions)

        assignments_to_remove: List[str] = []
        for consumer_id, partitions in self.current_assignment.items():
            if consumer_id not in self.members:
                # if a consumer that existed before (and had some partition assignments)
                # is now removed, remove it from current_assignment
                for partition in partitions:
                    del self.current_partition_consumer[partition]
                assignments_to_remove.append(consumer_id)
            else:
                # otherwise (the consumer still exists)
                partitions_to_remove = []
                for partition in partitions:
                    if partition not in self.partition_to_all_potential_consumers:
                        # if this topic partition of this consumer no longer exists
                        # remove it from current_assignment of the consumer
                        partitions_to_remove.append(partition)
                    elif partition.topic not in self.members[consumer_id].subscription:
                        # if this partition cannot remain assigned to its current
                        # consumer because the consumer is no longer subscribed to its
                        # topic remove it from current_assignment of the consumer
                        partitions_to_remove.append(partition)
                        self.revocation_required = True
                    else:
                        # otherwise, remove the topic partition from those that need to
                        # be assigned only if its current consumer is still subscribed
                        # to its topic (because it is already assigned and we would want
                        # to preserve that assignment as much as possible)
                        self.unassigned_partitions.remove(partition)
                for partition in partitions_to_remove:
                    self.current_assignment[consumer_id].remove(partition)
                    del self.current_partition_consumer[partition]
        for consumer_id in assignments_to_remove:
            del self.current_assignment[consumer_id]

    def _initialize_current_subscriptions(self) -> None:
        self.sorted_current_subscriptions = SortedSet(
            iterable=[
                ConsumerSubscription(consumer=consumer, partitions=tuple(partitions))
                for consumer, partitions in self.current_assignment.items()
            ],
            key=subscriptions_comparator_key,
        )

    def _get_consumer_with_least_subscriptions(self) -> str:
        if current_subscription := self.sorted_current_subscriptions.first():
            return current_subscription[0]
        raise ValueError("sorted_current_subscriptions is empty")

    def _get_consumer_with_most_subscriptions(self) -> str:
        if current_subscription := self.sorted_current_subscriptions.last():
            return current_subscription[0]
        raise ValueError("sorted_current_subscriptions is empty")

    def _remove_consumer_from_current_subscriptions_and_maintain_order(
        self, consumer: str
    ) -> None:
        self.sorted_current_subscriptions.remove(
            ConsumerSubscription(
                consumer=consumer,
                partitions=tuple(self.current_assignment[consumer]),
            )
        )

    def _add_consumer_to_current_subscriptions_and_maintain_order(
        self, consumer: str
    ) -> None:
        self.sorted_current_subscriptions.add(
            ConsumerSubscription(
                consumer=consumer,
                partitions=tuple(self.current_assignment[consumer]),
            )
        )

    def _is_balanced(self) -> bool:
        """Determines if the current assignment is a balanced one"""
        if (
            len(self.current_assignment[self._get_consumer_with_least_subscriptions()])
            >= len(
                self.current_assignment[self._get_consumer_with_most_subscriptions()]
            )
            - 1
        ):
            # if minimum and maximum numbers of partitions assigned to consumers differ
            # by at most one return true
            return True

        # create a mapping from partitions to the consumer assigned to them
        all_assigned_partitions = {}
        for consumer_id, consumer_partitions in self.current_assignment.items():
            for partition in consumer_partitions:
                if partition in all_assigned_partitions:
                    log.error(
                        "Partition %r is assigned to more than one consumer", partition
                    )
                all_assigned_partitions[partition] = consumer_id

        # for each consumer that does not have all the topic partitions it can get
        # make sure none of the topic partitions it could but did not get cannot be
        # moved to it (because that would break the balance)
        for consumer, _ in self.sorted_current_subscriptions:
            consumer_partition_count = len(self.current_assignment[consumer])
            # skip if this consumer already has all the topic partitions it can get
            if consumer_partition_count == len(
                self.consumer_to_all_potential_partitions[consumer]
            ):
                continue

            # otherwise make sure it cannot get any more
            for partition in self.consumer_to_all_potential_partitions[consumer]:
                if partition not in self.current_assignment[consumer]:
                    other_consumer = all_assigned_partitions[partition]
                    other_consumer_partition_count = len(
                        self.current_assignment[other_consumer]
                    )
                    if consumer_partition_count < other_consumer_partition_count:
                        return False
        return True

    def _assign_partition(self, partition: TopicPartition) -> None:
        for consumer, _ in self.sorted_current_subscriptions:
            if partition in self.consumer_to_all_potential_partitions[consumer]:
                self._remove_consumer_from_current_subscriptions_and_maintain_order(
                    consumer
                )
                self.current_assignment[consumer].append(partition)
                self.current_partition_consumer[partition] = consumer
                self._add_consumer_to_current_subscriptions_and_maintain_order(consumer)
                break

    def _can_partition_participate_in_reassignment(
        self, partition: TopicPartition
    ) -> bool:
        return len(self.partition_to_all_potential_consumers[partition]) >= 2

    def _can_consumer_participate_in_reassignment(self, consumer: str) -> bool:
        current_partitions = self.current_assignment[consumer]
        current_assignment_size = len(current_partitions)
        max_assignment_size = len(self.consumer_to_all_potential_partitions[consumer])
        if current_assignment_size > max_assignment_size:
            log.error(
                "The consumer %r is assigned more partitions than the maximum "
                "possible",
                consumer,
            )
        if current_assignment_size < max_assignment_size:
            # if a consumer is not assigned all its potential partitions it is subject
            # to reassignment
            return True
        for partition in current_partitions:
            # if any of the partitions assigned to a consumer is subject to reassignment
            # the consumer itself is subject to reassignment
            if self._can_partition_participate_in_reassignment(partition):
                return True
        return False

    def _perform_reassignments(
        self, reassignable_partitions: List[TopicPartition]
    ) -> bool:
        reassignment_performed = False

        # repeat reassignment until no partition can be moved to improve the balance
        while True:
            modified = False
            # reassign all reassignable partitions until the full list is processed or
            # a balance is achieved (starting from the partition with least potential
            # consumers and if needed)
            for partition in reassignable_partitions:
                if self._is_balanced():
                    break
                # the partition must have at least two potential consumers
                if len(self.partition_to_all_potential_consumers[partition]) <= 1:
                    log.error(
                        "Expected more than one potential consumer for partition %r",
                        partition,
                    )
                # the partition must have a current consumer
                consumer = self.current_partition_consumer.get(partition)
                if consumer is None:
                    log.error(
                        "Expected partition %r to be assigned to a consumer", partition
                    )

                if (
                    consumer is not None
                    and partition in self.previous_assignment
                    and len(self.current_assignment[consumer])
                    > len(
                        self.current_assignment[
                            self.previous_assignment[partition].consumer
                        ]
                    )
                    + 1
                ):
                    self._reassign_partition_to_consumer(
                        partition,
                        self.previous_assignment[partition].consumer,
                    )
                    reassignment_performed = True
                    modified = True
                    continue

                # check if a better-suited consumer exist for the partition; if so,
                # reassign it
                for other_consumer in self.partition_to_all_potential_consumers[
                    partition
                ]:
                    if (
                        consumer is not None
                        and len(self.current_assignment[consumer])
                        > len(self.current_assignment[other_consumer]) + 1
                    ):
                        self._reassign_partition(partition)
                        reassignment_performed = True
                        modified = True
                        break

            if not modified:
                break
        return reassignment_performed

    def _reassign_partition(self, partition: TopicPartition) -> None:
        new_consumer = None
        for another_consumer, _ in self.sorted_current_subscriptions:
            if partition in self.consumer_to_all_potential_partitions[another_consumer]:
                new_consumer = another_consumer
                break
        assert new_consumer is not None
        self._reassign_partition_to_consumer(partition, new_consumer)

    def _reassign_partition_to_consumer(
        self, partition: TopicPartition, new_consumer: str
    ) -> None:
        consumer = self.current_partition_consumer[partition]
        # find the correct partition movement considering the stickiness requirement
        partition_to_be_moved = self.partition_movements.get_partition_to_be_moved(
            partition, consumer, new_consumer
        )
        self._move_partition(partition_to_be_moved, new_consumer)

    def _move_partition(self, partition: TopicPartition, new_consumer: str) -> None:
        old_consumer = self.current_partition_consumer[partition]
        self._remove_consumer_from_current_subscriptions_and_maintain_order(
            old_consumer
        )
        self._remove_consumer_from_current_subscriptions_and_maintain_order(
            new_consumer
        )

        self.partition_movements.move_partition(partition, old_consumer, new_consumer)

        self.current_assignment[old_consumer].remove(partition)
        self.current_assignment[new_consumer].append(partition)
        self.current_partition_consumer[partition] = new_consumer

        self._add_consumer_to_current_subscriptions_and_maintain_order(new_consumer)
        self._add_consumer_to_current_subscriptions_and_maintain_order(old_consumer)

    @staticmethod
    def _get_balance_score(assignment: Dict[str, List[TopicPartition]]) -> int:
        """Calculates a balance score of a give assignment
        as the sum of assigned partitions size difference of all consumer pairs.
        A perfectly balanced assignment (with all consumers getting the same number of
        partitions) has a balance score of 0. Lower balance score indicates a more
        balanced assignment.

        Arguments:
          assignment (dict): {consumer: list of assigned topic partitions}

        Returns:
          the balance score of the assignment
        """
        score = 0
        consumer_to_assignment: Dict[str, int] = {}
        for consumer_id, partitions in assignment.items():
            consumer_to_assignment[consumer_id] = len(partitions)

        consumers_to_explore = set(consumer_to_assignment.keys())
        for consumer_id in consumer_to_assignment:
            if consumer_id in consumers_to_explore:
                consumers_to_explore.remove(consumer_id)
                for other_consumer_id in consumers_to_explore:
                    score += abs(
                        consumer_to_assignment[consumer_id]
                        - consumer_to_assignment[other_consumer_id]
                    )
        return score


class StickyPartitionAssignor(AbstractPartitionAssignor):
    """
    https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy

    The sticky assignor serves two purposes. First, it guarantees an assignment that is
    as balanced as possible, meaning either:
    - the numbers of topic partitions assigned to consumers differ by at most one; or
    - each consumer that has 2+ fewer topic partitions than some other consumer cannot
    get any of those topic partitions transferred to it.

    Second, it preserved as many existing assignment as possible when a reassignment
    occurs. This helps in saving some of the overhead processing when topic partitions
    move from one consumer to another.

    Starting fresh it would work by distributing the partitions over consumers as evenly
    as possible. Even though this may sound similar to how round robin assignor works,
    the second example below shows that it is not. During a reassignment it would
    perform the reassignment in such a way that in the new assignment
    - topic partitions are still distributed as evenly as possible, and
    - topic partitions stay with their previously assigned consumers as much as
    possible.

    The first goal above takes precedence over the second one.

    Example 1.
    Suppose there are three consumers C0, C1, C2,
    four topics t0, t1, t2, t3, and each topic has 2 partitions,
    resulting in partitions t0p0, t0p1, t1p0, t1p1, t2p0, t2p1, t3p0, t3p1.
    Each consumer is subscribed to all three topics.

    The assignment with both sticky and round robin assignors will be:
    - C0: [t0p0, t1p1, t3p0]
    - C1: [t0p1, t2p0, t3p1]
    - C2: [t1p0, t2p1]

    Now, let's assume C1 is removed and a reassignment is about to happen. The round
    robin assignor would produce:
    - C0: [t0p0, t1p0, t2p0, t3p0]
    - C2: [t0p1, t1p1, t2p1, t3p1]

    while the sticky assignor would result in:
    - C0 [t0p0, t1p1, t3p0, t2p0]
    - C2 [t1p0, t2p1, t0p1, t3p1]
    preserving all the previous assignments (unlike the round robin assignor).


    Example 2.
    There are three consumers C0, C1, C2,
    and three topics t0, t1, t2, with 1, 2, and 3 partitions respectively.
    Therefore, the partitions are t0p0, t1p0, t1p1, t2p0, t2p1, t2p2.
    C0 is subscribed to t0;
    C1 is subscribed to t0, t1;
    and C2 is subscribed to t0, t1, t2.

    The round robin assignor would come up with the following assignment:
    - C0 [t0p0]
    - C1 [t1p0]
    - C2 [t1p1, t2p0, t2p1, t2p2]

    which is not as balanced as the assignment suggested by sticky assignor:
    - C0 [t0p0]
    - C1 [t1p0, t1p1]
    - C2 [t2p0, t2p1, t2p2]

    Now, if consumer C0 is removed, these two assignors would produce the following
    assignments. Round Robin (preserves 3 partition assignments):
    - C1 [t0p0, t1p1]
    - C2 [t1p0, t2p0, t2p1, t2p2]

    Sticky (preserves 5 partition assignments):
    - C1 [t1p0, t1p1, t0p0]
    - C2 [t2p0, t2p1, t2p2]
    """

    DEFAULT_GENERATION_ID = -1

    name = "sticky"
    version = 0

    member_assignment: Optional[List[TopicPartition]] = None
    generation: int = DEFAULT_GENERATION_ID

    _latest_partition_movements: Optional[PartitionMovements] = None

    @classmethod
    def assign(
        cls,
        cluster: ClusterMetadata,
        members: Mapping[str, ConsumerProtocolMemberMetadata],
    ) -> Dict[str, ConsumerProtocolMemberAssignment]:
        """Performs group assignment given cluster metadata and member subscriptions

        Arguments:
            cluster (ClusterMetadata): cluster metadata
            members (dict of {member_id: MemberMetadata}): decoded metadata for each
            member in the group.

        Returns:
          dict: {member_id: MemberAssignment}
        """
        members_metadata: Dict[str, StickyAssignorMemberMetadataV1] = {}
        for consumer, member_metadata in members.items():
            members_metadata[consumer] = cls.parse_member_metadata(member_metadata)

        executor = StickyAssignmentExecutor(cluster, members_metadata)
        executor.perform_initial_assignment()
        executor.balance()

        cls._latest_partition_movements = executor.partition_movements

        assignment: Dict[str, ConsumerProtocolMemberAssignment] = {}
        for member_id in members:
            assignment[member_id] = ConsumerProtocolMemberAssignment(
                cls.version,
                sorted(executor.get_final_assignment(member_id)),
                b"",
            )
        return assignment

    @classmethod
    def parse_member_metadata(
        cls, metadata: ConsumerProtocolMemberMetadata
    ) -> StickyAssignorMemberMetadataV1:
        """
        Parses member metadata into a python object.
        This implementation only serializes and deserializes the
        StickyAssignorMemberMetadataV1 user data, since no StickyAssignor written in
        Python was deployed ever in the wild with version V0, meaning that there is no
        need to support backward compatibility with V0.

        Arguments:
          metadata (MemberMetadata): decoded metadata for a member of the group.

        Returns:
          parsed metadata (StickyAssignorMemberMetadataV1)
        """
        user_data = metadata.user_data
        if not user_data:
            return StickyAssignorMemberMetadataV1(
                partitions=[],
                generation=cls.DEFAULT_GENERATION_ID,
                subscription=metadata.subscription,
            )

        try:
            decoded_user_data = StickyAssignorUserDataV1.decode(user_data)
        except Exception as e:  # noqa: BLE001
            # ignore the consumer's previous assignment if it cannot be parsed
            log.error("Could not parse member data: %r", e)
            return StickyAssignorMemberMetadataV1(
                partitions=[],
                generation=cls.DEFAULT_GENERATION_ID,
                subscription=metadata.subscription,
            )

        member_partitions: List[TopicPartition] = []
        for (
            topic,
            partitions,
        ) in decoded_user_data.previous_assignment:
            member_partitions.extend(
                [TopicPartition(topic, partition) for partition in partitions]
            )
        return StickyAssignorMemberMetadataV1(
            partitions=member_partitions,
            generation=decoded_user_data.generation,
            subscription=metadata.subscription,
        )

    @classmethod
    def metadata(cls, topics: Iterable[str]) -> ConsumerProtocolMemberMetadata:
        return cls._metadata(topics, cls.member_assignment, cls.generation)

    @classmethod
    def _metadata(
        cls,
        topics: Iterable[str],
        member_assignment_partitions: Optional[List[TopicPartition]],
        generation: int = -1,
    ) -> ConsumerProtocolMemberMetadata:
        if member_assignment_partitions is None:
            log.debug("No member assignment available")
            user_data = b""
        else:
            log.debug(
                "Member assignment is available, generating the metadata: "
                "generation %d",
                cls.generation,
            )
            partitions_by_topic = defaultdict(list)
            for topic_partition in member_assignment_partitions:
                partitions_by_topic[topic_partition.topic].append(
                    topic_partition.partition
                )
            data = StickyAssignorUserDataV1(partitions_by_topic.items(), generation)
            user_data = data.encode()
        return ConsumerProtocolMemberMetadata(cls.version, list(topics), user_data)

    @classmethod
    def on_assignment(cls, assignment: ConsumerProtocolMemberAssignment) -> None:
        """Callback that runs on each assignment. Updates assignor's state.

        Arguments:
          assignment: MemberAssignment
        """
        log.debug("On assignment: assignment=%r", assignment)
        cls.member_assignment = assignment.partitions()

    @classmethod
    def on_generation_assignment(cls, generation: int) -> None:
        """Callback that runs on each assignment. Updates assignor's generation id.

        Arguments:
          generation: generation id
        """
        log.debug("On generation assignment: generation=%d", generation)
        cls.generation = generation
