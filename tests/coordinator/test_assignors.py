from collections import defaultdict
from collections.abc import Callable, Generator, Sequence
from random import randint, sample
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
    StickyPartitionAssignor,
)
from aiokafka.coordinator.protocol import (
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
)
from aiokafka.structs import TopicPartition


@pytest.fixture(autouse=True)
def reset_sticky_assignor() -> Generator[None, None, None]:
    yield
    StickyPartitionAssignor.member_assignment = None
    StickyPartitionAssignor.generation = -1


def create_cluster(
    mocker: MockerFixture,
    topics: set[str],
    topics_partitions: set[int] | None = None,
    topic_partitions_lambda: Callable[[str], set[int] | None] | None = None,
) -> MagicMock:
    cluster = mocker.MagicMock()
    cluster.topics.return_value = topics
    if topics_partitions is not None:
        cluster.partitions_for_topic.return_value = topics_partitions
    if topic_partitions_lambda is not None:
        cluster.partitions_for_topic.side_effect = topic_partitions_lambda
    return cluster


def test_assignor_roundrobin(mocker: MockerFixture) -> None:
    assignor = RoundRobinPartitionAssignor

    member_metadata = {
        "C0": assignor.metadata({"t0", "t1"}),
        "C1": assignor.metadata({"t0", "t1"}),
    }

    cluster = create_cluster(mocker, {"t0", "t1"}, topics_partitions={0, 1, 2})
    ret = assignor.assign(cluster, member_metadata)
    expected = {
        "C0": ConsumerProtocolMemberAssignment(
            assignor.version, [("t0", [0, 2]), ("t1", [1])], b""
        ),
        "C1": ConsumerProtocolMemberAssignment(
            assignor.version, [("t0", [1]), ("t1", [0, 2])], b""
        ),
    }
    assert ret == expected
    assert set(ret) == set(expected)
    for member in ret:
        assert ret[member].encode() == expected[member].encode()


def test_assignor_range(mocker: MockerFixture) -> None:
    assignor = RangePartitionAssignor

    member_metadata = {
        "C0": assignor.metadata({"t0", "t1"}),
        "C1": assignor.metadata({"t0", "t1"}),
    }

    cluster = create_cluster(mocker, {"t0", "t1"}, topics_partitions={0, 1, 2})
    ret = assignor.assign(cluster, member_metadata)
    expected = {
        "C0": ConsumerProtocolMemberAssignment(
            assignor.version, [("t0", [0, 1]), ("t1", [0, 1])], b""
        ),
        "C1": ConsumerProtocolMemberAssignment(
            assignor.version, [("t0", [2]), ("t1", [2])], b""
        ),
    }
    assert ret == expected
    assert set(ret) == set(expected)
    for member in ret:
        assert ret[member].encode() == expected[member].encode()


def test_sticky_assignor1(mocker: MockerFixture) -> None:
    """
    Given: there are three consumers C0, C1, C2,
        four topics t0, t1, t2, t3, and each topic has 2 partitions,
        resulting in partitions t0p0, t0p1, t1p0, t1p1, t2p0, t2p1, t3p0, t3p1.
        Each consumer is subscribed to all three topics.
    Then: perform fresh assignment
    Expected: the assignment is
    - C0: [t0p0, t1p1, t3p0]
    - C1: [t0p1, t2p0, t3p1]
    - C2: [t1p0, t2p1]
    Then: remove C1 consumer and perform the reassignment
    Expected: the new assignment is
    - C0 [t0p0, t1p1, t2p0, t3p0]
    - C2 [t0p1, t1p0, t2p1, t3p1]
    """
    cluster = create_cluster(
        mocker, topics={"t0", "t1", "t2", "t3"}, topics_partitions={0, 1}
    )

    subscriptions = {
        "C0": {"t0", "t1", "t2", "t3"},
        "C1": {"t0", "t1", "t2", "t3"},
        "C2": {"t0", "t1", "t2", "t3"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C0": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version,
            [("t0", [0]), ("t1", [1]), ("t3", [0])],
            b"",
        ),
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version,
            [("t0", [1]), ("t2", [0]), ("t3", [1])],
            b"",
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [0]), ("t2", [1])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)

    del subscriptions["C1"]
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, sticky_assignment[member].partitions()
        )

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C0": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version,
            [("t0", [0]), ("t1", [1]), ("t2", [0]), ("t3", [0])],
            b"",
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version,
            [("t0", [1]), ("t1", [0]), ("t2", [1]), ("t3", [1])],
            b"",
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_assignor2(mocker: MockerFixture) -> None:
    """
    Given: there are three consumers C0, C1, C2,
    and three topics t0, t1, t2, with 1, 2, and 3 partitions respectively.
    Therefore, the partitions are t0p0, t1p0, t1p1, t2p0, t2p1, t2p2.
    C0 is subscribed to t0;
    C1 is subscribed to t0, t1;
    and C2 is subscribed to t0, t1, t2.
    Then: perform the assignment
    Expected: the assignment is
    - C0 [t0p0]
    - C1 [t1p0, t1p1]
    - C2 [t2p0, t2p1, t2p2]
    Then: remove C0 and perform the assignment
    Expected: the assignment is
    - C1 [t0p0, t1p0, t1p1]
    - C2 [t2p0, t2p1, t2p2]
    """

    partitions = {"t0": {0}, "t1": {0, 1}, "t2": {0, 1, 2}}
    cluster = create_cluster(
        mocker,
        topics={"t0", "t1", "t2"},
        topic_partitions_lambda=lambda t: partitions[t],
    )

    subscriptions = {
        "C0": {"t0"},
        "C1": {"t0", "t1"},
        "C2": {"t0", "t1", "t2"},
    }
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(topics, [])

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C0": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t0", [0])], b""
        ),
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [0, 1])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t2", [0, 1, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)

    del subscriptions["C0"]
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, sticky_assignment[member].partitions()
        )

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t0", [0]), ("t1", [0, 1])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t2", [0, 1, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_one_consumer_no_topic(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics=set(), topics_partitions=set())

    subscriptions: dict[str, set[str]] = {
        "C": set(),
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(StickyPartitionAssignor.version, [], b""),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_one_consumer_nonexisting_topic(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics=set(), topics_partitions=set())

    subscriptions = {
        "C": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(StickyPartitionAssignor.version, [], b""),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_one_consumer_one_topic(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1, 2})

    subscriptions = {
        "C": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t", [0, 1, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_should_only_assign_partitions_from_subscribed_topics(
    mocker: MockerFixture,
) -> None:
    cluster = create_cluster(
        mocker, topics={"t", "other-t"}, topics_partitions={0, 1, 2}
    )

    subscriptions = {
        "C": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t", [0, 1, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_one_consumer_multiple_topics(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t1", "t2"}, topics_partitions={0, 1, 2})

    subscriptions = {
        "C": {"t1", "t2"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [0, 1, 2]), ("t2", [0, 1, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_two_consumers_one_topic_one_partition(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0})

    subscriptions = {
        "C1": {"t"},
        "C2": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t", [0])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_two_consumers_one_topic_two_partitions(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1})

    subscriptions = {
        "C1": {"t"},
        "C2": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t", [0])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t", [1])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_multiple_consumers_mixed_topic_subscriptions(
    mocker: MockerFixture,
) -> None:
    partitions = {"t1": {0, 1, 2}, "t2": {0, 1}}
    cluster = create_cluster(
        mocker, topics={"t1", "t2"}, topic_partitions_lambda=lambda t: partitions[t]
    )

    subscriptions = {
        "C1": {"t1"},
        "C2": {"t1", "t2"},
        "C3": {"t1"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [0, 2])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t2", [0, 1])], b""
        ),
        "C3": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [1])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_add_remove_consumer_one_topic(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1, 2})

    subscriptions = {
        "C1": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t", [0, 1, 2])], b""
        ),
    }
    assert_assignment(assignment, expected_assignment)

    subscriptions = {
        "C1": {"t"},
        "C2": {"t"},
    }
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions() if member in assignment else []
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)

    subscriptions = {
        "C2": {"t"},
    }
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions()
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert len(assignment["C2"].assignment[0][1]) == 3


def test_sticky_add_remove_topic_two_consumers(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t1", "t2"}, topics_partitions={0, 1, 2})

    subscriptions = {
        "C1": {"t1"},
        "C2": {"t1"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [0, 2])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [1])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)

    subscriptions = {
        "C1": {"t1", "t2"},
        "C2": {"t1", "t2"},
    }
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, sticky_assignment[member].partitions()
        )

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [0, 2]), ("t2", [1])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t1", [1]), ("t2", [0, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)

    subscriptions = {
        "C1": {"t2"},
        "C2": {"t2"},
    }
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, sticky_assignment[member].partitions()
        )

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C1": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t2", [1])], b""
        ),
        "C2": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t2", [0, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_sticky_reassignment_after_one_consumer_leaves(mocker: MockerFixture) -> None:
    partitions = {f"t{i}": set(range(i)) for i in range(1, 20)}
    cluster = create_cluster(
        mocker,
        topics={f"t{i}" for i in range(1, 20)},
        topic_partitions_lambda=lambda t: partitions[t],
    )

    subscriptions = {}
    for i in range(1, 20):
        topics = set()
        for j in range(1, i + 1):
            topics.add(f"t{j}")
        subscriptions[f"C{i}"] = topics

    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)

    del subscriptions["C10"]
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions()
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def test_sticky_reassignment_after_one_consumer_added(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions=set(range(20)))

    subscriptions = defaultdict(set)
    for i in range(1, 10):
        subscriptions[f"C{i}"] = {"t"}

    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)

    subscriptions["C10"] = {"t"}
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions() if member in assignment else []
        )
    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def test_sticky_same_subscriptions(mocker: MockerFixture) -> None:
    partitions = {f"t{i}": set(range(i)) for i in range(1, 15)}
    cluster = create_cluster(
        mocker,
        topics={f"t{i}" for i in range(1, 15)},
        topic_partitions_lambda=lambda t: partitions[t],
    )

    subscriptions = defaultdict(set)
    for i in range(1, 9):
        for j in range(1, len(partitions) + 1):
            subscriptions[f"C{i}"].add(f"t{j}")

    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)

    del subscriptions["C5"]
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions()
        )
    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def test_sticky_large_assignment_with_multiple_consumers_leaving(
    mocker: MockerFixture,
) -> None:
    n_topics = 40
    n_consumers = 200

    all_topics = {f"t{i}" for i in range(1, n_topics + 1)}
    partitions = {t: set(range(1, randint(0, 10) + 1)) for t in all_topics}
    cluster = create_cluster(
        mocker, topics=all_topics, topic_partitions_lambda=lambda t: partitions[t]
    )

    subscriptions = defaultdict(set)
    for i in range(1, n_consumers + 1):
        for _ in range(randint(1, 20)):
            subscriptions[f"C{i}"].add(f"t{randint(1, n_topics)}")

    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)

    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions()
        )

    for _ in range(50):
        member = f"C{randint(1, n_consumers)}"
        if member in subscriptions:
            del subscriptions[member]
            del member_metadata[member]

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def test_new_subscription(mocker: MockerFixture) -> None:
    cluster = create_cluster(
        mocker, topics={"t1", "t2", "t3", "t4"}, topics_partitions={0}
    )

    subscriptions = defaultdict(set)
    for i in range(3):
        for j in range(i, 3 * i - 2 + 1):
            subscriptions[f"C{i}"].add(f"t{j}")

    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)

    subscriptions["C0"].add("t1")
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(topics, [])

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def test_move_existing_assignments(mocker: MockerFixture) -> None:
    cluster = create_cluster(
        mocker, topics={"t1", "t2", "t3", "t4", "t5", "t6"}, topics_partitions={0}
    )

    subscriptions = {
        "C1": {"t1", "t2"},
        "C2": {"t1", "t2", "t3", "t4"},
        "C3": {"t2", "t3", "t4", "t5", "t6"},
    }
    member_assignments = {
        "C1": [TopicPartition("t1", 0)],
        "C2": [TopicPartition("t2", 0), TopicPartition("t3", 0)],
        "C3": [
            TopicPartition("t4", 0),
            TopicPartition("t5", 0),
            TopicPartition("t6", 0),
        ],
    }

    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, member_assignments[member]
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)


def test_stickiness(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1, 2})
    subscriptions = {
        "C1": {"t"},
        "C2": {"t"},
        "C3": {"t"},
        "C4": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    partitions_assigned = {}
    for consumer, consumer_assignment in assignment.items():
        assert (
            len(consumer_assignment.partitions()) <= 1
        ), f"Consumer {consumer} is assigned more topic partitions than expected."
        if len(consumer_assignment.partitions()) == 1:
            partitions_assigned[consumer] = consumer_assignment.partitions()[0]

    # removing the potential group leader
    del subscriptions["C1"]
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions()
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()

    for consumer, consumer_assignment in assignment.items():
        assert (
            len(consumer_assignment.partitions()) <= 1
        ), f"Consumer {consumer} is assigned more topic partitions than expected."
        assert (
            consumer not in partitions_assigned
            or partitions_assigned[consumer] in consumer_assignment.partitions()
        ), f"Stickiness was not honored for consumer {consumer}"


def test_assignment_updated_for_deleted_topic(mocker: MockerFixture) -> None:
    def topic_partitions(topic: str) -> set[int] | None:
        if topic == "t1":
            return {0}
        elif topic == "t3":
            return set(range(100))
        return None

    cluster = create_cluster(
        mocker, topics={"t1", "t3"}, topic_partitions_lambda=topic_partitions
    )

    subscriptions = {
        "C": {"t1", "t2", "t3"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version,
            [("t1", [0]), ("t3", list(range(100)))],
            b"",
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_no_exceptions_when_only_subscribed_topic_is_deleted(
    mocker: MockerFixture,
) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1, 2})

    subscriptions = {
        "C": {"t"},
    }
    member_metadata = make_member_metadata(subscriptions)

    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(
            StickyPartitionAssignor.version, [("t", [0, 1, 2])], b""
        ),
    }
    assert_assignment(sticky_assignment, expected_assignment)

    subscriptions = {
        "C": set(),
    }
    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, sticky_assignment[member].partitions()
        )

    cluster = create_cluster(mocker, topics=set(), topics_partitions=set())
    sticky_assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    expected_assignment = {
        "C": ConsumerProtocolMemberAssignment(StickyPartitionAssignor.version, [], b""),
    }
    assert_assignment(sticky_assignment, expected_assignment)


def test_conflicting_previous_assignments(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1})

    subscriptions = {
        "C1": {"t"},
        "C2": {"t"},
    }
    member_metadata = {}
    for member, topics in subscriptions.items():
        # assume both C1 and C2 have partition 1 assigned to them in generation 1
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, [TopicPartition("t", 0), TopicPartition("t", 0)], 1
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)


@pytest.mark.parametrize(
    "execution_number,n_topics,n_consumers",
    [(i, randint(10, 20), randint(20, 40)) for i in range(100)],
)
def test_reassignment_with_random_subscriptions_and_changes(
    mocker: MockerFixture, execution_number: int, n_topics: int, n_consumers: int
) -> None:
    all_topics = sorted([f"t{i}" for i in range(1, n_topics + 1)])
    partitions = {t: set(range(1, i + 1)) for i, t in enumerate(all_topics)}
    cluster = create_cluster(
        mocker,
        topics=all_topics,  # type: ignore[arg-type]
        topic_partitions_lambda=lambda t: partitions[t],
    )

    subscriptions = defaultdict(set)
    for i in range(n_consumers):
        topics_sample = sample(all_topics, randint(1, len(all_topics) - 1))
        subscriptions[f"C{i}"].update(topics_sample)

    member_metadata = make_member_metadata(subscriptions)

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)

    subscriptions = defaultdict(set)
    for i in range(n_consumers):
        topics_sample = sample(all_topics, randint(1, len(all_topics) - 1))
        subscriptions[f"C{i}"].update(topics_sample)

    member_metadata = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(
            topics, assignment[member].partitions()
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance(subscriptions, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def test_assignment_with_multiple_generations1(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1, 2, 3, 4, 5})

    member_metadata = {
        "C1": StickyPartitionAssignor._metadata({"t"}, []),
        "C2": StickyPartitionAssignor._metadata({"t"}, []),
        "C3": StickyPartitionAssignor._metadata({"t"}, []),
    }

    assignment1 = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance({"C1": {"t"}, "C2": {"t"}, "C3": {"t"}}, assignment1)
    assert len(assignment1["C1"].assignment[0][1]) == 2
    assert len(assignment1["C2"].assignment[0][1]) == 2
    assert len(assignment1["C3"].assignment[0][1]) == 2

    member_metadata = {
        "C1": StickyPartitionAssignor._metadata({"t"}, assignment1["C1"].partitions()),
        "C2": StickyPartitionAssignor._metadata({"t"}, assignment1["C2"].partitions()),
    }

    assignment2 = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance({"C1": {"t"}, "C2": {"t"}}, assignment2)
    assert len(assignment2["C1"].assignment[0][1]) == 3
    assert len(assignment2["C2"].assignment[0][1]) == 3
    assert all(
        partition in assignment2["C1"].assignment[0][1]
        for partition in assignment1["C1"].assignment[0][1]
    )
    assert all(
        partition in assignment2["C2"].assignment[0][1]
        for partition in assignment1["C2"].assignment[0][1]
    )
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()

    member_metadata = {
        "C2": StickyPartitionAssignor._metadata(
            {"t"}, assignment2["C2"].partitions(), 2
        ),
        "C3": StickyPartitionAssignor._metadata(
            {"t"}, assignment1["C3"].partitions(), 1
        ),
    }

    assignment3 = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance({"C2": {"t"}, "C3": {"t"}}, assignment3)
    assert len(assignment3["C2"].assignment[0][1]) == 3
    assert len(assignment3["C3"].assignment[0][1]) == 3
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def test_assignment_with_multiple_generations2(mocker: MockerFixture) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1, 2, 3, 4, 5})

    member_metadata = {
        "C1": StickyPartitionAssignor._metadata({"t"}, []),
        "C2": StickyPartitionAssignor._metadata({"t"}, []),
        "C3": StickyPartitionAssignor._metadata({"t"}, []),
    }

    assignment1 = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance({"C1": {"t"}, "C2": {"t"}, "C3": {"t"}}, assignment1)
    assert len(assignment1["C1"].assignment[0][1]) == 2
    assert len(assignment1["C2"].assignment[0][1]) == 2
    assert len(assignment1["C3"].assignment[0][1]) == 2

    member_metadata = {
        "C2": StickyPartitionAssignor._metadata(
            {"t"}, assignment1["C2"].partitions(), 1
        ),
    }

    assignment2 = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance({"C2": {"t"}}, assignment2)
    assert len(assignment2["C2"].assignment[0][1]) == 6
    assert all(
        partition in assignment2["C2"].assignment[0][1]
        for partition in assignment1["C2"].assignment[0][1]
    )
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()

    member_metadata = {
        "C1": StickyPartitionAssignor._metadata(
            {"t"}, assignment1["C1"].partitions(), 1
        ),
        "C2": StickyPartitionAssignor._metadata(
            {"t"}, assignment2["C2"].partitions(), 2
        ),
        "C3": StickyPartitionAssignor._metadata(
            {"t"}, assignment1["C3"].partitions(), 1
        ),
    }

    assignment3 = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance({"C1": {"t"}, "C2": {"t"}, "C3": {"t"}}, assignment3)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()
    assert set(assignment3["C1"].assignment[0][1]) == set(
        assignment1["C1"].assignment[0][1]
    )
    assert set(assignment3["C2"].assignment[0][1]) == set(
        assignment1["C2"].assignment[0][1]
    )
    assert set(assignment3["C3"].assignment[0][1]) == set(
        assignment1["C3"].assignment[0][1]
    )


@pytest.mark.parametrize("execution_number", range(50))
def test_assignment_with_conflicting_previous_generations(
    mocker: MockerFixture, execution_number: int
) -> None:
    cluster = create_cluster(mocker, topics={"t"}, topics_partitions={0, 1, 2, 3, 4, 5})

    member_assignments = {
        "C1": [TopicPartition("t", p) for p in [0, 1, 4]],
        "C2": [TopicPartition("t", p) for p in [0, 2, 3]],
        "C3": [TopicPartition("t", p) for p in [3, 4, 5]],
    }
    member_generations = {
        "C1": 1,
        "C2": 1,
        "C3": 2,
    }
    member_metadata: dict[str, ConsumerProtocolMemberMetadata] = {}
    for member in member_assignments:
        member_metadata[member] = StickyPartitionAssignor._metadata(
            {"t"}, member_assignments[member], member_generations[member]
        )

    assignment = StickyPartitionAssignor.assign(cluster, member_metadata)
    verify_validity_and_balance({"C1": {"t"}, "C2": {"t"}, "C3": {"t"}}, assignment)
    assert StickyPartitionAssignor._latest_partition_movements
    assert StickyPartitionAssignor._latest_partition_movements.are_sticky()


def make_member_metadata(
    subscriptions: dict[str, set[str]],
) -> dict[str, ConsumerProtocolMemberMetadata]:
    member_metadata: dict[str, ConsumerProtocolMemberMetadata] = {}
    for member, topics in subscriptions.items():
        member_metadata[member] = StickyPartitionAssignor._metadata(topics, [])
    return member_metadata


def assert_assignment(
    result_assignment: dict[str, ConsumerProtocolMemberAssignment],
    expected_assignment: dict[str, ConsumerProtocolMemberAssignment],
) -> None:
    assert result_assignment == expected_assignment
    assert set(result_assignment) == set(expected_assignment)
    for member in result_assignment:
        assert (
            result_assignment[member].encode() == expected_assignment[member].encode()
        )


def verify_validity_and_balance(
    subscriptions: dict[str, set[str]],
    assignment: dict[str, ConsumerProtocolMemberAssignment],
) -> None:
    """
    Verifies that the given assignment is valid with respect to the given subscriptions
    Validity requirements:
    - each consumer is subscribed to topics of all partitions assigned to it, and
    - each partition is assigned to no more than one consumer
    Balance requirements:
    - the assignment is fully balanced (the numbers of topic partitions assigned to
    consumers differ by at most one), or
    - there is no topic partition that can be moved from one consumer to another with
    2+ fewer topic partitions

    :param subscriptions  topic subscriptions of each consumer
    :param assignment: given assignment for balance check
    """
    assert subscriptions.keys() == assignment.keys()

    consumers = sorted(assignment.keys())
    for i in range(len(consumers)):
        consumer = consumers[i]
        partitions = assignment[consumer].partitions()
        for partition in partitions:
            assert partition.topic in subscriptions[consumer], (
                f"Error: Partition {partition} is assigned to consumer {consumers[i]}, "
                f"but it is not subscribed to topic {partition.topic}\n"
                f"Subscriptions: {subscriptions}\n"
                f"Assignments: {assignment}"
            )
        if i == len(consumers) - 1:
            continue

        for j in range(i + 1, len(consumers)):
            other_consumer = consumers[j]
            other_partitions = assignment[other_consumer].partitions()
            partitions_intersection = set(partitions).intersection(
                set(other_partitions)
            )
            assert partitions_intersection == set(), (
                f"Error: Consumers {consumer} and {other_consumer} have common "
                f"partitions assigned to them: {partitions_intersection}\n"
                f"Subscriptions: {subscriptions}\n"
                f"Assignments: {assignment}"
            )

            if abs(len(partitions) - len(other_partitions)) <= 1:
                continue

            assignments_by_topic = group_partitions_by_topic(partitions)
            other_assignments_by_topic = group_partitions_by_topic(other_partitions)
            if len(partitions) > len(other_partitions):
                for topic in assignments_by_topic:
                    assert topic not in other_assignments_by_topic, (
                        f"Error: Some partitions can be moved from {consumer} "
                        f"({len(partitions)} partitions) to {other_consumer} "
                        f"({len(other_partitions)} partitions) "
                        "to achieve a better balance\n"
                        f"Subscriptions: {subscriptions}\n"
                        f"Assignments: {assignment}"
                    )
            if len(other_partitions) > len(partitions):
                for topic in other_assignments_by_topic:
                    assert topic not in assignments_by_topic, (
                        f"Error: Some partitions can be moved from {other_consumer} "
                        f"({len(other_partitions)} partitions) to {consumer} "
                        f"({len(partitions)} partitions) to achieve a better balance\n"
                        f"Subscriptions: {subscriptions}\n"
                        f"Assignments: {assignment}"
                    )


def group_partitions_by_topic(
    partitions: Sequence[TopicPartition],
) -> dict[str, set[int]]:
    result = defaultdict(set)
    for p in partitions:
        result[p.topic].add(p.partition)
    return result
