from aiokafka.cluster import ClusterMetadata
from aiokafka.protocol.metadata import MetadataResponse_v0


def test_empty_broker_list():
    cluster = ClusterMetadata()
    assert len(cluster.brokers()) == 0

    cluster.update_metadata(
        MetadataResponse_v0([(0, "foo", 12), (1, "bar", 34)], []),
    )
    assert len(cluster.brokers()) == 2

    # empty broker list response should be ignored
    cluster.update_metadata(
        MetadataResponse_v0(
            [],  # empty brokers
            [(17, "foo", []), (17, "bar", [])],  # topics w/ error
        )
    )
    assert len(cluster.brokers()) == 2


def test_request_update_expecting_success():
    cluster = ClusterMetadata()
    updated_cluster = cluster.request_update()
    cluster.update_metadata(
        MetadataResponse_v0([(0, "foo", 12), (1, "bar", 34)], []),
    )
    assert updated_cluster.result() == cluster


def test_request_update_expecting_failure():
    cluster = ClusterMetadata()
    updated_cluster = cluster.request_update()
    test_metadata = MetadataResponse_v0(
        [],  # empty brokers
        [(17, "foo", []), (17, "bar", [])],  # topics w/ error
    )
    cluster.update_metadata(test_metadata)
    assert updated_cluster.exception() is not None


def test_ordered_partitions_for_topic_is_sorted():
    # Brokers are not required to report partitions in any particular order, so
    # the ordered accessors must not inherit the reply order (issue #1127).
    cluster = ClusterMetadata()
    out_of_order = [3, 0, 4, 1, 2]
    cluster.update_metadata(
        MetadataResponse_v0(
            [(0, "host", 9092)],
            [(0, "topic-1", [(0, p, 0, [0], [0]) for p in out_of_order])],
        )
    )

    assert cluster.ordered_partitions_for_topic("topic-1") == [0, 1, 2, 3, 4]
    assert cluster.ordered_available_partitions_for_topic("topic-1") == [0, 1, 2, 3, 4]
    # the set-returning accessors keep their existing contract
    assert cluster.partitions_for_topic("topic-1") == {0, 1, 2, 3, 4}
    assert cluster.available_partitions_for_topic("topic-1") == {0, 1, 2, 3, 4}

    assert cluster.ordered_partitions_for_topic("no-such-topic") is None
    assert cluster.ordered_available_partitions_for_topic("no-such-topic") is None


def test_ordered_available_partitions_for_topic_skips_leaderless():
    # A partition without a leader is excluded, and the rest stay sorted.
    cluster = ClusterMetadata()
    cluster.update_metadata(
        MetadataResponse_v0(
            [(0, "host", 9092)],
            [
                (
                    0,
                    "topic-1",
                    [
                        (0, 3, 0, [0], [0]),
                        (0, 1, -1, [], []),
                        (0, 2, 0, [0], [0]),
                        (0, 0, 0, [0], [0]),
                    ],
                )
            ],
        )
    )

    assert cluster.ordered_partitions_for_topic("topic-1") == [0, 1, 2, 3]
    assert cluster.ordered_available_partitions_for_topic("topic-1") == [0, 2, 3]
