import pytest

from aiokafka.cluster import ClusterMetadata
from aiokafka.partitioner import DefaultPartitioner, murmur2
from aiokafka.producer import AIOKafkaProducer
from aiokafka.protocol.metadata import MetadataResponse_v0


def test_default_partitioner():
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(100))
    # partitioner should return the same partition for the same key
    p1 = partitioner(b"foo", all_partitions, available)
    p2 = partitioner(b"foo", all_partitions, available)
    assert p1 == p2
    assert p1 in all_partitions

    # when key is None, choose one of available partitions
    assert partitioner(None, all_partitions, [123]) == 123

    # with fallback to all_partitions
    assert partitioner(None, all_partitions, []) in all_partitions


@pytest.mark.parametrize(
    "bytes_payload,partition_number",
    [
        (b"", 681),
        (b"a", 524),
        (b"ab", 434),
        (b"abc", 107),
        (b"123456789", 566),
        (b"\x00 ", 742),
    ],
)
def test_murmur2_java_compatibility(bytes_payload, partition_number):
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(1000))
    # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
    assert partitioner(bytes_payload, all_partitions, available) == partition_number


def test_murmur2_not_ascii():
    # Verify no regression of murmur2() bug encoding py2 bytes that don't ascii encode
    murmur2(b"\xa4")
    murmur2(b"\x81" * 1000)


def _cluster_with_partition_order(topic, order):
    # Build a real cluster from a MetadataResponse listing the partitions in
    # `order`, so the ordering the producer sees is the one the cluster derives
    # from the broker reply rather than one a stub asserts into existence.
    cluster = ClusterMetadata()
    cluster.update_metadata(
        MetadataResponse_v0(
            [(0, "host", 9092)],
            [(0, topic, [(0, p, 0, [0], [0]) for p in order])],
        )
    )
    return cluster


class _FakeProducer:
    # _partition() only touches these two attributes, so we can drive the real
    # method without spinning up a broker.
    def __init__(self, cluster):
        self._metadata = cluster
        self._partitioner = DefaultPartitioner()


def test_partition_selection_is_order_independent():
    # Nothing requires brokers to report partitions in a fixed order, and the
    # default partitioner indexes murmur2(key) % len(all_partitions) into the
    # list it is handed, so an unstable order maps the same key to different
    # partitions (see issue #1127). A given key must pick the same partition no
    # matter what order the metadata arrived in.
    topic = "t"
    key = b"user-42"
    front = [7, 3, 40, 1, 22, 0, 15]
    orders = [
        list(range(50)),
        list(reversed(range(50))),
        front + [p for p in range(50) if p not in front],
    ]
    chosen = set()
    for order in orders:
        producer = _FakeProducer(_cluster_with_partition_order(topic, order))
        chosen.add(
            AIOKafkaProducer._partition(producer, topic, None, key, None, key, None)
        )
    assert len(chosen) == 1
