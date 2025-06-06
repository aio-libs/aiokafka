from aiokafka.cluster import ClusterMetadata
from aiokafka.protocol.metadata import MetadataResponse


def test_empty_broker_list():
    cluster = ClusterMetadata()
    assert len(cluster.brokers()) == 0

    cluster.update_metadata(
        MetadataResponse[0]([(0, "foo", 12), (1, "bar", 34)], []),
    )
    assert len(cluster.brokers()) == 2

    # empty broker list response should be ignored
    cluster.update_metadata(
        MetadataResponse[0](
            [],  # empty brokers
            [(17, "foo", []), (17, "bar", [])],  # topics w/ error
        )
    )
    assert len(cluster.brokers()) == 2


def test_request_update_expecting_success():
    cluster = ClusterMetadata()
    updated_cluster = cluster.request_update()
    cluster.update_metadata(
        MetadataResponse[0]([(0, "foo", 12), (1, "bar", 34)], []),
    )
    assert updated_cluster.result() == cluster


def test_request_update_expecting_failure():
    cluster = ClusterMetadata()
    updated_cluster = cluster.request_update()
    test_metadata = MetadataResponse[0](
        [],  # empty brokers
        [(17, "foo", []), (17, "bar", [])],  # topics w/ error
    )
    cluster.update_metadata(test_metadata)
    assert updated_cluster.exception() is not None
