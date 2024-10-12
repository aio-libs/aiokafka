from aiokafka.cluster import ClusterMetadata
from aiokafka.protocol.metadata import MetadataResponse
from tests._testutil import run_until_complete


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

async def test_request_update(self):
    cluster = ClusterMetadata()
    updated_cluster = await cluster.request_update()
    assert updated_cluster == cluster
