import pytest

from aiokafka.errors import IncompatibleBrokerVersion
from aiokafka.protocol.admin import (
    AlterConfigsRequest,
    AlterConfigsRequest_v0,
    AlterConfigsRequest_v1,
    AlterPartitionReassignmentsRequest,
    AlterPartitionReassignmentsRequest_v0,
    ApiVersionRequest,
    ApiVersionRequest_v0,
    ApiVersionRequest_v1,
    ApiVersionRequest_v2,
    CreateAclsRequest,
    CreateAclsRequest_v0,
    CreateAclsRequest_v1,
    CreatePartitionsRequest,
    CreatePartitionsRequest_v0,
    CreatePartitionsRequest_v1,
    CreateTopicsRequest,
    CreateTopicsRequest_v0,
    CreateTopicsRequest_v1,
    DeleteAclsRequest,
    DeleteAclsRequest_v0,
    DeleteAclsRequest_v1,
    DeleteGroupsRequest,
    DeleteGroupsRequest_v0,
    DeleteRecordsRequest,
    DeleteRecordsRequest_v0,
    DeleteRecordsRequest_v1,
    DeleteRecordsRequest_v2,
    DeleteTopicsRequest,
    DeleteTopicsRequest_v0,
    DescribeAclsRequest,
    DescribeAclsRequest_v0,
    DescribeAclsRequest_v1,
    DescribeClientQuotasRequest,
    DescribeClientQuotasRequest_v0,
    DescribeConfigsRequest,
    DescribeConfigsRequest_v0,
    DescribeConfigsRequest_v1,
    DescribeConfigsRequest_v2,
    DescribeGroupsRequest,
    DescribeGroupsRequest_v0,
    DescribeGroupsRequest_v3,
    ListGroupsRequest,
    ListGroupsRequest_v0,
    ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsRequest_v0,
    SaslAuthenticateRequest,
    SaslAuthenticateRequest_v0,
    SaslAuthenticateRequest_v1,
    SaslHandShakeRequest,
    SaslHandShakeRequest_v0,
)
from aiokafka.protocol.api import Request, RequestStruct
from aiokafka.protocol.commit import (
    OffsetCommitRequest,
    OffsetCommitRequest_v2,
    OffsetCommitRequest_v3,
    OffsetFetchRequest,
    OffsetFetchRequest_v1,
    OffsetFetchRequest_v2,
    OffsetFetchRequest_v3,
)
from aiokafka.protocol.coordination import (
    FindCoordinatorRequest,
    FindCoordinatorRequest_v0,
    FindCoordinatorRequest_v1,
)
from aiokafka.protocol.fetch import (
    FetchRequest,
    FetchRequest_v1,
    FetchRequest_v3,
    FetchRequest_v4,
)
from aiokafka.protocol.group import (
    HeartbeatRequest,
    HeartbeatRequest_v0,
    HeartbeatRequest_v1,
    JoinGroupRequest,
    JoinGroupRequest_v2,
    JoinGroupRequest_v5,
    LeaveGroupRequest,
    LeaveGroupRequest_v0,
    LeaveGroupRequest_v1,
    SyncGroupRequest,
    SyncGroupRequest_v1,
    SyncGroupRequest_v3,
)
from aiokafka.protocol.metadata import (
    MetadataRequest,
    MetadataRequest_v0,
    MetadataRequest_v1,
    MetadataRequest_v4,
    MetadataRequest_v5,
)
from aiokafka.protocol.offset import (
    OffsetRequest,
    OffsetRequest_v0,
    OffsetRequest_v1,
    OffsetRequest_v2,
    OffsetRequest_v3,
)
from aiokafka.protocol.produce import (
    ProduceRequest,
    ProduceRequest_v0,
    ProduceRequest_v3,
)
from aiokafka.protocol.transaction import (
    AddOffsetsToTxnRequest,
    AddOffsetsToTxnRequest_v0,
    AddPartitionsToTxnRequest,
    AddPartitionsToTxnRequest_v0,
    EndTxnRequest,
    EndTxnRequest_v0,
    InitProducerIdRequest,
    InitProducerIdRequest_v0,
    TxnOffsetCommitRequest,
    TxnOffsetCommitRequest_v0,
)


class Versions:
    def __init__(self, expected, min_version=None, max_version=None):
        self.versions = (
            (min_version or 0, max_version or 0)
            if min_version is not None or max_version is not None
            else None
        )
        self.expected = expected


cases = [
    (
        ApiVersionRequest(),
        [
            Versions(expected=ApiVersionRequest_v0()),
            Versions(max_version=0, expected=ApiVersionRequest_v0()),
            Versions(max_version=1, expected=ApiVersionRequest_v1()),
            Versions(max_version=2, expected=ApiVersionRequest_v2()),
        ],
    ),
    (
        CreateTopicsRequest(
            create_topic_requests=[("topic1", 3, 1, {}, {}), ("topic2", 3, 1, {}, {})],
            timeout=1000,
            validate_only=False,
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=CreateTopicsRequest_v0(
                    [("topic1", 3, 1, {}, {}), ("topic2", 3, 1, {}, {})], 1000
                ),
            ),
            Versions(
                max_version=1,
                expected=CreateTopicsRequest_v1(
                    [("topic1", 3, 1, {}, {}), ("topic2", 3, 1, {}, {})], 1000, False
                ),
            ),
        ],
    ),
    (
        CreateTopicsRequest(
            create_topic_requests=[("topic1", 3, 1, {}, {})],
            timeout=1000,
            validate_only=True,
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=IncompatibleBrokerVersion),
            Versions(
                max_version=1,
                expected=CreateTopicsRequest_v1([("topic1", 3, 1, {}, {})], 1000, True),
            ),
        ],
    ),
    (
        DeleteTopicsRequest(topics=["a", "b"], timeout=1000),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=DeleteTopicsRequest_v0(["a", "b"], 1000)),
        ],
    ),
    (
        DescribeAclsRequest(
            resource_type=2,
            resource_name="res",
            resource_pattern_type_filter=3,
            principal="User:alice",
            host="*",
            operation=1,
            permission_type=3,
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=DescribeAclsRequest_v0(2, "res", "User:alice", "*", 1, 3),
            ),
            Versions(
                max_version=1,
                expected=DescribeAclsRequest_v1(2, "res", 3, "User:alice", "*", 1, 3),
            ),
        ],
    ),
    (
        CreateAclsRequest(
            resource_type=2,
            resource_name="res",
            resource_pattern_type_filter=3,
            principal="User:alice",
            host="*",
            operation=1,
            permission_type=3,
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=CreateAclsRequest_v0([(2, "res", "User:alice", "*", 1, 3)]),
            ),
            Versions(
                max_version=1,
                expected=CreateAclsRequest_v1([(2, "res", 3, "User:alice", "*", 1, 3)]),
            ),
        ],
    ),
    (
        DeleteAclsRequest(
            resource_type=2,
            resource_name="res",
            resource_pattern_type_filter=3,
            principal="User:alice",
            host="*",
            operation=1,
            permission_type=3,
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=DeleteAclsRequest_v0([(2, "res", "User:alice", "*", 1, 3)]),
            ),
            Versions(
                max_version=1,
                expected=DeleteAclsRequest_v1([(2, "res", 3, "User:alice", "*", 1, 3)]),
            ),
        ],
    ),
    (
        ListGroupsRequest(),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=ListGroupsRequest_v0()),
        ],
    ),
    (
        DescribeGroupsRequest(groups=["a", "b"]),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=DescribeGroupsRequest_v0(["a", "b"])),
        ],
    ),
    (
        DescribeGroupsRequest(groups=["a", "b"], include_authorized_operations=True),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=2, expected=IncompatibleBrokerVersion),
            Versions(
                max_version=3, expected=DescribeGroupsRequest_v3(["a", "b"], True)
            ),
        ],
    ),
    (
        SaslHandShakeRequest("PLAIN"),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=SaslHandShakeRequest_v0("PLAIN")),
        ],
    ),
    (
        AlterConfigsRequest(resources=[], validate_only=True),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=AlterConfigsRequest_v0([], True)),
            Versions(max_version=1, expected=AlterConfigsRequest_v1([], True)),
        ],
    ),
    (
        DescribeConfigsRequest(resources=[(2, "res", ["k"])], include_synonyms=False),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0, expected=DescribeConfigsRequest_v0([(2, "res", ["k"])])
            ),
            Versions(
                max_version=1,
                expected=DescribeConfigsRequest_v1([(2, "res", ["k"])], False),
            ),
        ],
    ),
    (
        DescribeConfigsRequest(resources=[(2, "res", ["k"])], include_synonyms=True),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=IncompatibleBrokerVersion),
            Versions(
                max_version=1,
                expected=DescribeConfigsRequest_v1([(2, "res", ["k"])], True),
            ),
            Versions(
                max_version=2,
                expected=DescribeConfigsRequest_v2([(2, "res", ["k"])], True),
            ),
        ],
    ),
    (
        SaslAuthenticateRequest(payload=b"abc"),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=SaslAuthenticateRequest_v0(b"abc")),
            Versions(max_version=1, expected=SaslAuthenticateRequest_v1(b"abc")),
        ],
    ),
    (
        CreatePartitionsRequest(
            topic_partitions=[("t", (1, []))], timeout=10, validate_only=False
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=CreatePartitionsRequest_v0([("t", (1, []))], 10, False),
            ),
            Versions(
                max_version=1,
                expected=CreatePartitionsRequest_v1([("t", (1, []))], 10, False),
            ),
        ],
    ),
    (
        DeleteGroupsRequest(group_names=["g1", "g2"]),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=DeleteGroupsRequest_v0(["g1", "g2"])),
        ],
    ),
    (
        DescribeClientQuotasRequest(components=[], strict=True),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=DescribeClientQuotasRequest_v0([], True)),
        ],
    ),
    (
        AlterPartitionReassignmentsRequest(timeout_ms=100, topics=[]),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=AlterPartitionReassignmentsRequest_v0(100, []),
            ),
        ],
    ),
    (
        ListPartitionReassignmentsRequest(timeout_ms=200, topics=[]),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=ListPartitionReassignmentsRequest_v0(200, []),
            ),
        ],
    ),
    (
        DeleteRecordsRequest(topics=[("t1", [(0, 123)])], timeout_ms=50),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0,
                expected=DeleteRecordsRequest_v0([("t1", [(0, 123)])], 50),
            ),
            Versions(
                max_version=1,
                expected=DeleteRecordsRequest_v1([("t1", [(0, 123)])], 50),
            ),
            Versions(
                max_version=2,
                expected=DeleteRecordsRequest_v2([("t1", [(0, 123)])], 50),
            ),
        ],
    ),
    (
        OffsetCommitRequest(
            consumer_group="grp",
            consumer_group_generation_id=1,
            consumer_id="cid",
            retention_time=1000,
            topics=[("t1", [(0, 10, "md")])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=2,
                expected=OffsetCommitRequest_v2(
                    "grp", 1, "cid", 1000, [("t1", [(0, 10, "md")])]
                ),
            ),
            Versions(
                max_version=3,
                expected=OffsetCommitRequest_v3(
                    "grp", 1, "cid", 1000, [("t1", [(0, 10, "md")])]
                ),
            ),
        ],
    ),
    (
        OffsetFetchRequest(
            consumer_group="grp",
            partitions=[("t1", [0, 1])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=1,
                expected=OffsetFetchRequest_v1("grp", [("t1", [0, 1])]),
            ),
            Versions(
                max_version=2,
                expected=OffsetFetchRequest_v2("grp", [("t1", [0, 1])]),
            ),
            Versions(
                max_version=3,
                expected=OffsetFetchRequest_v3("grp", [("t1", [0, 1])]),
            ),
        ],
    ),
    (
        OffsetFetchRequest(
            consumer_group="grp",
            partitions=None,
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=1, expected=IncompatibleBrokerVersion),
            Versions(max_version=2, expected=OffsetFetchRequest_v2("grp", None)),
            Versions(max_version=3, expected=OffsetFetchRequest_v3("grp", None)),
        ],
    ),
    (
        MetadataRequest(topics=None, allow_auto_topic_creation=None),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=MetadataRequest_v0([])),
            Versions(max_version=1, expected=MetadataRequest_v1(None)),
            Versions(max_version=4, expected=MetadataRequest_v4(None, True)),
            Versions(max_version=5, expected=MetadataRequest_v5(None, True)),
            Versions(min_version=6, expected=NotImplementedError),
        ],
    ),
    (
        MetadataRequest(topics=["t1", "t2"], allow_auto_topic_creation=False),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=1, expected=MetadataRequest_v1(["t1", "t2"])),
            Versions(max_version=4, expected=MetadataRequest_v4(["t1", "t2"], False)),
            Versions(max_version=5, expected=MetadataRequest_v5(["t1", "t2"], False)),
        ],
    ),
    (
        FindCoordinatorRequest(coordinator_key="grp", coordinator_type=0),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=FindCoordinatorRequest_v0("grp")),
            Versions(max_version=1, expected=FindCoordinatorRequest_v1("grp", 0)),
        ],
    ),
    (
        FindCoordinatorRequest(coordinator_key="grp", coordinator_type=1),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=IncompatibleBrokerVersion),
            Versions(max_version=1, expected=FindCoordinatorRequest_v1("grp", 1)),
        ],
    ),
    (
        JoinGroupRequest(
            group="g",
            session_timeout=10000,
            rebalance_timeout=20000,
            member_id="",
            group_instance_id="",
            protocol_type="consumer",
            group_protocols=[("range", b"meta")],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=2,
                expected=JoinGroupRequest_v2(
                    "g",
                    10000,
                    20000,
                    "",
                    "consumer",
                    [("range", b"meta")],
                ),
            ),
            Versions(
                max_version=5,
                expected=JoinGroupRequest_v5(
                    "g",
                    10000,
                    20000,
                    "",
                    "",
                    "consumer",
                    [("range", b"meta")],
                ),
            ),
        ],
    ),
    (
        SyncGroupRequest(
            group="g",
            generation_id=1,
            member_id="m",
            group_instance_id="",
            group_assignment=[("m", b"assign")],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=1,
                expected=SyncGroupRequest_v1("g", 1, "m", [("m", b"assign")]),
            ),
            Versions(
                max_version=3,
                expected=SyncGroupRequest_v3("g", 1, "m", "", [("m", b"assign")]),
            ),
        ],
    ),
    (
        HeartbeatRequest(group="g", generation_id=1, member_id="m"),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=HeartbeatRequest_v0("g", 1, "m")),
            Versions(max_version=1, expected=HeartbeatRequest_v1("g", 1, "m")),
        ],
    ),
    (
        LeaveGroupRequest(group="g", member_id="m"),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=LeaveGroupRequest_v0("g", "m")),
            Versions(max_version=1, expected=LeaveGroupRequest_v1("g", "m")),
        ],
    ),
    (
        ProduceRequest(
            transactional_id=None,
            required_acks=1,
            timeout=100,
            topics=[("t", [(0, b"data")])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=2,
                expected=ProduceRequest_v0(1, 100, [("t", [(0, b"data")])]),
            ),
            Versions(
                max_version=3,
                expected=ProduceRequest_v3(None, 1, 100, [("t", [(0, b"data")])]),
            ),
        ],
    ),
    (
        ProduceRequest(
            transactional_id="tx",
            required_acks=1,
            timeout=100,
            topics=[("t", [(0, b"data")])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=2, expected=IncompatibleBrokerVersion),
            Versions(
                max_version=3,
                expected=ProduceRequest_v3("tx", 1, 100, [("t", [(0, b"data")])]),
            ),
        ],
    ),
    (
        OffsetRequest(
            replica_id=-1,
            isolation_level=0,
            topics=[("t", [(0, -1)])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=0, expected=OffsetRequest_v0(-1, [("t", [(0, -1, 1)])])
            ),
            Versions(max_version=1, expected=OffsetRequest_v1(-1, [("t", [(0, -1)])])),
            Versions(
                max_version=2, expected=OffsetRequest_v2(-1, 0, [("t", [(0, -1)])])
            ),
            Versions(
                max_version=3, expected=OffsetRequest_v3(-1, 0, [("t", [(0, -1)])])
            ),
        ],
    ),
    (
        OffsetRequest(
            replica_id=-1,
            isolation_level=0,
            topics=[("t", [(0, 0)])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=0, expected=IncompatibleBrokerVersion),
            Versions(max_version=1, expected=OffsetRequest_v1(-1, [("t", [(0, 0)])])),
        ],
    ),
    (
        OffsetRequest(
            replica_id=-1,
            isolation_level=1,
            topics=[("t", [(0, -1)])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=1, expected=IncompatibleBrokerVersion),
            Versions(
                max_version=2, expected=OffsetRequest_v2(-1, 1, [("t", [(0, -1)])])
            ),
            Versions(
                max_version=3, expected=OffsetRequest_v3(-1, 1, [("t", [(0, -1)])])
            ),
        ],
    ),
    (
        FetchRequest(
            max_wait_time=10,
            min_bytes=1,
            max_bytes=1000,
            isolation_level=0,
            topics=[("t", [(0, 5, 100)])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(
                max_version=1,
                expected=FetchRequest_v1(-1, 10, 1, [("t", [(0, 5, 100)])]),
            ),
            Versions(
                max_version=3,
                expected=FetchRequest_v3(-1, 10, 1, 1000, [("t", [(0, 5, 100)])]),
            ),
            Versions(
                max_version=4,
                expected=FetchRequest_v4(-1, 10, 1, 1000, 0, [("t", [(0, 5, 100)])]),
            ),
        ],
    ),
    (
        FetchRequest(
            max_wait_time=10,
            min_bytes=1,
            max_bytes=1000,
            isolation_level=1,
            topics=[("t", [(0, 5, 100)])],
        ),
        [
            Versions(expected=IncompatibleBrokerVersion),
            Versions(max_version=3, expected=IncompatibleBrokerVersion),
            Versions(
                max_version=4,
                expected=FetchRequest_v4(-1, 10, 1, 1000, 1, [("t", [(0, 5, 100)])]),
            ),
        ],
    ),
    (
        InitProducerIdRequest(transactional_id="tx", transaction_timeout_ms=1000),
        [
            Versions(max_version=0, expected=InitProducerIdRequest_v0("tx", 1000)),
        ],
    ),
    (
        AddPartitionsToTxnRequest(
            transactional_id="tx",
            producer_id=1,
            producer_epoch=1,
            topics=[("t", [0, 1])],
        ),
        [
            Versions(
                max_version=0,
                expected=AddPartitionsToTxnRequest_v0("tx", 1, 1, [("t", [0, 1])]),
            ),
        ],
    ),
    (
        AddOffsetsToTxnRequest(
            transactional_id="tx",
            producer_id=1,
            producer_epoch=1,
            group_id="g",
        ),
        [
            Versions(
                max_version=0, expected=AddOffsetsToTxnRequest_v0("tx", 1, 1, "g")
            ),
        ],
    ),
    (
        EndTxnRequest(
            transactional_id="tx",
            producer_id=1,
            producer_epoch=1,
            transaction_result=True,
        ),
        [
            Versions(max_version=0, expected=EndTxnRequest_v0("tx", 1, 1, True)),
        ],
    ),
    (
        TxnOffsetCommitRequest(
            transactional_id="tx",
            group_id="g",
            producer_id=1,
            producer_epoch=1,
            topics=[("t", [(0, 10, "md")])],
        ),
        [
            Versions(
                max_version=0,
                expected=TxnOffsetCommitRequest_v0(
                    "tx", "g", 1, 1, [("t", [(0, 10, "md")])]
                ),
            ),
        ],
    ),
]


def parameter_to_id(val):
    if isinstance(val, Request | RequestStruct):
        return val.__class__.__name__
    if isinstance(val, tuple):
        return str(val)
    return None


def prepare_cases():
    for request, versions in cases:
        for version in versions:
            yield request, version.versions, version.expected


@pytest.mark.parametrize(
    "api_request,versions,expected", prepare_cases(), ids=parameter_to_id
)
def test_api_request_prepare(api_request, versions, expected):
    versions = {api_request.API_KEY: versions} if versions else {}
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            api_request.prepare(versions)
    else:
        assert api_request.prepare(versions) == expected


def test_creating_invalid_struct_classes():
    with pytest.raises(TypeError):

        class MissingFieldRequest_v0(RequestStruct):
            pass


def test_creating_invalid_request_classes():
    with pytest.raises(TypeError):

        class MissingFieldRequest(Request):
            pass

    with pytest.raises(TypeError):

        class WrongInheritence(int, Request):
            pass
