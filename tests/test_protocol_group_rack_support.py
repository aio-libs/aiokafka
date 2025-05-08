from aiokafka.protocol.group import JoinGroupRequest, JoinGroupRequest_v8


def test_join_group_request_v8_in_list():
    # Ensure version 8 is present in the request versions
    versions = [req.API_VERSION for req in JoinGroupRequest]
    assert 8 in versions, "JoinGroupRequest version 8 missing"


def test_join_group_request_v8_schema_names():
    # The last field in the schema should be 'rack_id'
    names = JoinGroupRequest_v8.SCHEMA.names
    assert (
        "rack_id" in names
    ), f"Expected last schema field to be 'rack_id', got {names[-1]}"


def test_to_object_includes_rack_id():
    # Instantiate a v8 join request with dummy data
    group = "test-group"
    session = 1234
    rebalance = 5678
    member = "member-1"
    instance = "instance-x"
    protocol_type = "consumer"
    protocols = [("proto1", b"meta1"), ("proto2", b"meta2")]
    rack_id = "rack-A"
    req = JoinGroupRequest_v8(
        group,
        session,
        rebalance,
        member,
        instance,
        protocol_type,
        protocols,
        rack_id,
    )
    obj = req.to_object()
    # Verify rack_id is included and correct
    assert obj.get("rack_id") == rack_id
    # Verify other fields round-trip
    assert obj.get("group") == group
    assert obj.get("session_timeout") == session
    assert obj.get("rebalance_timeout") == rebalance
    assert obj.get("member_id") == member
    assert obj.get("group_instance_id") == instance
    assert obj.get("protocol_type") == protocol_type
    # Protocols should be list of dicts with these keys
    protos = obj.get("group_protocols")
    assert isinstance(protos, list) and len(protos) == 2
    for i, entry in enumerate(protos):
        assert entry["protocol_name"] == protocols[i][0]
        assert entry["protocol_metadata"] == protocols[i][1]
