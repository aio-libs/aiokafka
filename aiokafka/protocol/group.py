from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Bytes, Int16, Int32, Schema, String
from kafka.protocol.group import (
    JoinGroupRequest_v0,
    JoinGroupRequest_v1,
    JoinGroupRequest_v2,
    JoinGroupResponse_v0,
    JoinGroupResponse_v1,
    SyncGroupRequest_v0,
    SyncGroupRequest_v1,
    SyncGroupResponse_v0,
    SyncGroupResponse_v1
    )


class JoinGroupResponse_v5(Response):
    API_KEY = 11
    API_VERSION = 5
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("generation_id", Int32),
        ("group_protocol", String("utf-8")),
        ("leader_id", String("utf-8")),
        ("member_id", String("utf-8")),
        (
            "members",
            Array(
                ("member_id", String("utf-8")),
                ("group_instance_id", String("utf-8")),
                ("member_metadata", Bytes),
            ),
        ),
    )


class JoinGroupRequest_v5(Request):
    API_KEY = 11
    API_VERSION = 5
    RESPONSE_TYPE = JoinGroupResponse_v5
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("session_timeout", Int32),
        ("rebalance_timeout", Int32),
        ("member_id", String("utf-8")),
        ("group_instance_id", String("utf-8")),
        ("protocol_type", String("utf-8")),
        (
            "group_protocols",
            Array(("protocol_name", String("utf-8")), ("protocol_metadata", Bytes)),
        ),
    )
    UNKNOWN_MEMBER_ID = ""


JoinGroupRequest = [
    JoinGroupRequest_v0,
    JoinGroupRequest_v1,
    JoinGroupRequest_v2,
    JoinGroupRequest_v5,
]
JoinGroupResponse = [
    JoinGroupResponse_v0,
    JoinGroupResponse_v1,
    JoinGroupRequest_v2,
    JoinGroupResponse_v5,
]


class SyncGroupResponse_v3(Response):
    API_KEY = 14
    API_VERSION = 3
    SCHEMA = SyncGroupResponse_v1.SCHEMA


class SyncGroupRequest_v3(Request):
    API_KEY = 14
    API_VERSION = 3
    RESPONSE_TYPE = SyncGroupResponse_v3
    SCHEMA = Schema(
        ("group", String("utf-8")),
        ("generation_id", Int32),
        ("member_id", String("utf-8")),
        ("group_instance_id", String("utf-8")),
        (
            "group_assignment",
            Array(("member_id", String("utf-8")), ("member_metadata", Bytes)),
        ),
    )


SyncGroupRequest = [SyncGroupRequest_v0, SyncGroupRequest_v1, SyncGroupRequest_v3]
SyncGroupResponse = [SyncGroupResponse_v0, SyncGroupResponse_v1, SyncGroupResponse_v3]
