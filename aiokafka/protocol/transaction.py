from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import (
    Int16, Int32, Int64, Schema, String, Array, Boolean
)


class InitProducerIdResponse_v0(Response):
    API_KEY = 22
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
    )


class InitProducerIdRequest_v0(Request):
    API_KEY = 22
    API_VERSION = 0
    RESPONSE_TYPE = InitProducerIdResponse_v0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('transaction_timeout_ms', Int32)
    )


class AddPartitionsToTxnResponse_v0(Response):
    API_KEY = 24
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('errors', Array(
            ('topic', String('utf-8')),
            ('partition_errors', Array(
                ('partition', Int32),
                ('error_code', Int16)))))
    )


class AddPartitionsToTxnRequest_v0(Request):
    API_KEY = 24
    API_VERSION = 0
    RESPONSE_TYPE = AddPartitionsToTxnResponse_v0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))))
    )


class EndTxnResponse_v0(Response):
    API_KEY = 26
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16)
    )


class EndTxnRequest_v0(Request):
    API_KEY = 26
    API_VERSION = 0
    RESPONSE_TYPE = EndTxnResponse_v0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
        ('transaction_result', Boolean)
    )


InitProducerIdRequest = [
    InitProducerIdRequest_v0
]
InitProducerIdResponse = [
    InitProducerIdResponse_v0
]

AddPartitionsToTxnRequest = [
    AddPartitionsToTxnRequest_v0
]
AddPartitionsToTxnResponse = [
    AddPartitionsToTxnResponse_v0
]

EndTxnRequest = [
    EndTxnRequest_v0
]

EndTxnResponse = [
    EndTxnResponse_v0
]
