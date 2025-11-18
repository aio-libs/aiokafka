from .api import Request, RequestStruct, Response
from .types import Array, Boolean, Int16, Int32, Int64, Schema, String


class InitProducerIdResponse_v0(Response):
    API_KEY = 22
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        ("error_code", Int16),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
    )


class InitProducerIdRequest_v0(RequestStruct):
    API_KEY = 22
    API_VERSION = 0
    RESPONSE_TYPE = InitProducerIdResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")), ("transaction_timeout_ms", Int32)
    )


class InitProducerIdRequest(Request):
    API_KEY = 22
    CLASSES = [InitProducerIdRequest_v0]

    def __init__(self, transactional_id: str, transaction_timeout_ms: int):
        self._transactional_id = transactional_id
        self._transaction_timeout_ms = transaction_timeout_ms

    def build(self, request_struct_class: type[RequestStruct]) -> RequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._transaction_timeout_ms,
        )


class AddPartitionsToTxnResponse_v0(Response):
    API_KEY = 24
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "errors",
            Array(
                ("topic", String("utf-8")),
                (
                    "partition_errors",
                    Array(("partition", Int32), ("error_code", Int16)),
                ),
            ),
        ),
    )


class AddPartitionsToTxnRequest_v0(RequestStruct):
    API_KEY = 24
    API_VERSION = 0
    RESPONSE_TYPE = AddPartitionsToTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("topics", Array(("topic", String("utf-8")), ("partitions", Array(Int32)))),
    )


class AddPartitionsToTxnRequest(Request):
    API_KEY = 24
    CLASSES = [AddPartitionsToTxnRequest_v0]

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        topics: list[tuple[str, list[int]]],
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._topics = topics

    def build(self, request_struct_class: type[RequestStruct]) -> RequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._topics,
        )


class AddOffsetsToTxnResponse_v0(Response):
    API_KEY = 25
    API_VERSION = 0
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class AddOffsetsToTxnRequest_v0(RequestStruct):
    API_KEY = 25
    API_VERSION = 0
    RESPONSE_TYPE = AddOffsetsToTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("group_id", String("utf-8")),
    )


class AddOffsetsToTxnRequest(Request):
    API_KEY = 25
    CLASSES = [AddOffsetsToTxnRequest_v0]

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        group_id: str,
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._group_id = group_id

    def build(self, request_struct_class: type[RequestStruct]) -> RequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._group_id,
        )


class EndTxnResponse_v0(Response):
    API_KEY = 26
    API_VERSION = 0
    SCHEMA = Schema(("throttle_time_ms", Int32), ("error_code", Int16))


class EndTxnRequest_v0(RequestStruct):
    API_KEY = 26
    API_VERSION = 0
    RESPONSE_TYPE = EndTxnResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        ("transaction_result", Boolean),
    )


class EndTxnRequest(Request):
    API_KEY = 26
    CLASSES = [EndTxnRequest_v0]

    def __init__(
        self,
        transactional_id: str,
        producer_id: int,
        producer_epoch: int,
        transaction_result: bool,
    ):
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._transaction_result = transaction_result

    def build(self, request_struct_class: type[RequestStruct]) -> RequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._producer_id,
            self._producer_epoch,
            self._transaction_result,
        )


class TxnOffsetCommitResponse_v0(Response):
    API_KEY = 28
    API_VERSION = 0
    SCHEMA = Schema(
        ("throttle_time_ms", Int32),
        (
            "errors",
            Array(
                ("topic", String("utf-8")),
                (
                    "partition_errors",
                    Array(("partition", Int32), ("error_code", Int16)),
                ),
            ),
        ),
    )


class TxnOffsetCommitRequest_v0(RequestStruct):
    API_KEY = 28
    API_VERSION = 0
    RESPONSE_TYPE = TxnOffsetCommitResponse_v0
    SCHEMA = Schema(
        ("transactional_id", String("utf-8")),
        ("group_id", String("utf-8")),
        ("producer_id", Int64),
        ("producer_epoch", Int16),
        (
            "topics",
            Array(
                ("topic", String("utf-8")),
                (
                    "partitions",
                    Array(
                        ("partition", Int32),
                        ("offset", Int64),
                        ("metadata", String("utf-8")),
                    ),
                ),
            ),
        ),
    )


class TxnOffsetCommitRequest(Request):
    API_KEY = 28
    CLASSES = [TxnOffsetCommitRequest_v0]

    def __init__(
        self,
        transactional_id: str,
        group_id: str,
        producer_id: int,
        producer_epoch: int,
        topics: list[tuple[str, list[tuple[int, int, str]]]],
    ):
        self._transactional_id = transactional_id
        self._group_id = group_id
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._topics = topics

    def build(self, request_struct_class: type[RequestStruct]) -> RequestStruct:
        return request_struct_class(
            self._transactional_id,
            self._group_id,
            self._producer_id,
            self._producer_epoch,
            self._topics,
        )
