from typing import Any, Generator, final

from typing_extensions import Literal, Never

from aiokafka.record._protocols import (
    LegacyRecordBatchBuilderProtocol,
    LegacyRecordBatchProtocol,
    LegacyRecordMetadataProtocol,
    LegacyRecordProtocol,
)

@final
class LegacyRecord(LegacyRecordProtocol):
    offset: int
    attributes: int
    key: bytes | None
    value: bytes | None
    crc: int
    def __init__(
        self,
        offset: int,
        timestamp: int,
        attributes: int,
        key: bytes | None,
        value: bytes | None,
        crc: int,
    ) -> None: ...
    @property
    def headers(self) -> list[Never]: ...
    @property
    def timestamp(self) -> int | None: ...
    @property
    def timestamp_type(self) -> Literal[0, 1] | None: ...
    @property
    def checksum(self) -> int: ...

@final
class LegacyRecordBatch(LegacyRecordBatchProtocol):
    is_control_batch: bool
    is_transactional: bool
    producer_id: int | None
    def __init__(self, buffer, magic: int) -> None: ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...
    def __iter__(self) -> Generator[LegacyRecord, None, None]: ...

@final
class LegacyRecordBatchBuilder(LegacyRecordBatchBuilderProtocol):
    def __init__(self, magic: int, compression_type: int, batch_size: int) -> None: ...
    def append(
        self,
        offset: int,
        timestamp: int | None,
        key: bytes | None,
        value: bytes | None,
        headers: Any = None,
    ) -> LegacyRecordMetadata: ...
    def size(self) -> int: ...
    @staticmethod
    def record_overhead(magic: int) -> int: ...
    def build(self) -> bytearray: ...

@final
class LegacyRecordMetadata(LegacyRecordMetadataProtocol):
    offset: int
    crc: int
    size: int
    timestamp: int
    def __init__(self, offset: int, crc: int, size: int, timestamp: int) -> None: ...
