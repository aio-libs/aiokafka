from typing import Any, ClassVar, Generator

from typing_extensions import Buffer, Literal, Never

class LegacyRecord:
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

class LegacyRecordBatch:
    RECORD_OVERHEAD_V0: ClassVar[int]
    RECORD_OVERHEAD_V1: ClassVar[int]
    CODEC_MASK: ClassVar[int]
    CODEC_GZIP: ClassVar[int]
    CODEC_SNAPPY: ClassVar[int]
    CODEC_LZ4: ClassVar[int]

    is_control_batch: bool
    is_transactional: bool
    producer_id: int | None
    def __init__(self, buffer: Buffer, magic: int) -> None: ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...
    def __iter__(self) -> Generator[LegacyRecord, None, None]: ...

class LegacyRecordBatchBuilder:
    CODEC_MASK: ClassVar[int]
    CODEC_GZIP: ClassVar[int]
    CODEC_SNAPPY: ClassVar[int]
    CODEC_LZ4: ClassVar[int]

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
    def size_in_bytes(
        self, offset: Any, timestamp: Any, key: Buffer | None, value: Buffer | None
    ) -> int: ...
    @staticmethod
    def record_overhead(magic: int) -> int: ...
    def build(self) -> bytearray: ...

class LegacyRecordMetadata:
    offset: int
    crc: int
    size: int
    timestamp: int
    def __init__(self, offset: int, crc: int, size: int, timestamp: int) -> None: ...
