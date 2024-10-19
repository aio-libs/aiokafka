from collections.abc import Generator
from typing import Any, ClassVar, final

from typing_extensions import Buffer, Literal, Never

from aiokafka.record._protocols import (
    LegacyRecordBatchBuilderProtocol,
    LegacyRecordBatchProtocol,
    LegacyRecordMetadataProtocol,
    LegacyRecordProtocol,
)
from aiokafka.record._types import (
    CodecGzipT,
    CodecLz4T,
    CodecMaskT,
    CodecSnappyT,
    LegacyCompressionTypeT,
)

@final
class LegacyRecord(LegacyRecordProtocol):
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
    def offset(self) -> int: ...
    @property
    def key(self) -> bytes | None: ...
    @property
    def value(self) -> bytes | None: ...
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
    RECORD_OVERHEAD_V0: ClassVar[int]
    RECORD_OVERHEAD_V1: ClassVar[int]
    CODEC_MASK: ClassVar[CodecMaskT]
    CODEC_GZIP: ClassVar[CodecGzipT]
    CODEC_SNAPPY: ClassVar[CodecSnappyT]
    CODEC_LZ4: ClassVar[CodecLz4T]

    is_control_batch: bool
    is_transactional: bool
    producer_id: int | None
    def __init__(self, buffer: Buffer, magic: int) -> None: ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...
    def __iter__(self) -> Generator[LegacyRecord, None, None]: ...

@final
class LegacyRecordBatchBuilder(LegacyRecordBatchBuilderProtocol):
    CODEC_MASK: ClassVar[CodecMaskT]
    CODEC_GZIP: ClassVar[CodecGzipT]
    CODEC_SNAPPY: ClassVar[CodecSnappyT]
    CODEC_LZ4: ClassVar[CodecLz4T]

    def __init__(
        self, magic: int, compression_type: LegacyCompressionTypeT, batch_size: int
    ) -> None: ...
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

@final
class LegacyRecordMetadata(LegacyRecordMetadataProtocol):
    offset: int
    crc: int
    size: int
    timestamp: int
    def __init__(self, offset: int, crc: int, size: int, timestamp: int) -> None: ...
