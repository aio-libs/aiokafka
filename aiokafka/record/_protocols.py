from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import (
    Any,
    ClassVar,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)

from typing_extensions import Literal, Never

from ._types import (
    CodecGzipT,
    CodecLz4T,
    CodecMaskT,
    CodecNoneT,
    CodecSnappyT,
    CodecZstdT,
    DefaultCompressionTypeT,
    LegacyCompressionTypeT,
)


class DefaultRecordBatchBuilderProtocol(Protocol):
    def __init__(
        self,
        magic: int,
        compression_type: DefaultCompressionTypeT,
        is_transactional: int,
        producer_id: int,
        producer_epoch: int,
        base_sequence: int,
        batch_size: int,
    ): ...
    def append(
        self,
        offset: int,
        timestamp: Optional[int],
        key: Optional[bytes],
        value: Optional[bytes],
        headers: list[tuple[str, Optional[bytes]]],
    ) -> Optional[DefaultRecordMetadataProtocol]: ...
    def build(self) -> bytearray: ...
    def size(self) -> int: ...
    def size_in_bytes(
        self,
        offset: int,
        timestamp: int,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: list[tuple[str, Optional[bytes]]],
    ) -> int: ...
    @classmethod
    def size_of(
        cls,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: list[tuple[str, Optional[bytes]]],
    ) -> int: ...
    @classmethod
    def estimate_size_in_bytes(
        cls,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: list[tuple[str, Optional[bytes]]],
    ) -> int: ...
    def set_producer_state(
        self, producer_id: int, producer_epoch: int, base_sequence: int
    ) -> None: ...
    @property
    def producer_id(self) -> int: ...
    @property
    def producer_epoch(self) -> int: ...
    @property
    def base_sequence(self) -> int: ...


class DefaultRecordMetadataProtocol(Protocol):
    def __init__(self, offset: int, size: int, timestamp: int) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def crc(self) -> None: ...
    @property
    def size(self) -> int: ...
    @property
    def timestamp(self) -> int: ...


class DefaultRecordBatchProtocol(Iterator["DefaultRecordProtocol"], Protocol):
    CODEC_MASK: ClassVar[CodecMaskT]
    CODEC_NONE: ClassVar[CodecNoneT]
    CODEC_GZIP: ClassVar[CodecGzipT]
    CODEC_SNAPPY: ClassVar[CodecSnappyT]
    CODEC_LZ4: ClassVar[CodecLz4T]
    CODEC_ZSTD: ClassVar[CodecZstdT]

    def __init__(self, buffer: Union[bytes, bytearray, memoryview]) -> None: ...
    @property
    def base_offset(self) -> int: ...
    @property
    def magic(self) -> int: ...
    @property
    def crc(self) -> int: ...
    @property
    def attributes(self) -> int: ...
    @property
    def compression_type(self) -> int: ...
    @property
    def timestamp_type(self) -> int: ...
    @property
    def is_transactional(self) -> bool: ...
    @property
    def is_control_batch(self) -> bool: ...
    @property
    def last_offset_delta(self) -> int: ...
    @property
    def first_timestamp(self) -> int: ...
    @property
    def max_timestamp(self) -> int: ...
    @property
    def producer_id(self) -> int: ...
    @property
    def producer_epoch(self) -> int: ...
    @property
    def base_sequence(self) -> int: ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...


@runtime_checkable
class DefaultRecordProtocol(Protocol):
    def __init__(
        self,
        offset: int,
        timestamp: int,
        timestamp_type: int,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: list[tuple[str, Optional[bytes]]],
    ) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def timestamp(self) -> int:
        """Epoch milliseconds"""

    @property
    def timestamp_type(self) -> int:
        """CREATE_TIME(0) or APPEND_TIME(1)"""

    @property
    def key(self) -> Optional[bytes]:
        """Bytes key or None"""

    @property
    def value(self) -> Optional[bytes]:
        """Bytes value or None"""

    @property
    def headers(self) -> list[tuple[str, Optional[bytes]]]: ...
    @property
    def checksum(self) -> None: ...


class LegacyRecordBatchBuilderProtocol(Protocol):
    def __init__(
        self,
        magic: Literal[0, 1],
        compression_type: LegacyCompressionTypeT,
        batch_size: int,
    ) -> None: ...
    def append(
        self,
        offset: int,
        timestamp: Optional[int],
        key: Optional[bytes],
        value: Optional[bytes],
        headers: Any = None,
    ) -> Optional[LegacyRecordMetadataProtocol]: ...
    def build(self) -> bytearray:
        """Compress batch to be ready for send"""

    def size(self) -> int:
        """Return current size of data written to buffer"""

    def size_in_bytes(
        self,
        offset: int,
        timestamp: int,
        key: Optional[bytes],
        value: Optional[bytes],
    ) -> int:
        """Actual size of message to add"""

    @classmethod
    def record_overhead(cls, magic: int) -> int: ...


class LegacyRecordMetadataProtocol(Protocol):
    def __init__(self, offset: int, crc: int, size: int, timestamp: int) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def crc(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def timestamp(self) -> int: ...


class LegacyRecordBatchProtocol(Iterable["LegacyRecordProtocol"], Protocol):
    CODEC_MASK: ClassVar[CodecMaskT]
    CODEC_GZIP: ClassVar[CodecGzipT]
    CODEC_SNAPPY: ClassVar[CodecSnappyT]
    CODEC_LZ4: ClassVar[CodecLz4T]

    is_control_batch: bool
    is_transactional: bool
    producer_id: Optional[int]

    def __init__(self, buffer: Union[bytes, bytearray, memoryview], magic: int): ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...


@runtime_checkable
class LegacyRecordProtocol(Protocol):
    def __init__(
        self,
        offset: int,
        timestamp: Optional[int],
        timestamp_type: Optional[Literal[0, 1]],
        key: Optional[bytes],
        value: Optional[bytes],
        crc: int,
    ) -> None: ...
    @property
    def offset(self) -> int: ...
    @property
    def timestamp(self) -> Optional[int]:
        """Epoch milliseconds"""

    @property
    def timestamp_type(self) -> Optional[Literal[0, 1]]:
        """CREATE_TIME(0) or APPEND_TIME(1)"""

    @property
    def key(self) -> Optional[bytes]:
        """Bytes key or None"""

    @property
    def value(self) -> Optional[bytes]:
        """Bytes value or None"""

    @property
    def headers(self) -> list[Never]: ...
    @property
    def checksum(self) -> int: ...


class MemoryRecordsProtocol(Protocol):
    def __init__(self, bytes_data: bytes) -> None: ...
    def size_in_bytes(self) -> int: ...
    def has_next(self) -> bool: ...
    def next_batch(
        self,
    ) -> Optional[Union[DefaultRecordBatchProtocol, LegacyRecordBatchProtocol]]: ...
