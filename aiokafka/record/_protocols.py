from typing import Any, Generator, List, Optional, Protocol, Tuple, Union

from typing_extensions import Literal, Never, Self


class DefaultRecordBatchBuilderProtocol(Protocol):
    def __init__(
        self,
        magic: int,
        compression_type: int,
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
        headers: List[Tuple[str, Optional[bytes]]],
    ) -> Optional["DefaultRecordMetadataProtocol"]: ...
    def write_header(self, use_compression_type: bool = True) -> None: ...
    def build(self) -> bytearray: ...
    def size(self) -> int: ...
    def size_in_bytes(
        self,
        offset: int,
        timestamp: int,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
    ) -> int: ...
    @classmethod
    def size_of(
        cls,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
    ) -> int: ...
    @classmethod
    def estimate_size_in_bytes(
        cls,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
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


class DefaultRecordBatchProtocol(Protocol):
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
    def __iter__(self) -> Self: ...
    def __next__(self) -> "DefaultRecordProtocol": ...
    def next(self) -> "DefaultRecordProtocol": ...
    def validate_crc(self) -> bool: ...


class DefaultRecordProtocol(Protocol):
    def __init__(
        self,
        offset: int,
        timestamp: int,
        timestamp_type: int,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
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
    def headers(self) -> List[Tuple[str, Optional[bytes]]]: ...
    @property
    def checksum(self) -> None: ...


class LegacyRecordBatchBuilderProtocol(Protocol):
    def __init__(
        self, magic: Literal[0, 1], compression_type: int, batch_size: int
    ) -> None: ...
    def append(
        self,
        offset: int,
        timestamp: Optional[int],
        key: Optional[bytes],
        value: Optional[bytes],
        headers: Any = None,
    ) -> Optional["LegacyRecordMetadataProtocol"]: ...
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
        headers: Optional[List[Any]] = None,
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


class LegacyRecordBatchProtocol(Protocol):
    is_control_batch: bool
    is_transactional: bool
    producer_id: Optional[int]

    def __init__(self, buffer: Union[bytes, bytearray, memoryview], magic: int): ...
    @property
    def timestamp_type(self) -> Optional[Literal[0, 1]]: ...
    @property
    def compression_type(self) -> int: ...
    @property
    def next_offset(self) -> int: ...
    def validate_crc(self) -> bool: ...
    def __iter__(self) -> Generator["LegacyRecordProtocol", None, None]: ...


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
    def headers(self) -> List[Never]: ...
    @property
    def checksum(self) -> int: ...


class MemoryRecordsProtocol(Protocol):
    def __init__(self, bytes_data: bytes) -> None: ...
    def size_in_bytes(self) -> int: ...
    def has_next(self) -> bool: ...
    def next_batch(
        self,
    ) -> Optional[Union[DefaultRecordBatchProtocol, LegacyRecordBatchProtocol]]: ...
