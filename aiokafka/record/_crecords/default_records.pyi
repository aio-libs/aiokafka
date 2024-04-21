from typing import Sequence, final

from typing_extensions import Self

from aiokafka.record._protocols import (
    DefaultRecordBatchBuilderProtocol,
    DefaultRecordBatchProtocol,
    DefaultRecordMetadataProtocol,
    DefaultRecordProtocol,
)

@final
class DefaultRecord(DefaultRecordProtocol):
    offset: int
    key: bytes | None
    value: bytes | None
    headers: Sequence[tuple[str, bytes | None]]
    checksum: None

    def __init__(
        self,
        offset: int,
        timestamp: int,
        timestamp_type: int,
        key: bytes | None,
        value: bytes | None,
        headers: Sequence[tuple[str, bytes | None]],
    ) -> None: ...
    @property
    def timestamp(self) -> int | None: ...
    @property
    def timestamp_type(self) -> int | None: ...

@final
class DefaultRecordBatch(DefaultRecordBatchProtocol):
    def __init__(self, buffer: bytes): ...
    @property
    def compression_type(self) -> int: ...
    @property
    def is_transactional(self) -> bool: ...
    @property
    def is_control_batch(self) -> bool: ...
    @property
    def next_offset(self) -> int: ...
    def __iter__(self) -> Self: ...
    def __next__(self) -> DefaultRecord: ...
    def validate_crc(self) -> bool: ...

@final
class DefaultRecordBatchBuilder(DefaultRecordBatchBuilderProtocol):
    producer_id: int
    producer_epoch: int
    base_sequence: int
    def __init__(
        self,
        magic: int,
        compression_type: int,
        is_transactional: int,
        producer_id: int,
        producer_epoch: int,
        base_sequence: int,
        batch_size: int,
    ) -> None: ...
    def set_producer_state(
        self, producer_id: int, producer_epoch: int, base_sequence: int
    ) -> None: ...
    def append(
        self,
        offset: int,
        timestamp: int | None,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> DefaultRecordMetadata: ...
    def build(self) -> bytearray: ...
    def size(self) -> int: ...
    def size_in_bytes(
        self,
        offset: int,
        timestamp: int,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int: ...
    @classmethod
    def size_of(
        cls,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int: ...
    @classmethod
    def estimate_size_in_bytes(
        cls,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
    ) -> int: ...

@final
class DefaultRecordMetadata(DefaultRecordMetadataProtocol):
    offset: int
    size: int
    timestamp: int
    def __init__(self, offset: int, size: int, timestamp: int): ...
