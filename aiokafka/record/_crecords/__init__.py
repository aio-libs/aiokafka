# util
from .cutil import (
    crc32c_cython,
    decode_varint_cython,
    encode_varint_cython,
    size_of_varint_cython,
)

# v2+
from .default_records import (
    DefaultRecord,
    DefaultRecordBatch,
    DefaultRecordBatchBuilder,
    DefaultRecordMetadata,
)

# v0 and v1
from .legacy_records import (
    LegacyRecord,
    LegacyRecordBatch,
    LegacyRecordBatchBuilder,
    LegacyRecordMetadata,
)

# abstract
from .memory_records import (
    MemoryRecords,
)

__all__ = [
    "decode_varint_cython",
    "encode_varint_cython",
    "size_of_varint_cython",
    "crc32c_cython",
    "MemoryRecords",
    "LegacyRecordBatch",
    "LegacyRecord",
    "LegacyRecordBatchBuilder",
    "LegacyRecordMetadata",
    "DefaultRecordBatch",
    "DefaultRecord",
    "DefaultRecordBatchBuilder",
    "DefaultRecordMetadata",
]
