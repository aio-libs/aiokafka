# This class takes advantage of the fact that all formats v0, v1 and v2 of
# messages storage has the same byte offsets for Length and Magic fields.
# Lets look closely at what leading bytes all versions have:
#
# V0 and V1 (Offset is MessageSet part, other bytes are Message ones):
#  Offset => Int64
#  BytesLength => Int32
#  CRC => Int32
#  Magic => Int8
#  ...
#
# V2:
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  ...
#
# So we can iterate over batches just by knowing offsets of Length. Magic is
# used to construct the correct class for Batch itself.

import struct

from aiokafka.errors import CorruptRecordException
from aiokafka.util import NO_EXTENSIONS
from .legacy_records import LegacyRecordBatch


class _MemoryRecordsPy:

    LENGTH_OFFSET = struct.calcsize(">q")
    LOG_OVERHEAD = struct.calcsize(">qi")
    MAGIC_OFFSET = struct.calcsize(">qii")

    # Minimum space requirements for Record V0
    MIN_SLICE = LOG_OVERHEAD + LegacyRecordBatch.RECORD_OVERHEAD_V0

    def __init__(self, bytes_data):
        self._buffer = bytes_data
        self._pos = 0
        # We keep one slice ahead so `has_next` will return very fast
        self._next_slice = None
        self._remaining_bytes = 0
        self._cache_next()

    def size_in_bytes(self):
        return len(self._buffer)

    # NOTE: we cache offsets here as kwargs for a bit more speed, as cPython
    # will use LOAD_FAST opcode in this case
    def _cache_next(self, len_offset=LENGTH_OFFSET, log_overhead=LOG_OVERHEAD):
        buffer = self._buffer
        buffer_len = len(buffer)
        pos = self._pos
        remaining = buffer_len - pos
        if remaining < log_overhead:
            # Will be re-checked in Fetcher for remaining bytes.
            self._remaining_bytes = remaining
            self._next_slice = None
            return

        length, = struct.unpack_from(
            ">i", buffer, pos + len_offset)

        slice_end = pos + log_overhead + length
        if slice_end > buffer_len:
            # Will be re-checked in Fetcher for remaining bytes
            self._remaining_bytes = remaining
            self._next_slice = None
            return

        self._next_slice = memoryview(buffer)[pos: slice_end]
        self._pos = slice_end

    def has_next(self):
        return self._next_slice is not None

    # NOTE: same cache for LOAD_FAST as above
    def next_batch(self, _min_slice=MIN_SLICE,
                   _magic_offset=MAGIC_OFFSET):
        next_slice = self._next_slice
        if next_slice is None:
            return None
        if len(next_slice) < _min_slice:
            raise CorruptRecordException(
                "Record size is less than the minimum record overhead "
                "({})".format(_min_slice - self.LOG_OVERHEAD))
        self._cache_next()
        magic = next_slice[_magic_offset]
        if magic >= 2:  # pragma: no cover
            raise NotImplementedError("Record V2 still not implemented")
        else:
            return LegacyRecordBatch(next_slice, magic)


if NO_EXTENSIONS:
    MemoryRecords = _MemoryRecordsPy
else:
    try:
        from ._memory_records import _MemoryRecordsCython
        MemoryRecords = _MemoryRecordsCython
    except ImportError as err:  # pragma: no cover
        MemoryRecords = _MemoryRecordsPy
