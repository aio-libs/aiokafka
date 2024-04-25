# See:
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecordBatch.java
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecord.java

# RecordBatch and Record implementation for magic 2 and above.
# The schema is given below:

# RecordBatch =>
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  CRC => Uint32
#  Attributes => Int16
#  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
#  FirstTimestamp => Int64
#  MaxTimestamp => Int64
#  ProducerId => Int64
#  ProducerEpoch => Int16
#  BaseSequence => Int32
#  Records => [Record]

# Record =>
#   Length => Varint
#   Attributes => Int8
#   TimestampDelta => Varlong
#   OffsetDelta => Varint
#   Key => Bytes
#   Value => Bytes
#   Headers => [HeaderKey HeaderValue]
#     HeaderKey => String
#     HeaderValue => Bytes

# Note that when compression is enabled (see attributes below), the compressed
# record data is serialized directly following the count of the number of
# records. (ie Records => [Record], but without length bytes)

# The CRC covers the data from the attributes to the end of the batch (i.e. all
# the bytes that follow the CRC). It is located after the magic byte, which
# means that clients must parse the magic byte before deciding how to interpret
# the bytes between the batch length and the magic byte. The partition leader
# epoch field is not included in the CRC computation to avoid the need to
# recompute the CRC when this field is assigned for every batch that is
# received by the broker. The CRC-32C (Castagnoli) polynomial is used for the
# computation.

# The current RecordBatch attributes are given below:
#
# * Unused (6-15)
# * Control (5)
# * Transactional (4)
# * Timestamp Type (3)
# * Compression Type (0-2)

import struct
import time
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sized, Tuple, Type, Union, final

from typing_extensions import Self, TypeIs, assert_never

import aiokafka.codec as codecs
from aiokafka.codec import (
    gzip_decode,
    gzip_encode,
    lz4_decode,
    lz4_encode,
    snappy_decode,
    snappy_encode,
    zstd_decode,
    zstd_encode,
)
from aiokafka.errors import CorruptRecordException, UnsupportedCodecError
from aiokafka.util import NO_EXTENSIONS

from ._protocols import (
    DefaultRecordBatchBuilderProtocol,
    DefaultRecordBatchProtocol,
    DefaultRecordMetadataProtocol,
    DefaultRecordProtocol,
)
from ._types import (
    CodecGzipT,
    CodecLz4T,
    CodecMaskT,
    CodecNoneT,
    CodecSnappyT,
    CodecZstdT,
)
from .util import calc_crc32c, decode_varint, encode_varint, size_of_varint


class DefaultRecordBase:
    __slots__ = ()

    HEADER_STRUCT = struct.Struct(
        ">q"  # BaseOffset => Int64
        "i"  # Length => Int32
        "i"  # PartitionLeaderEpoch => Int32
        "b"  # Magic => Int8
        "I"  # CRC => Uint32
        "h"  # Attributes => Int16
        "i"  # LastOffsetDelta => Int32 // also serves as LastSequenceDelta
        "q"  # FirstTimestamp => Int64
        "q"  # MaxTimestamp => Int64
        "q"  # ProducerId => Int64
        "h"  # ProducerEpoch => Int16
        "i"  # BaseSequence => Int32
        "i"  # Records count => Int32
    )
    # Byte offset in HEADER_STRUCT of attributes field. Used to calculate CRC
    ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
    CRC_OFFSET = struct.calcsize(">qiib")
    AFTER_LEN_OFFSET = struct.calcsize(">qi")

    CODEC_MASK: CodecMaskT = 0x07
    CODEC_NONE: CodecNoneT = 0x00
    CODEC_GZIP: CodecGzipT = 0x01
    CODEC_SNAPPY: CodecSnappyT = 0x02
    CODEC_LZ4: CodecLz4T = 0x03
    CODEC_ZSTD: CodecZstdT = 0x04
    TIMESTAMP_TYPE_MASK = 0x08
    TRANSACTIONAL_MASK = 0x10
    CONTROL_MASK = 0x20

    LOG_APPEND_TIME = 1
    CREATE_TIME = 0

    NO_PARTITION_LEADER_EPOCH = -1

    def _assert_has_codec(
        self, compression_type: int
    ) -> TypeIs[Union[CodecGzipT, CodecSnappyT, CodecLz4T, CodecZstdT]]:
        if compression_type == self.CODEC_GZIP:
            checker, name = codecs.has_gzip, "gzip"
        elif compression_type == self.CODEC_SNAPPY:
            checker, name = codecs.has_snappy, "snappy"
        elif compression_type == self.CODEC_LZ4:
            checker, name = codecs.has_lz4, "lz4"
        elif compression_type == self.CODEC_ZSTD:
            checker, name = codecs.has_zstd, "zstd"
        else:
            raise UnsupportedCodecError(
                f"Unknown compression codec {compression_type:#04x}"
            )
        if not checker():
            raise UnsupportedCodecError(
                f"Libraries for {name} compression codec not found"
            )
        return True


@final
class _DefaultRecordBatchPy(DefaultRecordBase, DefaultRecordBatchProtocol):
    def __init__(self, buffer: Union[bytes, bytearray, memoryview]) -> None:
        self._buffer = bytearray(buffer)
        self._header_data: Tuple[
            int, int, int, int, int, int, int, int, int, int, int, int, int
        ] = self.HEADER_STRUCT.unpack_from(self._buffer)
        self._pos = self.HEADER_STRUCT.size
        self._num_records = self._header_data[12]
        self._next_record_index = 0
        self._decompressed = False

    @property
    def base_offset(self) -> int:
        return self._header_data[0]

    @property
    def magic(self) -> int:
        return self._header_data[3]

    @property
    def crc(self) -> int:
        return self._header_data[4]

    @property
    def attributes(self) -> int:
        return self._header_data[5]

    @property
    def compression_type(self) -> int:
        return self.attributes & self.CODEC_MASK

    @property
    def timestamp_type(self) -> int:
        return int(bool(self.attributes & self.TIMESTAMP_TYPE_MASK))

    @property
    def is_transactional(self) -> bool:
        return bool(self.attributes & self.TRANSACTIONAL_MASK)

    @property
    def is_control_batch(self) -> bool:
        return bool(self.attributes & self.CONTROL_MASK)

    @property
    def last_offset_delta(self) -> int:
        return self._header_data[6]

    @property
    def first_timestamp(self) -> int:
        return self._header_data[7]

    @property
    def max_timestamp(self) -> int:
        return self._header_data[8]

    @property
    def producer_id(self) -> int:
        return self._header_data[9]

    @property
    def producer_epoch(self) -> int:
        return self._header_data[10]

    @property
    def base_sequence(self) -> int:
        return self._header_data[11]

    @property
    def next_offset(self) -> int:
        return self.base_offset + self.last_offset_delta + 1

    def _maybe_uncompress(self) -> None:
        if not self._decompressed:
            compression_type = self.compression_type
            if compression_type != self.CODEC_NONE:
                assert self._assert_has_codec(compression_type)
                data = memoryview(self._buffer)[self._pos :]
                if compression_type == self.CODEC_GZIP:
                    uncompressed = gzip_decode(data)
                elif compression_type == self.CODEC_SNAPPY:
                    uncompressed = snappy_decode(data.tobytes())
                elif compression_type == self.CODEC_LZ4:
                    uncompressed = lz4_decode(data.tobytes())
                elif compression_type == self.CODEC_ZSTD:
                    uncompressed = zstd_decode(data.tobytes())
                else:
                    assert_never(compression_type)
                self._buffer = bytearray(uncompressed)
                self._pos = 0
        self._decompressed = True

    def _read_msg(
        self, decode_varint: Callable[[bytearray, int], Tuple[int, int]] = decode_varint
    ) -> "_DefaultRecordPy":
        # Record =>
        #   Length => Varint
        #   Attributes => Int8
        #   TimestampDelta => Varlong
        #   OffsetDelta => Varint
        #   Key => Bytes
        #   Value => Bytes
        #   Headers => [HeaderKey HeaderValue]
        #     HeaderKey => String
        #     HeaderValue => Bytes

        buffer = self._buffer
        pos = self._pos
        length, pos = decode_varint(buffer, pos)
        start_pos = pos
        _, pos = decode_varint(buffer, pos)  # attrs can be skipped for now

        ts_delta, pos = decode_varint(buffer, pos)
        if self.timestamp_type == self.LOG_APPEND_TIME:
            timestamp = self.max_timestamp
        else:
            timestamp = self.first_timestamp + ts_delta

        offset_delta, pos = decode_varint(buffer, pos)
        offset = self.base_offset + offset_delta

        key_len, pos = decode_varint(buffer, pos)
        if key_len >= 0:
            key = bytes(buffer[pos : pos + key_len])
            pos += key_len
        else:
            key = None

        value_len, pos = decode_varint(buffer, pos)
        if value_len >= 0:
            value = bytes(buffer[pos : pos + value_len])
            pos += value_len
        else:
            value = None

        header_count, pos = decode_varint(buffer, pos)
        if header_count < 0:
            raise CorruptRecordException(
                f"Found invalid number of record headers {header_count}"
            )
        headers: List[Tuple[str, Optional[bytes]]] = []
        while header_count:
            # Header key is of type String, that can't be None
            h_key_len, pos = decode_varint(buffer, pos)
            if h_key_len < 0:
                raise CorruptRecordException(
                    f"Invalid negative header key size {h_key_len}"
                )
            h_key = buffer[pos : pos + h_key_len].decode("utf-8")
            pos += h_key_len

            # Value is of type NULLABLE_BYTES, so it can be None
            h_value_len, pos = decode_varint(buffer, pos)
            if h_value_len >= 0:
                h_value = bytes(buffer[pos : pos + h_value_len])
                pos += h_value_len
            else:
                h_value = None

            headers.append((h_key, h_value))
            header_count -= 1

        # validate whether we have read all header bytes in the current record
        if pos - start_pos != length:
            raise CorruptRecordException(
                f"Invalid record size: expected to read {length} bytes in record "
                f"payload, but instead read {pos - start_pos}"
            )
        self._pos = pos

        return _DefaultRecordPy(
            offset, timestamp, self.timestamp_type, key, value, headers
        )

    def __iter__(self) -> Self:
        self._maybe_uncompress()
        return self

    def __next__(self) -> "_DefaultRecordPy":
        if self._next_record_index >= self._num_records:
            if self._pos != len(self._buffer):
                raise CorruptRecordException(
                    f"{len(self._buffer) - self._pos}"
                    " unconsumed bytes after all records consumed"
                )
            raise StopIteration
        try:
            msg = self._read_msg()
        except (ValueError, IndexError) as err:
            raise CorruptRecordException(
                f"Found invalid record structure: {err!r}"
            ) from err
        else:
            self._next_record_index += 1
        return msg

    def validate_crc(self) -> bool:
        assert self._decompressed is False, "Validate should be called before iteration"

        crc = self.crc
        data_view = memoryview(self._buffer)[self.ATTRIBUTES_OFFSET :]
        verify_crc = calc_crc32c(data_view.tobytes())
        return crc == verify_crc


@final
@dataclass(frozen=True)
class _DefaultRecordPy(DefaultRecordProtocol):
    __slots__ = ("offset", "timestamp", "timestamp_type", "key", "value", "headers")

    offset: int
    timestamp: int
    timestamp_type: int
    key: Optional[bytes]
    value: Optional[bytes]
    headers: List[Tuple[str, Optional[bytes]]]

    @property
    def checksum(self) -> None:
        return None

    def __repr__(self) -> str:
        return (
            f"DefaultRecord(offset={self.offset!r}, timestamp={self.timestamp!r},"
            f" timestamp_type={self.timestamp_type!r}, key={self.key!r},"
            f" value={self.value!r}, headers={self.headers!r})"
        )


@final
class _DefaultRecordBatchBuilderPy(
    DefaultRecordBase, DefaultRecordBatchBuilderProtocol
):
    # excluding key, value and headers:
    # 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes
    MAX_RECORD_OVERHEAD = 21

    def __init__(
        self,
        magic: int,
        compression_type: int,
        is_transactional: int,
        producer_id: int,
        producer_epoch: int,
        base_sequence: int,
        batch_size: int,
    ):
        assert magic >= 2
        self._magic = magic
        self._compression_type = compression_type & self.CODEC_MASK
        self._batch_size = batch_size
        self._is_transactional = bool(is_transactional)
        # KIP-98 fields for EOS
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._base_sequence = base_sequence

        self._first_timestamp: Optional[int] = None
        self._max_timestamp: Optional[int] = None
        self._last_offset = 0
        self._num_records = 0

        self._buffer = bytearray(self.HEADER_STRUCT.size)

    def _get_attributes(self, include_compression_type: bool = True) -> int:
        attrs = 0
        if include_compression_type:
            attrs |= self._compression_type
        # Timestamp Type is set by Broker
        if self._is_transactional:
            attrs |= self.TRANSACTIONAL_MASK
        # Control batches are only created by Broker
        return attrs

    def append(
        self,
        offset: int,
        timestamp: Optional[int],
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
        # Cache for LOAD_FAST opcodes
        encode_varint: Callable[[int, Callable[[int], None]], int] = encode_varint,
        size_of_varint: Callable[[int], int] = size_of_varint,
        get_type: Callable[[Any], type] = type,
        type_int: Type[int] = int,
        time_time: Callable[[], float] = time.time,
        byte_like: Tuple[Type[bytes], Type[bytearray], Type[memoryview]] = (
            bytes,
            bytearray,
            memoryview,
        ),
        bytearray_type: Type[bytearray] = bytearray,
        len_func: Callable[[Sized], int] = len,
        zero_len_varint: int = 1,
    ) -> Optional["_DefaultRecordMetadataPy"]:
        """Write message to messageset buffer with MsgVersion 2"""
        # Check types
        if get_type(offset) != type_int:
            raise TypeError(offset)
        if timestamp is None:
            timestamp = type_int(time_time() * 1000)
        elif get_type(timestamp) != type_int:
            raise TypeError(timestamp)
        if not (key is None or get_type(key) in byte_like):
            raise TypeError(f"Not supported type for key: {type(key)}")
        if not (value is None or get_type(value) in byte_like):
            raise TypeError(f"Not supported type for value: {type(value)}")

        # We will always add the first message, so those will be set
        if self._first_timestamp is None:
            self._first_timestamp = timestamp
            self._max_timestamp = timestamp
            timestamp_delta = 0
            first_message = 1
        else:
            timestamp_delta = timestamp - self._first_timestamp
            first_message = 0

        # We can't write record right away to out buffer, we need to
        # precompute the length as first value...
        message_buffer = bytearray_type(b"\x00")  # Attributes
        write_byte = message_buffer.append
        write = message_buffer.extend

        encode_varint(timestamp_delta, write_byte)
        # Base offset is always 0 on Produce
        encode_varint(offset, write_byte)

        if key is not None:
            encode_varint(len_func(key), write_byte)
            write(key)
        else:
            write_byte(zero_len_varint)

        if value is not None:
            encode_varint(len_func(value), write_byte)
            write(value)
        else:
            write_byte(zero_len_varint)

        encode_varint(len_func(headers), write_byte)

        for h_key, h_value in headers:
            h_key_bytes = h_key.encode("utf-8")
            encode_varint(len_func(h_key_bytes), write_byte)
            write(h_key_bytes)
            if h_value is not None:
                encode_varint(len_func(h_value), write_byte)
                write(h_value)
            else:
                write_byte(zero_len_varint)

        message_len = len_func(message_buffer)
        main_buffer = self._buffer

        required_size = message_len + size_of_varint(message_len)
        # Check if we can write this message
        if (
            required_size + len_func(main_buffer) > self._batch_size
            and not first_message
        ):
            return None

        # Those should be updated after the length check
        assert self._max_timestamp is not None
        if self._max_timestamp < timestamp:
            self._max_timestamp = timestamp
        self._num_records += 1
        self._last_offset = offset

        encode_varint(message_len, main_buffer.append)
        main_buffer.extend(message_buffer)

        return _DefaultRecordMetadataPy(offset, required_size, timestamp)

    def _write_header(self, use_compression_type: bool = True) -> None:
        batch_len = len(self._buffer)
        self.HEADER_STRUCT.pack_into(
            self._buffer,
            0,
            0,  # BaseOffset, set by broker
            batch_len - self.AFTER_LEN_OFFSET,  # Size from here to end
            self.NO_PARTITION_LEADER_EPOCH,
            self._magic,
            0,  # CRC will be set below, as we need a filled buffer for it
            self._get_attributes(use_compression_type),
            self._last_offset,
            self._first_timestamp or 0,
            self._max_timestamp or 0,
            self._producer_id,
            self._producer_epoch,
            self._base_sequence,
            self._num_records,
        )
        crc = calc_crc32c(self._buffer[self.ATTRIBUTES_OFFSET :])
        struct.pack_into(">I", self._buffer, self.CRC_OFFSET, crc)

    def _maybe_compress(self) -> bool:
        if self._compression_type != self.CODEC_NONE:
            assert self._assert_has_codec(self._compression_type)
            header_size = self.HEADER_STRUCT.size
            data = bytes(self._buffer[header_size:])
            if self._compression_type == self.CODEC_GZIP:
                compressed = gzip_encode(data)
            elif self._compression_type == self.CODEC_SNAPPY:
                compressed = snappy_encode(data)
            elif self._compression_type == self.CODEC_LZ4:
                compressed = lz4_encode(data)
            elif self._compression_type == self.CODEC_ZSTD:
                compressed = zstd_encode(data)
            else:
                assert_never(self._compression_type)
            compressed_size = len(compressed)
            if len(data) <= compressed_size:
                # We did not get any benefit from compression, lets send
                # uncompressed
                return False
            else:
                # Trim bytearray to the required size
                needed_size = header_size + compressed_size
                del self._buffer[needed_size:]
                self._buffer[header_size:needed_size] = compressed
                return True
        return False

    def build(self) -> bytearray:
        send_compressed = self._maybe_compress()
        self._write_header(send_compressed)
        return self._buffer

    def size(self) -> int:
        """Return current size of data written to buffer"""
        return len(self._buffer)

    def size_in_bytes(
        self,
        offset: int,
        timestamp: int,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
    ) -> int:
        if self._first_timestamp is not None:
            timestamp_delta = timestamp - self._first_timestamp
        else:
            timestamp_delta = 0
        size_of_body = (
            1  # Attrs
            + size_of_varint(offset)
            + size_of_varint(timestamp_delta)
            + self.size_of(key, value, headers)
        )
        return size_of_body + size_of_varint(size_of_body)

    @classmethod
    def size_of(
        cls,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
    ) -> int:
        size = 0
        # Key size
        if key is None:
            size += 1
        else:
            key_len = len(key)
            size += size_of_varint(key_len) + key_len
        # Value size
        if value is None:
            size += 1
        else:
            value_len = len(value)
            size += size_of_varint(value_len) + value_len
        # Header size
        size += size_of_varint(len(headers))
        for h_key, h_value in headers:
            h_key_len = len(h_key.encode("utf-8"))
            size += size_of_varint(h_key_len) + h_key_len

            if h_value is None:
                size += 1
            else:
                h_value_len = len(h_value)
                size += size_of_varint(h_value_len) + h_value_len
        return size

    @classmethod
    def estimate_size_in_bytes(
        cls,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: List[Tuple[str, Optional[bytes]]],
    ) -> int:
        """Get the upper bound estimate on the size of record"""
        return (
            cls.HEADER_STRUCT.size
            + cls.MAX_RECORD_OVERHEAD
            + cls.size_of(key, value, headers)
        )

    def set_producer_state(
        self, producer_id: int, producer_epoch: int, base_sequence: int
    ) -> None:
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._base_sequence = base_sequence

    @property
    def producer_id(self) -> int:
        return self._producer_id

    @property
    def producer_epoch(self) -> int:
        return self._producer_epoch

    @property
    def base_sequence(self) -> int:
        return self._base_sequence


@final
@dataclass(frozen=True)
class _DefaultRecordMetadataPy(DefaultRecordMetadataProtocol):
    __slots__ = ("size", "timestamp", "offset")

    offset: int
    size: int
    timestamp: int

    @property
    def crc(self) -> None:
        return None

    def __repr__(self) -> str:
        return (
            f"DefaultRecordMetadata(offset={self.offset!r},"
            f" size={self.size!r}, timestamp={self.timestamp!r})"
        )


DefaultRecordBatchBuilder: Type[DefaultRecordBatchBuilderProtocol]
DefaultRecordMetadata: Type[DefaultRecordMetadataProtocol]
DefaultRecordBatch: Type[DefaultRecordBatchProtocol]
DefaultRecord: Type[DefaultRecordProtocol]

if NO_EXTENSIONS:
    DefaultRecordBatchBuilder = _DefaultRecordBatchBuilderPy
    DefaultRecordMetadata = _DefaultRecordMetadataPy
    DefaultRecordBatch = _DefaultRecordBatchPy
    DefaultRecord = _DefaultRecordPy
else:
    try:
        from ._crecords import (
            DefaultRecord as _DefaultRecordCython,
        )
        from ._crecords import (
            DefaultRecordBatch as _DefaultRecordBatchCython,
        )
        from ._crecords import (
            DefaultRecordBatchBuilder as _DefaultRecordBatchBuilderCython,
        )
        from ._crecords import (
            DefaultRecordMetadata as _DefaultRecordMetadataCython,
        )

        DefaultRecordBatchBuilder = _DefaultRecordBatchBuilderCython
        DefaultRecordMetadata = _DefaultRecordMetadataCython
        DefaultRecordBatch = _DefaultRecordBatchCython
        DefaultRecord = _DefaultRecordCython
    except ImportError:  # pragma: no cover
        DefaultRecordBatchBuilder = _DefaultRecordBatchBuilderPy
        DefaultRecordMetadata = _DefaultRecordMetadataPy
        DefaultRecordBatch = _DefaultRecordBatchPy
        DefaultRecord = _DefaultRecordPy
