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
from aiokafka.record.util import (
    encode_varint, calc_crc32c, size_of_varint
)

from aiokafka.errors import CorruptRecordException
from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode,
    gzip_decode, snappy_decode, lz4_decode
)

from cpython cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_WRITABLE, \
                     PyBUF_SIMPLE, PyBUF_READ, Py_buffer, \
                     PyBytes_FromStringAndSize
from libc.stdint cimport int32_t, int64_t, uint32_t
from libc.string cimport memcpy
cimport cython
cdef extern from "Python.h":
    ssize_t PyByteArray_GET_SIZE(object)
    char* PyByteArray_AS_STRING(bytearray ba)
    int PyByteArray_Resize(object, ssize_t) except -1

    object PyMemoryView_FromMemory(char *mem, ssize_t size, int flags)

# This should be before _cutil to generate include for `winsock2.h` before
# `windows.h`
from . cimport hton
from . cimport cutil

include "consts.pxi"

# Header structure offsets
DEF BASE_OFFSET_OFFSET = 0
DEF LENGTH_OFFSET = BASE_OFFSET_OFFSET + 8
DEF PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + 4
DEF MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + 4
DEF CRC_OFFSET = MAGIC_OFFSET + 1
DEF ATTRIBUTES_OFFSET = CRC_OFFSET + 4
DEF LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + 2
DEF FIRST_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + 4
DEF MAX_TIMESTAMP_OFFSET = FIRST_TIMESTAMP_OFFSET + 8
DEF PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + 8
DEF PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + 8
DEF BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + 2
DEF RECORD_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + 4
DEF FIRST_RECORD_OFFSET = RECORD_COUNT_OFFSET + 4


class DefaultRecordBase:

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
    # ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
    # CRC_OFFSET = struct.calcsize(">qiib")
    AFTER_LEN_OFFSET = struct.calcsize(">qi")

    CODEC_MASK = 0x07
    CODEC_NONE = 0x00
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    TIMESTAMP_TYPE_MASK = 0x08
    TRANSACTIONAL_MASK = 0x10
    CONTROL_MASK = 0x20

    LOG_APPEND_TIME = 1
    CREATE_TIME = 0


@cython.no_gc_clear
@cython.final
@cython.freelist(_DEFAULT_RECORD_BATCH_FREELIST_SIZE)
cdef class DefaultRecordBatch:

    CODEC_NONE = _ATTR_CODEC_NONE
    CODEC_GZIP = _ATTR_CODEC_GZIP
    CODEC_SNAPPY = _ATTR_CODEC_SNAPPY
    CODEC_LZ4 = _ATTR_CODEC_LZ4

    def __init__(self, object buffer):
        PyObject_GetBuffer(buffer, &self._buffer, PyBUF_SIMPLE)
        self._decompressed = 0
        self._read_header()
        self._pos = FIRST_RECORD_OFFSET
        self._next_record_index = 0

    @staticmethod
    cdef inline DefaultRecordBatch new(
            bytes buffer, Py_ssize_t pos, Py_ssize_t slice_end, char magic):
        """ Fast constructor to initialize from C.
            NOTE: We take ownership of the Py_buffer object, so caller does not
                  need to call PyBuffer_Release.
        """
        cdef:
            DefaultRecordBatch batch
            char* buf
        batch = DefaultRecordBatch.__new__(DefaultRecordBatch)
        PyObject_GetBuffer(buffer, &batch._buffer, PyBUF_SIMPLE)
        buf = <char *>batch._buffer.buf
        # Change the buffer to include a proper slice
        batch._buffer.buf = <void *> &buf[pos]
        batch._buffer.len = slice_end - pos

        batch._decompressed = 0
        batch._read_header()
        batch._pos = FIRST_RECORD_OFFSET
        batch._next_record_index = 0
        return batch


    def __dealloc__(self):
        PyBuffer_Release(&self._buffer)

    @property
    def compression_type(self):
        return self.attributes & _ATTR_CODEC_MASK

    @property
    def is_transactional(self):
        if self.attributes & _TRANSACTIONAL_MASK:
            return True
        else:
            return False

    @property
    def is_control_batch(self):
        if self.attributes & _CONTROL_MASK:
            return True
        else:
            return False

    cdef inline _read_header(self):
        # NOTE: We move the data to it's slot in the structure because we will
        #       lose the header part after decompression.

        cdef:
            char* buf
            Py_ssize_t pos = 0

        buf = <char*> self._buffer.buf
        self.base_offset = hton.unpack_int64(&buf[pos + BASE_OFFSET_OFFSET])
        self.length = hton.unpack_int32(&buf[pos + LENGTH_OFFSET])
        self.magic = buf[pos + MAGIC_OFFSET]
        self.crc = <uint32_t> hton.unpack_int32(&buf[pos + CRC_OFFSET])
        self.attributes = hton.unpack_int16(&buf[pos + ATTRIBUTES_OFFSET])
        self.first_timestamp = \
            hton.unpack_int64(&buf[pos + FIRST_TIMESTAMP_OFFSET])
        self.max_timestamp = \
            hton.unpack_int64(&buf[pos + MAX_TIMESTAMP_OFFSET])
        self.num_records = hton.unpack_int32(&buf[pos + RECORD_COUNT_OFFSET])
        if self.attributes & _TIMESTAMP_TYPE_MASK:
            self.timestamp_type = 1
        else:
            self.timestamp_type = 0

    cdef _maybe_uncompress(self):
        cdef:
            char compression_type
        if not self._decompressed:
            compression_type = <char> self.attributes & _ATTR_CODEC_MASK
            if compression_type != _ATTR_CODEC_NONE:
                data = PyMemoryView_FromMemory(
                    <char *>self._buffer.buf, <Py_ssize_t>self._buffer.len,
                    PyBUF_READ)
                if compression_type == _ATTR_CODEC_GZIP:
                    uncompressed = gzip_decode(data)
                if compression_type == _ATTR_CODEC_SNAPPY:
                    uncompressed = snappy_decode(data.tobytes())
                if compression_type == _ATTR_CODEC_LZ4:
                    uncompressed = lz4_decode(data.tobytes())
        
                PyBuffer_Release(&self._buffer)
                PyObject_GetBuffer(uncompressed, &self._buffer, PyBUF_SIMPLE)
                self._pos = 0
        self._decompressed = 1

    cdef inline int _check_bounds(
            self, Py_ssize_t pos, Py_ssize_t size) except -1:
        """ Confirm that the slice is not outside buffer range
        """
        if pos + size > self._buffer.len:
            raise CorruptRecordException(
                "Can't read {} bytes from pos {}".format(size, pos))

    cdef DefaultRecord _read_msg(self):
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

        cdef:
            Py_ssize_t pos = self._pos
            char* buf

            int64_t length
            int64_t attrs
            int64_t ts_delta
            int64_t offset_delta
            int64_t key_len
            int64_t value_len
            int64_t header_count
            Py_ssize_t start_pos

            int64_t offset
            int64_t timestamp
            object key
            object value
            list headers
            bytes h_key
            object h_value
            # char attrs,
            # uint32_t crc

        # Minimum record size check

        buf = <char*> self._buffer.buf
        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &length)
        start_pos = pos
        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &attrs)

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &ts_delta)
        if self.attributes & _TIMESTAMP_TYPE_MASK:  # LOG_APPEND_TIME
            timestamp = self.max_timestamp
        else:
            timestamp = self.first_timestamp + ts_delta

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &offset_delta)
        offset = self.base_offset + offset_delta

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &key_len)
        if key_len >= 0:
            self._check_bounds(pos, <Py_ssize_t> key_len)
            key = PyBytes_FromStringAndSize(
                &buf[pos], <Py_ssize_t> key_len)
            pos += <Py_ssize_t> key_len
        else:
            key = None

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &value_len)
        if value_len >= 0:
            self._check_bounds(pos, <Py_ssize_t> value_len)
            value = PyBytes_FromStringAndSize(
                &buf[pos], <Py_ssize_t> value_len)
            pos += <Py_ssize_t> value_len
        else:
            value = None

        self._check_bounds(pos, 1)
        cutil.decode_varint64(buf, &pos, &header_count)
        if header_count < 0:
            raise CorruptRecordException("Found invalid number of record "
                                         "headers {}".format(header_count))
        headers = []
        while header_count > 0:
            # Header key is of type String, that can't be None
            cutil.decode_varint64(buf, &pos, &key_len)
            if key_len < 0:
                raise CorruptRecordException(
                    "Invalid negative header key size %d" % (key_len, ))
            self._check_bounds(pos, <Py_ssize_t> key_len)
            h_key = PyBytes_FromStringAndSize(
                &buf[pos], <Py_ssize_t> key_len)
            pos += <Py_ssize_t> key_len

            # Value is of type NULLABLE_BYTES, so it can be None
            cutil.decode_varint64(buf, &pos, &value_len)
            if value_len >= 0:
                self._check_bounds(pos, <Py_ssize_t> value_len)
                h_value = PyBytes_FromStringAndSize(
                    &buf[pos], <Py_ssize_t> value_len)
                pos += <Py_ssize_t> value_len
            else:
                h_value = None

            headers.append((h_key.decode("utf-8"), h_value))
            header_count -= 1

        # validate whether we have read all bytes in the current record
        if pos - start_pos != <Py_ssize_t> length:
            raise CorruptRecordException(
                "Invalid record size: expected to read {} bytes in record "
                "payload, but instead read {}".format(length, pos - start_pos))
        self._pos = pos

        return DefaultRecord.new(
            offset, timestamp, self.timestamp_type, key, value, headers)

    def __iter__(self):
        assert self._next_record_index == 0
        self._maybe_uncompress()
        return self

    def __next__(self):
        if self._next_record_index >= self.num_records:
            if self._pos != self._buffer.len:
                raise CorruptRecordException(
                    "{} unconsumed bytes after all records consumed".format(
                        self._buffer.len - self._pos))
            self._next_record_index = 0    
            raise StopIteration

        msg = self._read_msg()
        self._next_record_index += 1
        return msg

    # NOTE from CYTHON docs:
    # ```
    #    Do NOT explicitly give your type a next() method, or bad things
    #    could happen.
    # ```

    def validate_crc(self):
        assert self._decompressed == 0, \
            "Validate should be called before iteration"

        cdef:
            uint32_t verify_crc

        crc = self.crc
        data_view = PyMemoryView_FromMemory(
            <char *>self._buffer.buf, <Py_ssize_t>self._buffer.len,
            PyBUF_READ)
        data_view = data_view[self.ATTRIBUTES_OFFSET:]
        verify_crc = calc_crc32c(data_view.tobytes())
        return crc == verify_crc


@cython.no_gc_clear
@cython.final
@cython.freelist(_DEFAULT_RECORD_FREELIST_SIZE)
cdef class DefaultRecord:

    # V2 does not include a record checksum
    checksum = None

    def __init__(self, int64_t offset, int64_t timestamp, char timestamp_type,
                 object key, object value, object headers):
        self.offset = offset
        self.timestamp = timestamp
        self.timestamp_type = timestamp_type
        self.key = key
        self.value = value
        self.headers = headers

    @staticmethod
    cdef inline DefaultRecord new(
            int64_t offset, int64_t timestamp, char timestamp_type,
            object key, object value, object headers):
        """ Fast constructor to initialize from C.
        """
        cdef DefaultRecord record
        record = DefaultRecord.__new__(DefaultRecord)
        record.offset = offset
        record.timestamp = timestamp
        record.timestamp_type = timestamp_type
        record.key = key
        record.value = value
        record.headers = headers
        return record

    @property
    def timestamp(self):
        if self.timestamp != -1:
            return self.timestamp
        else:
            return None

    @property
    def timestamp_type(self):
        if self.timestamp != -1:
            return self.timestamp_type
        else:
            return None

    def __repr__(self):
        return (
            "DefaultRecord(offset={!r}, timestamp={!r}, timestamp_type={!r},"
            " key={!r}, value={!r}, headers={!r})".format(
                self.offset, self.timestamp, self.timestamp_type,
                self.key, self.value, self.headers)
        )



class DefaultRecordBatchBuilder(DefaultRecordBase):

    # excluding key, value and headers:
    # 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1 byte attributes
    MAX_RECORD_OVERHEAD = 21

    def __init__(
            self, magic, compression_type, is_transactional,
            producer_id, producer_epoch, base_sequence, batch_size):
        assert magic >= 2
        self._magic = magic
        self._compression_type = compression_type & self.CODEC_MASK
        self._batch_size = batch_size
        self._is_transactional = bool(is_transactional)
        # KIP-98 fields for EOS
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._base_sequence = base_sequence

        self._first_timestamp = None
        self._max_timestamp = None
        self._last_offset = 0
        self._num_records = 0

        self._buffer = bytearray(self.HEADER_STRUCT.size)

    def _get_attributes(self, include_compression_type=True):
        attrs = 0
        if include_compression_type:
            attrs |= self._compression_type
        # Timestamp Type is set by Broker
        if self._is_transactional:
            attrs |= self.TRANSACTIONAL_MASK
        # Control batches are only created by Broker
        return attrs

    def append(self, offset, timestamp, key, value, headers,
               # Cache for LOAD_FAST opcodes
               encode_varint=encode_varint, size_of_varint=size_of_varint,
               get_type=type, type_int=int, time_time=time.time,
               byte_like=(bytes, bytearray, memoryview),
               bytearray_type=bytearray, len_func=len, zero_len_varint=1
               ):
        """ Write message to messageset buffer with MsgVersion 2
        """
        # Check types
        if get_type(offset) != type_int:
            raise TypeError(offset)
        if timestamp is None:
            timestamp = type_int(time_time() * 1000)
        elif get_type(timestamp) != type_int:
            raise TypeError(timestamp)
        if not (key is None or get_type(key) in byte_like):
            raise TypeError(
                "Not supported type for key: {}".format(type(key)))
        if not (value is None or get_type(value) in byte_like):
            raise TypeError(
                "Not supported type for value: {}".format(type(value)))

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
            h_key = h_key.encode("utf-8")
            encode_varint(len_func(h_key), write_byte)
            write(h_key)
            if h_value is not None:
                encode_varint(len_func(h_value), write_byte)
                write(h_value)
            else:
                write_byte(zero_len_varint)

        message_len = len_func(message_buffer)
        main_buffer = self._buffer

        required_size = message_len + size_of_varint(message_len)
        # Check if we can write this message
        if (required_size + len_func(main_buffer) > self._batch_size and
                not first_message):
            return None

        # Those should be updated after the length check
        if self._max_timestamp < timestamp:
            self._max_timestamp = timestamp
        self._num_records += 1
        self._last_offset = offset

        encode_varint(message_len, main_buffer.append)
        main_buffer.extend(message_buffer)

        return DefaultRecordMetadata(offset, required_size, timestamp)

    def write_header(self, use_compression_type=True):
        batch_len = len(self._buffer)
        self.HEADER_STRUCT.pack_into(
            self._buffer, 0,
            0,  # BaseOffset, set by broker
            batch_len - self.AFTER_LEN_OFFSET,  # Size from here to end
            0,  # PartitionLeaderEpoch, set by broker
            self._magic,
            0,  # CRC will be set below, as we need a filled buffer for it
            self._get_attributes(use_compression_type),
            self._last_offset,
            self._first_timestamp,
            self._max_timestamp,
            self._producer_id,
            self._producer_epoch,
            self._base_sequence,
            self._num_records
        )
        crc = calc_crc32c(self._buffer[self.ATTRIBUTES_OFFSET:])
        struct.pack_into(">I", self._buffer, self.CRC_OFFSET, crc)

    def _maybe_compress(self):
        if self._compression_type != self.CODEC_NONE:
            header_size = self.HEADER_STRUCT.size
            data = bytes(self._buffer[header_size:])
            if self._compression_type == self.CODEC_GZIP:
                compressed = gzip_encode(data)
            elif self._compression_type == self.CODEC_SNAPPY:
                compressed = snappy_encode(data)
            elif self._compression_type == self.CODEC_LZ4:
                compressed = lz4_encode(data)
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

    def build(self):
        send_compressed = self._maybe_compress()
        self.write_header(send_compressed)
        return self._buffer

    def size(self):
        """ Return current size of data written to buffer
        """
        return len(self._buffer)

    def size_in_bytes(self, offset, timestamp, key, value, headers):
        if self._first_timestamp is not None:
            timestamp_delta = timestamp - self._first_timestamp
        else:
            timestamp_delta = 0
        size_of_body = (
            1 +  # Attrs
            size_of_varint(offset) +
            size_of_varint(timestamp_delta) +
            self.size_of(key, value, headers)
        )
        return size_of_body + size_of_varint(size_of_body)

    @classmethod
    def size_of(cls, key, value, headers):
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
    def estimate_size_in_bytes(cls, key, value, headers):
        """ Get the upper bound estimate on the size of record
        """
        return (
            cls.HEADER_STRUCT.size + cls.MAX_RECORD_OVERHEAD +
            cls.size_of(key, value, headers)
        )


class DefaultRecordMetadata:

    __slots__ = ("_size", "_timestamp", "_offset")

    def __init__(self, offset, size, timestamp):
        self._offset = offset
        self._size = size
        self._timestamp = timestamp

    @property
    def offset(self):
        return self._offset

    @property
    def crc(self):
        return None

    @property
    def size(self):
        return self._size

    @property
    def timestamp(self):
        return self._timestamp

    def __repr__(self):
        return (
            "DefaultRecordMetadata(offset={!r}, size={!r}, timestamp={!r})"
            .format(self._offset, self._size, self._timestamp)
        )
