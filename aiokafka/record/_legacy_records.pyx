#cython: language_level=3

from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode, lz4_encode_old_kafka,
)
from aiokafka.errors import CorruptRecordException
from zlib import crc32 as py_crc32  # needed for windows macro

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
from aiokafka.record cimport _hton as hton
from aiokafka.record cimport _cutil as cutil

# Those are used for fast size calculations
DEF RECORD_OVERHEAD_V0_DEF = 14
DEF RECORD_OVERHEAD_V1_DEF = 22
DEF KEY_OFFSET_V0 = 18
DEF KEY_OFFSET_V1 = 26
DEF LOG_OVERHEAD = 12
DEF KEY_LENGTH = 4
DEF VALUE_LENGTH = 4

# Field offsets
DEF LENGTH_OFFSET = 8
DEF CRC_OFFSET = LENGTH_OFFSET + 4
DEF MAGIC_OFFSET = CRC_OFFSET + 4
DEF ATTRIBUTES_OFFSET = MAGIC_OFFSET + 1
DEF TIMESTAMP_OFFSET = ATTRIBUTES_OFFSET + 1

# Attribute parsing flags
DEF ATTR_CODEC_MASK = 0x07
DEF ATTR_CODEC_GZIP = 0x01
DEF ATTR_CODEC_SNAPPY = 0x02
DEF ATTR_CODEC_LZ4 = 0x03

DEF LEGACY_RECORD_METADATA_FREELIST_SIZE = 20


cdef class _LegacyRecordBatchBuilderCython:

    cdef:
        char _magic
        char _compression_type
        Py_ssize_t _batch_size
        bytearray _buffer

    CODEC_GZIP = ATTR_CODEC_GZIP
    CODEC_SNAPPY = ATTR_CODEC_SNAPPY
    CODEC_LZ4 = ATTR_CODEC_LZ4

    def __cinit__(self, char magic, char compression_type,
                  Py_ssize_t batch_size):
        self._magic = magic
        self._compression_type = compression_type
        self._batch_size = batch_size
        self._buffer = bytearray()

    def append(self, int64_t offset, timestamp, key, value):
        """ Append message to batch.
        """
        cdef:
            Py_ssize_t pos
            Py_ssize_t size
            char *buf
            int64_t ts
            LegacyRecordMetadata metadata
            uint32_t crc

        if self._magic == 0:
            ts = -1
        elif timestamp is None:
            ts = cutil.get_time_as_unix_ms()
        else:
            ts = timestamp

        # Check if we have room for another message
        pos = PyByteArray_GET_SIZE(self._buffer)
        size = _size_in_bytes(self._magic, key, value)
        # We always allow at least one record to be appended
        if offset != 0 and pos + size >= self._batch_size:
            return None

        # Allocate proper buffer length
        PyByteArray_Resize(self._buffer, pos + size)

        # Encode message
        buf = PyByteArray_AS_STRING(self._buffer)
        _encode_msg(
            self._magic, pos, buf,
            offset, ts, key, value, 0, &crc)

        metadata = LegacyRecordMetadata.new(offset, crc, size, ts)
        return metadata

    def size(self):
        """ Return current size of data written to buffer
        """
        return PyByteArray_GET_SIZE(self._buffer)

    # Size calculations. Just copied Java's implementation

    def size_in_bytes(self, offset, timestamp, key, value):
        """ Actual size of message to add
        """
        return _size_in_bytes(self._magic, key, value)

    @staticmethod
    def record_overhead(char magic):
        if magic == 0:
            return RECORD_OVERHEAD_V0_DEF
        else:
            return RECORD_OVERHEAD_V1_DEF

    cdef int _maybe_compress(self) except -1:
        cdef:
            object compressed
            char *buf
            Py_ssize_t size
            uint32_t crc

        if self._compression_type != 0:
            if self._compression_type == ATTR_CODEC_GZIP:
                compressed = gzip_encode(self._buffer)
            elif self._compression_type == ATTR_CODEC_SNAPPY:
                compressed = snappy_encode(self._buffer)
            elif self._compression_type == ATTR_CODEC_LZ4:
                if self._magic == 0:
                    compressed = lz4_encode_old_kafka(bytes(self._buffer))
                else:
                    compressed = lz4_encode(bytes(self._buffer))
            else:
                return 0
            size = _size_in_bytes(self._magic, key=None, value=compressed)
            # We will just write the result into the same memory space.
            PyByteArray_Resize(self._buffer, size)

            buf = PyByteArray_AS_STRING(self._buffer)
            _encode_msg(
                self._magic, start_pos=0, buf=buf,
                offset=0, timestamp=0, key=None, value=compressed,
                attributes=self._compression_type, crc_out=&crc)
            return 1
        return 0

    def build(self):
        """Compress batch to be ready for send"""
        self._maybe_compress()
        return self._buffer


cdef Py_ssize_t _size_in_bytes(char magic, object key, object value) except -1:
    """ Actual size of message to add
    """
    cdef:
        Py_buffer buf
        Py_ssize_t key_len
        Py_ssize_t value_len

    if key is None:
        key_len = 0
    else:
        PyObject_GetBuffer(key, &buf, PyBUF_SIMPLE)
        key_len = buf.len
        PyBuffer_Release(&buf)

    if value is None:
        value_len = 0
    else:
        PyObject_GetBuffer(value, &buf, PyBUF_SIMPLE)
        value_len = buf.len
        PyBuffer_Release(&buf)

    if magic == 0:
        return LOG_OVERHEAD + RECORD_OVERHEAD_V0_DEF + key_len + value_len
    else:
        return LOG_OVERHEAD + RECORD_OVERHEAD_V1_DEF + key_len + value_len


cdef int _encode_msg(
        char magic, Py_ssize_t start_pos, char *buf,
        long offset, long timestamp, object key, object value,
        char attributes, uint32_t* crc_out) except -1:
    """ Encode msg data into the `msg_buffer`, which should be allocated
        to at least the size of this message.
    """
    cdef:
        Py_buffer key_val_buf
        Py_ssize_t pos = start_pos
        int32_t length
        uint32_t crc

    # Write key and value
    pos += KEY_OFFSET_V0 if magic == 0 else KEY_OFFSET_V1

    if key is None:
        hton.pack_int32(&buf[pos], -1)
        pos += KEY_LENGTH
    else:
        PyObject_GetBuffer(key, &key_val_buf, PyBUF_SIMPLE)
        hton.pack_int32(&buf[pos], <int32_t>key_val_buf.len)
        pos += KEY_LENGTH
        memcpy(&buf[pos], <char*>key_val_buf.buf, <size_t>key_val_buf.len)
        pos += key_val_buf.len
        PyBuffer_Release(&key_val_buf)

    if value is None:
        hton.pack_int32(&buf[pos], -1)
        pos += VALUE_LENGTH
    else:
        PyObject_GetBuffer(value, &key_val_buf, PyBUF_SIMPLE)
        hton.pack_int32(&buf[pos], <int32_t>key_val_buf.len)
        pos += VALUE_LENGTH
        memcpy(&buf[pos], <char*>key_val_buf.buf, <size_t>key_val_buf.len)
        pos += key_val_buf.len
        PyBuffer_Release(&key_val_buf)
    length = <int32_t> ((pos - start_pos) - LOG_OVERHEAD)

    # Write msg header. Note, that Crc should be updated last
    hton.pack_int64(&buf[start_pos], <int64_t>offset)
    hton.pack_int32(&buf[start_pos + LENGTH_OFFSET], length)
    buf[start_pos + MAGIC_OFFSET] = magic
    buf[start_pos + ATTRIBUTES_OFFSET] = attributes
    if magic == 1:
        hton.pack_int64(&buf[start_pos + TIMESTAMP_OFFSET], <int64_t>timestamp)

    crc = <uint32_t> cutil.calc_crc32(
        0,
        <unsigned char*> &buf[start_pos + MAGIC_OFFSET],
        (pos - (start_pos + MAGIC_OFFSET))
    )
    hton.pack_int32(&buf[start_pos + CRC_OFFSET], <int32_t> crc)

    crc_out[0] = crc
    return 0


@cython.no_gc_clear
@cython.final
@cython.freelist(LEGACY_RECORD_METADATA_FREELIST_SIZE)
cdef class LegacyRecordMetadata:

    cdef:
        readonly int64_t offset
        readonly uint32_t crc
        readonly Py_ssize_t size
        readonly int64_t timestamp

    def __init__(self, int64_t offset, uint32_t crc, Py_ssize_t size,
                 int64_t timestamp):
        self.offset = offset
        self.crc = crc
        self.size = size
        self.timestamp = timestamp

    @staticmethod
    cdef inline LegacyRecordMetadata new(
            int64_t offset, uint32_t crc, Py_ssize_t size,
            int64_t timestamp):
        """ Fast constructor to initialize from C.
        """
        cdef LegacyRecordMetadata metadata
        metadata = LegacyRecordMetadata.__new__(LegacyRecordMetadata)
        metadata.offset = offset
        metadata.crc = crc
        metadata.size = size
        metadata.timestamp = timestamp
        return metadata

    def __repr__(self):
        return (
            "LegacyRecordMetadata(offset={!r}, crc={!r}, size={!r},"
            " timestamp={!r})".format(
                self.offset, self.crc, self.size, self.timestamp)
        )
