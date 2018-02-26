import struct
from unittest import mock

import pytest
from aiokafka.record.legacy_records import (
    LegacyRecordBatch, LegacyRecordBatchBuilder
)
from aiokafka.errors import CorruptRecordException


@pytest.mark.parametrize("magic", [0, 1])
@pytest.mark.parametrize("key,value,checksum", [
    (b"test", b"Super", [278251978, -2095076219]),
    (b"test", None, [580701536, 164492157]),
    (None, b"Super", [2797021502, 3315209433]),
    (b"", b"Super", [1446809667, 890351012]),
    (b"test", b"", [4230475139, 3614888862]),
])
def test_read_write_serde_v0_v1_no_compression(magic, key, value, checksum):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024)
    builder.append(0, timestamp=9999999, key=key, value=value)
    buffer = builder.build()

    batch = LegacyRecordBatch(buffer, magic)
    assert batch.validate_crc()
    msgs = list(batch)
    assert len(msgs) == 1
    msg = msgs[0]

    assert msg.offset == 0
    assert msg.timestamp == (9999999 if magic else None)
    assert msg.timestamp_type == (0 if magic else None)
    assert msg.key == key
    assert msg.value == value
    assert msg.checksum == checksum[magic] & 0xffffffff


@pytest.mark.parametrize("compression_type", [
    LegacyRecordBatch.CODEC_GZIP,
    LegacyRecordBatch.CODEC_SNAPPY,
    LegacyRecordBatch.CODEC_LZ4
])
@pytest.mark.parametrize("magic", [0, 1])
def test_read_write_serde_v0_v1_with_compression(compression_type, magic):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=compression_type, batch_size=1024 * 1024)
    for offset in range(10):
        builder.append(
            offset, timestamp=9999999, key=b"test", value=b"Super")
    buffer = builder.build()

    batch = LegacyRecordBatch(buffer, magic)
    assert batch.validate_crc()
    msgs = list(batch)

    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == (9999999 if magic else None)
        assert msg.timestamp_type == (0 if magic else None)
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.checksum == (-2095076219 if magic else 278251978) & \
            0xffffffff


@pytest.mark.parametrize("magic", [0, 1])
def test_written_bytes_equals_size_in_bytes(magic):
    key = b"test"
    value = b"Super"
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024)

    size_in_bytes = builder.size_in_bytes(
        0, timestamp=9999999, key=key, value=value)

    pos = builder.size()
    builder.append(0, timestamp=9999999, key=key, value=value)

    assert builder.size() - pos == size_in_bytes


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_batch_builder_validates_arguments(magic):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024)

    # Key should not be str
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp=9999999, key="some string", value=None)

    # Value should not be str
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp=9999999, key=None, value="some string")

    # Timestamp should be of proper type (timestamp is ignored for magic == 0)
    if magic != 0:
        with pytest.raises(TypeError):
            builder.append(
                0, timestamp="1243812793", key=None, value=b"some string")

    # Offset of invalid type
    with pytest.raises(TypeError):
        builder.append(
            "0", timestamp=9999999, key=None, value=b"some string")

    # Unknown struct errors are passed through. These are theoretical and
    # indicate a bug in the implementation. The C implementation locates
    # _encode_msg elsewhere and is less vulnerable to such bugs since it's
    # statically typed, so we skip the test there.
    if hasattr(builder, "_encode_msg"):
        with mock.patch.object(builder, "_encode_msg") as mocked:
            err = struct.error("test error")
            mocked.side_effect = err
            with pytest.raises(struct.error) as excinfo:
                builder.append(
                    0, timestamp=None, key=None, value=b"some string")
            assert excinfo.value == err

    # Ok to pass value as None
    builder.append(
        0, timestamp=9999999, key=b"123", value=None)

    # Timestamp can be None
    builder.append(
        1, timestamp=None, key=None, value=b"some string")

    # Ok to pass offsets in not incremental order. This should not happen thou
    builder.append(
        5, timestamp=9999999, key=b"123", value=None)

    # in case error handling code fails to fix inner buffer in builder
    assert len(builder.build()) == 119 if magic else 95


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_correct_metadata_response(magic):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024)
    meta = builder.append(
        0, timestamp=9999999, key=b"test", value=b"Super")

    assert meta.offset == 0
    assert meta.timestamp == (9999999 if magic else -1)
    assert meta.crc == (-2095076219 if magic else 278251978) & 0xffffffff
    assert repr(meta) == (
        "LegacyRecordMetadata(offset=0, crc={}, size={}, "
        "timestamp={})".format(meta.crc, meta.size, meta.timestamp)
    )


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_batch_size_limit(magic):
    # First message can be added even if it's too big
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024)
    meta = builder.append(0, timestamp=None, key=None, value=b"M" * 2000)
    assert meta.size > 0
    assert meta.crc is not None
    assert meta.offset == 0
    assert meta.timestamp is not None
    assert len(builder.build()) > 2000

    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024)
    meta = builder.append(0, timestamp=None, key=None, value=b"M" * 700)
    assert meta is not None
    meta = builder.append(1, timestamp=None, key=None, value=b"M" * 700)
    assert meta is None
    meta = builder.append(2, timestamp=None, key=None, value=b"M" * 700)
    assert meta is None
    assert len(builder.build()) < 1000


ATTRIBUTES_OFFSET = 17
TIMESTAMP_OFFSET = 18
TIMESTAMP_TYPE_MASK = 0x08


def _make_compressed_batch(magic):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=LegacyRecordBatch.CODEC_GZIP,
        batch_size=1024 * 1024)
    for offset in range(10):
        builder.append(
            offset, timestamp=9999999, key=b"test", value=b"Super")
    return builder.build()


def test_read_log_append_time_v1():
    buffer = _make_compressed_batch(1)

    # As Builder does not support creating data with `timestamp_type==1` we
    # patch the result manually

    buffer[ATTRIBUTES_OFFSET] |= TIMESTAMP_TYPE_MASK
    expected_timestamp = 10000000
    struct.pack_into(">q", buffer, TIMESTAMP_OFFSET, expected_timestamp)

    batch = LegacyRecordBatch(buffer, 1)
    msgs = list(batch)

    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == expected_timestamp
        assert msg.timestamp_type == 1


@pytest.mark.parametrize("magic", [0, 1])
def test_reader_corrupt_record_v0_v1(magic):
    buffer = _make_compressed_batch(magic)
    len_offset = 8

    # If the wrapper of compressed messages has a key it will just be ignored.
    key_offset = 26 if magic else 18
    new_buffer = (
        buffer[:key_offset] +
        b"\x00\x00\x00\x03123" +  # Insert some KEY into wrapper
        buffer[key_offset + 4:]  # Ignore the 4 byte -1 value for old KEY==None
    )
    struct.pack_into(">i", new_buffer, len_offset, len(new_buffer) - 12)
    batch = LegacyRecordBatch(new_buffer, magic)
    msgs = list(batch)
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == (9999999 if magic else None)
        assert msg.timestamp_type == (0 if magic else None)
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.checksum == (-2095076219 if magic else 278251978) & \
            0xffffffff

    # If the wrapper does not contain a `value` it's corrupted
    value_offset = 30 if magic else 22
    new_buffer = (
        buffer[:value_offset] +
        b"\xff\xff\xff\xff"  # Set `value` to None by altering size to -1
    )
    struct.pack_into(">i", new_buffer, len_offset, len(new_buffer) - 12)
    with pytest.raises(
            CorruptRecordException,
            match="Value of compressed message is None"):
        batch = LegacyRecordBatch(new_buffer, magic)
        list(batch)


def test_record_overhead():
    known = {
        0: 14,
        1: 22,
    }
    for magic, size in known.items():
        assert LegacyRecordBatchBuilder.record_overhead(magic) == size
