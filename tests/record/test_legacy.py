import pytest
from aiokafka.record.legacy_records import (
    LegacyRecordBatch, LegacyRecordBatchBuilder
)


@pytest.mark.parametrize("magic", [0, 1])
def test_read_write_serde_v0_v1_no_compression(magic):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024 * 1024)
    builder.append(
        0, timestamp=9999999, key=b"test", value=b"Super")
    buffer = builder.build()

    batch = LegacyRecordBatch(buffer, magic)
    msgs = list(batch)
    assert len(msgs) == 1
    msg = msgs[0]

    assert msg.offset == 0
    assert msg.timestamp == (9999999 if magic else None)
    assert msg.timestamp_type == (0 if magic else None)
    assert msg.key == b"test"
    assert msg.value == b"Super"
    assert msg.checksum == (-2095076219 if magic else 278251978) & 0xffffffff


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


# @pytest.mark.parametrize("magic", [0, 1])
# @pytest.mark.parametrize("compression_type", [
#     0,
#     LegacyRecordBatch.CODEC_GZIP,
#     LegacyRecordBatch.CODEC_SNAPPY,
#     LegacyRecordBatch.CODEC_LZ4
# ])
# def test_estimate_size_in_bytes_bigger_than_batch(magic, compression_type):
#     key = b"Super Key"
#     value = b"1" * 100
#     estimate_size = LegacyRecordBatchBuilder.estimate_size_in_bytes(
#         magic, compression_type, key, value)

#     builder = LegacyRecordBatchBuilder(
#         magic=magic, compression_type=compression_type)
#     builder.append(
#         0, timestamp=9999999, key=key, value=value)
#     buf = builder.build()
#     assert len(buf.getvalue()) <= estimate_size, \
#         "Estimate should always be upper bound"


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

    # Timestamp should be of proper type
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp="1243812793", key=None, value=b"some string")

    # Offset of invalid type
    with pytest.raises(TypeError):
        builder.append(
            "0", timestamp=9999999, key=None, value=b"some string")

    # Ok to pass value as None
    builder.append(
        0, timestamp=9999999, key=b"123", value=None)

    # Timestamp can be None
    builder.append(
        1, timestamp=None, key=None, value=b"some string")

    # Ok to pass offsets in not incremental order. This should not happen thou
    builder.append(
        5, timestamp=9999999, key=b"123", value=None)


@pytest.mark.parametrize("magic", [0, 1])
def test_legacy_batch_size_limit(magic):
    # First message can be added even if it's too big
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024)
    crc, size = builder.append(0, timestamp=None, key=None, value=b"M" * 2000)
    assert size > 0
    assert crc is not None
    assert len(builder.build()) > 2000

    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=1024)
    crc, size = builder.append(0, timestamp=None, key=None, value=b"M" * 700)
    assert size > 0
    crc, size = builder.append(1, timestamp=None, key=None, value=b"M" * 700)
    assert size == 0
    assert crc is None
    crc, size = builder.append(2, timestamp=None, key=None, value=b"M" * 700)
    assert size == 0
    assert crc is None
    assert len(builder.build()) < 1000
