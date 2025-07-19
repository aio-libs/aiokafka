from typing import Literal
from unittest import mock

import pytest

import aiokafka.codec
from aiokafka.errors import UnsupportedCodecError
from aiokafka.record.default_records import (
    DefaultRecordBatch,
    DefaultRecordBatchBuilder,
)

HeadersT = list[tuple[str, bytes | None]]


@pytest.mark.parametrize(
    "compression_type,crc",
    [
        pytest.param(DefaultRecordBatch.CODEC_NONE, 3950153926, id="none"),
        # Gzip header includes timestamp, so checksum varies
        pytest.param(DefaultRecordBatch.CODEC_GZIP, None, id="gzip"),
        pytest.param(DefaultRecordBatch.CODEC_SNAPPY, 2171068483, id="snappy"),
        # cramjam uses different parameters for LZ4, so checksum varies from
        # version to version
        pytest.param(DefaultRecordBatch.CODEC_LZ4, None, id="lz4"),
        pytest.param(DefaultRecordBatch.CODEC_ZSTD, 1714138923, id="zstd"),
    ],
)
def test_read_write_serde_v2(
    compression_type: Literal[0x00, 0x01, 0x02, 0x03, 0x04], crc: int
) -> None:
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=compression_type,
        is_transactional=1,
        producer_id=123456,
        producer_epoch=123,
        base_sequence=9999,
        batch_size=999999,
    )
    headers: HeadersT = [("header1", b"aaa"), ("header2", b"bbb")]
    for offset in range(10):
        builder.append(
            offset,
            timestamp=9999999 + offset,
            key=b"test",
            value=b"Super",
            headers=headers,
        )
    buffer = builder.build()
    reader = DefaultRecordBatch(bytes(buffer))
    assert reader.validate_crc()
    msgs = list(reader)

    assert reader.is_transactional is True
    assert reader.is_control_batch is False
    assert reader.compression_type == compression_type
    assert reader.magic == 2
    assert reader.timestamp_type == 0
    assert reader.base_offset == 0
    assert reader.last_offset_delta == 9
    assert reader.next_offset == 10
    assert reader.first_timestamp == 9999999
    assert reader.max_timestamp == 10000008
    if crc is not None:
        assert reader.crc == crc
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == 9999999 + offset
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.headers == headers


def test_written_bytes_equals_size_in_bytes_v2() -> None:
    key = b"test"
    value = b"Super"
    headers: HeadersT = [("header1", b"aaa"), ("header2", b"bbb"), ("xx", None)]
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=999999,
    )

    size_in_bytes = builder.size_in_bytes(
        0, timestamp=9999999, key=key, value=value, headers=headers
    )

    pos = builder.size()
    meta = builder.append(0, timestamp=9999999, key=key, value=value, headers=headers)

    assert builder.size() - pos == size_in_bytes
    assert meta is not None
    assert meta.size == size_in_bytes


def test_estimate_size_in_bytes_bigger_than_batch_v2() -> None:
    key = b"Super Key"
    value = b"1" * 100
    headers: HeadersT = [("header1", b"aaa"), ("header2", b"bbb")]
    estimate_size = DefaultRecordBatchBuilder.estimate_size_in_bytes(
        key, value, headers
    )

    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=999999,
    )
    builder.append(0, timestamp=9999999, key=key, value=value, headers=headers)
    buf = builder.build()
    assert len(buf) <= estimate_size, "Estimate should always be upper bound"


def test_default_batch_builder_validates_arguments() -> None:
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=999999,
    )

    # Key should not be str
    with pytest.raises(TypeError):
        builder.append(0, timestamp=9999999, key="some string", value=None, headers=[])  # type: ignore[arg-type]

    # Value should not be str
    with pytest.raises(TypeError):
        builder.append(0, timestamp=9999999, key=None, value="some string", headers=[])  # type: ignore[arg-type]

    # Timestamp should be of proper type
    with pytest.raises(TypeError):
        builder.append(
            0,
            timestamp="1243812793",  # type: ignore[arg-type]
            key=None,
            value=b"some string",
            headers=[],
        )

    # Offset of invalid type
    with pytest.raises(TypeError):
        builder.append(
            "0",  # type: ignore[arg-type]
            timestamp=9999999,
            key=None,
            value=b"some string",
            headers=[],
        )

    # Ok to pass value as None
    builder.append(0, timestamp=9999999, key=b"123", value=None, headers=[])

    # Timestamp can be None
    builder.append(1, timestamp=None, key=None, value=b"some string", headers=[])

    # Ok to pass offsets in not incremental order. This should not happen thou
    builder.append(5, timestamp=9999999, key=b"123", value=None, headers=[])

    # in case error handling code fails to fix inner buffer in builder
    assert len(builder.build()) == 104


def test_default_correct_metadata_response() -> None:
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=1024 * 1024,
    )
    meta = builder.append(0, timestamp=9999999, key=b"test", value=b"Super", headers=[])

    assert meta is not None
    assert meta.offset == 0
    assert meta.timestamp == 9999999
    assert meta.crc is None
    assert meta.size == 16
    assert repr(meta) == (
        f"DefaultRecordMetadata(offset=0, size={meta.size}, timestamp={meta.timestamp})"
    )


def test_default_batch_size_limit() -> None:
    # First message can be added even if it's too big
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=1024,
    )

    meta = builder.append(0, timestamp=None, key=None, value=b"M" * 2000, headers=[])
    assert meta is not None
    assert meta.size > 0
    assert meta.crc is None
    assert meta.offset == 0
    assert meta.timestamp is not None
    assert len(builder.build()) > 2000

    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=1024,
    )
    meta = builder.append(0, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is not None
    meta = builder.append(1, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is None
    meta = builder.append(2, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is None
    assert len(builder.build()) < 1000


@pytest.mark.parametrize(
    "compression_type,name,checker_name",
    [
        (DefaultRecordBatch.CODEC_GZIP, "gzip", "has_gzip"),
        (DefaultRecordBatch.CODEC_SNAPPY, "snappy", "has_snappy"),
        (DefaultRecordBatch.CODEC_LZ4, "lz4", "has_lz4"),
        (DefaultRecordBatch.CODEC_ZSTD, "zstd", "has_zstd"),
    ],
)
def test_unavailable_codec(
    compression_type: Literal[0x01, 0x02, 0x03, 0x04], name: str, checker_name: str
) -> None:
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=compression_type,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=1024,
    )
    builder.append(0, timestamp=None, key=None, value=b"M" * 2000, headers=[])
    correct_buffer = builder.build()

    with mock.patch.object(aiokafka.codec, checker_name, return_value=False):
        # Check that builder raises error
        builder = DefaultRecordBatchBuilder(
            magic=2,
            compression_type=compression_type,
            is_transactional=0,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=-1,
            batch_size=1024,
        )
        error_msg = f"Libraries for {name} compression codec not found"
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            builder.append(0, timestamp=None, key=None, value=b"M", headers=[])
            builder.build()

        # Check that reader raises same error
        batch = DefaultRecordBatch(bytes(correct_buffer))
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            list(batch)


def test_unsupported_yet_codec() -> None:
    compression_type = DefaultRecordBatch.CODEC_MASK  # It doesn't exist
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=compression_type,  # type: ignore[arg-type]
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=1024,
    )
    with pytest.raises(UnsupportedCodecError):
        builder.append(0, timestamp=None, key=None, value=b"M", headers=[])
        builder.build()


def test_build_without_append() -> None:
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=1,
        producer_id=123456,
        producer_epoch=123,
        base_sequence=9999,
        batch_size=999999,
    )
    buffer = builder.build()

    reader = DefaultRecordBatch(bytes(buffer))
    msgs = list(reader)
    assert not msgs


def test_set_producer_state() -> None:
    builder = DefaultRecordBatchBuilder(
        magic=2,
        compression_type=0,
        is_transactional=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        batch_size=999999,
    )
    builder.set_producer_state(producer_id=700, producer_epoch=5, base_sequence=17)
    assert builder.producer_id == 700
    buffer = builder.build()

    reader = DefaultRecordBatch(bytes(buffer))
    assert reader.producer_id == 700
    assert reader.producer_epoch == 5
    assert reader.base_sequence == 17
