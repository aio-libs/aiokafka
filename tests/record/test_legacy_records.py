from typing import Literal

import pytest

from aiokafka import codec as codecs
from aiokafka.record.legacy_records import LegacyRecordBatch, LegacyRecordBatchBuilder


@pytest.mark.parametrize(
    "magic,compression_type",
    [
        pytest.param(
            0,
            LegacyRecordBatch.CODEC_GZIP,
            marks=pytest.mark.skipif(
                not codecs.has_gzip(), reason="Gzip not available"
            ),
            id="v0-gzip",
        ),
        pytest.param(
            0,
            LegacyRecordBatch.CODEC_SNAPPY,
            marks=pytest.mark.skipif(
                not codecs.has_snappy(), reason="Snappy not available"
            ),
            id="v0-snappy",
        ),
        pytest.param(
            1,
            LegacyRecordBatch.CODEC_GZIP,
            marks=pytest.mark.skipif(
                not codecs.has_gzip(), reason="Gzip not available"
            ),
            id="v1-gzip",
        ),
        pytest.param(
            1,
            LegacyRecordBatch.CODEC_SNAPPY,
            marks=pytest.mark.skipif(
                not codecs.has_snappy(), reason="Snappy not available"
            ),
            id="v1-snappy",
        ),
        pytest.param(
            1,
            LegacyRecordBatch.CODEC_LZ4,
            marks=pytest.mark.skipif(not codecs.has_lz4(), reason="LZ4 not available"),
            id="v1-lz4",
        ),
    ],
)
def test_compressed_legacy_records(magic: Literal[0, 1], compression_type: int) -> None:
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=compression_type, batch_size=100000
    )
    value = b"test" * 1000
    builder.append(0, timestamp=1000, key=b"key", value=value)
    batch = LegacyRecordBatch(bytes(builder.build()), magic=magic)

    assert batch.validate_crc()
    records = list(batch)
    assert len(records) == 1
    assert records[0].key == b"key"
    assert records[0].value == value
