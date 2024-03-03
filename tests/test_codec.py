import struct

import pytest

from aiokafka import codec as codecs
from aiokafka.codec import (
    gzip_decode,
    gzip_encode,
    has_lz4,
    has_snappy,
    has_zstd,
    lz4_decode,
    lz4_encode,
    snappy_decode,
    snappy_encode,
    zstd_decode,
    zstd_encode,
)

from ._testutil import random_string


def test_gzip() -> None:
    for _ in range(1000):
        b1 = random_string(100)
        b2 = gzip_decode(gzip_encode(b1))
        assert b1 == b2


@pytest.mark.skipif(not has_snappy(), reason="Snappy not available")
def test_snappy() -> None:
    for _ in range(1000):
        b1 = random_string(100)
        b2 = snappy_decode(snappy_encode(b1))
        assert b1 == b2


@pytest.mark.skipif(not has_snappy(), reason="Snappy not available")
def test_snappy_detect_xerial() -> None:
    _detect_xerial_stream = codecs._detect_xerial_stream

    header = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01Some extra bytes"
    false_header = b"\x01SNAPPY\x00\x00\x00\x01\x00\x00\x00\x01"
    default_snappy = snappy_encode(b"foobar" * 50)
    random_snappy = snappy_encode(b"SNAPPY" * 50, xerial_compatible=False)
    short_data = b"\x01\x02\x03\x04"

    assert _detect_xerial_stream(header) is True
    assert _detect_xerial_stream(b"") is False
    assert _detect_xerial_stream(b"\x00") is False
    assert _detect_xerial_stream(false_header) is False
    assert _detect_xerial_stream(default_snappy) is True
    assert _detect_xerial_stream(random_snappy) is False
    assert _detect_xerial_stream(short_data) is False


@pytest.mark.skipif(not has_snappy(), reason="Snappy not available")
def test_snappy_decode_xerial() -> None:
    header = b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01"
    random_snappy = snappy_encode(b"SNAPPY" * 50, xerial_compatible=False)
    block_len = len(random_snappy)
    random_snappy2 = snappy_encode(b"XERIAL" * 50, xerial_compatible=False)
    block_len2 = len(random_snappy2)

    to_test = (
        header
        + struct.pack("!i", block_len)
        + random_snappy
        + struct.pack("!i", block_len2)
        + random_snappy2
    )
    assert snappy_decode(to_test) == (b"SNAPPY" * 50) + (b"XERIAL" * 50)


@pytest.mark.skipif(not has_snappy(), reason="Snappy not available")
def test_snappy_encode_xerial() -> None:
    to_ensure = (
        b"\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x00\x00\x00\x18\xac\x02\x14SNAPPY\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00"
        b"\xfe\x06\x00\x96\x06\x00\x00\x00\x00\x18\xac\x02"
        b"\x14XERIAL\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\x96\x06\x00"
    )

    to_test = (b"SNAPPY" * 50) + (b"XERIAL" * 50)

    compressed = snappy_encode(to_test, xerial_compatible=True, xerial_blocksize=300)
    assert compressed == to_ensure


@pytest.mark.skipif(not has_lz4(), reason="LZ4 not available")
def test_lz4() -> None:
    for _ in range(1000):
        b1 = random_string(100)
        b2 = lz4_decode(lz4_encode(b1))
        assert len(b1) == len(b2)
        assert b1 == b2


@pytest.mark.skipif(not has_lz4(), reason="LZ4 not available")
def test_lz4_incremental() -> None:
    for _ in range(1000):
        # lz4 max single block size is 4MB
        # make sure we test with multiple-blocks
        b1 = random_string(100) * 50000
        b2 = lz4_decode(lz4_encode(b1))
        assert len(b1) == len(b2)
        assert b1 == b2


@pytest.mark.skipif(not has_zstd(), reason="Zstd not available")
def test_zstd() -> None:
    for _ in range(1000):
        b1 = random_string(100)
        b2 = zstd_decode(zstd_encode(b1))
        assert b1 == b2
