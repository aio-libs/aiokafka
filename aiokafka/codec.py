import gzip
import io
import struct

_XERIAL_V1_HEADER = (-126, b"S", b"N", b"A", b"P", b"P", b"Y", 0, 1, 1)
_XERIAL_V1_FORMAT = "bccccccBii"
ZSTD_MAX_OUTPUT_SIZE = 1024 * 1024

try:
    import cramjam
except ImportError:
    cramjam = None

try:
    import lz4.frame as lz4

    def _lz4_compress(payload, **kwargs):
        # Kafka does not support LZ4 dependent blocks
        # https://cwiki.apache.org/confluence/display/KAFKA/KIP-57+-+Interoperable+LZ4+Framing
        kwargs.pop("block_linked", None)
        return lz4.compress(payload, block_linked=False, **kwargs)

except ImportError:
    lz4 = None

try:
    import lz4f
except ImportError:
    lz4f = None

try:
    import lz4framed
except ImportError:
    lz4framed = None


def has_gzip():
    return True


def has_snappy():
    return cramjam is not None


def has_zstd():
    return cramjam is not None


def has_lz4():
    if lz4 is not None:
        return True
    if lz4f is not None:
        return True
    if lz4framed is not None:
        return True
    return False


def gzip_encode(payload, compresslevel=None):
    if not compresslevel:
        compresslevel = 9

    buf = io.BytesIO()

    # Gzip context manager introduced in python 2.7
    # so old-fashioned way until we decide to not support 2.6
    gzipper = gzip.GzipFile(fileobj=buf, mode="w", compresslevel=compresslevel)
    try:
        gzipper.write(payload)
    finally:
        gzipper.close()

    return buf.getvalue()


def gzip_decode(payload):
    buf = io.BytesIO(payload)

    # Gzip context manager introduced in python 2.7
    # so old-fashioned way until we decide to not support 2.6
    gzipper = gzip.GzipFile(fileobj=buf, mode="r")
    try:
        return gzipper.read()
    finally:
        gzipper.close()


def snappy_encode(payload, xerial_compatible=True, xerial_blocksize=32 * 1024):
    """Encodes the given data with snappy compression.

    If xerial_compatible is set then the stream is encoded in a fashion
    compatible with the xerial snappy library.

    The block size (xerial_blocksize) controls how frequent the blocking occurs
    32k is the default in the xerial library.

    The format winds up being:


        +-------------+------------+--------------+------------+--------------+
        |   Header    | Block1 len | Block1 data  | Blockn len | Blockn data  |
        +-------------+------------+--------------+------------+--------------+
        |  16 bytes   |  BE int32  | snappy bytes |  BE int32  | snappy bytes |
        +-------------+------------+--------------+------------+--------------+


    It is important to note that the blocksize is the amount of uncompressed
    data presented to snappy at each block, whereas the blocklen is the number
    of bytes that will be present in the stream; so the length will always be
    <= blocksize.

    """

    if not has_snappy():
        raise NotImplementedError("Snappy codec is not available")

    if not xerial_compatible:
        return cramjam.snappy.compress_raw(payload)

    out = io.BytesIO()
    for fmt, dat in zip(_XERIAL_V1_FORMAT, _XERIAL_V1_HEADER):
        out.write(struct.pack("!" + fmt, dat))

    # Chunk through buffers to avoid creating intermediate slice copies
    def chunker(payload, i, size):
        return memoryview(payload)[i:size + i]

    for chunk in (
        chunker(payload, i, xerial_blocksize)
        for i in range(0, len(payload), xerial_blocksize)
    ):

        block = cramjam.snappy.compress_raw(chunk)
        block_size = len(block)
        out.write(struct.pack("!i", block_size))
        out.write(block)

    return out.getvalue()


def _detect_xerial_stream(payload):
    """Detects if the data given might have been encoded with the blocking mode
    of the xerial snappy library.

    This mode writes a magic header of the format:
        +--------+--------------+------------+---------+--------+
        | Marker | Magic String | Null / Pad | Version | Compat |
        +--------+--------------+------------+---------+--------+
        |  byte  |   c-string   |    byte    |  int32  | int32  |
        +--------+--------------+------------+---------+--------+
        |  -126  |   'SNAPPY'   |     \0     |         |        |
        +--------+--------------+------------+---------+--------+

    The pad appears to be to ensure that SNAPPY is a valid cstring
    The version is the version of this format as written by xerial,
    in the wild this is currently 1 as such we only support v1.

    Compat is there to claim the minimum supported version that
    can read a xerial block stream, presently in the wild this is
    1.
    """

    if len(payload) > 16:
        header = struct.unpack("!" + _XERIAL_V1_FORMAT, bytes(payload)[:16])
        return header == _XERIAL_V1_HEADER
    return False


def snappy_decode(payload):
    if not has_snappy():
        raise NotImplementedError("Snappy codec is not available")

    if _detect_xerial_stream(payload):
        # TODO ? Should become a fileobj ?
        out = io.BytesIO()
        byt = payload[16:]
        length = len(byt)
        cursor = 0

        while cursor < length:
            block_size = struct.unpack_from("!i", byt[cursor:])[0]
            # Skip the block size
            cursor += 4
            end = cursor + block_size
            out.write(cramjam.snappy.decompress_raw(byt[cursor:end]))
            cursor = end

        out.seek(0)
        return out.read()
    else:
        return bytes(cramjam.snappy.decompress_raw(payload))


if lz4:
    lz4_encode = _lz4_compress  # pylint: disable-msg=no-member
elif lz4f:
    lz4_encode = lz4f.compressFrame  # pylint: disable-msg=no-member
elif lz4framed:
    lz4_encode = lz4framed.compress  # pylint: disable-msg=no-member
else:
    lz4_encode = None


def lz4f_decode(payload):
    """Decode payload using interoperable LZ4 framing. Requires Kafka >= 0.10"""
    # pylint: disable-msg=no-member
    ctx = lz4f.createDecompContext()
    data = lz4f.decompressFrame(payload, ctx)
    lz4f.freeDecompContext(ctx)

    # lz4f python module does not expose how much of the payload was
    # actually read if the decompression was only partial.
    if data["next"] != 0:
        raise RuntimeError("lz4f unable to decompress full payload")
    return data["decomp"]


if lz4:
    lz4_decode = lz4.decompress  # pylint: disable-msg=no-member
elif lz4f:
    lz4_decode = lz4f_decode
elif lz4framed:
    lz4_decode = lz4framed.decompress  # pylint: disable-msg=no-member
else:
    lz4_decode = None


def zstd_encode(payload, level=None):
    if not has_zstd():
        raise NotImplementedError("Zstd codec is not available")

    if level is None:
        # Default for kafka broker
        # https://cwiki.apache.org/confluence/display/KAFKA/KIP-390%3A+Support+Compression+Level
        level = 3

    return bytes(cramjam.zstd.compress(payload, level=level))


def zstd_decode(payload):
    if not has_zstd():
        raise NotImplementedError("Zstd codec is not available")

    return bytes(cramjam.zstd.decompress(payload))
