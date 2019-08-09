from cpython cimport (
    PyBytes_AS_STRING
)
from libc.stdint cimport uint32_t


# https://github.com/apache/kafka/blob/0.8.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L244
def murmur2(bytes in_bytes):
    """Cython Murmur2 implementation.

    Based on java client, see org.apache.kafka.common.utils.Utils.murmur2

    Args:
        data (bytes): opaque bytes

    Returns: MurmurHash2 of data
    """
    cdef uint32_t length, seed, r, length4, i, i4, extra_bytes
    cdef uint32_t m, h, k
    cdef char* data

    data = PyBytes_AS_STRING(in_bytes)
    length = len(data)
    seed = 0x9747b28c
    # 'm' and 'r' are mixing constants generated offline.
    # They're not really 'magic', they just happen to work well.
    m = 0x5bd1e995
    r = 24

    # Initialize the hash to a random value
    h = seed ^ length
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = (
                (data[i4 + 0] & 0xff) +
                ((data[i4 + 1] & 0xff) << 8) +
                ((data[i4 + 2] & 0xff) << 16) +
                ((data[i4 + 3] & 0xff) << 24)
        )
        k *= m
        k ^= k >> r  # k ^= k >>> r
        k *= m

        h *= m
        h ^= k

    # Handle the last few bytes of the input array
    extra_bytes = length % 4
    if extra_bytes >= 3:
        h ^= (data[(length & ~3) + 2] & 0xff) << 16
    if extra_bytes >= 2:
        h ^= (data[(length & ~3) + 1] & 0xff) << 8
    if extra_bytes >= 1:
        h ^= (data[length & ~3] & 0xff)
        h *= m

    h ^= h >> 13  # h >>> 13;
    h *= m
    h ^= h >> 15  # h >>> 15;

    return h
