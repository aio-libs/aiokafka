from aiokafka.errors import CorruptRecordException

# VarInt implementation

cdef inline int decode_varint64(
        char* buf, Py_ssize_t* read_pos, int64_t* out_value) except -1:
    cdef:
        char shift = 0
        char byte
        Py_ssize_t pos = read_pos[0]
        uint64_t value = 0

    while True:
        byte = buf[pos]
        pos += 1
        if byte & 0x80 != 0:
            value |= <uint64_t>(byte & 0x7f) << shift
            shift += 7
        else:
            value |= <uint64_t>byte << shift
            break
        if shift > 63:
            raise CorruptRecordException("Out of double range")
    # Normalize sign
    out_value[0] = <int64_t>(value >> 1) ^ -<int64_t>(value & 1)
    read_pos[0] = pos
    return 0


def decode_varint(buffer, pos=0):
    """ Decode an integer from a varint presentation. See
    https://developers.google.com/protocol-buffers/docs/encoding?csw=1#varints
    on how those can be produced.

        Arguments:
            buffer (bytearry): buffer to read from.
            pos (int): optional position to read from

        Returns:
            (int, int): Decoded int value and next read position
    """
    cdef:
        Py_buffer buf
        Py_ssize_t read_pos
        int64_t out_value = 0

    read_pos = pos

    PyObject_GetBuffer(buffer, &buf, PyBUF_SIMPLE)
    try:
        decode_varint64(<char*>buf.buf, &read_pos, &out_value)
    except CorruptRecordException:
        raise ValueError("Out of double range")
    finally:
        PyBuffer_Release(&buf)
    return out_value, read_pos

# END: VarInt implementation
