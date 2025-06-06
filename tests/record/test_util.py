import struct

import pytest

from aiokafka.record import util

varint_data: list[tuple[bytes, int]] = [
    (b"\x00", 0),
    (b"\x01", -1),
    (b"\x02", 1),
    (b"\x7e", 63),
    (b"\x7f", -64),
    (b"\x80\x01", 64),
    (b"\x81\x01", -65),
    (b"\xfe\x7f", 8191),
    (b"\xff\x7f", -8192),
    (b"\x80\x80\x01", 8192),
    (b"\x81\x80\x01", -8193),
    (b"\xfe\xff\x7f", 1048575),
    (b"\xff\xff\x7f", -1048576),
    (b"\x80\x80\x80\x01", 1048576),
    (b"\x81\x80\x80\x01", -1048577),
    (b"\xfe\xff\xff\x7f", 134217727),
    (b"\xff\xff\xff\x7f", -134217728),
    (b"\x80\x80\x80\x80\x01", 134217728),
    (b"\x81\x80\x80\x80\x01", -134217729),
    (b"\xfe\xff\xff\xff\x7f", 17179869183),
    (b"\xff\xff\xff\xff\x7f", -17179869184),
    (b"\x80\x80\x80\x80\x80\x01", 17179869184),
    (b"\x81\x80\x80\x80\x80\x01", -17179869185),
    (b"\xfe\xff\xff\xff\xff\x7f", 2199023255551),
    (b"\xff\xff\xff\xff\xff\x7f", -2199023255552),
    (b"\x80\x80\x80\x80\x80\x80\x01", 2199023255552),
    (b"\x81\x80\x80\x80\x80\x80\x01", -2199023255553),
    (b"\xfe\xff\xff\xff\xff\xff\x7f", 281474976710655),
    (b"\xff\xff\xff\xff\xff\xff\x7f", -281474976710656),
    (b"\x80\x80\x80\x80\x80\x80\x80\x01", 281474976710656),
    (b"\x81\x80\x80\x80\x80\x80\x80\x01", -281474976710657),
    (b"\xfe\xff\xff\xff\xff\xff\xff\x7f", 36028797018963967),
    (b"\xff\xff\xff\xff\xff\xff\xff\x7f", -36028797018963968),
    (b"\x80\x80\x80\x80\x80\x80\x80\x80\x01", 36028797018963968),
    (b"\x81\x80\x80\x80\x80\x80\x80\x80\x01", -36028797018963969),
    (b"\xfe\xff\xff\xff\xff\xff\xff\xff\x7f", 4611686018427387903),
    (b"\xff\xff\xff\xff\xff\xff\xff\xff\x7f", -4611686018427387904),
    (b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01", 4611686018427387904),
    (b"\x81\x80\x80\x80\x80\x80\x80\x80\x80\x01", -4611686018427387905),
]


@pytest.mark.parametrize("encoded, decoded", varint_data)
def test_encode_varint(encoded: bytes, decoded: int) -> None:
    res = bytearray()
    util.encode_varint(decoded, res.append)
    assert res == encoded


@pytest.mark.parametrize("encoded, decoded", varint_data)
def test_decode_varint(encoded: bytes, decoded: int) -> None:
    # We add a bit of bytes around just to check position is calculated
    # correctly
    value, pos = util.decode_varint(bytearray(b"\x01\xf0" + encoded + b"\xff\x01"), 2)
    assert value == decoded
    assert pos - 2 == len(encoded)


@pytest.mark.parametrize("encoded, decoded", varint_data)
def test_size_of_varint(encoded: bytes, decoded: int) -> None:
    assert util.size_of_varint(decoded) == len(encoded)


def test_crc32c() -> None:
    def make_crc(data: bytes) -> bytes:
        crc = util.calc_crc32c(data)
        return struct.pack(">I", crc)

    assert make_crc(b"") == b"\x00\x00\x00\x00"
    assert make_crc(b"a") == b"\xc1\xd0\x43\x30"

    # Took from librdkafka testcase
    long_text = b"""\
  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the author be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution."""
    assert make_crc(long_text) == b"\x7d\xcd\xe1\x13"
