import io
import struct

import pytest

from aiokafka.protocol.api import RequestHeader_v1, RequestStruct, Response
from aiokafka.protocol.coordination import FindCoordinatorRequest_v0
from aiokafka.protocol.fetch import FetchRequest_v0, FetchResponse_v0
from aiokafka.protocol.message import Message, MessageSet, PartialMessage
from aiokafka.protocol.metadata import MetadataRequest_v0
from aiokafka.protocol.types import (
    Array,
    Bytes,
    Int16,
    Int32,
    Int64,
    String,
    UnsignedVarInt32,
)


def test_create_message() -> None:
    payload = b"test"
    key = b"key"
    msg = Message(value=payload, key=key, magic=0, attributes=0, crc=0)
    assert msg.magic == 0
    assert msg.attributes == 0
    assert msg.key == key
    assert msg.value == payload


def test_encode_message_v0() -> None:
    message = Message(value=b"test", key=b"key", magic=0, attributes=0, crc=0)
    encoded = message.encode()
    expect = b"".join(
        [
            struct.pack(">i", -1427009701),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 3),  # Length of key
            b"key",  # key
            struct.pack(">i", 4),  # Length of value
            b"test",  # value
        ]
    )
    assert encoded == expect


def test_encode_message_v1() -> None:
    message = Message(
        value=b"test", key=b"key", magic=1, attributes=0, crc=0, timestamp=1234
    )
    encoded = message.encode()
    expect = b"".join(
        [
            struct.pack(">i", 1331087195),  # CRC
            struct.pack(">bb", 1, 0),  # Magic, flags
            struct.pack(">q", 1234),  # Timestamp
            struct.pack(">i", 3),  # Length of key
            b"key",  # key
            struct.pack(">i", 4),  # Length of value
            b"test",  # value
        ]
    )
    assert encoded == expect


def test_decode_message() -> None:
    encoded = b"".join(
        [
            struct.pack(">i", -1427009701),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 3),  # Length of key
            b"key",  # key
            struct.pack(">i", 4),  # Length of value
            b"test",  # value
        ]
    )
    decoded_message = Message.decode(encoded)
    msg = Message(value=b"test", key=b"key", magic=0, attributes=0, crc=0)
    msg.encode()  # crc is recalculated during encoding
    assert decoded_message == msg


def test_decode_message_validate_crc() -> None:
    encoded = b"".join(
        [
            struct.pack(">i", -1427009701),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 3),  # Length of key
            b"key",  # key
            struct.pack(">i", 4),  # Length of value
            b"test",  # value
        ]
    )
    decoded_message = Message.decode(encoded)
    assert decoded_message.validate_crc() is True

    encoded = b"".join(
        [
            struct.pack(">i", 1234),  # Incorrect CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 3),  # Length of key
            b"key",  # key
            struct.pack(">i", 4),  # Length of value
            b"test",  # value
        ]
    )
    decoded_message = Message.decode(encoded)
    assert decoded_message.validate_crc() is False


def test_encode_message_set() -> None:
    messages = [
        Message(value=b"v1", key=b"k1", magic=0, attributes=0, crc=0),
        Message(value=b"v2", key=b"k2", magic=0, attributes=0, crc=0),
    ]
    encoded = MessageSet.encode([(0, msg.encode()) for msg in messages])
    expect = b"".join(
        [
            struct.pack(">q", 0),  # MsgSet Offset
            struct.pack(">i", 18),  # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k1",  # Key
            struct.pack(">i", 2),  # Length of value
            b"v1",  # Value
            struct.pack(">q", 0),  # MsgSet Offset
            struct.pack(">i", 18),  # Msg Size
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k2",  # Key
            struct.pack(">i", 2),  # Length of value
            b"v2",  # Value
        ]
    )
    expect = struct.pack(">i", len(expect)) + expect
    assert encoded == expect


def test_decode_message_set() -> None:
    encoded = b"".join(
        [
            struct.pack(">q", 0),  # MsgSet Offset
            struct.pack(">i", 18),  # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k1",  # Key
            struct.pack(">i", 2),  # Length of value
            b"v1",  # Value
            struct.pack(">q", 1),  # MsgSet Offset
            struct.pack(">i", 18),  # Msg Size
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k2",  # Key
            struct.pack(">i", 2),  # Length of value
            b"v2",  # Value
        ]
    )

    msgs = MessageSet.decode(encoded, bytes_to_read=len(encoded))
    assert len(msgs) == 2
    msg1, msg2 = msgs

    returned_offset1, _message1_size, decoded_message1 = msg1
    returned_offset2, _message2_size, decoded_message2 = msg2

    assert returned_offset1 == 0
    message1 = Message(value=b"v1", key=b"k1", magic=0, attributes=0, crc=0)
    message1.encode()
    assert decoded_message1 == message1

    assert returned_offset2 == 1
    message2 = Message(value=b"v2", key=b"k2", magic=0, attributes=0, crc=0)
    message2.encode()
    assert decoded_message2 == message2


def test_encode_message_header() -> None:
    expect = b"".join(
        [
            struct.pack(">h", 10),  # API Key
            struct.pack(">h", 0),  # API Version
            struct.pack(">i", 4),  # Correlation Id
            struct.pack(">h", len("client3")),  # Length of clientId
            b"client3",  # ClientId
        ]
    )

    req = FindCoordinatorRequest_v0("foo")
    header = RequestHeader_v1(req, correlation_id=4, client_id="client3")
    assert header.encode() == expect


def test_decode_message_set_partial() -> None:
    encoded = b"".join(
        [
            struct.pack(">q", 0),  # Msg Offset
            struct.pack(">i", 18),  # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k1",  # Key
            struct.pack(">i", 2),  # Length of value
            b"v1",  # Value
            struct.pack(">q", 1),  # Msg Offset
            struct.pack(">i", 24),  # Msg Size (larger than remaining MsgSet size)
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k2",  # Key
            struct.pack(">i", 8),  # Length of value
            b"ar",  # Value (truncated)
        ]
    )

    msgs = MessageSet.decode(encoded, bytes_to_read=len(encoded))
    assert len(msgs) == 2
    msg1, msg2 = msgs

    returned_offset1, _message1_size, decoded_message1 = msg1
    returned_offset2, message2_size, decoded_message2 = msg2

    assert returned_offset1 == 0
    message1 = Message(value=b"v1", key=b"k1", magic=0, attributes=0, crc=0)
    message1.encode()
    assert decoded_message1 == message1

    assert returned_offset2 is None
    assert message2_size is None
    assert decoded_message2 == PartialMessage()


def test_decode_fetch_response_partial() -> None:
    encoded = b"".join(
        [
            Int32.encode(1),  # Num Topics (Array)
            String("utf-8").encode("foobar", flexible=False),
            Int32.encode(2),  # Num Partitions (Array)
            Int32.encode(0),  # Partition id
            Int16.encode(0),  # Error Code
            Int64.encode(1234),  # Highwater offset
            Int32.encode(52),  # MessageSet size
            Int64.encode(0),  # Msg Offset
            Int32.encode(18),  # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k1",  # Key
            struct.pack(">i", 2),  # Length of value
            b"v1",  # Value
            Int64.encode(1),  # Msg Offset
            struct.pack(">i", 24),  # Msg Size (larger than remaining MsgSet size)
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k2",  # Key
            struct.pack(">i", 8),  # Length of value
            b"ar",  # Value (truncated)
            Int32.encode(1),
            Int16.encode(0),
            Int64.encode(2345),
            Int32.encode(52),  # MessageSet size
            Int64.encode(0),  # Msg Offset
            Int32.encode(18),  # Msg Size
            struct.pack(">i", 1474775406),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k1",  # Key
            struct.pack(">i", 2),  # Length of value
            b"v1",  # Value
            Int64.encode(1),  # Msg Offset
            struct.pack(">i", 24),  # Msg Size (larger than remaining MsgSet size)
            struct.pack(">i", -16383415),  # CRC
            struct.pack(">bb", 0, 0),  # Magic, flags
            struct.pack(">i", 2),  # Length of key
            b"k2",  # Key
            struct.pack(">i", 8),  # Length of value
            b"ar",  # Value (truncated)
        ]
    )
    resp = FetchResponse_v0.decode(io.BytesIO(encoded))
    assert resp.topics is not None
    assert len(resp.topics) == 1
    topic, partitions = resp.topics[0]
    assert topic == "foobar"
    assert len(partitions) == 2

    m1 = MessageSet.decode(partitions[0][3], bytes_to_read=len(partitions[0][3]))
    assert len(m1) == 2
    assert m1[1] == (None, None, PartialMessage())


def test_struct_unrecognized_kwargs() -> None:
    # Structs should not allow unrecognized kwargs
    with pytest.raises(ValueError):
        MetadataRequest_v0(topicz="foo")


def test_struct_missing_kwargs() -> None:
    fr = FetchRequest_v0(max_wait_time=100)
    assert fr.min_bytes is None


def test_unsigned_varint_serde() -> None:
    pairs = {
        0: [0],
        -1: [0xFF, 0xFF, 0xFF, 0xFF, 0x0F],
        1: [1],
        63: [0x3F],
        -64: [0xC0, 0xFF, 0xFF, 0xFF, 0x0F],
        64: [0x40],
        8191: [0xFF, 0x3F],
        -8192: [0x80, 0xC0, 0xFF, 0xFF, 0x0F],
        8192: [0x80, 0x40],
        -8193: [0xFF, 0xBF, 0xFF, 0xFF, 0x0F],
        1048575: [0xFF, 0xFF, 0x3F],
    }
    for value, expected_encoded in pairs.items():
        value &= 0xFFFFFFFF
        encoded = UnsignedVarInt32.encode(value)
        assert encoded == b"".join(struct.pack(">B", x) for x in expected_encoded)
        assert value == UnsignedVarInt32.decode(io.BytesIO(encoded))


def test_compact_data_structs() -> None:
    cs = String()
    encoded = cs.encode(None, flexible=True)
    assert encoded == struct.pack("B", 0)
    decoded = cs.decode(io.BytesIO(encoded), flexible=True)
    assert decoded is None
    assert cs.encode("", flexible=True) == b"\x01"
    assert cs.decode(io.BytesIO(b"\x01"), flexible=True) == ""
    encoded = cs.encode("foobarbaz", flexible=True)
    assert cs.decode(io.BytesIO(encoded), flexible=True) == "foobarbaz"

    arr = Array(String())
    assert arr.encode(None, flexible=True) == b"\x00"
    assert arr.decode(io.BytesIO(b"\x00"), flexible=True) is None
    enc = arr.encode([], flexible=True)
    assert enc == b"\x01"
    assert arr.decode(io.BytesIO(enc), flexible=True) == []
    encoded = arr.encode(["foo", "bar", "baz", "quux"], flexible=True)
    assert arr.decode(io.BytesIO(encoded), flexible=True) == [
        "foo",
        "bar",
        "baz",
        "quux",
    ]

    enc = Bytes.encode(None, flexible=True)
    assert enc == b"\x00"
    assert Bytes.decode(io.BytesIO(b"\x00"), flexible=True) is None
    enc = Bytes.encode(b"", flexible=True)
    assert enc == b"\x01"
    assert Bytes.decode(io.BytesIO(b"\x01"), flexible=True) == b""
    enc = Bytes.encode(b"foo", flexible=True)
    assert Bytes.decode(io.BytesIO(enc), flexible=True) == b"foo"


attr_names = [
    n
    for n in dir(RequestStruct)
    if isinstance(getattr(RequestStruct, n), property)
    and getattr(RequestStruct, n).__isabstractmethod__ is True
]


@pytest.mark.parametrize("klass", RequestStruct.__subclasses__())
@pytest.mark.parametrize("attr_name", attr_names)
def test_request_type_conformance(klass: type[RequestStruct], attr_name: str) -> None:
    assert hasattr(klass, attr_name)


attr_names = [
    n
    for n in dir(Response)
    if isinstance(getattr(Response, n), property)
    and getattr(Response, n).__isabstractmethod__ is True
]


@pytest.mark.parametrize("klass", Response.__subclasses__())
@pytest.mark.parametrize("attr_name", attr_names)
def test_response_type_conformance(klass: type[Response], attr_name: str) -> None:
    assert hasattr(klass, attr_name)
