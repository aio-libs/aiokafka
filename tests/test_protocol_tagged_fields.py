import pytest

from aiokafka.protocol.struct import Struct
from aiokafka.protocol.types import (
    CompactArray,
    CompactString,
    Int16,
    Schema,
)


def _make_test_class(schema: Schema) -> type[Struct]:
    class TestClass(Struct):
        SCHEMA = schema

    return TestClass


def test_invalid_tagged_field_definition() -> None:
    with pytest.raises(ValueError):
        Schema(
            ("name", CompactString("utf-8")),
            tagged_fields=(
                (6, "tagged_field2", CompactString("utf-8")),
                (0, "tagged_field1", CompactString("utf-8")),
            ),
        )


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (
            (
                b"\x06hello"  # name="hello"
                b"\x02"  # 2 tagged fields follow
                b"\x00"  # tag=0
                b"\x06"  # encoded value is 6 bytes
                b"\x06world"  # tagged_field1="world"
                b"\x06"  # tag=6
                b"\x02"  # encoded value is 2 bytes
                b"\x02!"  # tagged_field2="!"
            ),
            ("hello", "world", "!"),
        ),
        (
            (
                b"\x06hello"  # name="hello"
                b"\x01"  # 1 tagged fields follow
                b"\x06"  # tag=6
                b"\x02"  # encoded value is 2 bytes
                b"\x02!"  # tagged_field2="!"
            ),
            ("hello", None, "!"),
        ),
        (
            (
                b"\x06hello"  # name="hello"
                b"\x02"  # 2 tagged fields follow
                b"\x00"  # tag=0
                b"\x06"  # encoded value is 6 bytes
                b"\x06world"  # tagged_field1="world"
                b"\x08"  # tag=8
                b"\x02"  # encoded value is 2 bytes
                b"\x02!"  # tagged_field2="!"
            ),
            ("hello", "world", None),
        ),
    ],
)
def test_decode_tagged_fields(data, expected) -> None:
    TestClass = _make_test_class(
        Schema(
            ("name", CompactString("utf-8")),
            tagged_fields=(
                (0, "tagged_field1", CompactString("utf-8")),
                (6, "tagged_field2", CompactString("utf-8")),
            ),
        )
    )

    tc = TestClass.decode(data)
    v1, v2, v3 = expected
    assert tc.get_item("name") == v1
    assert tc.get_item("tagged_field1") == v2
    assert tc.get_item("tagged_field2") == v3


def test_happy_roundtrip() -> None:
    TestClass = _make_test_class(
        Schema(
            ("name", CompactString("utf-8")),
            ("myarray", CompactArray(Int16)),
            tagged_fields=(
                (0, "tagged_field1", CompactString("utf-8")),
                (42, "tagged_field2", Int16),
                (
                    53,
                    "tagged_field3",
                    CompactArray(
                        ("name", CompactString("utf-8")),
                        tagged_fields=(
                            (0, "tag1", Int16),
                            (1, "tag2", Int16),
                        ),
                    ),
                ),
            ),
        ),
    )

    tc = TestClass(
        name="foo",
        myarray=[1, 2, 3],
        tagged_field1="bar",
        tagged_field2=23,
        tagged_field3=[("hello", 1, 2), ("world", 3, 4)],
    )
    encoded = tc.encode()
    assert tc.get_item("name") == "foo"
    assert tc.get_item("myarray") == [1, 2, 3]
    assert tc.get_item("tagged_field1") == "bar"
    assert tc.get_item("tagged_field2") == 23
    assert tc.get_item("tagged_field3") == [("hello", 1, 2), ("world", 3, 4)]

    tc = TestClass.decode(encoded)
    assert tc.get_item("name") == "foo"
    assert tc.get_item("myarray") == [1, 2, 3]
    assert tc.get_item("tagged_field1") == "bar"
    assert tc.get_item("tagged_field2") == 23
    assert tc.get_item("tagged_field3") == [("hello", 1, 2), ("world", 3, 4)]
