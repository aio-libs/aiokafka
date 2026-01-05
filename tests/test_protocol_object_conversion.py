from typing import TypeVar

import pytest

from aiokafka.protocol.admin import RequestStruct, Response
from aiokafka.protocol.metadata import MetadataResponse_v5
from aiokafka.protocol.types import Array, Int16, Schema, String

C = TypeVar("C", bound=type[RequestStruct | Response])


def _make_test_class(
    klass: type[RequestStruct | Response], schema: Schema, flexible: bool = False
) -> type[RequestStruct | Response]:
    if klass is RequestStruct:

        class RequestTestClass(RequestStruct):
            API_KEY = 0
            API_VERSION = 0
            RESPONSE_TYPE = Response
            SCHEMA = schema
            FLEXIBLE_VERSION = flexible

        return RequestTestClass
    else:

        class ResponseTestClass(Response):
            API_KEY = 0
            API_VERSION = 0
            SCHEMA = schema
            FLEXIBLE_VERSION = flexible

        return ResponseTestClass


@pytest.mark.parametrize("superclass", (RequestStruct, Response))
class TestObjectConversion:
    def test_get_item(self, superclass: type[RequestStruct | Response]) -> None:
        TestClass = _make_test_class(superclass, Schema(("myobject", Int16)))

        tc = TestClass(myobject=0)
        assert tc.get_item("myobject") == 0
        with pytest.raises(KeyError):
            tc.get_item("does-not-exist")

    def test_with_empty_schema(
        self, superclass: type[RequestStruct | Response]
    ) -> None:
        TestClass = _make_test_class(superclass, Schema())

        tc = TestClass()
        tc.encode()
        assert tc.to_object() == {}

    def test_with_basic_schema(
        self, superclass: type[RequestStruct | Response]
    ) -> None:
        TestClass = _make_test_class(superclass, Schema(("myobject", Int16)))

        tc = TestClass(myobject=0)
        tc.encode()
        assert tc.to_object() == {"myobject": 0}

    def test_with_basic_array_schema(
        self, superclass: type[RequestStruct | Response]
    ) -> None:
        TestClass = _make_test_class(superclass, Schema(("myarray", Array(Int16))))

        tc = TestClass(myarray=[1, 2, 3])
        tc.encode()
        assert tc.to_object()["myarray"] == [1, 2, 3]

    def test_with_complex_array_schema(
        self, superclass: type[RequestStruct | Response]
    ) -> None:
        TestClass = _make_test_class(
            superclass,
            Schema(
                (
                    "myarray",
                    Array(("subobject", Int16), ("othersubobject", String("utf-8"))),
                )
            ),
        )

        tc = TestClass(myarray=[[10, "hello"]])
        tc.encode()
        obj = tc.to_object()
        assert len(obj["myarray"]) == 1
        assert obj["myarray"][0]["subobject"] == 10
        assert obj["myarray"][0]["othersubobject"] == "hello"

    def test_with_array_and_other(
        self, superclass: type[RequestStruct | Response]
    ) -> None:
        TestClass = _make_test_class(
            superclass,
            Schema(
                (
                    "myarray",
                    Array(("subobject", Int16), ("othersubobject", String("utf-8"))),
                ),
                ("notarray", Int16),
            ),
        )

        tc = TestClass(myarray=[[10, "hello"]], notarray=42)

        obj = tc.to_object()
        assert len(obj["myarray"]) == 1
        assert obj["myarray"][0]["subobject"] == 10
        assert obj["myarray"][0]["othersubobject"] == "hello"
        assert obj["notarray"] == 42

    def test_with_nested_array(
        self, superclass: type[RequestStruct | Response]
    ) -> None:
        TestClass = _make_test_class(
            superclass,
            Schema(
                (
                    "myarray",
                    Array(("subarray", Array(Int16)), ("otherobject", Int16)),
                )
            ),
        )

        tc = TestClass(
            myarray=[
                [[1, 2], 2],
                [[2, 3], 4],
            ]
        )
        print(tc.encode())

        obj = tc.to_object()
        assert len(obj["myarray"]) == 2
        assert obj["myarray"][0]["subarray"] == [1, 2]
        assert obj["myarray"][0]["otherobject"] == 2
        assert obj["myarray"][1]["subarray"] == [2, 3]
        assert obj["myarray"][1]["otherobject"] == 4

    def test_with_complex_nested_array(
        self, superclass: type[RequestStruct | Response]
    ) -> None:
        TestClass = _make_test_class(
            superclass,
            Schema(
                (
                    "myarray",
                    Array(
                        (
                            "subarray",
                            Array(
                                ("innertest", String("utf-8")),
                                ("otherinnertest", String("utf-8")),
                            ),
                        ),
                        ("othersubarray", Array(Int16)),
                    ),
                ),
                ("notarray", String("utf-8")),
            ),
        )

        tc = TestClass(
            myarray=[
                [[["hello", "hello"], ["hello again", "hello again"]], [0]],
                [[["hello", "hello again"]], [1]],
            ],
            notarray="notarray",
        )
        tc.encode()

        obj = tc.to_object()

        assert obj["notarray"] == "notarray"
        myarray = obj["myarray"]
        assert len(myarray) == 2

        assert myarray[0]["othersubarray"] == [0]
        assert len(myarray[0]["subarray"]) == 2
        assert myarray[0]["subarray"][0]["innertest"] == "hello"
        assert myarray[0]["subarray"][0]["otherinnertest"] == "hello"
        assert myarray[0]["subarray"][1]["innertest"] == "hello again"
        assert myarray[0]["subarray"][1]["otherinnertest"] == "hello again"

        assert myarray[1]["othersubarray"] == [1]
        assert len(myarray[1]["subarray"]) == 1
        assert myarray[1]["subarray"][0]["innertest"] == "hello"
        assert myarray[1]["subarray"][0]["otherinnertest"] == "hello again"

    def test_flexible_version(self, superclass: type[RequestStruct | Response]) -> None:
        TestClass = _make_test_class(
            superclass,
            Schema(
                ("name", String("utf-8")),
                ("myarray", Array(Int16)),
                (("tagged_field1", 0), String("utf-8")),
                (("tagged_field2", 42), Int16),
                (
                    ("tagged_field3", 53),
                    Array(
                        ("name", String("utf-8")),
                        (("tag1", 0), Int16),
                        (("tag2", 1), Int16),
                    ),
                ),
            ),
            flexible=True,
        )

        tc = TestClass(
            name="foo",
            myarray=[1, 2, 3],
            tagged_field1="bar",
            tagged_field2=23,
            tagged_field3=[("hello", 1, 2), ("world", 3, 4)],
        )
        encoded = tc.encode()
        assert tc.to_object()["name"] == "foo"
        assert tc.to_object()["myarray"] == [1, 2, 3]
        assert tc.to_object()["tagged_field1"] == "bar"
        assert tc.to_object()["tagged_field2"] == 23
        assert tc.to_object()["tagged_field3"] == [
            {"name": "hello", "tag1": 1, "tag2": 2},
            {"name": "world", "tag1": 3, "tag2": 4},
        ]

        tc = TestClass.decode(encoded)
        assert tc.to_object()["name"] == "foo"
        assert tc.to_object()["myarray"] == [1, 2, 3]
        assert tc.to_object()["tagged_field1"] == "bar"
        assert tc.to_object()["tagged_field2"] == 23
        assert tc.to_object()["tagged_field3"] == [
            {"name": "hello", "tag1": 1, "tag2": 2},
            {"name": "world", "tag1": 3, "tag2": 4},
        ]


def test_with_metadata_response() -> None:
    tc = MetadataResponse_v5(
        throttle_time_ms=0,
        brokers=[
            [0, "testhost0", 9092, "testrack0"],
            [1, "testhost1", 9092, "testrack1"],
        ],
        cluster_id="abcd",
        controller_id=0,
        topics=[
            [
                0,
                "testtopic1",
                False,
                [
                    [0, 0, 0, [0, 1], [0, 1], []],
                    [0, 1, 1, [1, 0], [1, 0], []],
                ],
            ],
            [
                0,
                "other-test-topic",
                True,
                [
                    [0, 0, 0, [0, 1], [0, 1], []],
                ],
            ],
        ],
    )
    tc.encode()  # Make sure this object encodes successfully

    obj = tc.to_object()

    assert obj["throttle_time_ms"] == 0

    assert len(obj["brokers"]) == 2
    assert obj["brokers"][0]["node_id"] == 0
    assert obj["brokers"][0]["host"] == "testhost0"
    assert obj["brokers"][0]["port"] == 9092
    assert obj["brokers"][0]["rack"] == "testrack0"
    assert obj["brokers"][1]["node_id"] == 1
    assert obj["brokers"][1]["host"] == "testhost1"
    assert obj["brokers"][1]["port"] == 9092
    assert obj["brokers"][1]["rack"] == "testrack1"

    assert obj["cluster_id"] == "abcd"
    assert obj["controller_id"] == 0

    assert len(obj["topics"]) == 2
    assert obj["topics"][0]["error_code"] == 0
    assert obj["topics"][0]["topic"] == "testtopic1"
    assert obj["topics"][0]["is_internal"] is False
    assert len(obj["topics"][0]["partitions"]) == 2
    assert obj["topics"][0]["partitions"][0]["error_code"] == 0
    assert obj["topics"][0]["partitions"][0]["partition"] == 0
    assert obj["topics"][0]["partitions"][0]["leader"] == 0
    assert obj["topics"][0]["partitions"][0]["replicas"] == [0, 1]
    assert obj["topics"][0]["partitions"][0]["isr"] == [0, 1]
    assert obj["topics"][0]["partitions"][0]["offline_replicas"] == []
    assert obj["topics"][0]["partitions"][1]["error_code"] == 0
    assert obj["topics"][0]["partitions"][1]["partition"] == 1
    assert obj["topics"][0]["partitions"][1]["leader"] == 1
    assert obj["topics"][0]["partitions"][1]["replicas"] == [1, 0]
    assert obj["topics"][0]["partitions"][1]["isr"] == [1, 0]
    assert obj["topics"][0]["partitions"][1]["offline_replicas"] == []

    assert obj["topics"][1]["error_code"] == 0
    assert obj["topics"][1]["topic"] == "other-test-topic"
    assert obj["topics"][1]["is_internal"] is True
    assert len(obj["topics"][1]["partitions"]) == 1
    assert obj["topics"][1]["partitions"][0]["error_code"] == 0
    assert obj["topics"][1]["partitions"][0]["partition"] == 0
    assert obj["topics"][1]["partitions"][0]["leader"] == 0
    assert obj["topics"][1]["partitions"][0]["replicas"] == [0, 1]
    assert obj["topics"][1]["partitions"][0]["isr"] == [0, 1]
    assert obj["topics"][1]["partitions"][0]["offline_replicas"] == []

    tc.encode()
