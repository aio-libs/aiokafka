import struct
from collections.abc import Callable, Sequence
from io import BytesIO
from struct import error
from typing import (
    Any,
    TypeAlias,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import Buffer

from .abstract import AbstractType

T = TypeVar("T")

ValueT: TypeAlias = Union[type[AbstractType[Any]], "String", "Array", "Schema"]

TaggedFieldId: TypeAlias = tuple[str, int]

FieldId: TypeAlias = TaggedFieldId | str


def _pack(f: Callable[[T], bytes], value: T) -> bytes:
    try:
        return f(value)
    except error as e:
        raise ValueError(
            "Error encountered when attempting to convert value: "
            f"{value!r} to struct format: '{f}', hit error: {e}"
        ) from e


def _unpack(f: Callable[[Buffer], tuple[T, ...]], data: Buffer) -> T:
    try:
        (value,) = f(data)
    except error as e:
        raise ValueError(
            "Error encountered when attempting to convert value: "
            f"{data!r} to struct format: '{f}', hit error: {e}"
        ) from e
    else:
        return value


class Int8(AbstractType[int]):
    _pack = struct.Struct(">b").pack
    _unpack = struct.Struct(">b").unpack

    @classmethod
    def encode(cls, value: int, flexible: bool = False) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool = False) -> int:
        return _unpack(cls._unpack, data.read(1))


class Int16(AbstractType[int]):
    _pack = struct.Struct(">h").pack
    _unpack = struct.Struct(">h").unpack

    @classmethod
    def encode(cls, value: int, flexible: bool = False) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool = False) -> int:
        return _unpack(cls._unpack, data.read(2))


class Int32(AbstractType[int]):
    _pack = struct.Struct(">i").pack
    _unpack = struct.Struct(">i").unpack

    @classmethod
    def encode(cls, value: int, flexible: bool = False) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool = False) -> int:
        return _unpack(cls._unpack, data.read(4))


class UInt32(AbstractType[int]):
    _pack = struct.Struct(">I").pack
    _unpack = struct.Struct(">I").unpack

    @classmethod
    def encode(cls, value: int, flexible: bool = False) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool = False) -> int:
        return _unpack(cls._unpack, data.read(4))


class Int64(AbstractType[int]):
    _pack = struct.Struct(">q").pack
    _unpack = struct.Struct(">q").unpack

    @classmethod
    def encode(cls, value: int, flexible: bool = False) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool = False) -> int:
        return _unpack(cls._unpack, data.read(8))


class Float64(AbstractType[float]):
    _pack = struct.Struct(">d").pack
    _unpack = struct.Struct(">d").unpack

    @classmethod
    def encode(cls, value: float, flexible: bool = False) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool = False) -> float:
        return _unpack(cls._unpack, data.read(8))


class String:
    def __init__(self, encoding: str = "utf-8", allow_flexible: bool = True):
        self.encoding = encoding
        self.allow_flexible = allow_flexible

    def encode(self, value: str | None, flexible: bool) -> bytes:
        if value is None:
            return (
                UnsignedVarInt32.encode(0)
                if flexible and self.allow_flexible
                else Int16.encode(-1, flexible)
            )
        encoded_value = str(value).encode(self.encoding)
        return (
            UnsignedVarInt32.encode(len(encoded_value) + 1) + encoded_value
            if flexible and self.allow_flexible
            else Int16.encode(len(encoded_value), flexible) + encoded_value
        )

    def decode(self, data: BytesIO, flexible: bool) -> str | None:
        length = (
            UnsignedVarInt32.decode(data) - 1
            if flexible and self.allow_flexible
            else Int16.decode(data, flexible)
        )
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError("Buffer underrun decoding string")
        return value.decode(self.encoding)

    @classmethod
    def repr(cls, value: str) -> str:
        return repr(value)


class Bytes(AbstractType[bytes | None]):
    @classmethod
    def encode(cls, value: bytes | None, flexible: bool) -> bytes:
        if value is None:
            return (
                UnsignedVarInt32.encode(0) if flexible else Int32.encode(-1, flexible)
            )
        else:
            return (
                UnsignedVarInt32.encode(len(value) + 1) + value
                if flexible
                else Int32.encode(len(value), flexible) + value
            )

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool) -> bytes | None:
        length = (
            UnsignedVarInt32.decode(data) - 1
            if flexible
            else Int32.decode(data, flexible)
        )
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError("Buffer underrun decoding Bytes")
        return value

    @classmethod
    def repr(cls, value: bytes | None) -> str:
        return repr(
            value[:100] + b"..." if value is not None and len(value) > 100 else value
        )


class Boolean(AbstractType[bool]):
    _pack = struct.Struct(">?").pack
    _unpack = struct.Struct(">?").unpack

    @classmethod
    def encode(cls, value: bool, flexible: bool) -> bytes:
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data: BytesIO, flexible: bool) -> bool:
        return _unpack(cls._unpack, data.read(1))


class Schema:
    names: tuple[str, ...]
    tags: tuple[int, ...]
    fields: tuple[ValueT, ...]

    def __init__(self, *fields: tuple[FieldId, ValueT]):
        if fields:
            tagged_names, values = zip(
                *(
                    (key, value) if isinstance(key, tuple) else ((key, None), value)
                    for key, value in fields
                ),
                strict=False,
            )
            self.names = tuple(name for name, _ in tagged_names)
            self.tags = tuple(tag for _, tag in tagged_names)
            self.fields = tuple(values)
        else:
            self.names, self.tags, self.fields = (), (), ()

    def encode(self, item: Sequence[Any], flexible: bool) -> bytes:
        if len(item) != len(self.fields):
            raise ValueError("Item field count does not match Schema")
        return b"".join(
            field.encode(item[i], flexible)
            for i, field in enumerate(self.fields)
            if self.tags[i] is None
        ) + (
            self._encode_tagged_fields(
                {
                    self.tags[i]: field.encode(item[i], flexible)
                    for i, field in enumerate(self.fields)
                    if self.tags[i] is not None
                }
            )
            if flexible
            else b""
        )

    def decode(
        self, data: BytesIO, flexible: bool
    ) -> tuple[Any | str | None | list[Any | tuple[Any, ...]], ...]:
        result = [
            field.decode(data, flexible) if self.tags[i] is None else None
            for i, field in enumerate(self.fields)
        ]
        if flexible:
            tagged_fields = self._decode_tagged_fields(data)
            for i, tag in enumerate(self.tags):
                if tag is not None:
                    encoded_value = tagged_fields.get(tag)
                    if encoded_value is not None:
                        result[i] = self.fields[i].decode(
                            BytesIO(encoded_value), flexible
                        )

        return tuple(result)

    @staticmethod
    def _encode_tagged_fields(value: dict[int, bytes]) -> bytes:
        ret = UnsignedVarInt32.encode(len(value))
        for k, v in value.items():
            assert isinstance(k, int) and k >= 0, f"Key {k} is not a positive integer"
            ret += UnsignedVarInt32.encode(k)
            ret += UnsignedVarInt32.encode(len(v))
            ret += v
        return ret

    @staticmethod
    def _decode_tagged_fields(data: BytesIO) -> dict[int, bytes]:
        num_fields = UnsignedVarInt32.decode(data)
        ret: dict[int, bytes] = {}
        if not num_fields:
            return ret
        for _ in range(num_fields):
            tag = UnsignedVarInt32.decode(data)
            size = UnsignedVarInt32.decode(data)
            val = data.read(size)
            ret[tag] = val
        return ret

    def __len__(self) -> int:
        return len(self.fields)

    def repr(self, value: Any) -> str:
        key_vals: list[str] = []
        try:
            for i in range(len(self)):
                try:
                    field_val = getattr(value, self.names[i])
                except AttributeError:
                    field_val = value[i]
                key_vals.append(f"{self.names[i]}={self.fields[i].repr(field_val)}")
            return "(" + ", ".join(key_vals) + ")"
        except Exception:  # noqa: BLE001
            return repr(value)


class Array:
    array_of: ValueT

    @overload
    def __init__(self, array_of_0: ValueT): ...

    @overload
    def __init__(
        self,
        array_of_0: tuple[FieldId, ValueT],
        *array_of: tuple[FieldId, ValueT],
    ): ...

    def __init__(
        self,
        array_of_0: ValueT | tuple[FieldId, ValueT],
        *array_of: tuple[FieldId, ValueT],
    ) -> None:
        if array_of:
            array_of_0 = cast(tuple[FieldId, ValueT], array_of_0)
            self.array_of = Schema(array_of_0, *array_of)
        else:
            array_of_0 = cast(ValueT, array_of_0)
            if isinstance(array_of_0, String | Array | Schema) or issubclass(
                array_of_0, AbstractType
            ):
                self.array_of = array_of_0
            else:
                raise ValueError("Array instantiated with no array_of type")

    def encode(self, items: Sequence[Any] | None, flexible: bool) -> bytes:
        if items is None:
            return (
                UnsignedVarInt32.encode(0) if flexible else Int32.encode(-1, flexible)
            )
        encoded_items = (self.array_of.encode(item, flexible) for item in items)
        return (
            b"".join(
                (UnsignedVarInt32.encode(len(items) + 1), *encoded_items),
            )
            if flexible
            else b"".join(
                (Int32.encode(len(items), flexible), *encoded_items),
            )
        )

    def decode(
        self, data: BytesIO, flexible: bool
    ) -> list[Any | tuple[Any, ...]] | None:
        length = (
            UnsignedVarInt32.decode(data) - 1
            if flexible
            else Int32.decode(data, flexible)
        )
        if length == -1:
            return None
        return [self.array_of.decode(data, flexible) for _ in range(length)]

    def repr(self, list_of_items: Sequence[Any] | None) -> str:
        if list_of_items is None:
            return "NULL"
        return "[" + ", ".join(self.array_of.repr(item) for item in list_of_items) + "]"


class UnsignedVarInt32:
    @classmethod
    def decode(cls, data: BytesIO) -> int:
        value, i = 0, 0
        b: int
        while True:
            (b,) = struct.unpack("B", data.read(1))
            if not (b & 0x80):
                break
            value |= (b & 0x7F) << i
            i += 7
            if i > 28:
                raise ValueError(f"Invalid value {value}")
        value |= b << i
        return value

    @classmethod
    def encode(cls, value: int) -> bytes:
        value &= 0xFFFFFFFF
        ret = b""
        while (value & 0xFFFFFF80) != 0:
            b = (value & 0x7F) | 0x80
            ret += struct.pack("B", b)
            value >>= 7
        ret += struct.pack("B", value)
        return ret
