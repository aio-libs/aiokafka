import abc
from io import BytesIO
from typing import Generic, TypeVar

T = TypeVar("T")


class AbstractType(Generic[T], metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def encode(cls, value: T, flexible: bool) -> bytes: ...

    @classmethod
    @abc.abstractmethod
    def decode(cls, data: BytesIO, flexible: bool) -> T: ...

    @classmethod
    def repr(cls, value: T) -> str:
        return repr(value)
