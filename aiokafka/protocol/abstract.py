import abc
import io
from typing import Generic, Optional, TypeVar

from typing_extensions import TypeAlias

T = TypeVar("T")
RawData: TypeAlias = io.BytesIO


class AbstractType(Generic[T], metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def encode(self, value: Optional[T]) -> bytes: ...

    @classmethod
    @abc.abstractmethod
    def decode(self, data: RawData) -> Optional[T]: ...

    @classmethod
    def repr(self, value: T) -> str:
        return repr(value)
