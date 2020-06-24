import abc
from typing import Any

class Serializer(metaclass=abc.ABCMeta):
    __meta__: Any = ...
    def __init__(self, **config: Any) -> None: ...
    @abc.abstractmethod
    def serialize(self, topic: Any, value: Any) -> Any: ...
    def close(self) -> None: ...

class Deserializer(metaclass=abc.ABCMeta):
    __meta__: Any = ...
    def __init__(self, **config: Any) -> None: ...
    @abc.abstractmethod
    def deserialize(self, topic: Any, bytes_: Any) -> Any: ...
    def close(self) -> None: ...
