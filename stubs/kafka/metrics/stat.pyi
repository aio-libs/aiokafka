import abc
from typing import Any

class AbstractStat(metaclass=abc.ABCMeta):
    __metaclass__: Any = ...
    @abc.abstractmethod
    def record(self, config: Any, value: Any, time_ms: Any) -> Any: ...
