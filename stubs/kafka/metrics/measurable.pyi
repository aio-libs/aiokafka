import abc
from typing import Any

class AbstractMeasurable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def measure(self, config: Any, now: Any) -> Any: ...

class AnonMeasurable(AbstractMeasurable):
    def __init__(self, measure_fn: Any) -> None: ...
    def measure(self, config: Any, now: Any): ...
