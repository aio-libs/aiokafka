import abc
from typing import Any

log: Any

class AbstractPartitionAssignor(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def name(self) -> Any: ...
    @abc.abstractmethod
    def assign(self, cluster: Any, members: Any) -> Any: ...
    @abc.abstractmethod
    def metadata(self, topics: Any) -> Any: ...
    @abc.abstractmethod
    def on_assignment(self, assignment: Any) -> Any: ...
