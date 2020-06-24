import abc
from typing import Any

ABC: Any

class AbstractTokenProvider(ABC, metaclass=abc.ABCMeta):
    def __init__(self, **config: Any) -> None: ...
    @abc.abstractmethod
    def token(self) -> Any: ...
    def extensions(self): ...
