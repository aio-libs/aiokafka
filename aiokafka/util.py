import asyncio
import os
import weakref
from asyncio import AbstractEventLoop
from types import MethodType
from typing import Any, Awaitable, Coroutine, Dict, Tuple, TypeVar, Union, cast

import async_timeout
from packaging.version import Version

from .structs import OffsetAndMetadata, TopicPartition


__all__ = [
    "create_task",
    "create_future",
    "NO_EXTENSIONS",
    "INTEGER_MAX_VALUE",
    "INTEGER_MIN_VALUE",
]


T = TypeVar("T")


def create_task(coro: Coroutine[Any, Any, T]) -> "asyncio.Task[T]":
    loop = get_running_loop()
    return loop.create_task(coro)


def create_future(loop: AbstractEventLoop = None) -> "asyncio.Future[T]":
    if loop is None:
        loop = get_running_loop()
    return loop.create_future()


async def wait_for(fut: Awaitable[T], timeout: Union[None, int, float] = None) -> T:
    # A replacement for buggy (since 3.8.6) `asyncio.wait_for()`
    # https://bugs.python.org/issue42130
    async with async_timeout.timeout(timeout):
        return await fut


def parse_kafka_version(api_version: str) -> Tuple[int, int, int]:
    parsed = Version(api_version).release
    if not 2 <= len(parsed) <= 3:
        raise ValueError(api_version)
    version = cast(Tuple[int, int, int], (parsed + (0,))[:3])

    if not (0, 9) <= version < (3, 0):
        raise ValueError(api_version)
    return version


def commit_structure_validate(
    offsets: Dict[TopicPartition, Union[int, Tuple[int, str], OffsetAndMetadata]]
) -> Dict[TopicPartition, OffsetAndMetadata]:
    # validate `offsets` structure
    if not offsets or not isinstance(offsets, dict):
        raise ValueError(offsets)

    formatted_offsets = {}
    for tp, offset_and_metadata in offsets.items():
        if not isinstance(tp, TopicPartition):
            raise ValueError("Key should be TopicPartition instance")

        if isinstance(offset_and_metadata, int):
            offset, metadata = offset_and_metadata, ""
        else:
            try:
                offset, metadata = offset_and_metadata
            except Exception:
                raise ValueError(offsets)

            if not isinstance(metadata, str):
                raise ValueError("Metadata should be a string")

        formatted_offsets[tp] = OffsetAndMetadata(offset, metadata)
    return formatted_offsets


def get_running_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.get_event_loop()
    if not loop.is_running():
        raise RuntimeError(
            "The object should be created within an async function or "
            "provide loop directly."
        )
    return loop


NO_EXTENSIONS = bool(os.environ.get("AIOKAFKA_NO_EXTENSIONS"))

INTEGER_MAX_VALUE = 2**31 - 1
INTEGER_MIN_VALUE = -(2**31)


class WeakMethod(object):
    """
    Callable that weakly references a method and the object it is bound to. It
    is based on https://stackoverflow.com/a/24287465.

    Arguments:

        object_dot_method: A bound instance method (i.e. 'object.method').
    """

    def __init__(self, object_dot_method: MethodType) -> None:
        self.target = weakref.ref(object_dot_method.__self__)
        self._target_id = id(self.target())
        self.method = weakref.ref(object_dot_method.__func__)
        self._method_id = id(self.method())

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Calls the method on target with args and kwargs.
        """
        method = self.method()
        assert method is not None
        return method(self.target(), *args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.target) ^ hash(self.method)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, WeakMethod):
            return False
        return (
            self._target_id == other._target_id and self._method_id == other._method_id
        )
