import asyncio
import os
from asyncio import AbstractEventLoop
from distutils.version import StrictVersion
from typing import Awaitable, Dict, Tuple, TypeVar, Union

from .structs import OffsetAndMetadata, TopicPartition


__all__ = [
    "create_task",
    "create_future",
    "NO_EXTENSIONS",
    "INTEGER_MAX_VALUE",
    "INTEGER_MIN_VALUE",
]


T = TypeVar("T")


def create_task(coro: Awaitable[T]) -> "asyncio.Task[T]":
    loop = get_running_loop()
    return loop.create_task(coro)


def create_future(loop: AbstractEventLoop = None) -> "asyncio.Future[T]":
    if loop is None:
        loop = get_running_loop()
    return loop.create_future()


def parse_kafka_version(api_version: str) -> Tuple[int, int, int]:
    version = StrictVersion(api_version).version
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

INTEGER_MAX_VALUE = 2 ** 31 - 1
INTEGER_MIN_VALUE = -(2 ** 31)
