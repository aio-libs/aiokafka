from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable
from typing import Any, TypeVar

from .structs import OffsetAndMetadata, TopicPartition

__all__ = [
    "INTEGER_MAX_VALUE",
    "INTEGER_MIN_VALUE",
    "NO_EXTENSIONS",
]


T = TypeVar("T")


async def cancel_and_wait(fut: asyncio.Future[Any]) -> None:
    """Cancel a future/task and wait for its completion.

    Propagate non-cancellation exceptions raised by the future. If the caller is
    cancelled, the future may still be running its cleanup.
    """
    if not fut.done():
        fut.cancel()
        # Use asyncio.wait() instead of awaiting `fut` directly so `fut`'s own
        # CancelledError is not raised here—any CancelledError thrown by this
        # await unambiguously means *the caller* was cancelled.
        await asyncio.wait({fut})

    # Surface any real exception raised before or during cancellation (e.g., in
    # a `finally` block) and mark it as retrieved to prevent unretrieved-task
    # warnings.
    if not fut.cancelled():
        fut.result()


async def wait_for(fut: Awaitable[T], timeout: float | None = None) -> T:  # noqa: ASYNC109
    # A replacement for buggy (since 3.8.6) `asyncio.wait_for()`
    # https://bugs.python.org/issue42130
    async with asyncio.timeout(timeout):
        return await fut


def commit_structure_validate(
    offsets: dict[TopicPartition, int | tuple[int, str] | OffsetAndMetadata],
) -> dict[TopicPartition, OffsetAndMetadata]:
    # validate `offsets` structure
    if not offsets or not isinstance(offsets, dict):
        raise ValueError(offsets)

    formatted_offsets = {}
    for tp, offset_and_metadata in offsets.items():
        if not isinstance(tp, TopicPartition):
            raise TypeError("Key should be TopicPartition instance")

        if isinstance(offset_and_metadata, int):
            offset, metadata = offset_and_metadata, ""
        else:
            try:
                offset, metadata = offset_and_metadata
            except Exception as exc:
                raise ValueError(offsets) from exc

            if not isinstance(metadata, str):
                raise TypeError("Metadata should be a string")

        formatted_offsets[tp] = OffsetAndMetadata(offset, metadata)
    return formatted_offsets


NO_EXTENSIONS = bool(os.environ.get("AIOKAFKA_NO_EXTENSIONS"))

INTEGER_MAX_VALUE = 2**31 - 1
INTEGER_MIN_VALUE = -(2**31)
