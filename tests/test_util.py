import asyncio
from contextlib import suppress

import pytest

from aiokafka.util import cancel_and_wait


async def test_cancel_before_start() -> None:
    started = False

    async def child() -> None:
        nonlocal started
        started = True

    task = asyncio.create_task(child())
    await cancel_and_wait(task)

    assert not started
    assert task.cancelled()


async def test_caller_cancelled_during_cleanup() -> None:
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def child() -> None:
        try:
            started.set()
            await asyncio.Future()
        finally:
            cleanup_started.set()
            await finish_cleanup.wait()

    task = asyncio.create_task(child())
    caller = None
    try:
        async with asyncio.timeout(5):
            await started.wait()
            caller = asyncio.create_task(cancel_and_wait(task))
            await cleanup_started.wait()
            caller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await caller
            assert not task.done()
    finally:
        if caller is not None and not caller.done():
            caller.cancel()
            with suppress(asyncio.CancelledError):
                await caller
        if not cleanup_started.is_set():
            task.cancel()
        finish_cleanup.set()
        with suppress(asyncio.CancelledError):
            await task

    assert task.cancelled()


async def test_cleanup_error() -> None:
    started = asyncio.Event()

    async def child() -> None:
        try:
            started.set()
            await asyncio.Future()
        finally:
            raise RuntimeError("cleanup failed")

    task = asyncio.create_task(child())
    await started.wait()
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await cancel_and_wait(task)


async def test_finished_error() -> None:
    future = asyncio.get_running_loop().create_future()
    future.set_exception(RuntimeError("task failed"))
    with pytest.raises(RuntimeError, match="task failed"):
        await cancel_and_wait(future)
