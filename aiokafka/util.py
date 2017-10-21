import os
import sys
import asyncio

__all__ = ["ensure_future", "create_future", "PY_35"]


try:
    from asyncio import ensure_future
except ImportError:
    exec("from asyncio import async as ensure_future")


def create_future(loop):
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


PY_35 = sys.version_info >= (3, 5)
NO_EXTENSIONS = bool(os.environ.get('AIOKAFKA_NO_EXTENSIONS'))
