import os
import sys
import asyncio
from distutils.version import StrictVersion

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


def parse_kafka_version(api_version):
    version = StrictVersion(api_version).version
    if not (0, 9) <= version < (3, 0):
        raise ValueError(api_version)
    return version


PY_341 = sys.version_info >= (3, 4, 1)
PY_35 = sys.version_info >= (3, 5)
PY_352 = sys.version_info >= (3, 5, 2)
PY_36 = sys.version_info >= (3, 6)
NO_EXTENSIONS = bool(os.environ.get('AIOKAFKA_NO_EXTENSIONS'))
