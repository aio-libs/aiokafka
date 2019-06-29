import os
import sys
import asyncio
from distutils.version import StrictVersion

from .structs import TopicPartition, OffsetAndMetadata

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


def commit_structure_validate(offsets):
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


PY_35 = sys.version_info >= (3, 5)
PY_352 = sys.version_info >= (3, 5, 2)
PY_36 = sys.version_info >= (3, 6)
NO_EXTENSIONS = bool(os.environ.get('AIOKAFKA_NO_EXTENSIONS'))

INTEGER_MAX_VALUE = 2 ** 31 - 1
INTEGER_MIN_VALUE = - 2 ** 31
