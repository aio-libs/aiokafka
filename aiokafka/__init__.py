import sys

from .errors import ConsumerStoppedError

try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future

__version__ = '0.2.2'
PY_35 = sys.version_info >= (3, 5)

from .client import AIOKafkaClient  # noqa
from .producer import AIOKafkaProducer  # noqa
from .consumer import AIOKafkaConsumer  # noqa

__all__ = [
    "AIOKafkaProducer",
    "AIOKafkaConsumer",
    "ConsumerStoppedError",
]

(AIOKafkaClient, ensure_future)
