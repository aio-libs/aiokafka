import sys

from .errors import ConsumerStoppedError, IllegalOperation
from .abc import ConsumerRebalanceListener

try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future

__version__ = '0.3.1'
PY_35 = sys.version_info >= (3, 5)

from .structs import TopicPartition  # noqa
from .client import AIOKafkaClient  # noqa
from .producer import AIOKafkaProducer  # noqa
from .consumer import AIOKafkaConsumer  # noqa
from aiokafka.fetcher import ConsumerRecord  # noqa

__all__ = [
    # Clients API
    "AIOKafkaProducer",
    "AIOKafkaConsumer",
    # ABC's
    "ConsumerRebalanceListener",
    # Errors
    "ConsumerStoppedError", "IllegalOperation",
    # Structs
    "ConsumerRecord", "TopicPartition"
]

(AIOKafkaClient, ensure_future)
