
try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future

__version__ = '0.0.1'

from .client import AIOKafkaClient  # noqa
from .producer import AIOKafkaProducer  # noqa

# from .consumer import SimpleAIOConsumer

(AIOKafkaClient, AIOKafkaProducer, ensure_future)
