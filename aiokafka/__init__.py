__version__ = '0.0.1'

try:
    from asyncio import ensure_future
except ImportError:
    ensure_future = asyncio.async

from .client import AIOKafkaClient
from .producer import AIOKafkaProducer
#from .consumer import SimpleAIOConsumer

(AIOKafkaClient, AIOKafkaProducer)
