
try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future
import sys  # noqa
PY_35 = sys.version_info >= (3, 5)

__version__ = '0.1.3'

from .client import AIOKafkaClient  # noqa
from .producer import AIOKafkaProducer  # noqa
from .consumer import AIOKafkaConsumer  # noqa


(AIOKafkaClient, AIOKafkaProducer, AIOKafkaConsumer, ensure_future)
