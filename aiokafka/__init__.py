__version__ = '0.6.1.dev0'  # noqa

from .abc import ConsumerRebalanceListener
from .client import AIOKafkaClient
from .consumer import AIOKafkaConsumer
from .errors import ConsumerStoppedError, IllegalOperation
from .producer import AIOKafkaProducer
from .structs import (
    TopicPartition, ConsumerRecord, OffsetAndTimestamp, OffsetAndMetadata
)
from .util import ensure_future


__all__ = [
    # Clients API
    "AIOKafkaProducer",
    "AIOKafkaConsumer",
    # ABC's
    "ConsumerRebalanceListener",
    # Errors
    "ConsumerStoppedError", "IllegalOperation",
    # Structs
    "ConsumerRecord", "TopicPartition", "OffsetAndTimestamp",
    "OffsetAndMetadata"
]

(ensure_future, AIOKafkaClient)
