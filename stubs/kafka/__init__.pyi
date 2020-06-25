import logging
from kafka.admin import KafkaAdminClient as KafkaAdminClient
from kafka.client_async import KafkaClient as KafkaClient
from kafka.conn import BrokerConnection as BrokerConnection
from kafka.consumer import KafkaConsumer as KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener as ConsumerRebalanceListener
from kafka.producer import KafkaProducer as KafkaProducer
from typing import Any

class NullHandler(logging.Handler):
    def emit(self, record: Any) -> None: ...
