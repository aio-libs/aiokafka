import threading
from kafka.metrics.measurable import AnonMeasurable as AnonMeasurable
from kafka.metrics.stats import Avg as Avg, Max as Max, Rate as Rate
from kafka.protocol.produce import ProduceRequest as ProduceRequest
from kafka.structs import TopicPartition as TopicPartition
from typing import Any, Optional

log: Any

class Sender(threading.Thread):
    DEFAULT_CONFIG: Any = ...
    config: Any = ...
    name: Any = ...
    def __init__(self, client: Any, metadata: Any, accumulator: Any, metrics: Any, **configs: Any) -> None: ...
    def run(self) -> None: ...
    def run_once(self) -> None: ...
    def initiate_close(self) -> None: ...
    def force_close(self) -> None: ...
    def add_topic(self, topic: Any) -> None: ...
    def wakeup(self) -> None: ...
    def bootstrap_connected(self): ...

class SenderMetrics:
    metrics: Any = ...
    batch_size_sensor: Any = ...
    compression_rate_sensor: Any = ...
    queue_time_sensor: Any = ...
    produce_throttle_time_sensor: Any = ...
    records_per_request_sensor: Any = ...
    byte_rate_sensor: Any = ...
    retry_sensor: Any = ...
    error_sensor: Any = ...
    max_record_size_sensor: Any = ...
    def __init__(self, metrics: Any, client: Any, metadata: Any): ...
    def add_metric(self, metric_name: Any, measurable: Any, group_name: str = ..., description: Optional[Any] = ..., tags: Optional[Any] = ..., sensor_name: Optional[Any] = ...) -> None: ...
    def maybe_register_topic_metrics(self, topic: Any): ...
    def update_produce_request_metrics(self, batches_map: Any) -> None: ...
    def record_retries(self, topic: Any, count: Any) -> None: ...
    def record_errors(self, topic: Any, count: Any) -> None: ...
    def record_throttle_time(self, throttle_time_ms: Any, node: Optional[Any] = ...) -> None: ...
