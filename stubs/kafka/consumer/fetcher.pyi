import kafka.errors as Errors
import six
from collections import namedtuple
from kafka.future import Future as Future
from kafka.metrics.stats import Avg as Avg, Count as Count, Max as Max, Rate as Rate
from kafka.protocol.fetch import FetchRequest as FetchRequest
from kafka.protocol.offset import OffsetRequest as OffsetRequest, OffsetResetStrategy as OffsetResetStrategy, UNKNOWN_OFFSET as UNKNOWN_OFFSET
from kafka.record import MemoryRecords as MemoryRecords
from kafka.serializer import Deserializer as Deserializer
from kafka.structs import OffsetAndTimestamp as OffsetAndTimestamp, TopicPartition as TopicPartition
from typing import Any, Optional

log: Any
READ_UNCOMMITTED: int
READ_COMMITTED: int

ConsumerRecord = namedtuple('ConsumerRecord', ['topic', 'partition', 'offset', 'timestamp', 'timestamp_type', 'key', 'value', 'headers', 'checksum', 'serialized_key_size', 'serialized_value_size', 'serialized_header_size'])

CompletedFetch = namedtuple('CompletedFetch', ['topic_partition', 'fetched_offset', 'response_version', 'partition_data', 'metric_aggregator'])

class NoOffsetForPartitionError(Errors.KafkaError): ...
class RecordTooLargeError(Errors.KafkaError): ...

class Fetcher(six.Iterator):
    DEFAULT_CONFIG: Any = ...
    config: Any = ...
    def __init__(self, client: Any, subscriptions: Any, metrics: Any, **configs: Any) -> None: ...
    def send_fetches(self): ...
    def reset_offsets_if_needed(self, partitions: Any) -> None: ...
    def in_flight_fetches(self): ...
    def update_fetch_positions(self, partitions: Any) -> None: ...
    def get_offsets_by_times(self, timestamps: Any, timeout_ms: Any): ...
    def beginning_offsets(self, partitions: Any, timeout_ms: Any): ...
    def end_offsets(self, partitions: Any, timeout_ms: Any): ...
    def beginning_or_end_offset(self, partitions: Any, timestamp: Any, timeout_ms: Any): ...
    def fetched_records(self, max_records: Optional[Any] = ..., update_offsets: bool = ...): ...
    def __iter__(self) -> Any: ...
    def __next__(self): ...
    class PartitionRecords:
        fetch_offset: Any = ...
        topic_partition: Any = ...
        messages: Any = ...
        message_idx: Any = ...
        def __init__(self, fetch_offset: Any, tp: Any, messages: Any) -> None: ...
        def __len__(self): ...
        def discard(self) -> None: ...
        def take(self, n: Optional[Any] = ...): ...

class FetchResponseMetricAggregator:
    sensors: Any = ...
    unrecorded_partitions: Any = ...
    total_bytes: int = ...
    total_records: int = ...
    def __init__(self, sensors: Any, partitions: Any) -> None: ...
    def record(self, partition: Any, num_bytes: Any, num_records: Any) -> None: ...

class FetchManagerMetrics:
    metrics: Any = ...
    group_name: Any = ...
    bytes_fetched: Any = ...
    records_fetched: Any = ...
    fetch_latency: Any = ...
    records_fetch_lag: Any = ...
    fetch_throttle_time_sensor: Any = ...
    def __init__(self, metrics: Any, prefix: Any) -> None: ...
    def record_topic_fetch_metrics(self, topic: Any, num_bytes: Any, num_records: Any) -> None: ...
