import runpy
from functools import partial
from pathlib import Path
from unittest import mock

import prometheus_client
import pytest

from aiokafka import TopicPartition


@pytest.fixture
def example(monkeypatch):
    registry = prometheus_client.CollectorRegistry()
    for name in ("Counter", "Gauge", "Histogram"):
        metric = getattr(prometheus_client, name)
        monkeypatch.setattr(prometheus_client, name, partial(metric, registry=registry))
    path = Path(__file__).resolve().parents[1] / "examples" / "prometheus_metrics.py"
    namespace = runpy.run_path(str(path))
    return namespace, registry


def test_revoked_partition_lag_is_removed(example):
    namespace, registry = example
    collector = namespace["PrometheusConsumerMetrics"]()
    listener = namespace["LagMetricsRebalanceListener"]()
    revoked = TopicPartition("topic", 0)
    retained = TopicPartition("topic", 1)
    never_fetched = TopicPartition("topic", 2)
    name = "aiokafka_consumer_records_lag"

    collector.on_records_lag(topic=revoked.topic, partition=revoked.partition, lag=100)
    collector.on_records_lag(topic=retained.topic, partition=retained.partition, lag=7)
    assert registry.get_sample_value(name, {"topic": "topic", "partition": "0"}) == 100

    listener.on_partitions_revoked([revoked, never_fetched])
    listener.on_partitions_revoked([revoked])
    assert registry.get_sample_value(name, {"topic": "topic", "partition": "0"}) is None
    assert registry.get_sample_value(name, {"topic": "topic", "partition": "1"}) == 7
    assert registry.get_sample_value(name, {"topic": "topic", "partition": "2"}) is None

    listener.on_partitions_assigned([revoked])
    assert registry.get_sample_value(name, {"topic": "topic", "partition": "0"}) is None
    collector.on_records_lag(topic=revoked.topic, partition=revoked.partition, lag=3)
    assert registry.get_sample_value(name, {"topic": "topic", "partition": "0"}) == 3


async def test_consumer_registers_rebalance_listener_before_start(example):
    namespace, _ = example
    consume = namespace["consume"]
    consumer = mock.MagicMock()

    async def start():
        consumer.subscribe.assert_called_once()
        return consumer

    consumer.__aenter__.side_effect = start
    consumer.getmany = mock.AsyncMock(side_effect=RuntimeError("stop example"))
    with (
        mock.patch.dict(
            consume.__globals__,
            AIOKafkaConsumer=mock.Mock(return_value=consumer),
            start_http_server=mock.Mock(),
        ),
        pytest.raises(RuntimeError, match="stop example"),
    ):
        await consume()
    listener = consumer.subscribe.call_args.kwargs["listener"]
    assert isinstance(listener, namespace["LagMetricsRebalanceListener"])
    assert consumer.subscribe.call_args.args == (["test-topic"],)
    consumer.__aexit__.assert_awaited_once()
