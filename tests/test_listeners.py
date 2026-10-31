import asyncio
import warnings
from unittest import mock

import pytest

from aiokafka import (
    AIOKafkaClient,
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConnectionListener,
    ConsumerRebalanceListener,
    ProducerListener,
)
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.structs import TopicPartition


async def test_noop_listener_creates_no_callbacks():
    producer = AIOKafkaProducer(listeners=[ProducerListener()])
    try:
        assert not producer._message_accumulator._callbacks
    finally:
        await producer.stop()


async def test_duplicate_listener_is_rejected():
    listener = ProducerListener()
    with pytest.raises(ValueError, match="same instance"):
        AIOKafkaProducer(listeners=[listener, listener])


@pytest.mark.parametrize(
    "event",
    ["on_batch_dispatched", "on_batch_completed", "on_batch_failed", "on_buffer_wait"],
)
async def test_invalid_producer_callback_is_rejected(event):
    listener = ProducerListener()
    setattr(listener, event, False)
    with pytest.raises(TypeError, match=f"{event} must be callable"):
        AIOKafkaProducer(listeners=[listener])


async def test_registration_snapshots_generator_and_preserves_overrides():
    events = []

    class Parent(ProducerListener):
        def on_buffer_wait(self, **event):
            events.append(event)

    class Child(Parent):
        pass

    instance = ProducerListener()
    instance.on_buffer_wait = mock.Mock()
    listeners = [Child(), instance]
    producer = AIOKafkaProducer(listeners=iter(listeners))
    listeners.clear()
    try:
        producer._message_accumulator._report_buffer_wait(TopicPartition("topic", 0), 1)
        assert events == [{"topic": "topic", "partition": 0, "wait_seconds": 1}]
        instance.on_buffer_wait.assert_called_once_with(
            topic="topic", partition=0, wait_seconds=1
        )
    finally:
        await producer.stop()


def test_listener_can_combine_connection_batch_and_existing_rebalance_methods():
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        class ApplicationListener(ProducerListener, ConsumerRebalanceListener):
            def on_partitions_revoked(self, partitions):
                pass

            async def on_partitions_assigned(self, partitions):
                pass

    listener = ApplicationListener()
    assert isinstance(listener, ConnectionListener)
    assert isinstance(listener, ConsumerRebalanceListener)


@pytest.mark.parametrize("cls", [AIOKafkaClient, AIOKafkaProducer, AIOKafkaConsumer])
@pytest.mark.parametrize("listener_cls", [ConnectionListener, ProducerListener])
async def test_noop_listeners_create_no_connection_dispatcher(cls, listener_cls):
    obj = cls(listeners=[listener_cls()])
    client = (
        obj if cls is AIOKafkaClient else getattr(obj, "_client", None) or obj.client
    )
    try:
        assert client._connection_callbacks is None
        if cls is AIOKafkaProducer:
            assert not obj._message_accumulator._callbacks
        if cls is AIOKafkaConsumer:
            obj.subscribe(["topic"])
            assert obj._subscription.listener is None
    finally:
        await (obj.close() if cls is AIOKafkaClient else obj.stop())


async def test_legacy_rebalance_registration_stays_unchanged():
    legacy = mock.Mock(spec=ConsumerRebalanceListener)
    consumer = AIOKafkaConsumer(listeners=[ConnectionListener()])
    try:
        consumer.subscribe(["topic"], listener=legacy)
        assert consumer._subscription.listener is legacy
        consumer.subscribe(["other"])
        assert consumer._subscription.listener is None
    finally:
        await consumer.stop()


async def test_connection_registration_preserves_async_rebalance_contract():
    entered, release = asyncio.Event(), asyncio.Event()

    class ApplicationListener(ProducerListener, ConsumerRebalanceListener):
        async def on_partitions_revoked(self, partitions):
            entered.set()
            await release.wait()

        def on_partitions_assigned(self, partitions):
            pass

    listener = ApplicationListener()
    consumer = AIOKafkaConsumer(listeners=[listener])
    consumer.subscribe(["topic"], listener=listener)
    coordinator = mock.Mock(spec=GroupCoordinator)
    coordinator._subscription = consumer._subscription
    coordinator.group_id = "group"
    task = asyncio.create_task(GroupCoordinator._on_join_prepare(coordinator, None))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        assert consumer._subscription.listener is listener
        assert not task.done()
        release.set()
        await asyncio.wait_for(task, timeout=1)
    finally:
        release.set()
        await task
        await consumer.stop()
