from unittest import mock

import pytest

from aiokafka import AIOKafkaProducer, ProducerListener
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
