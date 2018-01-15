import pytest
import re

from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.errors import IllegalStateError
from aiokafka.structs import TopicPartition
from aiokafka.abc import ConsumerRebalanceListener


@pytest.fixture
def subscription_state(loop):
    return SubscriptionState(loop=loop)


class MockListener(ConsumerRebalanceListener):

    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        pass


def test_subscribe_topic(subscription_state):
    mock_listener = MockListener()
    subscription_state.subscribe({"tp1", "tp2"}, listener=mock_listener)
    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"tp1", "tp2"}
    assert subscription_state.subscription.assignment is None
    assert subscription_state.subscription.active is True
    assert subscription_state.subscription.unsubscribe_future.done() is False

    # After subscription to topic we can't change the subscription to pattern
    # or user assignment
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe_pattern(
            pattern=re.compile("^tests-.*$"), listener=mock_listener)
    with pytest.raises(IllegalStateError):
        subscription_state.assign_from_user([TopicPartition("topic", 0)])

    # Subsciption of the same type can be applied
    old_subsciption = subscription_state.subscription
    subscription_state.subscribe(
        {"tp1", "tp2", "tp3"}, listener=mock_listener)

    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"tp1", "tp2", "tp3"}

    assert old_subsciption is not subscription_state.subscription
    assert old_subsciption.active is False
    assert old_subsciption.unsubscribe_future.done() is True


def test_subscribe_pattern(subscription_state):
    mock_listener = MockListener()
    pattern = re.compile("^tests-.*$")
    subscription_state.subscribe_pattern(
        pattern=pattern, listener=mock_listener)

    assert subscription_state.subscription is None
    assert subscription_state.subscribed_pattern == pattern

    # After subscription to a pattern we can't change the subscription until
    # `unsubscribe` called
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe(
            topics={"tp1", "tp2", "tp3"}, listener=mock_listener)
    with pytest.raises(IllegalStateError):
        subscription_state.assign_from_user([TopicPartition("topic", 0)])


def test_user_assignment(subscription_state):
    topic_partitions = {
        TopicPartition("topic1", 0),
        TopicPartition("topic1", 1),
        TopicPartition("topic2", 0)
    }
    subscription_state.assign_from_user(topic_partitions)
    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"topic1", "topic2"}
    assert subscription_state.subscription.assignment is not None
    assert subscription_state.subscription.active is True
    assert subscription_state.subscription.unsubscribe_future.done() is False
    assignment = subscription_state.subscription.assignment
    assert assignment.active is True
    assert assignment.unassign_future.done() is False
    assert assignment.tps == topic_partitions

    # After manual assignment no other subscription is possible
    mock_listener = MockListener()
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe(
            topics={"tp1", "tp2", "tp3"}, listener=mock_listener)
    with pytest.raises(IllegalStateError):
        subscription_state.subscribe_pattern(
            pattern=re.compile("^tests-.*$"), listener=mock_listener)

    # Assignment can be changed manually again thou
    new_tps = {
        TopicPartition("topic3", 0),
        TopicPartition("topic3", 1),
        TopicPartition("topic4", 0)
    }
    subscription_state.assign_from_user(new_tps)

    assert subscription_state.subscription is not None
    assert subscription_state.subscription.topics == {"topic3", "topic4"}
    new_assignment = subscription_state.subscription.assignment
    assert new_assignment.tps == new_tps

    assert assignment is not new_assignment
    assert assignment.active is False
    assert assignment.unassign_future.done() is True


def test_unsubscribe(subscription_state):
    subscription_state.subscribe({"tp1", "tp2"})
    assert subscription_state.subscription is not None

    subscription_state.unsubscribe()
    assert subscription_state.subscription is None

    # After unsubscribe you can change the type to say pattern.
    subscription_state.subscribe_pattern(re.compile("pattern"))

    assert subscription_state.subscription is not None


def test_seek(subscription_state):
    tp = TopicPartition("topic1", 0)
    tp2 = TopicPartition("topic2", 0)
    subscription_state.assign_from_user({tp, tp2})

    assignment = subscription_state.subscription.assignment
    assert assignment.state_value(tp) is not None
    assert assignment.state_value(tp).position is None

    subscription_state.seek(tp, 1000)
