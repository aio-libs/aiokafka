from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
import time
from enum import Enum
from typing import (
    Awaitable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Pattern,
    Set,
    TypeVar,
    Union,
)

from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.errors import IllegalStateError
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from aiokafka.util import create_future, get_running_loop

log = logging.getLogger(__name__)

T = TypeVar("T")


class SubscriptionType(Enum):
    NONE = 1
    AUTO_TOPICS = 2
    AUTO_PATTERN = 3
    USER_ASSIGNED = 4


class SubscriptionState:
    """Intermediate bridge to coordinate work between Consumer, Coordinator
    and Fetcher primitives.

        The class is different from kafka-python's implementation to provide
    a more friendly way to interact in asynchronous paradigm. The changes
    focus on making the internals less mutable (subscription, topic state etc.)
    paired with futures for when those change.
        Before there was a lot of trouble if user say did a subscribe between
    yield statements of a rebalance or other critical IO operation.
    """

    _subscription_type: SubscriptionType = SubscriptionType.NONE
    _subscribed_pattern: Optional[str] = None
    _subscription: "Subscription" = None
    _listener: Optional[ConsumerRebalanceListener] = None

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        if loop is None:
            loop = get_running_loop()
        self._loop = loop

        self._subscription_waiters: List[asyncio.Future[T]] = []
        self._assignment_waiters: List[asyncio.Future[T]] = []

        # Fetch contexts
        self._fetch_count: int = 0
        self._last_fetch_ended: float = time.monotonic()

    @property
    def subscription(self) -> "Subscription":
        return self._subscription

    @property
    def subscribed_pattern(self) -> Optional[Pattern]:
        return self._subscribed_pattern

    @property
    def listener(self) -> ConsumerRebalanceListener:
        return self._listener

    @property
    def topics(self) -> Union[FrozenSet[str], set[None]]:
        if self._subscription is not None:
            return self._subscription.topics
        return set()

    def assigned_partitions(self) -> Iterable[TopicPartition]:
        if self._subscription is None:
            return set()
        if self._subscription.assignment is None:
            return set()
        return self._subscription.assignment.tps

    @property
    def reassignment_in_progress(self) -> bool:
        if self._subscription is None:
            return True
        return self._subscription._reassignment_in_progress

    def partitions_auto_assigned(self) -> bool:
        return (
            self._subscription_type == SubscriptionType.AUTO_TOPICS
            or self._subscription_type == SubscriptionType.AUTO_PATTERN
        )

    def is_assigned(self, tp: TopicPartition) -> bool:
        if self._subscription is None:
            return False
        if self._subscription.assignment is None:
            return False
        return tp in self._subscription.assignment.tps

    def _set_subscription_type(self, subscription_type: SubscriptionType) -> None:
        if (
            self._subscription_type == SubscriptionType.NONE
            or self._subscription_type == subscription_type
        ):
            self._subscription_type = subscription_type
        else:
            raise IllegalStateError(
                "Subscription to topics, partitions and pattern are mutually "
                "exclusive"
            )

    def _change_subscription(self, subscription: "Subscription") -> None:
        log.info("Updating subscribed topics to: %s", subscription.topics)
        # Set old subscription as inactive
        if self._subscription is not None:
            self._subscription._unsubscribe()
        self._subscription = subscription
        self._notify_subscription_waiters()

    def _assigned_state(self, tp: TopicPartition) -> "TopicPartitionState":
        assert self._subscription is not None
        assert self._subscription.assignment is not None
        tp_state = self._subscription.assignment.state_value(tp)
        if tp_state is None:
            raise IllegalStateError(f"No current assignment for partition {tp}")
        return tp_state

    def _notify_subscription_waiters(self) -> None:
        for waiter in self._subscription_waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._subscription_waiters.clear()

    def _notify_assignment_waiters(self) -> None:
        for waiter in self._assignment_waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._assignment_waiters.clear()

    # Consumer callable API:

    def subscribe(
        self, topics: Set[str], listener: Optional[ConsumerRebalanceListener] = None
    ) -> None:
        """Subscribe to a list (or tuple) of topics

        Caller: Consumer.
        Affects: SubscriptionState.subscription
        """
        assert isinstance(topics, set)
        assert listener is None or isinstance(listener, ConsumerRebalanceListener)
        self._set_subscription_type(SubscriptionType.AUTO_TOPICS)

        self._change_subscription(Subscription(topics, loop=self._loop))
        self._listener = listener
        self._notify_subscription_waiters()

    def subscribe_pattern(
        self, pattern: Pattern, listener: Optional[ConsumerRebalanceListener] = None
    ) -> None:
        """Subscribe to all topics matching a regex pattern.
        Subsequent calls `subscribe_from_pattern()` by Coordinator will provide
        the actual subscription topics.

        Caller: Consumer.
        Affects: SubscriptionState.subscribed_pattern
        """
        assert hasattr(pattern, "match"), "Expected Pattern type"
        assert listener is None or isinstance(listener, ConsumerRebalanceListener)
        self._set_subscription_type(SubscriptionType.AUTO_PATTERN)

        self._subscribed_pattern: Optional[Pattern] = pattern
        self._listener: Optional[ConsumerRebalanceListener] = listener

    def assign_from_user(self, partitions: Iterable[TopicPartition]) -> None:
        """Manually assign partitions. After this call automatic assignment
        will be impossible and will raise an ``IllegalStateError``.

        Caller: Consumer.
        Affects: SubscriptionState.subscription
        """
        self._set_subscription_type(SubscriptionType.USER_ASSIGNED)

        self._change_subscription(ManualSubscription(partitions, loop=self._loop))
        self._notify_assignment_waiters()

    def unsubscribe(self) -> None:
        """Unsubscribe from the last subscription. This will also clear the
        subscription type.

        Caller: Consumer.
        Affects: SubscriptionState.subscription
        """
        # Set old subscription as inactive
        if self._subscription is not None:
            self._subscription._unsubscribe()

        self._subscription: Optional[Subscription] = None
        self._subscribed_pattern: Optional[Pattern] = None
        self._listener: Optional[ConsumerRebalanceListener] = None
        self._subscription_type: SubscriptionType = SubscriptionType.NONE

    # Coordinator callable API:

    def subscribe_from_pattern(self, topics: Set[str]) -> None:
        """Change subscription on cluster metadata update if a new topic
        created or one is removed.

        Caller: Coordinator
        Affects: SubscriptionState.subscription
        """
        assert self._subscription_type == SubscriptionType.AUTO_PATTERN
        self._change_subscription(Subscription(topics))

    def assign_from_subscribed(self, assignment: Set[TopicPartition]) -> None:
        """Set assignment if automatic assignment is used.

        Caller: Coordinator
        Affects: SubscriptionState.subscription.assignment
        """
        assert self._subscription_type in [
            SubscriptionType.AUTO_PATTERN,
            SubscriptionType.AUTO_TOPICS,
        ]

        self._subscription._assign(assignment)
        self._notify_assignment_waiters()

    def begin_reassignment(self) -> None:
        """Signal from Coordinator that a group re-join is needed. For example
        this will be called if a commit or heartbeat fails with an
        InvalidMember error.

        Caller: Coordinator
        """
        if self._subscription is not None:
            self._subscription._begin_reassignment()

    # Fetcher callable API:

    def seek(self, tp: TopicPartition, offset: int) -> None:
        """Force reset of position to the specified offset.

        Caller: Consumer, Fetcher
        Affects: TopicPartitionState.position
        """
        self._assigned_state(tp).seek(offset)

    # Waiters

    def wait_for_subscription(self) -> asyncio.Future[T]:
        """Wait for subscription change. This will always wait for next
        subscription.
        """
        fut: asyncio.Future[T] = create_future()
        self._subscription_waiters.append(fut)
        return fut

    def wait_for_assignment(self) -> asyncio.Future[T]:
        """Wait for next assignment. Be careful, as this will always wait for
        next assignment, even if the current one is active.
        """
        fut: asyncio.Future[T] = create_future()
        self._assignment_waiters.append(fut)
        return fut

    def register_fetch_waiters(self, waiters) -> None:
        self._fetch_waiters = waiters

    def abort_waiters(self, exc: Exception) -> None:
        """Critical error occurred, we will abort any pending waiter"""
        for waiter in self._assignment_waiters:
            if not waiter.done():
                waiter.set_exception(copy.copy(exc))
        self._subscription_waiters.clear()

        for waiter in self._fetch_waiters:
            if not waiter.done():
                waiter.set_exception(copy.copy(exc))

    # Pause/Resume API

    def pause(self, tp: TopicPartition) -> None:
        self._assigned_state(tp).pause()

    def paused_partitions(self) -> Set[TopicPartition]:
        res = set()
        for tp in self.assigned_partitions():
            if self._assigned_state(tp).paused:
                res.add(tp)
        return res

    def resume(self, tp: TopicPartition) -> None:
        self._assigned_state(tp).resume()

    # Fetch context

    @contextlib.contextmanager
    def fetch_context(self):
        self._fetch_count += 1
        yield
        self._fetch_count -= 1
        if self._fetch_count == 0:
            self._last_fetch_ended = time.monotonic()

    @property
    def fetcher_idle_time(self) -> float:
        """How much time (in seconds) spent without consuming any records"""
        if self._fetch_count == 0:
            return time.monotonic() - self._last_fetch_ended
        else:
            return 0


class Subscription:
    """Describes current subscription to a list of topics. In case of pattern
    subscription a new instance of this class will be created if number of
    matched topics change.

    States:
        * Subscribed
        * Assigned (assignment was set)
        * Unsubscribed
    """

    def __init__(
        self, topics: Iterable[str], loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        if loop is None:
            loop = get_running_loop()

        self._topics: FrozenSet[str] = frozenset(topics)
        self._assignment: Optional[Assignment] = None
        self.unsubscribe_future: asyncio.Future[T] = loop.create_future()
        self._reassignment_in_progress: bool = True

    @property
    def active(self) -> bool:
        return self.unsubscribe_future.done() is False

    @property
    def topics(self) -> FrozenSet[str]:
        return self._topics

    @property
    def assignment(self) -> "Assignment":
        return self._assignment

    def _assign(self, topic_partitions: Iterable[TopicPartition]) -> None:
        for tp in topic_partitions:
            assert (
                tp.topic in self._topics
            ), f"Received an assignment for unsubscribed topic: {tp}"

        if self._assignment is not None:
            self._assignment._unassign()

        self._assignment = Assignment(topic_partitions)
        self._reassignment_in_progress = False

    def _unsubscribe(self) -> None:
        self.unsubscribe_future.set_result(None)
        if self._assignment is not None:
            self._assignment._unassign()

    def _begin_reassignment(self) -> None:
        self._reassignment_in_progress = True


class ManualSubscription(Subscription):
    """Describes a user assignment"""

    def __init__(
        self,
        user_assignment: Iterable[TopicPartition],
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        topics = (tp.topic for tp in user_assignment)
        super().__init__(topics, loop=loop)
        self._assignment = Assignment(user_assignment)

    def _assign(
        self, topic_partitions: Iterable[TopicPartition]
    ) -> None:  # pragma: no cover
        assert False, "Should not be called"

    @property
    def _reassignment_in_progress(self) -> bool:
        return False

    @_reassignment_in_progress.setter
    def _reassignment_in_progress(self, value: bool) -> None:
        pass

    def _begin_reassignment(self) -> None:  # pragma: no cover
        assert False, "Should not be called"


class Assignment:
    """Describes current partition assignment. New instance will be created
    on each group rebalance if automatic assignment is used.

    States:
        * Assigned
        * Unassigned
    """

    def __init__(self, topic_partitions: Iterable[TopicPartition]) -> None:
        assert isinstance(topic_partitions, (list, set, tuple))

        self._topic_partitions: FrozenSet[TopicPartition] = frozenset(topic_partitions)

        self._tp_state: Dict[TopicPartition, TopicPartitionState] = {}
        for tp in self._topic_partitions:
            self._tp_state[tp] = TopicPartitionState(self)

        self.unassign_future: asyncio.Future[T] = create_future()
        self.commit_refresh_needed = asyncio.Event()

    @property
    def tps(self) -> Iterable[TopicPartition]:
        return self._topic_partitions

    @property
    def active(self) -> bool:
        return self.unassign_future.done() is False

    def _unassign(self) -> None:
        self.unassign_future.set_result(None)

    def state_value(self, topic_partition: TopicPartition) -> "TopicPartitionState":
        return self._tp_state.get(topic_partition)

    def all_consumed_offsets(self) -> Dict[TopicPartition, OffsetAndMetadata]:
        """Returns consumed offsets as {TopicPartition: OffsetAndMetadata}"""
        all_consumed = {}
        for topic_partition in self._topic_partitions:
            state = self.state_value(topic_partition)
            if state.has_valid_position:
                all_consumed[topic_partition] = OffsetAndMetadata(state.position, "")
        return all_consumed

    def requesting_committed(self) -> List[TopicPartition]:
        """Return all partitions that are requesting commit point fetch"""
        requesting = []
        for topic_partition in self._topic_partitions:
            tp_state = self.state_value(topic_partition)
            if tp_state._committed_futs:
                requesting.append(topic_partition)
        return requesting


class PartitionStatus(Enum):
    AWAITING_RESET = 0
    CONSUMING = 1
    UNASSIGNED = 2


class TopicPartitionState:
    """Shared Partition metadata state.

    After creation the workflow is similar to:

        * Partition assigned to this consumer (AWAITING_RESET)
        * Fetcher either uses commit save point or resets position in respect
          to defined reset policy (AWAITING_RESET -> CONSUMING)
        * Fetcher loads a new batch of records, yields results to consumer
          and updates consumed position (CONSUMING)
        * Assignment changed or subscription changed (CONSUMING -> UNASSIGNED)

    """

    def __init__(self, assignment: Assignment) -> None:
        # Synchronized values
        self._committed_futs: List[asyncio.Future[T]] = []

        self.highwater = None  # Last fetched highwater mark
        self.lso = None  # Last fetched stable offset mark
        self.timestamp: Optional[float] = None  # timestamp of last poll
        self._position: Optional[int] = None  # The current position of the topic
        self._position_fut: asyncio.Future[T] = create_future()

        # Will be set by `seek_to_beginning` or `seek_to_end` if called by user
        # or by Fetcher after confirming that current position is no longer
        # reachable.
        self._reset_strategy: Optional[int] = None
        self._status: PartitionStatus = PartitionStatus.AWAITING_RESET

        self._assignment: Assignment = assignment

        self._paused: bool = False
        self._resume_fut: Optional[asyncio.Future[T]] = None

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def resume_fut(self) -> Optional[asyncio.Future[T]]:
        return self._resume_fut

    @property
    def has_valid_position(self) -> bool:
        return self._position is not None

    @property
    def position(self) -> int:
        assert self._position is not None
        return self._position

    @property
    def awaiting_reset(self) -> bool:
        return self._reset_strategy is not None

    @property
    def reset_strategy(self) -> Optional[int]:
        return self._reset_strategy

    def await_reset(self, strategy: int) -> None:
        """Called by either Consumer in `seek_to_*` or by Coordinator after
        setting initial committed point.
        """
        self._reset_strategy = strategy
        self._position = None
        if self._position_fut.done():
            self._position_fut = create_future()
        self._status = PartitionStatus.AWAITING_RESET

    # Committed manipulation

    def fetch_committed(self) -> asyncio.Future[T]:
        fut: asyncio.Future[T] = create_future()
        self._committed_futs.append(fut)
        self._assignment.commit_refresh_needed.set()
        return fut

    def update_committed(self, offset_meta: OffsetAndMetadata) -> None:
        """Called by Coordinator on successful commit to update commit cache."""
        for fut in self._committed_futs:
            if not fut.done():
                fut.set_result(offset_meta)
        self._committed_futs.clear()

    # Position manipulation

    def consumed_to(self, position: int) -> None:
        """Called by Fetcher when yielding results to Consumer"""
        assert self._status == PartitionStatus.CONSUMING
        self._position = position

    def reset_to(self, position: int) -> None:
        """Called by Fetcher after performing a reset to force position to
        a new point.
        """
        assert self._status == PartitionStatus.AWAITING_RESET
        self._position = position
        self._reset_strategy = None
        self._status = PartitionStatus.CONSUMING
        if not self._position_fut.done():
            self._position_fut.set_result(None)

    def seek(self, position: int) -> None:
        """Called by Consumer to force position to a specific offset"""
        self._position = position
        self._reset_strategy = None
        self._status = PartitionStatus.CONSUMING
        if not self._position_fut.done():
            self._position_fut.set_result(None)

    def wait_for_position(self) -> Awaitable[T]:
        return asyncio.shield(self._position_fut)

    # Pause/Unpause
    def pause(self) -> None:
        if not self._paused:
            self._paused = True
            assert self._resume_fut is None
            self._resume_fut = create_future()

    def resume(self) -> None:
        if self._paused:
            self._paused = False
            if self._resume_fut is not None:
                self._resume_fut.set_result(None)
            self._resume_fut = None

    def __repr__(self) -> str:
        return f"TopicPartitionState<Status={self._status} position={self._position}>"
