from collections.abc import AsyncIterator, Callable
from dataclasses import FrozenInstanceError, dataclass

import pytest
from pytest_mock import MockerFixture

from aiokafka.client import AIOKafkaClient
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.coordinator.assignors.sticky.sticky_assignor import (
    StickyPartitionAssignor,
)
from aiokafka.coordinator.protocol import ConsumerProtocolMemberAssignment
from aiokafka.structs import TopicPartition

pytestmark = pytest.mark.usefixtures("loop")


class CustomStickyAssignor(StickyPartitionAssignor):
    __slots__ = ("configuration",)

    def __init__(self, configuration: object) -> None:
        self.configuration = configuration


@dataclass(frozen=True)
class FrozenStickyAssignor(StickyPartitionAssignor):
    configuration: object


@dataclass(frozen=True, slots=True)
class FrozenSlottedStickyAssignor(StickyPartitionAssignor):
    configuration: object


@pytest.fixture
async def coordinator_factory(
    mocker: MockerFixture,
) -> AsyncIterator[Callable[[object], GroupCoordinator]]:
    # Exercise the real join-complete callback without network/background work.
    async def idle(coordinator: GroupCoordinator) -> None:
        await coordinator._closing

    mocker.patch.object(GroupCoordinator, "_coordination_routine", idle)
    mocker.patch.object(GroupCoordinator, "start_commit_offsets_refresh_task")
    clients = []
    coordinators = []

    def create(assignor: object) -> GroupCoordinator:
        client = AIOKafkaClient()
        clients.append(client)
        subscription = SubscriptionState()
        subscription.subscribe({"first", "second"})
        coordinator = GroupCoordinator(
            client, subscription, assignors=(assignor,), enable_auto_commit=False
        )
        coordinators.append(coordinator)
        return coordinator

    yield create
    for coordinator in coordinators:
        await coordinator.close()
    for client in clients:
        await client.close()


@pytest.mark.parametrize(
    "kind",
    [
        pytest.param("class", id="builtin_sticky_class"),
        pytest.param("subclass", id="subclass_requiring_configuration"),
        pytest.param("instance", id="configured_slotted_instance"),
        pytest.param("frozen", id="configured_frozen_dataclass"),
        pytest.param("frozen_slots", id="configured_frozen_slotted_dataclass"),
    ],
)
async def test_sticky_join_state_is_isolated(
    coordinator_factory: Callable[[object], GroupCoordinator], kind: str
) -> None:
    configuration = object()
    if kind == "class":
        supplied: (
            type[StickyPartitionAssignor]
            | CustomStickyAssignor
            | FrozenStickyAssignor
            | FrozenSlottedStickyAssignor
        ) = StickyPartitionAssignor
    elif kind == "subclass":
        supplied = CustomStickyAssignor
    elif kind == "frozen":
        supplied = FrozenStickyAssignor(configuration)
    elif kind == "frozen_slots":
        supplied = FrozenSlottedStickyAssignor(configuration)
    else:
        supplied = CustomStickyAssignor(configuration)
    original_type = type(supplied)
    original_metadata = supplied.metadata({"first"}).user_data
    first = coordinator_factory(supplied)
    second = coordinator_factory(supplied)
    a = first._lookup_assignor("sticky")
    b = second._lookup_assignor("sticky")

    first_assignment = ConsumerProtocolMemberAssignment(0, [("first", [0])], b"")
    second_assignment = ConsumerProtocolMemberAssignment(0, [("second", [1])], b"")
    await first._on_join_complete(
        1, "first-member", "sticky", first_assignment.encode()
    )
    await second._on_join_complete(
        1, "second-member", "sticky", second_assignment.encode()
    )

    assert StickyPartitionAssignor.parse_member_metadata(
        a.metadata({"first"})
    ).partitions == [TopicPartition("first", 0)]
    assert StickyPartitionAssignor.parse_member_metadata(
        b.metadata({"second"})
    ).partitions == [TopicPartition("second", 1)]

    # A later assignment must not overwrite the other coordinator's history.
    next_assignment = ConsumerProtocolMemberAssignment(0, [("first", [0, 2])], b"")
    await first._on_join_complete(2, "first-member", "sticky", next_assignment.encode())
    assert b.member_assignment == [TopicPartition("second", 1)]
    if not isinstance(supplied, type):
        assert a is not supplied and b is not supplied and a is not b
        assert a.configuration is configuration
        assert b.configuration is configuration
        assert type(supplied) is original_type
        assert supplied.configuration is configuration
        assert supplied.metadata({"first"}).user_data == original_metadata
    if isinstance(supplied, (FrozenStickyAssignor, FrozenSlottedStickyAssignor)):
        for assignor in (supplied, a, b):
            with pytest.raises(FrozenInstanceError):
                assignor.configuration = object()  # type: ignore[misc]


async def test_new_sticky_consumer_has_no_previous_assignment(
    coordinator_factory: Callable[[object], GroupCoordinator], mocker: MockerFixture
) -> None:
    mocker.patch.object(
        StickyPartitionAssignor, "member_assignment", [TopicPartition("first", 0)]
    )
    mocker.patch.object(StickyPartitionAssignor, "generation", 42)
    first = coordinator_factory(StickyPartitionAssignor)._lookup_assignor("sticky")
    second = coordinator_factory(StickyPartitionAssignor)._lookup_assignor("sticky")
    assert first.metadata({"first"}).user_data == b""
    assert second.metadata({"second"}).user_data == b""
    first.on_generation_assignment(7)
    assert second.generation == StickyPartitionAssignor.DEFAULT_GENERATION_ID
    assert StickyPartitionAssignor.generation == 42
    assert StickyPartitionAssignor.member_assignment == [TopicPartition("first", 0)]


async def test_sticky_generation_metadata_is_isolated(
    coordinator_factory: Callable[[object], GroupCoordinator], mocker: MockerFixture
) -> None:
    mocker.patch.object(StickyPartitionAssignor, "member_assignment", None)
    mocker.patch.object(StickyPartitionAssignor, "generation", -1)
    first = coordinator_factory(StickyPartitionAssignor)._lookup_assignor("sticky")
    second = coordinator_factory(StickyPartitionAssignor)._lookup_assignor("sticky")
    first.on_assignment(ConsumerProtocolMemberAssignment(0, [("first", [0])], b""))
    first.on_generation_assignment(10)
    second.on_assignment(ConsumerProtocolMemberAssignment(0, [("second", [1])], b""))
    second.on_generation_assignment(20)

    # Exercise the generation callback directly: dispatch from the coordinator
    # is a separate issue. Verify the metadata sent on the next join.
    first_metadata = StickyPartitionAssignor.parse_member_metadata(
        first.metadata({"first"})
    )
    assert first_metadata.generation == 10
    assert first_metadata.partitions == [TopicPartition("first", 0)]
    first.on_generation_assignment(11)
    second_metadata = StickyPartitionAssignor.parse_member_metadata(
        second.metadata({"second"})
    )
    assert second_metadata.generation == 20
    assert second_metadata.partitions == [TopicPartition("second", 1)]


@pytest.mark.parametrize(
    "as_instance",
    [
        pytest.param(False, id="class_requiring_configuration"),
        pytest.param(True, id="configured_instance"),
    ],
)
async def test_custom_assignor_is_preserved(
    coordinator_factory: Callable[[object], GroupCoordinator], as_instance: bool
) -> None:
    class ConfiguredAssignor(RoundRobinPartitionAssignor):
        def __init__(self, configuration: object) -> None:
            self.configuration = configuration

    supplied = ConfiguredAssignor(object()) if as_instance else ConfiguredAssignor
    coordinator = coordinator_factory(supplied)
    assert coordinator._lookup_assignor("roundrobin") is supplied


async def test_sticky_instance_copy_does_not_mutate_input() -> None:
    class UncopyableAssignor(StickyPartitionAssignor):
        def __copy__(self) -> "UncopyableAssignor":
            return self

    supplied = UncopyableAssignor()
    client = AIOKafkaClient()
    try:
        with pytest.raises(TypeError, match="independent copying"):
            GroupCoordinator(client, SubscriptionState(), assignors=(supplied,))
        assert type(supplied) is UncopyableAssignor
    finally:
        await client.close()
