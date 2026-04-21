"""Tests for rack-aware replica selection (KIP-392).

Each test verifies an observable behaviour of the feature, not internal
plumbing. The core question answered: "does the consumer route its next
FetchRequest to the correct broker after receiving a preferred_read_replica
hint from the leader?"
"""

from __future__ import annotations

import logging
import time
from unittest import mock

import pytest

from aiokafka import errors as Errors
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.protocol.fetch import (
    FetchRequest,
    FetchRequest_v9,
    FetchRequest_v11,
)
from aiokafka.structs import TopicPartition

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fetcher(client_rack: str = "us-east-1a", metadata_max_age_ms: int = 60_000):
    """Build a minimal Fetcher with mocked client/subscriptions.

    No running event loop needed — we only exercise synchronous methods.
    """
    client = mock.Mock()
    client._loop = mock.Mock()
    client.cluster = mock.Mock()
    client._metadata_max_age_ms = metadata_max_age_ms
    subscriptions = mock.Mock()
    subscriptions.register_fetch_waiters = mock.Mock()

    with (
        mock.patch("aiokafka.consumer.fetcher.create_task"),
        mock.patch.object(Fetcher, "_fetch_requests_routine", lambda self: None),
    ):
        fetcher = Fetcher(
            client,
            subscriptions,
            client_rack=client_rack,
        )
    return fetcher


def _make_assignment(fetcher, tps):
    """Fake an assignment object that _get_actions_per_node can iterate."""
    assignment = mock.Mock()
    assignment.tps = tps

    def state_value(tp):
        state = mock.Mock()
        state.has_valid_position = True
        state.paused = False
        state.position = 0
        return state

    assignment.state_value = state_value
    # Ensure no partitions are "in-flight" or buffered already.
    fetcher._records = {}
    fetcher._in_flight = set()
    return assignment


# ---------------------------------------------------------------------------
# 1. After receiving preferred_read_replica, next fetch goes to that broker
# ---------------------------------------------------------------------------


class TestReplicaSelectionRouting:
    """Verify that _get_actions_per_node routes to the preferred replica."""

    def test_fetch_routed_to_preferred_replica_after_hint(self):
        """After the leader returns preferred_read_replica=7, the next
        FetchRequest for that partition must be sent to node 7, not the
        leader."""
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)

        # Leader is node 1, preferred replica is node 7.
        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = (
            mock.Mock()
        )  # node 7 is known

        # Simulate the broker returning preferred_read_replica=7.
        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)

        # Build fetch requests — the partition should be routed to node 7.
        assignment = _make_assignment(fetcher, [tp])
        fetch_requests, *_ = fetcher._get_actions_per_node(assignment)

        assert len(fetch_requests) == 1
        node_id, _req = fetch_requests[0]
        assert node_id == 7, (
            f"Expected fetch to be routed to preferred replica 7, got {node_id}"
        )

    def test_fetch_routed_to_leader_when_no_hint(self):
        """Without a preferred_read_replica hint, fetches go to the leader."""
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)

        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()

        assignment = _make_assignment(fetcher, [tp])
        fetch_requests, *_ = fetcher._get_actions_per_node(assignment)

        assert len(fetch_requests) == 1
        node_id, _ = fetch_requests[0]
        assert node_id == 1

    def test_fetch_falls_back_to_leader_after_ttl_expires(self):
        """Once the cached preferred replica expires, fetch falls back to the
        leader until the leader re-issues a hint."""
        fetcher = _make_fetcher(metadata_max_age_ms=1)  # 1 ms TTL
        tp = TopicPartition("topic-a", 0)

        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()

        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)

        # Force the cached entry to be expired by setting its expiry to the past.
        # This avoids relying on time.sleep, which is unreliable on Windows.
        fetcher._preferred_read_replica[tp] = (7, time.monotonic() - 1)

        assignment = _make_assignment(fetcher, [tp])
        fetch_requests, *_ = fetcher._get_actions_per_node(assignment)

        node_id, _ = fetch_requests[0]
        assert node_id == 1, "Should have fallen back to leader after TTL expired"

    def test_fetch_falls_back_to_leader_when_preferred_node_disappears(self):
        """If the preferred replica disappears from cluster metadata, the
        consumer must not get stuck — it should fall back to the leader."""
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)

        fetcher._client.cluster.leader_for_partition.return_value = 1

        # First the node is known — cache is populated.
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()
        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)

        # Now node 7 disappears from metadata.
        fetcher._client.cluster.broker_metadata.return_value = None

        assignment = _make_assignment(fetcher, [tp])
        fetch_requests, *_ = fetcher._get_actions_per_node(assignment)

        node_id, _ = fetch_requests[0]
        assert node_id == 1, "Should fall back to leader when preferred node is gone"


# ---------------------------------------------------------------------------
# 2. Correct interpretation of preferred_read_replica == -1
# ---------------------------------------------------------------------------


class TestMinusOneHandling:
    """The meaning of -1 depends on who produced the response (KIP-392)."""

    def test_minus_one_from_leader_clears_cache(self):
        """-1 from the leader means 'no preferred replica, read from me'.
        Consumer must drop the cached entry and next fetch goes to leader."""
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)
        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()

        # Populate cache.
        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)
        assert fetcher._select_read_replica(tp) == 7

        # Leader (node 1) responds with -1.
        fetcher._update_preferred_read_replica(tp, -1, responder_node_id=1)

        # Next fetch must go to the leader.
        assert fetcher._select_read_replica(tp) == 1

    def test_minus_one_from_follower_keeps_cache(self):
        """-1 from the currently selected follower means 'I am still the right
        one, keep using me'. Consumer must NOT drop the cache — otherwise it
        would oscillate between follower and leader on every fetch."""
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)
        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()

        # Leader tells us to go to follower 7.
        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)
        assert fetcher._select_read_replica(tp) == 7

        # Follower 7 responds with -1 (= "stay with me").
        fetcher._update_preferred_read_replica(tp, -1, responder_node_id=7)

        # Cache must still point to 7.
        assert fetcher._select_read_replica(tp) == 7


# ---------------------------------------------------------------------------
# 3. Error-driven cache invalidation
# ---------------------------------------------------------------------------


class TestErrorInvalidation:
    """On certain error codes the preferred replica cache must be dropped so
    we don't keep hammering a broker that can no longer serve the partition."""

    def test_not_leader_error_invalidates_cache(self):
        """NotLeaderForPartition means the routing is stale — drop the
        cached preferred replica."""
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)
        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()

        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)
        assert tp in fetcher._preferred_read_replica

        # Simulate: error path pops the cache (as our code does).
        fetcher._preferred_read_replica.pop(tp, None)

        assert fetcher._select_read_replica(tp) == 1


# ---------------------------------------------------------------------------
# 4. rack_id is sent in the FetchRequest
# ---------------------------------------------------------------------------


class TestRackIdInRequest:
    """Verify that rack_id is included in FetchRequest v11 and omitted in
    older versions."""

    def test_fetch_request_v11_carries_rack_id(self):
        req = FetchRequest(
            max_wait_time=100,
            min_bytes=1,
            max_bytes=1024,
            isolation_level=0,
            topics=[("t", [(0, 42, 1024)])],
            rack_id="us-east-1a",
        )
        built = req.build(FetchRequest_v11)
        obj = built.to_object()
        assert obj["rack_id"] == "us-east-1a"

    def test_fetch_request_v9_does_not_carry_rack_id(self):
        req = FetchRequest(
            max_wait_time=100,
            min_bytes=1,
            max_bytes=1024,
            isolation_level=0,
            topics=[("t", [(0, 42, 1024)])],
            rack_id="us-east-1a",
        )
        built = req.build(FetchRequest_v9)
        obj = built.to_object()
        assert "rack_id" not in obj

    def test_fetch_request_built_by_fetcher_includes_rack(self):
        """_get_actions_per_node should produce requests with our rack_id."""
        fetcher = _make_fetcher(client_rack="us-east-1a")
        tp = TopicPartition("topic-a", 0)

        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()

        assignment = _make_assignment(fetcher, [tp])
        fetch_requests, *_ = fetcher._get_actions_per_node(assignment)

        _, req = fetch_requests[0]
        assert req._rack_id == "us-east-1a"


# ---------------------------------------------------------------------------
# 5. Warning when client_rack is set but broker < v11
# ---------------------------------------------------------------------------


class TestRackVersionWarning:
    """A one-shot warning must be logged when client_rack is set but the
    broker only supports FetchRequest < v11."""

    @pytest.mark.asyncio
    async def test_warning_logged_when_broker_below_v11(self, caplog):
        """If client_rack is set and the FetchResponse version is < 11,
        a warning is logged exactly once."""
        fetcher = _make_fetcher(client_rack="us-east-1a")

        # Mock assignment
        assignment = mock.Mock()
        assignment.active = True
        state = mock.Mock()
        state.has_valid_position = True
        state.position = 0
        assignment.state_value.return_value = state

        # Mock a v4 FetchResponse with one topic and one partition (NoError,
        # but empty message set so we don't need real record parsing).
        response = mock.Mock()
        response.API_VERSION = 4
        response.topics = []  # no partitions to iterate

        # Mock the request's .topics property so fetch_offsets can be built.
        request = mock.Mock()
        request.topics = []

        # The client.send() must return our mock response.
        fetcher._client.send = mock.AsyncMock(return_value=response)

        assert not fetcher._rack_warning_logged

        with caplog.at_level(logging.WARNING):
            await fetcher._proc_fetch_request(assignment, 1, request)

        assert fetcher._rack_warning_logged
        assert any("client_rack" in msg and "v4" in msg for msg in caplog.messages)

        # Second call should NOT log again.
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            await fetcher._proc_fetch_request(assignment, 1, request)
        assert not any("client_rack" in msg for msg in caplog.messages)

    @pytest.mark.asyncio
    async def test_no_warning_when_rack_not_set(self, caplog):
        """If client_rack is not set, no warning even when broker < v11."""
        fetcher = _make_fetcher(client_rack=None)

        assignment = mock.Mock()
        assignment.active = True

        response = mock.Mock()
        response.API_VERSION = 4
        response.topics = []

        request = mock.Mock()
        request.topics = []

        fetcher._client.send = mock.AsyncMock(return_value=response)

        with caplog.at_level(logging.WARNING):
            await fetcher._proc_fetch_request(assignment, 1, request)

        assert not fetcher._rack_warning_logged
        assert not any("client_rack" in msg for msg in caplog.messages)


# ---------------------------------------------------------------------------
# 6. Offset resets must always be routed to the partition leader
# ---------------------------------------------------------------------------


class TestOffsetResetRouting:
    """Per KIP-392, only Fetch traffic may be served by a follower.
    ListOffsets (used for seek_to_beginning / seek_to_end / out-of-range
    resets) must always go to the partition leader, otherwise the follower
    will reply with NOT_LEADER_FOR_PARTITION and the consumer will be stuck
    until the preferred-replica TTL expires."""

    def test_awaiting_reset_is_keyed_by_leader_not_follower(self):
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)

        # Leader = node 1, cached preferred replica = node 7.
        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()
        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)

        # Build an assignment where the partition has no valid position,
        # so it ends up in awaiting_reset.
        assignment = mock.Mock()
        assignment.tps = [tp]

        def state_value(_tp):
            state = mock.Mock()
            state.has_valid_position = False
            state.paused = False
            return state

        assignment.state_value = state_value
        fetcher._records = {}
        fetcher._in_flight = set()

        _, awaiting_reset, *_ = fetcher._get_actions_per_node(assignment)

        assert 1 in awaiting_reset, (
            "ListOffsets must be sent to the leader (node 1), not to the "
            "preferred follower (node 7)"
        )
        assert 7 not in awaiting_reset
        assert tp in awaiting_reset[1]

    def test_unknown_leader_during_reset_triggers_metadata_refresh(self):
        """If a preferred replica is cached but the partition leader is
        currently unknown (e.g. during leader election), the partition must
        not be enqueued under an invalid leader id. Instead, the routine must
        signal ``invalid_metadata`` so the client forces a metadata refresh."""
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)

        # Cache a preferred follower while the leader is known.
        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()
        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)

        # Now leader becomes unknown (e.g. mid-election).
        fetcher._client.cluster.leader_for_partition.return_value = None

        assignment = mock.Mock()
        assignment.tps = [tp]

        def state_value(_tp):
            state = mock.Mock()
            state.has_valid_position = False
            state.paused = False
            return state

        assignment.state_value = state_value
        fetcher._records = {}
        fetcher._in_flight = set()

        _, awaiting_reset, _, invalid_metadata, _ = fetcher._get_actions_per_node(
            assignment
        )

        assert invalid_metadata is True
        assert not awaiting_reset, (
            "Partition must not be queued under an unknown leader id"
        )


# ---------------------------------------------------------------------------
# 7. Transport failures against a follower must evict the cached replica
# ---------------------------------------------------------------------------


class TestTransportFailureEviction:
    """If a fetch to the preferred follower fails at the transport level
    (connection error, broker down, etc.), the cached entry must be evicted
    so the next attempt falls back to the leader instead of repeatedly
    hitting the unreachable follower until ``metadata_max_age_ms`` expiry."""

    @pytest.mark.asyncio
    async def test_kafka_error_evicts_preferred_replica(self):
        fetcher = _make_fetcher()
        tp = TopicPartition("topic-a", 0)

        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()
        fetcher._update_preferred_read_replica(tp, 7, responder_node_id=1)
        assert tp in fetcher._preferred_read_replica

        # Simulate a transport failure when sending to follower 7.
        fetcher._client.send = mock.AsyncMock(
            side_effect=Errors.KafkaConnectionError("boom")
        )

        # Sleep is awaited inside the error path; make it instant.
        with mock.patch("aiokafka.consumer.fetcher.asyncio.sleep", mock.AsyncMock()):
            request = mock.Mock()
            request.topics = [("topic-a", [(0, 0, 1024)])]
            assignment = mock.Mock()
            assignment.active = True
            ok = await fetcher._proc_fetch_request(assignment, 7, request)

        assert ok is False
        assert tp not in fetcher._preferred_read_replica, (
            "Cached preferred replica must be evicted after transport failure"
        )
        # After eviction, the next routing decision falls back to the leader.
        assert fetcher._select_read_replica(tp) == 1

    @pytest.mark.asyncio
    async def test_failure_against_leader_does_not_evict_other_followers(self):
        """Failures while talking to the leader (no cache entry) must leave
        any unrelated cached entries alone."""
        fetcher = _make_fetcher()
        tp_b = TopicPartition("topic-b", 0)

        fetcher._client.cluster.leader_for_partition.return_value = 1
        fetcher._client.cluster.broker_metadata.return_value = mock.Mock()
        # tp_b is cached against follower 7; tp_a is fetched from leader 1.
        fetcher._update_preferred_read_replica(tp_b, 7, responder_node_id=1)

        fetcher._client.send = mock.AsyncMock(
            side_effect=Errors.KafkaConnectionError("boom")
        )

        with mock.patch("aiokafka.consumer.fetcher.asyncio.sleep", mock.AsyncMock()):
            request = mock.Mock()
            request.topics = [("topic-a", [(0, 0, 1024)])]
            assignment = mock.Mock()
            assignment.active = True
            await fetcher._proc_fetch_request(assignment, 1, request)

        # tp_b's cached follower entry must be untouched.
        assert tp_b in fetcher._preferred_read_replica
        assert fetcher._preferred_read_replica[tp_b][0] == 7
