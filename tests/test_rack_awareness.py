"""Unit tests for rack awareness (KIP-392) plumbing in aiokafka.

These tests do NOT require a running Kafka broker -- they exercise the
protocol-level builder for FetchRequest and the in-memory bookkeeping for
the preferred read replica cache in :class:`aiokafka.consumer.fetcher.Fetcher`.
"""

from __future__ import annotations

import time
from unittest import mock

import pytest

from aiokafka.protocol.fetch import (
    FetchRequest,
    FetchRequest_v0,
    FetchRequest_v4,
    FetchRequest_v5,
    FetchRequest_v9,
    FetchRequest_v11,
)
from aiokafka.structs import TopicPartition


# ---------------------------------------------------------------------------
# Protocol layer
# ---------------------------------------------------------------------------


def test_fetch_request_v11_includes_rack_id():
    req = FetchRequest(
        max_wait_time=100,
        min_bytes=1,
        max_bytes=1024,
        isolation_level=0,
        topics=[("t", [(0, 42, 1024)])],
        rack_id="nl",
    )
    built = req.build(FetchRequest_v11)
    assert isinstance(built, FetchRequest_v11)
    obj = built.to_object()
    assert obj["rack_id"] == "nl"
    # session_id / session_epoch defaults
    assert obj["session_id"] == 0
    assert obj["session_epoch"] == -1
    # forgotten topics is sent as an empty list
    assert obj["forgotten_topics_data"] == []
    # current_leader_epoch and log_start_offset are filled with -1 sentinels
    [topic] = obj["topics"]
    assert topic["topic"] == "t"
    [partition] = topic["partitions"]
    assert partition["partition"] == 0
    assert partition["fetch_offset"] == 42
    assert partition["log_start_offset"] == -1
    assert partition["current_leader_epoch"] == -1
    assert partition["max_bytes"] == 1024


def test_fetch_request_v9_does_not_send_rack_id():
    req = FetchRequest(
        max_wait_time=100,
        min_bytes=1,
        max_bytes=1024,
        isolation_level=0,
        topics=[("t", [(0, 42, 1024)])],
        rack_id="nl",
    )
    built = req.build(FetchRequest_v9)
    obj = built.to_object()
    # v9 has no rack_id field; ensure we still produced a valid struct
    assert "rack_id" not in obj
    [topic] = obj["topics"]
    [partition] = topic["partitions"]
    assert partition["current_leader_epoch"] == -1
    assert partition["log_start_offset"] == -1


def test_fetch_request_v5_log_start_offset_no_leader_epoch():
    req = FetchRequest(
        max_wait_time=100,
        min_bytes=1,
        max_bytes=1024,
        isolation_level=0,
        topics=[("t", [(7, 9, 32)])],
    )
    built = req.build(FetchRequest_v5)
    obj = built.to_object()
    [topic] = obj["topics"]
    [partition] = topic["partitions"]
    # v5 has log_start_offset but NOT current_leader_epoch
    assert partition["partition"] == 7
    assert partition["fetch_offset"] == 9
    assert partition["log_start_offset"] == -1
    assert partition["max_bytes"] == 32
    assert "current_leader_epoch" not in partition


def test_fetch_request_v4_keeps_isolation_level():
    req = FetchRequest(
        max_wait_time=100,
        min_bytes=1,
        max_bytes=1024,
        isolation_level=1,
        topics=[("t", [(0, 0, 1)])],
        rack_id="ignored",
    )
    built = req.build(FetchRequest_v4)
    obj = built.to_object()
    assert obj["isolation_level"] == 1


def test_fetch_request_v0_rejects_isolation_level():
    req = FetchRequest(
        max_wait_time=100,
        min_bytes=1,
        max_bytes=1024,
        isolation_level=1,
        topics=[("t", [(0, 0, 1)])],
    )
    from aiokafka.errors import IncompatibleBrokerVersion

    with pytest.raises(IncompatibleBrokerVersion):
        req.build(FetchRequest_v0)


def test_fetch_request_prepare_picks_v11_when_available():
    """The version negotiation should pick v11 when the broker supports it."""
    req = FetchRequest(
        max_wait_time=100,
        min_bytes=1,
        max_bytes=1024,
        isolation_level=0,
        topics=[("t", [(0, 0, 1)])],
        rack_id="nl",
    )
    built = req.prepare({1: (0, 11)})
    assert built.API_VERSION == 11
    assert built.to_object()["rack_id"] == "nl"


# ---------------------------------------------------------------------------
# Fetcher: preferred read replica cache (KIP-392)
# ---------------------------------------------------------------------------


def _make_fetcher(client_rack: str = "nl"):
    """Build a Fetcher without starting its background task."""
    from aiokafka.consumer.fetcher import Fetcher

    client = mock.Mock()
    client._loop = mock.Mock()
    client.cluster = mock.Mock()
    subscriptions = mock.Mock()
    subscriptions.register_fetch_waiters = mock.Mock()

    # Avoid creating the background asyncio task -- there's no running loop
    # in these tests and we don't need it.
    with (
        mock.patch("aiokafka.consumer.fetcher.create_task"),
        mock.patch.object(
            Fetcher, "_fetch_requests_routine", lambda self: None
        ),
    ):
        fetcher = Fetcher(
            client,
            subscriptions,
            client_rack=client_rack,
            metadata_max_age_ms=60_000,
        )
    return fetcher


def test_select_read_replica_uses_leader_when_no_preferred():
    fetcher = _make_fetcher()
    tp = TopicPartition("t", 0)
    fetcher._client.cluster.leader_for_partition.return_value = 1
    assert fetcher._select_read_replica(tp) == 1


def test_select_read_replica_uses_cached_preferred_replica():
    fetcher = _make_fetcher()
    tp = TopicPartition("t", 0)
    fetcher._client.cluster.leader_for_partition.return_value = 1
    fetcher._client.cluster.broker_metadata.return_value = mock.Mock()  # known
    fetcher._update_preferred_read_replica(tp, 7)
    assert fetcher._select_read_replica(tp) == 7


def test_select_read_replica_falls_back_when_node_unknown():
    fetcher = _make_fetcher()
    tp = TopicPartition("t", 0)
    fetcher._client.cluster.leader_for_partition.return_value = 1
    fetcher._client.cluster.broker_metadata.return_value = None  # unknown node
    fetcher._update_preferred_read_replica(tp, 7)
    assert fetcher._select_read_replica(tp) == 1
    # Stale entry should have been evicted
    assert tp not in fetcher._preferred_read_replica


def test_select_read_replica_expires_after_ttl():
    fetcher = _make_fetcher()
    tp = TopicPartition("t", 0)
    fetcher._preferred_replica_ttl = 0.0  # immediately expired
    fetcher._client.cluster.leader_for_partition.return_value = 1
    fetcher._client.cluster.broker_metadata.return_value = mock.Mock()
    fetcher._update_preferred_read_replica(tp, 7)
    # Sleep a hair to ensure monotonic clock advances past expiry
    time.sleep(0.001)
    assert fetcher._select_read_replica(tp) == 1
    assert tp not in fetcher._preferred_read_replica


def test_update_preferred_read_replica_with_minus_one_clears_cache():
    fetcher = _make_fetcher()
    tp = TopicPartition("t", 0)
    fetcher._client.cluster.broker_metadata.return_value = mock.Mock()
    fetcher._update_preferred_read_replica(tp, 7)
    assert tp in fetcher._preferred_read_replica
    # Broker tells us to go back to the leader
    fetcher._update_preferred_read_replica(tp, -1)
    assert tp not in fetcher._preferred_read_replica


def test_default_client_rack_is_empty_string():
    fetcher = _make_fetcher(client_rack="")
    assert fetcher._client_rack == ""
