import asyncio
import pytest
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.client import AIOKafkaClient
from aiokafka.structs import TopicPartition
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.protocol.fetch import FetchRequest_v10, FetchResponse_v10

@pytest.mark.asyncio
async def test_custom_fetch_v10_format():
    client = AIOKafkaClient(bootstrap_servers=[])
    subs   = SubscriptionState()
    fetcher = Fetcher(client, subs)

    tp = TopicPartition("test-topic", 0)
    subs.assign_from_user({tp})
    assignment = subs.subscription.assignment
    assignment.state_value(tp).seek(0)

    fetch_request = FetchRequest_v10(
        replica_id=-1,
        max_wait_time=100,
        min_bytes=100,
        max_bytes=1048576,
        isolation_level=0,
        session_id=0,
        session_epoch=-1,
        topics=[("test-topic", [(0, -1, 0, 0, 100000)])],   # (part, leader_epoch, off, log_start, max_bytes)
        forgotten_topics_data=[],
        rack_id="",
    )

    async def mock_send(node, request):
        return FetchResponse_v10(
            throttle_time_ms=0,
            error_code=0,
            session_id=0,
            responses=[("test-topic", [(0, 0, 0, 0, 0, [], b"")])],
        )

    client.send = mock_send
    fetcher._in_flight.add(0)

    needs_wakeup = await fetcher._proc_fetch_request(assignment, 0, fetch_request)
    assert needs_wakeup is False