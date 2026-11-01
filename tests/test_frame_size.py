import asyncio
import struct
from contextlib import suppress
from unittest import mock

import pytest

from aiokafka.conn import AIOKafkaConnection
from aiokafka.errors import InvalidReceiveError, KafkaConnectionError
from aiokafka.protocol.metadata import MetadataRequest

FRAME_SIZE_LIMIT = 1000


@pytest.fixture
async def connected():
    connection = AIOKafkaConnection(
        "localhost", 9092, receive_message_max_bytes=FRAME_SIZE_LIMIT
    )
    connection._reader = asyncio.StreamReader()
    connection._reader.readexactly = mock.AsyncMock(
        wraps=connection._reader.readexactly
    )
    connection._writer = mock.Mock()
    read_task = connection._read_task = connection._create_reader_task()
    try:
        yield connection
    finally:
        connection.close()
        # Let cancellation finish so teardown leaves no pending reader task.
        with suppress(asyncio.CancelledError, InvalidReceiveError):
            await read_task


@pytest.mark.parametrize("size", [-1, FRAME_SIZE_LIMIT + 1])
async def test_invalid_frame_closes_connection_and_fails_request(connected, size):
    connected._versions = {MetadataRequest.API_KEY: (0, 0)}
    reader = connected._reader
    writer = connected._writer
    response = connected.send(MetadataRequest([]))

    # The header alone must be sufficient to reject the frame.
    reader.feed_data(struct.pack(">i", size))

    with pytest.raises(KafkaConnectionError) as error:
        await asyncio.wait_for(response, timeout=1)

    assert isinstance(error.value.__cause__, InvalidReceiveError)
    reader.readexactly.assert_awaited_once_with(4)
    writer.close.assert_called_once_with()
    assert not connected.connected()


@pytest.mark.parametrize(
    "size",
    [
        0,  # Empty legacy SASL responses must remain valid.
        FRAME_SIZE_LIMIT,  # A frame exactly at the configured limit must be accepted.
    ],
)
async def test_frame_size_boundary_is_accepted(connected, size):
    response = connected._send_sasl_token(b"token")
    payload = b"x" * size
    connected._reader.feed_data(struct.pack(">i", size) + payload)

    assert await asyncio.wait_for(response, timeout=1) == payload
    assert connected.connected()
