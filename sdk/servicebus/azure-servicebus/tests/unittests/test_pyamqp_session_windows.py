# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from azure.servicebus._pyamqp._encode import encode_uint, encode_ulong
from azure.servicebus._pyamqp.constants import SessionState, SessionTransferState
from azure.servicebus._pyamqp.session import Session
from azure.servicebus._pyamqp.aio._session_async import Session as AsyncSession


def _session_kwargs():
    return {
        "network_trace": False,
        "network_trace_params": {},
        "incoming_window": 1,
        "outgoing_window": 1,
    }


def _delivery():
    return SimpleNamespace(
        frame={
            "payload": b"x",
            "handle": 0,
            "delivery_tag": b"tag",
            "message_format": 0,
            "settled": False,
            "more": False,
            "rcv_settle_mode": None,
            "state": None,
            "resume": False,
            "aborted": False,
            "batchable": False,
        },
        transfer_state=None,
    )


@pytest.mark.parametrize("encoder", [encode_uint, encode_ulong])
@pytest.mark.parametrize("value", [-1, -255, -256])
def test_unsigned_encoders_reject_negative_values(encoder, value):
    with pytest.raises(ValueError):
        encoder(bytearray(), value)


def test_outgoing_window_does_not_drift_below_zero():
    connection = MagicMock()
    connection._remote_max_frame_size = 1024
    session = Session(connection, 0, **_session_kwargs())
    session.state = SessionState.MAPPED
    session.remote_incoming_window = 2

    first = _delivery()
    second = _delivery()
    session._outgoing_transfer(first, None)
    session._outgoing_transfer(second, None)

    assert first.transfer_state == SessionTransferState.OKAY
    assert second.transfer_state == SessionTransferState.OKAY
    assert session.outgoing_window == 1
    assert session.remote_incoming_window == 0


def test_incoming_window_is_restored_if_link_processing_raises():
    connection = MagicMock()
    session = Session(connection, 0, **_session_kwargs())
    session.state = SessionState.MAPPED
    session.next_incoming_id = 0
    session.remote_outgoing_window = 1
    link = MagicMock()
    link._incoming_transfer.side_effect = RuntimeError("link failure")
    session._input_handles[0] = link

    with pytest.raises(RuntimeError, match="link failure"):
        session._incoming_transfer([0])

    assert session.incoming_window == session.target_incoming_window
    flow_frame = connection._process_outgoing_frame.call_args.args[1]
    assert flow_frame.incoming_window == session.target_incoming_window
    assert flow_frame.outgoing_window >= 0


@pytest.mark.asyncio
async def test_async_incoming_window_is_restored_if_link_processing_raises():
    connection = MagicMock()
    connection._process_outgoing_frame = AsyncMock()
    session = AsyncSession(connection, 0, **_session_kwargs())
    session.state = SessionState.MAPPED
    session.next_incoming_id = 0
    session.remote_outgoing_window = 1
    link = MagicMock()
    link._incoming_transfer = AsyncMock(side_effect=RuntimeError("link failure"))
    session._input_handles[0] = link

    with pytest.raises(RuntimeError, match="link failure"):
        await session._incoming_transfer([0])

    assert session.incoming_window == session.target_incoming_window
    flow_frame = connection._process_outgoing_frame.call_args.args[1]
    assert flow_frame.incoming_window == session.target_incoming_window
    assert flow_frame.outgoing_window >= 0
