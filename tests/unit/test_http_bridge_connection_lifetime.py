from __future__ import annotations

import asyncio
import json
from collections import deque
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import anyio
import pytest

from app.core.clients.proxy import ProxyResponseError
from app.core.config.settings import Settings
from app.core.errors import openai_error
from app.db.models import AccountStatus
from app.modules.proxy import service as proxy_service
from app.modules.proxy._service.http_bridge.helpers import _http_bridge_request_can_renew_upstream
from tests.simulation.virtual_time import VirtualClock

pytestmark = pytest.mark.unit
_LIMIT = "websocket_connection_limit_reached"


@pytest.fixture
def bridge(monkeypatch):
    clock = VirtualClock(monotonic_value=4000.0)
    service = proxy_service.ProxyService(cast(Any, nullcontext()), clock=clock)
    account = SimpleNamespace(id="acc-lifetime", status=AccountStatus.ACTIVE, plan_type="plus")
    request = proxy_service._WebSocketRequestState(
        request_id="req-lifetime",
        model="gpt-5.1",
        service_tier=None,
        reasoning_effort=None,
        api_key_reservation=None,
        started_at=clock.monotonic(),
        awaiting_response_created=True,
        request_text='{"type":"response.create","model":"gpt-5.1","input":"complete context"}',
        transport="http",
        event_queue=asyncio.Queue(),
        skip_request_log=True,
    )
    upstream = SimpleNamespace(close=AsyncMock(), send_text=AsyncMock(), response_header=lambda name: None)
    replacement = SimpleNamespace(close=AsyncMock(), send_text=AsyncMock(), response_header=lambda name: None)
    session = proxy_service._HTTPBridgeSession(
        key=proxy_service._HTTPBridgeSessionKey("prompt_cache", "lifetime-key", None),
        headers={},
        affinity=proxy_service._AffinityPolicy(key="lifetime-key", kind=proxy_service.StickySessionKind.PROMPT_CACHE),
        request_model=request.model,
        account=cast(Any, account),
        upstream=cast(Any, upstream),
        upstream_control=proxy_service._WebSocketUpstreamControl(opened_at=100.0),
        pending_requests=deque(),
        pending_lock=anyio.Lock(),
        response_create_gate=asyncio.Semaphore(1),
        queued_request_count=0,
        last_used_at=clock.monotonic(),
        idle_ttl_seconds=3600.0,
    )
    service._http_bridge_sessions[session.key] = session
    select = AsyncMock(return_value=proxy_service.AccountSelection(account=cast(Any, account), error_message=None))
    monkeypatch.setattr(service, "_select_account_with_budget_for_stream", select)
    monkeypatch.setattr(service, "_ensure_fresh_with_budget", AsyncMock(return_value=account))
    monkeypatch.setattr(service, "_open_upstream_websocket_with_budget", AsyncMock(return_value=replacement))
    monkeypatch.setattr(service, "_spawn_http_bridge_upstream_reader", Mock(return_value=None))
    monkeypatch.setattr(service, "_finalize_websocket_request_state", AsyncMock())
    monkeypatch.setattr(service, "_handle_stream_error", AsyncMock())
    monkeypatch.setattr(service, "_acquire_account_response_create_lease_or_overload", AsyncMock(return_value=object()))
    monkeypatch.setattr(proxy_service, "get_settings", lambda: Settings())
    monkeypatch.setattr(
        proxy_service,
        "get_settings_cache",
        lambda: SimpleNamespace(
            get=AsyncMock(
                return_value=SimpleNamespace(
                    prefer_earlier_reset_accounts=False,
                    prefer_earlier_reset_window="secondary",
                    routing_strategy="usage_weighted",
                )
            )
        ),
    )
    return SimpleNamespace(
        service=service,
        session=session,
        request=request,
        upstream=upstream,
        replacement=replacement,
        select=select,
        clock=clock,
    )


def _limit_event(event_type="error", **extra):
    error = {"code": _LIMIT, "type": "invalid_request_error", "message": "Create a new websocket connection"}
    if event_type == "response.failed":
        return json.dumps({"type": event_type, "response": {"status": "failed", "error": error}, **extra})
    return json.dumps({"type": "error", "error": error, **extra})


@pytest.mark.asyncio
async def test_renewal_keeps_owner_context_and_does_not_consume_replay(bridge):
    b = bridge
    b.session.upstream_turn_state = "turn-owner"
    b.session.last_completed_response_id = "resp-owner"
    b.session.last_completed_response_account_id = b.session.account.id
    b.request.previous_response_id = "resp-owner"
    b.request.proxy_injected_previous_response_id = True
    b.request.hard_continuity_anchor = True
    b.request.fresh_upstream_request_text = b.request.request_text
    b.request.fresh_upstream_request_is_retry_safe = True
    original = b.request.request_text
    text = await b.service._maybe_renew_http_bridge_upstream(
        b.session, request_state=b.request, text_data='{"previous_response_id":"resp-owner","input":"delta"}'
    )
    assert text == original
    assert b.request.previous_response_id is None
    assert b.request.proxy_injected_previous_response_id is False
    assert b.request.preferred_account_id == b.session.account.id == "acc-lifetime"
    assert b.request.replay_count == 0
    assert b.session.upstream is b.replacement
    assert b.session.upstream_control.opened_at == 4000.0
    assert b.session.upstream_turn_state == "turn-owner"
    assert b.session.last_completed_response_id == "resp-owner"
    assert b.session.last_completed_response_account_id == "acc-lifetime"
    assert b.service._http_bridge_sessions[b.session.key] is b.session
    b.upstream.close.assert_awaited_once()
    b.replacement.send_text.assert_not_awaited()
    assert b.select.await_args.kwargs["preferred_account_id"] == "acc-lifetime"
    assert b.select.await_args.kwargs["fallback_on_preferred_account_unavailable"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason", ["pending", "client_anchor", "hard_anchor", "retiring", "closed", "young", "unknown"]
)
async def test_renewal_never_interrupts_work_or_discards_missing_context(bridge, reason):
    b = bridge
    if reason == "pending":
        b.session.pending_requests.append(b.request)
    elif reason == "client_anchor":
        b.request.previous_response_id = "resp-client-owned"
        # Even a retained body does not authorize dropping a client anchor.
        b.request.fresh_upstream_request_text = b.request.request_text
        b.request.fresh_upstream_request_is_retry_safe = True
    elif reason == "hard_anchor":
        b.request.hard_continuity_anchor = True
    elif reason == "retiring":
        b.session.upstream_control.retire_after_drain = True
    elif reason == "closed":
        b.session.closed = True
    elif reason == "young":
        b.session.upstream_control.opened_at = 701.0
    else:
        b.session.upstream_control.opened_at = None
    original = b.request.request_text
    result = await b.service._maybe_renew_http_bridge_upstream(b.session, request_state=b.request, text_data=original)
    assert result == original
    assert b.session.upstream is b.upstream
    b.upstream.close.assert_not_awaited()
    b.select.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_renewal_swaps_one_transport(bridge):
    b = bridge
    await asyncio.gather(
        *(
            b.service._maybe_renew_http_bridge_upstream(
                b.session, request_state=b.request, text_data=b.request.request_text
            )
            for _ in range(2)
        )
    )
    assert b.select.await_count == 1
    b.upstream.close.assert_awaited_once()
    assert b.session.upstream is b.replacement


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fence",
    [
        "created",
        "output",
        "reasoning",
        "sequence",
        "tool",
        "replayed",
        "client_anchor",
        "hard_anchor",
        "sibling",
        "event_id",
    ],
)
async def test_connection_limit_never_replays_an_unsafe_turn(bridge, fence):
    b = bridge
    if fence == "created":
        b.request.response_id = "resp-accepted"
        b.request.awaiting_response_created = False
        b.request.response_event_count = 1
    elif fence == "output":
        b.request.downstream_visible = True
    elif fence == "reasoning":
        b.request.upstream_model_output_seen = True
        b.request.deferred_reasoning_downstream_texts.append("reasoning already observed")
    elif fence == "sequence":
        b.request.last_downstream_sequence_number = 0
    elif fence == "tool":
        b.request.pending_function_call_ids.append("call-existing")
    elif fence == "replayed":
        b.request.replay_count = 1
    elif fence == "client_anchor":
        b.request.previous_response_id = "resp-client-owned"
    elif fence == "hard_anchor":
        b.request.hard_continuity_anchor = True
    b.session.pending_requests.append(b.request)
    b.session.queued_request_count = 1
    if fence == "sibling":
        from dataclasses import replace

        sibling = replace(
            b.request, request_id="req-sibling", response_id="resp-sibling", awaiting_response_created=False
        )
        b.session.pending_requests.append(sibling)
        b.session.queued_request_count += 1
    event = _limit_event(response_id="resp-accepted") if fence == "event_id" else _limit_event()
    await b.service._process_http_bridge_upstream_text(b.session, event)
    b.replacement.send_text.assert_not_awaited()
    b.select.assert_not_awaited()
    assert b.session.upstream_control.retire_after_drain is True
    assert b.request.event_queue is not None
    delivered = []
    while not b.request.event_queue.empty():
        delivered.append(b.request.event_queue.get_nowait())
    assert any(isinstance(block, str) and _LIMIT in block for block in delivered)
    if fence == "reasoning":
        assert delivered[0] == "reasoning already observed"


@pytest.mark.asyncio
@pytest.mark.parametrize("key_kind", ["prompt_cache", "session_header", "turn_state_header"])
@pytest.mark.parametrize("event_type", ["error", "response.failed"])
@pytest.mark.parametrize("body_kind", ["neutral", "file", "encrypted"])
async def test_connection_limit_replays_verified_proxy_body_on_same_owner(bridge, key_kind, event_type, body_kind):
    b = bridge
    b.service._http_bridge_sessions.pop(b.session.key)
    b.session.key = proxy_service._HTTPBridgeSessionKey(key_kind, "lifetime-key", None)
    b.service._http_bridge_sessions[b.session.key] = b.session
    if key_kind != "prompt_cache":
        b.session.affinity = proxy_service._AffinityPolicy(
            key="lifetime-key", kind=proxy_service.StickySessionKind.CODEX_SESSION
        )
    if body_kind == "file":
        complete_input = [{"role": "user", "content": [{"type": "input_file", "file_id": "file-retained-owner"}]}]
    elif body_kind == "encrypted":
        complete_input = [{"type": "reasoning", "encrypted_content": "opaque-retained-context"}]
    else:
        complete_input = "complete context"
    original = json.dumps({"type": "response.create", "model": "gpt-5.1", "input": complete_input})
    b.request.previous_response_id = "resp-proxy-injected"
    b.request.proxy_injected_previous_response_id = True
    b.request.hard_continuity_anchor = True
    b.request.fresh_upstream_request_text = original
    b.request.fresh_upstream_request_is_retry_safe = True
    b.request.request_text = '{"type":"response.create","previous_response_id":"resp-proxy-injected","input":"delta"}'
    b.session.pending_requests.append(b.request)
    b.session.queued_request_count = 1
    await b.service._process_http_bridge_upstream_text(b.session, _limit_event(event_type=event_type))
    assert b.request.event_queue.empty()
    assert b.session.pending_requests == deque([b.request])
    assert b.session.queued_request_count == 1
    assert b.request.previous_response_id is None
    assert b.request.replay_count == 1
    assert b.request.preferred_account_id == "acc-lifetime"
    assert not b.request.excluded_account_ids
    assert b.session.upstream_control.opened_at == 4000.0
    b.replacement.send_text.assert_awaited_once()
    sent = json.loads(b.replacement.send_text.await_args.args[0])
    assert "previous_response_id" not in sent
    assert sent["input"] == complete_input
    assert b.select.await_args.kwargs["fallback_on_preferred_account_unavailable"] is False
    b.service._handle_stream_error.assert_not_awaited()
    # A clean close on the replacement may not buy a third send either.
    b.session.last_upstream_close_code = 1000
    assert await b.service._retry_http_bridge_precreated_request(b.session, request_state=b.request) is False
    assert b.replacement.send_text.await_count == 1


@pytest.mark.asyncio
async def test_connection_limit_keeps_account_scoped_file_on_its_owner(bridge):
    b = bridge
    b.request.request_text = json.dumps(
        {
            "type": "response.create",
            "model": "gpt-5.1",
            "input": [{"role": "user", "content": [{"type": "input_file", "file_id": "file-owner-bound"}]}],
        }
    )
    b.request.preferred_account_id = "acc-lifetime"
    b.request.file_required_preferred_account = True
    b.session.pending_requests.append(b.request)
    b.session.queued_request_count = 1
    await b.service._process_http_bridge_upstream_text(b.session, _limit_event())
    assert b.request.replay_count == 1
    assert b.request.preferred_account_id == "acc-lifetime"
    assert not b.request.excluded_account_ids
    assert b.select.await_args.kwargs["preferred_account_id"] == "acc-lifetime"
    assert b.select.await_args.kwargs["fallback_on_preferred_account_unavailable"] is False
    sent = json.loads(b.replacement.send_text.await_args.args[0])
    assert sent["input"][0]["content"][0]["file_id"] == "file-owner-bound"


@pytest.mark.asyncio
async def test_proactive_reconnect_failure_never_sends_or_drops_client_context(bridge, monkeypatch):
    b = bridge
    monkeypatch.setattr(
        b.service,
        "_open_upstream_websocket_with_budget",
        AsyncMock(
            side_effect=ProxyResponseError(502, openai_error("upstream_unavailable", "synthetic connect failure"))
        ),
    )
    with pytest.raises(ProxyResponseError):
        await b.service._maybe_renew_http_bridge_upstream(
            b.session, request_state=b.request, text_data=b.request.request_text
        )
    b.upstream.send_text.assert_not_awaited()
    b.replacement.send_text.assert_not_awaited()
    assert b.session.closed
    assert b.request.request_text.endswith('"complete context"}')
    assert not b.service._http_bridge_inflight_sessions


@pytest.mark.asyncio
async def test_renewal_does_not_cancel_a_popped_terminal_delivery(bridge, monkeypatch):
    from dataclasses import replace

    b = bridge
    next_request = replace(b.request, request_id="req-next-turn", event_queue=asyncio.Queue())
    b.request.response_id = "resp-finishing"
    b.request.awaiting_response_created = False
    b.request.response_event_count = 1
    b.session.pending_requests.append(b.request)
    b.session.queued_request_count = 1
    registering = asyncio.Event()
    release_registration = asyncio.Event()
    original_register = b.service._register_http_bridge_previous_response_id

    async def blocked_register(*args, **kwargs):
        registering.set()
        await release_registration.wait()
        return await original_register(*args, **kwargs)

    monkeypatch.setattr(b.service, "_register_http_bridge_previous_response_id", blocked_register)
    terminal = json.dumps(
        {
            "type": "response.completed",
            "response": {
                "id": "resp-finishing",
                "object": "response",
                "status": "completed",
                "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 0, "total_tokens": 1},
            },
        }
    )
    reader = asyncio.create_task(b.service._process_http_bridge_upstream_text(b.session, terminal))
    b.session.upstream_reader = reader
    try:
        await asyncio.wait_for(registering.wait(), timeout=5)
        assert not b.session.pending_requests  # the deceptive idle window
        await b.service._maybe_renew_http_bridge_upstream(
            b.session, request_state=next_request, text_data=next_request.request_text
        )
        release_registration.set()
        await asyncio.gather(reader, return_exceptions=True)
        delivered = []
        while not b.request.event_queue.empty():
            delivered.append(b.request.event_queue.get_nowait())
        assert any(isinstance(block, str) and '"response.completed"' in block for block in delivered)
        assert not reader.cancelled()
        assert b.session.last_completed_response_id == "resp-finishing"
        b.select.assert_not_awaited()
        assert b.session.upstream is b.upstream
        # Completion releases the busy fence; the next safe turn can renew.
        await b.service._maybe_renew_http_bridge_upstream(
            b.session, request_state=next_request, text_data=next_request.request_text
        )
        assert b.session.upstream is b.replacement
    finally:
        release_registration.set()
        if not reader.done():
            reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


def test_renewal_requires_original_request_text(bridge):
    bridge.request.request_text = None
    assert _http_bridge_request_can_renew_upstream(bridge.request) is False
