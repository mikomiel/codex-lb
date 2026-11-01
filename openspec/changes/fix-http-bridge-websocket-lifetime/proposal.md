# Fix HTTP bridge upstream WebSocket lifetime

## Why

[Upstream issue #2658](https://github.com/Soju06/codex-lb/issues/2658)
reports `websocket_connection_limit_reached` on reused HTTP Responses bridges.
Idle eviction does not bound total socket age: regularly used bridges can
outlive the upstream 60-minute connection cap.

## What changes

- Track the monotonic opening time of each upstream bridge socket.
- Renew at 55 minutes between safe, complete-context turns on the same account.
- Recover one explicit pre-response lifetime refusal on a new same-owner socket.
- Keep existing response, output, tool, context, ownership and retry fences.
- Drain an expired socket when a request cannot safely be replayed.

## Scope

HTTP-to-WebSocket Responses bridging only. No database migration, new setting,
transport-policy change, native client WebSocket replay change or release action.
Idle TTLs keep their existing meaning. A client continuation without a verified
complete body cannot be reconstructed and is not silently restarted.

## Verification

Synthetic HTTP regressions reproduce the original 400 and unbounded socket
reuse. Unit tests cover renewal boundaries, concurrent renewal, owner retention,
proxy-added versus client anchors, accepted/output/tool fences, retry bounds and
connect failure. Existing HTTP bridge, replay, lease and integration tests remain
acceptance checks. No provider account or production data is needed.
