## ADDED Requirements

### Requirement: HTTP bridge sockets have an independent maximum reuse age

The HTTP bridge MUST record the monotonic opening time of each upstream socket, including replacements. At 3300 seconds it MUST renew before sending an unsent complete-context turn when no response or reader bookkeeping is pending. Renewal MUST keep the account and durable bridge identity, MUST NOT consume the request replay allowance, and MUST NOT interrupt another response. Idle activity MUST NOT reset socket age.

#### Scenario: Continuous reuse does not extend socket lifetime
- **GIVEN** a bridge socket is 3299 seconds old and was used recently
- **WHEN** a complete standalone turn is submitted
- **THEN** it reuses the socket
- **WHEN** another such turn is submitted at socket age 3300 seconds
- **THEN** it opens a new same-account socket before sending
- **AND** subsequent age checks use the replacement socket's opening time

#### Scenario: Renewal preserves complete proxy-owned context
- **GIVEN** the proxy added a previous-response anchor and retained a verified complete request body
- **WHEN** the old socket is renewed between turns
- **THEN** the complete body replaces the proxy-added anchor
- **AND** the durable session, account, previous-response ownership and turn-state ownership remain intact

#### Scenario: Pending work is never interrupted for age renewal
- **GIVEN** another response is pending or its popped terminal is still being registered or delivered
- **WHEN** a new turn is submitted after the renewal threshold
- **THEN** the bridge does not cancel the reader or close the socket for proactive renewal
- **AND** the completed response and its continuity registration are delivered normally

### Requirement: HTTP bridge lifetime refusals have a bounded same-owner recovery

An explicit `websocket_connection_limit_reached` error before any response acceptance or output MAY retry once on a fresh same-account socket. The request MUST be the only pending turn, have no response identity in the error, and carry complete context. The bridge MUST NOT penalize or exclude the account for this transport lifetime. A failed or refused replay MUST NOT permit an additional clean-close replay.

#### Scenario: The first unaccepted turn encounters the upstream cap
- **GIVEN** upstream returns the lifetime error without any response events or response identity
- **WHEN** the sole request has complete context and its retry is unused
- **THEN** the bridge reconnects to the same account and sends the request once
- **AND** a successful replacement produces the normal JSON or SSE response without exposing the first refusal

#### Scenario: The replacement also refuses the request
- **GIVEN** the request was already retried after a lifetime refusal
- **WHEN** the replacement socket returns the error or closes cleanly
- **THEN** the request terminates without a third send

### Requirement: Lifetime recovery preserves context and output fences

Lifetime renewal or recovery MUST NOT discard a client-supplied previous-response anchor or reconstruct context that was not retained. It MUST NOT replay an accepted response, visible output, reasoning prelude, sequenced event or pending tool call. Unsafe lifetime refusals MUST remain terminal and mark the expired socket for retirement after existing pending work drains. Native WebSocket replay rules MUST remain unchanged.

#### Scenario: A continuation cannot be safely reconstructed
- **GIVEN** the request depends on client-owned or unavailable context
- **WHEN** its upstream socket reaches the lifetime cap
- **THEN** no transparent replay strips its anchor or changes its owner
- **AND** the existing terminal error is delivered

#### Scenario: Output or another response already exists
- **GIVEN** a response was accepted, produced output or reasoning, exposed a sequence, or has a pending sibling
- **WHEN** the lifetime error arrives
- **THEN** it is delivered without replay
- **AND** existing pending work is not interrupted by proactive renewal
