# Fix owner-forward affinity-key transport

## Why

[Upstream issue #2655](https://github.com/Soju06/codex-lb/issues/2655) reports
that cross-replica forwarding rejects internally generated session/thread
selection keys. Their deliberate LF namespace is valid database identity but
not an HTTP header. Raw-header parsing would also trim the LF and change the
signed key. Four synthetic regressions reproduce the rejection at the previous
candidate head in both signature modes.

## What changes

- Encode only the internal LF-namespaced affinity key for HTTP transport.
- Use an explicit encoding header, not an ambiguous prefix on a legacy key.
- Decode through one shared function before signature checks and session lookup.
- Keep signatures bound to the original logical key and retain all other guards.
- Preserve header-safe legacy keys, including literal encoding-looking prefixes.

## Scope and rollout

No database, configuration, dependency, workflow, account selection or replay
change. This is not a deployment. Old receivers cannot decode the newly encoded
LF keys; a mixed replica fleet can still reject those forwards. Coordinate any
future replica upgrade separately. Legacy safe keys without the marker keep their
existing format. No production data, provider request or live replica is needed
for the synthetic tests.

## Verification

Exercise session/thread keys, both signature modes, file/reservation proofs,
primary-signature compatibility, literal legacy keys, injected bridge headers,
malformed encodings, tampering and control-character rejection. Run the existing
bridge/WebSocket regressions, static checks and specification validation. Obtain
independent review of the final source head before requesting public approval.
