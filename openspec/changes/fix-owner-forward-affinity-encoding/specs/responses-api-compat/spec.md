## ADDED Requirements

### Requirement: Namespaced affinity keys survive owner forwarding

Owner forwarding MUST encode internal LF-namespaced affinity keys as canonical UTF-8 base64url with an explicit versioned transport marker. Both signature verification and forwarded-session lookup MUST use the decoded logical key without trimming its LF. Encoding MUST NOT change affinity kind, API-key scope, reservation, file ownership or replay policy. Header-safe legacy keys without the marker MUST retain their original meaning.

#### Scenario: A namespaced key crosses the owner hop
- **GIVEN** a server-generated session or thread selection key with its LF namespace
- **WHEN** an origin forwards the request to its owner in either signature mode
- **THEN** the wire headers contain no illegal control characters
- **AND** signature verification and session lookup recover the exact original key
- **AND** file-owner and reservation proofs retain their existing meaning

#### Scenario: A legacy key resembles an encoding prefix
- **GIVEN** a legacy key such as `b64:YWJj` without an encoding marker
- **WHEN** the owner receives a signed forward
- **THEN** the key remains literal and is not decoded
- **AND** the existing primary-signature compatibility path remains available

### Requirement: Affinity encoding preserves forward authentication guards

The origin MUST strip externally supplied internal bridge headers. Only the deliberate leading LF in the internal affinity namespace MAY be encoded; other illegal metadata controls MUST remain rejected. Unknown encodings, malformed or noncanonical base64url, invalid UTF-8 and decoded keys outside that namespace MUST fail closed. Removing the marker or changing the decoded identity MUST NOT bypass existing signature checks.

#### Scenario: An encoded forward is tampered with
- **GIVEN** a signed forward carrying a namespaced affinity key
- **WHEN** its encoding marker is removed or its logical key, kind or payload changes
- **THEN** the owner rejects the forward with `bridge_forward_invalid`

#### Scenario: Encoded metadata is malformed
- **WHEN** a forward declares an unsupported encoding or carries malformed encoded data
- **THEN** it fails with `bridge_forward_invalid` before session selection
- **AND** encoding does not make additional CR, LF, NUL or DEL characters acceptable

