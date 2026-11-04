## ADDED Requirements

### Requirement: Confirmed reset-credit refresh may release the quota block it reset

A forced refresh MAY bypass the rate-limit cooldown only for upstream `code="reset"` and `windows_reset > 0`. It MUST use pre-consume start time, cooldown markers and newest-fetch window deadlines. Markers MUST remain unchanged; the block's second MUST precede the start. Exactly one baseline window MUST match the old deadline within five seconds; its fresh deadline MUST move by more than five seconds. Existing availability checks and all other recovery rules MUST remain unchanged.

#### Scenario: Confirmed reset credit releases a rate-limited account with available weekly quota

- **GIVEN** a `RATE_LIMITED` account whose persisted `reset_at` equals its weekly window's reset deadline, with a `blocked_at` marker
- **WHEN** upstream reset-credit redemption returns `code="reset"` and `windows_reset > 0`
- **AND** the forced usage refresh reports that window with a new reset deadline and available quota
- **THEN** the account becomes `ACTIVE`
- **AND** its `reset_at` and `blocked_at` markers are cleared

#### Scenario: Automatic pre-expiry redemption releases a rate-limited account

- **GIVEN** a `RATE_LIMITED` account whose persisted `reset_at` equals its weekly window's reset deadline
- **AND** automatic reset-credit redemption before expiry is enabled and a credit is inside the redemption window
- **WHEN** the scheduler redeems the credit and upstream returns `code="reset"` and `windows_reset > 0`
- **AND** the forced usage refresh reports that window with a new reset deadline and available quota
- **THEN** the account becomes `ACTIVE`

#### Scenario: Unconfirmed reset credit does not bypass the cooldown

- **GIVEN** a `RATE_LIMITED` account has a future persisted reset deadline
- **WHEN** redemption does not return both `code="reset"` and `windows_reset > 0`
- **THEN** the account remains `RATE_LIMITED` while the deadline is in the future

#### Scenario: A refresh recorded during the redemption does not hide the blocked window

- **GIVEN** a `RATE_LIMITED` account whose persisted `reset_at` equals its weekly window's reset deadline
- **WHEN** another usage refresh records the post-reset weekly deadline while the redemption is in flight
- **AND** the redemption returns `code="reset"` and `windows_reset > 0` and the forced refresh reports available quota
- **THEN** the blocked window is still identified from the baseline captured before the consume request, whatever that refresh's `recorded_at`
- **AND** the account becomes `ACTIVE`

#### Scenario: A row from an older fetch does not make the block ambiguous

- **GIVEN** a `RATE_LIMITED` account whose persisted `reset_at` equals its weekly primary window's reset deadline
- **AND** an older usage fetch left a secondary row carrying the same deadline
- **WHEN** upstream confirms a reset credit and the forced usage refresh reports the weekly window with a new deadline and available quota
- **THEN** only the newest fetch counts toward the baseline, and the account becomes `ACTIVE`

#### Scenario: Block not bound to exactly one quota window stays blocked

- **GIVEN** a `RATE_LIMITED` account whose persisted `reset_at` matches no quota window's reset deadline (for example a Retry-After or model-scoped throttle), or matches more than one
- **WHEN** upstream confirms a reset credit and the refreshed usage shows available quota
- **THEN** the account remains `RATE_LIMITED`

#### Scenario: Blocked window that did not reset stays blocked

- **GIVEN** a `RATE_LIMITED` account whose persisted `reset_at` matches its weekly window's reset deadline
- **WHEN** upstream confirms a reset credit but the refreshed weekly window still reports the persisted deadline
- **THEN** the account remains `RATE_LIMITED`

#### Scenario: A 429 recorded during the redemption is not released

- **GIVEN** a reset-credit redemption request has started
- **WHEN** a new 429 sets `blocked_at` to a time in or after the second in which the redemption started
- **THEN** the forced refresh for that redemption does not release the account

#### Scenario: A changed block from a slower-clock replica is preserved

- **GIVEN** a reset-credit redemption has captured the account cooldown markers
- **WHEN** another replica changes those markers during the consume call but records an earlier wall-clock timestamp
- **THEN** the forced refresh does not release that changed block on any redemption path
- **AND** the scheduler cannot reinterpret its freshly reloaded row as the original cooldown

#### Scenario: Confirmed redemption with exhausted refreshed quota remains blocked

- **GIVEN** a `RATE_LIMITED` account has a future persisted reset deadline
- **WHEN** upstream confirms a reset whose window is bound to the block
- **AND** the forced usage refresh still reports the primary quota at 100 percent
- **THEN** the account remains `RATE_LIMITED`
