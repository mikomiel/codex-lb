## Implementation

- [x] Capture the redemption start time and a newest-fetch window baseline before the consume request, thread them through forced usage refresh, and bypass the rate-limited cooldown guard only when the block's second precedes the start and the persisted deadline is bound to exactly one baseline window whose fresh deadline moved.
- [x] Pass the pre-consume redemption context from every reset-credit redemption path (accounts API, dashboard API, scheduler auto-redeem) only for `code == "reset"` with `windows_reset > 0`.
- [x] Add real-database integration coverage for each redemption path and for negative outcomes: unconfirmed redemption, exhausted usage, unbound or ambiguous deadline, unmoved blocked window, a 429 recorded during the redemption on both the dashboard and scheduler paths, a usage refresh recorded during the redemption (including one with a backdated `recorded_at`), a stale row from an older fetch, a failed baseline read, and unconfirmed accounts-API redemptions; plus a release with the reported raw plan type.
- [x] Bind recovery to the pre-consume cooldown markers and test changed blocks from a slower-clock replica through all three entry paths.
- [x] Select the real-database reset-credit regressions in PostgreSQL CI.
- [x] Record the known fail-closed limits, marker-identity boundary and the #2602 follow-up gates in context.
- [x] Remove the temporary weekly reset reproduction test after recording its findings in context.
- [x] Run scoped tests, lint/format and timing-seam checks; validate the change strictly and all source specs with the normal profile.
