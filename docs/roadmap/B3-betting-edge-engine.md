# B3 — Betting edge engine

> **Status: SPEC STUB — to be written by Fable (Brief Part B), implemented later by Opus.**

## Objective
Turn the model into an actual edge engine. Compute model-probability vs bookmaker-odds →
EV → Kelly / half-Kelly staking → **populate the `value_bets` JSON the UI already references
but nothing computes**. Add a public PnL ledger (the anti-tipster track record) with
closing-line-value (CLV) tracking. Ground it in "True EV" — honest edges, honest staking.

## Fable to fill in
- Files touched
- Concrete step-by-step (edge calc, Kelly fraction, staking bounds, ledger schema, CLV)
- Data contracts (`value_bets` shape; ledger/PnL JSON; CLV fields — leverage the closing
  odds now available from the B1 football-data.co.uk feed)
- Verification method (recompute a historical week by hand; ledger reconciles)
