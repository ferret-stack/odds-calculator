# B3 — Betting edge engine

> **Status: SPEC — execution-ready.** Written by Fable (Brief Part B), to be
> implemented by Opus. Depends on B1 for closing odds (CLV); everything else
> can land before B1 with CLV fields left `null`.

## Objective

Turn the model into an actual edge engine, in the open. Compute model
probability vs bookmaker odds → EV → fractional-Kelly staking → populate the
`value_bets` array **the odds page already reads**
(`odds-calculator.html` filters `fixture.value_bets` on `bet.edge` — nothing
has ever computed it). Then make the track record public and unfakeable: an
append-only ledger with results, PnL and closing-line value. "True EV" rules:
honest edges, honest staking, every bet — winner or loser — on the record.

## Files touched

| File | Change |
|---|---|
| `edge_engine.py` | NEW — EV, Kelly, value-bet selection, ledger settle |
| `odds_calculator.py` | call the engine after the odds fetch; write ledger/PnL JSONs |
| `data/value_bets` | no new file — bets embed in `upcoming_fixtures.json` (existing UI contract) |
| `data/ledger.json` | NEW — append-only bet ledger |
| `data/pnl_summary.json` | NEW — aggregates for the public track-record page |
| `tests/test_edge_engine.py` | NEW |
| `ferret-stack.github.io` | (follow-up, small) track-record section fed by `pnl_summary.json` |

## The maths (write it exactly like this, and unit-test it)

For a market with model probability `p` and best available decimal odds `o`:

- `edge = p * o - 1`            (expected value per unit staked)
- Kelly fraction on net odds `b = o - 1`:
  `f_star = (p * b - (1 - p)) / b`   (equivalently `edge / b`)
- **Staking policy (house style — fractional Kelly, more conservative at
  short odds where model error dominates):**
  - stake fraction `f = f_star * kelly_divisor` where `kelly_divisor`:
    - odds < 1.7: pass entirely (no bet — model error > edge at this end)
    - 1.7 ≤ odds < 2.5: 1/8 Kelly
    - 2.5 ≤ odds < 5.0: 1/4 Kelly
    - odds ≥ 5.0: 1/8 Kelly (longshot variance control)
  - hard caps: `f ≤ 3%` of bankroll per bet, `Σf ≤ 12%` per matchweek
    (drop lowest-edge bets first when the cap binds)
- **Selection thresholds:** `edge ≥ 0.05` AND model prob source band has
  `total_games ≥ 100` (no bets off sparse bands 8–10). One bet max per
  match outcome market; O/U and BTTS allowed alongside a WDL bet on the
  same match but count toward the weekly cap.
- Model probabilities come from `elo_calculator.calculate_fair_odds(home_elo,
  away_elo, elo_bands, venue_multipliers)` — the venue-adjusted band model,
  nothing hand-tweaked. Goal markets use the band `over_XX_pct` / `btts_pct`
  directly (no venue adjustment — never invent one without calibrating it).

## Data contracts

`upcoming_fixtures.json` — each fixture gains:

```json
"value_bets": [
  {
    "market": "h2h",               // h2h | over_25 | under_25 | btts_yes | btts_no | ...
    "selection": "away",           // home | draw | away | yes | no | over | under
    "model_prob": 0.31,
    "fair_odds": 3.23,
    "book_odds": 4.36,
    "edge": 0.351,                 // fraction, not percent - UI multiplies by 100
    "kelly_fraction": 0.104,       // raw f*
    "kelly_divisor": 0.25,
    "stake_pct": 2.6,              // % of bankroll actually recommended
    "confidence": "high",          // high (edge>=.25) | medium (>=.12) | low (>=.05)
    "band": 3, "band_games": 360
  }
]
```

(Note the existing UI compares `bet.edge >= evThreshold` where the threshold
select is in percent — reconcile by shipping `edge` as a fraction AND fixing
the page's comparison in the same PR: `bet.edge * 100 >= evThreshold`.)

`data/ledger.json` — append-only; one entry per recommended bet, created at
publish time, settled after the matchweek:

```json
{
  "id": "2026-08-22-E0-Chelsea-Brentford-h2h-away",
  "placed_at": "2026-08-20",
  "league": "E0",
  "match": {"date": "2026-08-22", "home": "Chelsea", "away": "Brentford"},
  "market": "h2h", "selection": "away",
  "model_prob": 0.31, "book_odds": 4.36, "fair_odds": 3.23,
  "edge": 0.351, "stake_pct": 2.6,
  "closing_odds": 3.90,            // from B1 facts (AvgC*); null until B1
  "clv": 0.118,                    // book_odds / closing_odds - 1
  "result": "won",                 // pending | won | lost | void
  "pnl_pct": 8.74                  // won: stake*(odds-1); lost: -stake
}
```

`data/pnl_summary.json` — regenerated from the ledger, never edited by hand:

```json
{
  "as_of": "2026-09-01",
  "starting_bankroll_pct": 100.0,
  "bankroll_curve": [{"date": "2026-08-22", "bankroll_pct": 103.1}],
  "totals": {"bets": 12, "won": 5, "roi_pct": 3.1,
              "avg_edge": 0.14, "avg_clv": 0.032, "clv_positive_rate": 0.67}
}
```

CLV is the honesty metric: if `avg_clv` is persistently positive the model
beats the closing line (real edge); if PnL is up but CLV is negative, we're
running lucky and must say so. Both go on the public page.

## Step-by-step

1. `edge_engine.py`: pure functions `edge(p, odds)`, `kelly(p, odds)`,
   `select_value_bets(fixture, fair, bands, policy) -> list`, plus
   `settle(ledger, facts)` and `summarise(ledger) -> pnl_summary`.
   No I/O inside the maths functions — unit-testable.
2. Wire into `odds_calculator.generate_all_json_files`: after the odds fetch,
   compute `value_bets` per fixture (needs `current_elo`, `elo_bands`,
   `venue_adjustment` — all in memory post-rebuild), then upsert ledger
   entries for newly published bets (idempotent on `id`).
3. Settlement: on each pipeline run, join pending ledger entries against the
   facts store on `(date, home, away)`; mark won/lost from the market outcome;
   pull `closing_odds` from the B1 facts `closing_odds` block when present;
   compute `clv` and `pnl_pct`; regenerate `pnl_summary.json`.
4. Mirror the two new JSONs to `assets/data/` alongside the rest.
5. Site follow-up (can trail): a track-record block — bankroll sparkline from
   `bankroll_curve`, the totals row, and the full ledger table (scrollable,
   see B2 patterns). The anti-tipster pitch IS this table.
6. Backtest mode (`edge_engine.py --backtest`): replay historical seasons
   with the walk-forward calibration machinery (`calibration.eval_new`) as
   the probability source and `closing_odds` as the book — report what the
   policy would have returned and its CLV. This is due diligence for the
   staking policy parameters (divisors, thresholds); tune them ONLY on
   seasons ≤ 2024-25 and report 2025-26 untouched as holdout.

## Verification method

- `pytest tests/test_edge_engine.py`:
  - `kelly(0.31, 4.36)` == `(0.31*3.36 - 0.69)/3.36` ≈ 0.1046
  - negative-edge market → no bet; odds < 1.7 → no bet regardless of edge
  - weekly cap binds → lowest-edge bet dropped first
  - settle: won/lost/void paths; pnl arithmetic; CLV null-safe pre-B1
- **Hand-recompute one week** (the brief's test): pick MW22's published bets
  (`_posts/2026-01-12-Matchweek-22.md` has odds + stakes), run the engine on
  that week's inputs, and reconcile its recommendations against the table in
  the post — differences must be explainable (policy changes, not bugs).
- Ledger reconciliation invariant, asserted in `summarise()`:
  `starting + Σ pnl_pct == bankroll_curve[-1]` to the penny.
- Backtest report committed to `data/reference/edge_backtest.json` with the
  holdout season clearly separated.
