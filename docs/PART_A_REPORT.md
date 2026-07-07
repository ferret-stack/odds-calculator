# Part A — Foundation Rebuild Report (July 2026)

What was done, what changed, and the numbers that prove it. Companion to
`docs/FABLE_BRIEF.md` Part A; produced by the off-season audit.

## The root cause, dissolved

The public numbers were computed from ELO fields *stored on matches* — stamped
by whichever code path touched the match last: the scraper (which never loaded
`current_elo`, so it stamped 1500 vs 1500), the Excel importer, or
`update_elo_ratings` (which overwrote them with **post**-match ratings and
never re-derived `winner`). The fix is architectural, not a patch:

- **`matches_data.json` is facts-only.** Teams, date, goals, cards, referee,
  xG, possession. Nothing ELO-derived is stored — there is no field left to
  go stale or to bake a default into. (Verified: 0 rating fields in the facts
  file; the legacy file carried 74 baked-1500 matches.)
- **One deterministic replay derives everything** (`rebuild.py`, ~2s): each
  match is stamped with the **pre-match** ratings — the only ratings a
  forecast could have known — and `elo_diff`, `elo_band`, `winner` follow
  from those. `sanity_check()` asserts the old failure modes on every run.

## Data repairs found along the way (`tools/repair_facts.py`)

Beyond the brief's diagnosis, the audit found and deterministically repaired:

| Problem | Count | Fix |
|---|---|---|
| Duplicate rows (matchweeks scraped twice) | 15 | keep richer / re-scraped row |
| Day/month-swapped dates from the Excel import (pandas parsed d/m/Y month-first when day ≤ 12) | 396 | constraint solver: close-season + dataset-span + fixture-uniqueness + 2-day-rest elimination, then decisive matchweek-neighbour vote; 0 unresolved |
| Dates stamped a year late by the scraper's `datetime.now().year` fallback | 6 | season-window year shift |

Spot checks against known fixtures all pass (opening day 2020-09-12,
Villa 7–2 Liverpool 2020-10-04, Man Utd 1–6 Spurs 2020-10-04,
Arsenal 4–1 Villa 2025-12-30…). Full audit trail:
`data/reference/repair_report.json`.

Season coverage after repair — **~90 matches are missing** (the silent
scraper failures; B1 backfills from football-data.co.uk):
2020-21: 379 · 2021-22: 379 · 2022-23: 380 · 2023-24: 355 · 2024-25: 329 ·
2025-26: 368.

## Band 1, before / after

| | stronger win | draw | weaker win | n |
|---|---|---|---|---|
| **Before** (shipped V6) | 29.6% | 27.3% | **43.1%** | 534 |
| **After** (pre-match rebuild) | **40.9%** | 25.6% | 33.6% | 673 |

The shipped table said the *weaker* team was the most likely winner of a
near-even match — an artifact of the 1500-default + tie-mislabel + post-match
stamping knot. The rebuilt table is monotone across all ten bands (stronger
win % rises 40.9% → 85%+; the sanity check asserts
`stronger_win_pct ≥ weaker_win_pct` for every populated band on every run).

## Calibration (walk-forward, 1,432 matches, 2022-08 onward)

Predictions use only pre-kickoff information (pre-match ratings; band and
venue tables accumulated over strictly earlier matches, Laplace-smoothed).
Log-loss / Brier, lower is better:

| Model | log-loss | Brier |
|---|---|---|
| uniform ⅓ | 1.0986 | 0.6667 |
| home/draw/away base rates | 1.0698 | 0.6473 |
| **OLD shipped pipeline** (emulated from archived data, bugs and all) | 1.0193 | 0.6069 |
| **NEW: long ELO + v6blog MOV + global venue** | **0.9917** | **0.5903** |

- **New beats old: paired bootstrap P = 0.999.** The old pipeline barely beat
  a home-advantage-only null model; the new one is decisively better than both.
- Reliability of the chosen config tracks the diagonal in every decile
  (`data/reference/calibration_results.json`).

## Decisions (by backtest, not preference)

1. **Driver ELO: LONG** (continuous since 2020, all teams start 1500).
   Long-vs-rolling is within noise (bootstrap 0.57), so the tie-break is
   simplicity and stability. **Rolling ELO** (730-day window, re-baselined
   to 1500) ships alongside in `current_elo.json` and per match in
   `matches_derived.json` for the class-vs-form narrative.
2. **MOV formula: the one the V6 post documented** —
   `0.7 + ln(goal_diff + 1) × 0.5`, with the winner's base scaled by
   `1 + |elo_diff|/500` when the winner was rated > 50 points below the loser.
   The code had silently been running a FiveThirtyEight-style formula instead;
   both were backtested; the documented formula won every cell (narrowly —
   bootstrap 0.55, i.e. within noise), so docs and code were reconciled in
   the direction of the published methodology. The V6 post carries a dated
   correction (its worked examples were also wrong and are fixed).
3. **Venue adjustment: data-measured GLOBAL multipliers, applied.** Clear
   calibration winner (venue-off costs ~0.009 log-loss; per-band splits
   overfit the sparse bands and lose to global). The dead
   `calculate_home_advantage_multipliers` / hardcoded 1.11/0.89/0.95/1.05
   constants are gone; `rebuild.calculate_venue_adjustment` measures
   win/draw/loss multipliers by venue of the stronger team (currently
   1.096 / 0.900, draw 0.962 / 1.039, weaker 1.178 / 0.828) and both the
   Python model and the site JS consume them from `venue_adjustment.json`.
4. **Exact-equality rule (documented):** when raw pre-match ratings are
   equal, the **home side is the de-facto stronger team** — consistent with
   the +100 home advantage already inside the update formula. 11 matches in
   the whole dataset are exact ties; none of their labels is an accident any
   more, they follow the stated rule.

## Housekeeping

- **Secrets:** the three the-odds-api keys are out of the source; the
  pipeline reads `ODDS_API_KEYS` / `ODDS_API_KEY` from the environment.
  ⚠️ **Owner action: rotate all three keys** — they remain in git history.
- Duplicate `import_excel` and `calculate_fair_odds` definitions removed;
  `get_band_probabilities` no longer KeyErrors on real bands; the phantom
  `value_bets` summary block is gone (B3 builds the real thing); selenium
  imports are lazy so derive/backtest runs need no browser;
  band range labels now match the maths (band 1 is 0–49, not "0–50").
- Tests: `python -m pytest tests/` covers band edges, the equality rule,
  both MOV formulas, pre-match stamping and determinism.
- Site (`ferret-stack.github.io`): data mirrored, JS venue multipliers read
  from the JSON, V6 post corrected.

## How to re-verify everything

```bash
python -m pytest tests/            # unit invariants
python rebuild.py                  # full derive + sanity asserts (~2s)
python calibration.py              # the whole backtest grid (~15s)
```
