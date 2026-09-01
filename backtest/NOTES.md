# Backtest harness — forks, blockers and defects found

Written as the brief requires: what forked from the plan, what blocked, and
what turned up on the way. Nothing in production was edited; `git status`
shows `backtest/` as the only change.

---

## 1. Headline answer

Over 2021-22 → 2025-26 (2020-21 is warm-up), against market-average closing
odds, the production ELO band model **does not beat the market**:

| | bets | staked | profit | yield | flat ROI | strike rate |
|---|---|---|---|---|---|---|
| Avg\* (primary) | 1,298 | 22,972.80 | **−2,350.70** | **−10.23%** | −11.68% | 31.74% |
| B365\* (secondary) | 1,254 | 21,982.74 | **−2,149.88** | **−9.78%** | −11.13% | 31.50% |

A £1,000 bankroll compounding these stakes ends at **£67.46** (94.1% max
drawdown). The single most useful number is not the yield but the
calibration: the model's mean probability on the bets it selected is **0.42**
against a **31.7%** strike rate, and it is overconfident in every probability
bucket — at a model probability of 0.15 the true rate is 0.07, at 0.73 it is
0.56. The de-vigged market is closer to truth in every bucket. The "edges"
are the model's overconfidence, measured against a sharper price.

Not every cut is negative — band 2 (+4.2% on B365), 2024-25 (+1.5%), and the
away-underdog ≥20% cut (+2.1%) are positive. On 160–290 bets each and against
a −10% base rate, treat those as noise unless they survive on new data.

---

## 2. Fork: `tools/rebuild_elo.py:rebuild()` is not reusable for a walk-forward

Reported rather than worked around, per the brief.

`rebuild()` cannot be called season-by-season. Three reasons, the third
decisive:

1. It orders by `match_id`, which the football-data CSVs do not have.
2. It repairs scraped date corruption (`DATE_CORRECTIONS`,
   `repair_transposed_dates`) that this source does not have — 17.5% of
   production's dataset, 0% of this one.
3. It stamps every match's band **after** running the chain to completion.
   That is precisely the lookahead a walk-forward must not have.

**What was done instead:** the harness reuses the pieces that are
state-free — `ELOCalculator` (rating, MOV, update), `elo_band`,
`classify_winner`, `calculate_elo_bands`, `SEED_ELO`, and `rebuild_elo`'s own
`season_of` / `bottom_four_average` seeding helpers, unchanged — and drives
its own chronological loop. No production code was refactored to fit.

---

## 3. Defect found: `calculate_fair_odds` crashes the live pipeline on a band-9 or band-10 fixture

**This one is not about the backtest.** It is a live bug in the weekly run.

`calculate_fair_odds` (called by `run_pipeline.price_fixture` for *every*
fixture) ends by inverting each probability to a fair price:

```python
'fair_odds': round(1 / draw_prob, 2)
```

with no guard against a zero. In the shipped `data/elo_bands.json`:

| band | n | stronger | draw | weaker |
|---|---|---|---|---|
| 9 (401–450) | 6 | 0.8333 | 0.1667 | **0.0** |
| 10 (450+) | 4 | 1.0 | **0.0** | **0.0** |

So **any fixture with an ELO gap of 401 or more raises `ZeroDivisionError`
and takes down the whole run**, not just that fixture — `find_edges` has no
per-fixture try/except.

This is reachable now, not hypothetically. In `data/current_elo.json`,
**Arsenal (2020) v Burnley (1613) is a 407-point gap → band 9**. Both are in
this season's Premier League. Seven further pairs in the file exceed 401, topping out at
Arsenal–Southampton (486), but those all involve a team no longer in the
division — Arsenal v Burnley is the one that can actually be scheduled.

Not fixed here — the brief is read-only against production. Flagging it as
the highest-priority item found.

**Harness workaround:** the backtest prices through
`get_venue_adjusted_probabilities` instead. It derives the same probabilities
from the same band row, through the same `adjust_probability_for_venue` and
the same normalisation, rounded the same way — verified identical wherever
both run. It simply does not invert, so it does not crash.

## 3b. Related: `calculate_elo_bands` can emit an all-zero band row

When every match in a band has `winner == 'even'`, the function falls back to
`rated = band_matches` and all three predicates miss, producing
`0.0 / 0.0 / 0.0`. `calculate_fair_odds` then divides by their sum.

A walk-forward hits this immediately: every team seeds at `SEED_ELO`, so
every opening fixture is evenly rated and lands in band 1. It cannot fire
against the shipped table, which is why it has not been seen. The harness
detects the row (`engine.band_is_degenerate`) and declines to price the
fixture — 8 occurrences, all inside the warm-up season, none affecting a bet.

---

## 4. Band 1 default-1500 bug: instrumented, never fired

The brief asked for it to be logged and flagged rather than fixed. It cannot
occur: `ELOCalculator` defaults to `SEED_ELO` (1784), the 1500 sentinel
checks are gone, and the harness seeds both sides explicitly before pricing.
The check is in place anyway (`engine.BAND1_BUG_RATING`) and fires on any
pre-match rating of exactly 1500. **Zero sightings across 2,280 matches.**
The absence is evidence, not an assumption.

---

## 5. The modelling decision this harness had to make

Production prices from `data/elo_bands.json`, built by `calculate_elo_bands`
over the whole match history. Replaying that table against the matches it was
built from is not a backtest — every fixture's price knows its own result.

Default is `--bands walkforward`: the table is rebuilt at each new match date
from matches completed **strictly before** it, with 2020-21 as warm-up.
`--bands frozen` uses the shipped table for comparison. The gap between them
is the size of the leak, and it is large:

| bands | yield | flat ROI |
|---|---|---|
| walkforward (honest) | **−10.23%** | −11.68% |
| frozen (lookahead) | −2.19% | **+1.32%** |

**The frozen run's positive flat ROI is entirely lookahead.** Any past
self-test of this model that priced off the shipped band table was measuring
this artefact, which is the likely reason the model "beats itself".

---

## 6. Premise corrections

Three things in the brief do not match the repository. Flagging rather than
quietly building to them:

- **There is no away-underdog `implausible_edge` pattern in
  `Themed_Findings.md` Theme 2.** Theme 2 is "Automated Pipeline" and
  mentions neither. `Odds_Calculator_System_Source_of_Truth.md:64` explicitly
  corrects this reading: *"It has never been an 'away-underdog' flag; venue
  and underdog status appear nowhere in the trigger, only edge magnitude."*
  The away-underdog observation is from `Matchweek_2.md`, where six away
  picks happened to trip an edge-magnitude check. The harness reports the
  full 2×2 (away-underdog × ≥20% edge) so the two are never conflated again.
  The data agrees with the doc: at ≥20% edge, away-underdogs return +2.1%
  and everything else −16.1%, but away-underdogs *below* 20% return −26.1%.
  Venue and underdog status are not what the flag is picking up.
- **`Elo_Approach.md` does not exist in this repo**, though the Source of
  Truth cites it for the MOV function. The MOV logic was read from
  `elo_calculator.margin_of_victory_multiplier` and reused directly.
- **The 1x2 model is not Poisson.** `calculate_fair_odds` prices 1x2 from
  empirical band frequencies with venue multipliers. Poisson is used for
  goals markets and Super 6 (`Themed_Findings` Theme 4), not for the bets
  backtested here.

---

## 7. Other things worth knowing before trusting a bucket

- **Returning teams carry stale ratings.** Fulham (2022-23), Burnley and
  Sheffield United (2023-24), Leicester and Southampton (2024-25), Burnley
  and Leeds (2025-26) re-enter on the rating they were relegated with, not a
  promoted-team seed. That is what production's replay does — `rebuild()`
  only seeds teams absent from `current_elo` — so it is reproduced
  deliberately, not fixed.
- **Five bets outside warm-up were priced from the 0.333/0.333/0.334
  empty-band fallback** (Watford v Man City 2021-12-04, and four more, all
  the first time an extreme gap appeared). That is not a model opinion.
  Production would stake them too, so they are flagged (`anomalies.csv`) and
  left in rather than quietly dropped.
- **109 of 1,298 Avg\* bets were priced off a band with fewer than 30
  completed matches**, some as few as 1. `band_games` is on every row of
  `bets.csv` so any cut can be re-run excluding them.
- **Stakes are fractions of a fixed £1,000 notional**, so a bucket's yield is
  not an artefact of where in the run its bets fell. The compounded curve is
  reported separately from the same fractions.
- **Production's own checks are applied**, not a simplified Kelly: the
  Eighth-Kelly downgrade above +20% (548 bets), the same-market block (288
  selections), and the 3% per-bet clamp (296 bets).
- **Weekly exposure (`check_weekly_exposure`) is not simulated.** It flags
  and never rescales, so it cannot change a P&L figure.
- **No new dependencies.** Standard library only — `pandas` is in
  `requirements.txt` but is not installed in this environment, and nothing
  the harness imports needs it.
