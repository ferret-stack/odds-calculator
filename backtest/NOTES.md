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

---

## 8. Shrinkage grid search: blending the model toward the market

`python3 backtest/shrinkage.py` → `out/shrinkage.md`, `shrinkage_grid.csv`,
`shrinkage_by_season.csv`, `shrinkage_ev_floor_control.csv`. The probability
handed to `size_bet` becomes `p_w = w·model + (1−w)·market_fair`, w on a 0.05
grid. `engine.run(shrinkage=w)` applies it at the one point the model
probability is used; `w=1.0` is the default and reproduces section 1's
numbers byte-identically (asserted, not assumed —
`test_shrinkage_default_is_the_unshrunk_model`). Staking is untouched: same
+5% EV floor, Quarter-Kelly ceiling, Eighth-Kelly downgrade, 3% clamp.

**Metric 1 is Brier, not log loss.** `calculate_elo_bands` emits exact 0.0
rates whenever no match in a band went that way — shipped bands 9 and 10 both
do (section 3), and a walk-forward rebuilding from small early samples hits
such rows constantly. Log loss is infinite there, so it would need an epsilon
clip whose value would then set the ranking. Brier is bounded, needs no clip,
and reads the two fields every fixture row already carries.

**Metric 1 is scored on a fixed population.** All 5,700 priced selections per
book, not the bets a given w selected. The ELO chain, bands and seeding read
no staking decision, so that population is identical for every w — checked at
every grid point by `_assert_population_is_stable`, not assumed. Scored on
selected bets the column would be meaningless: a w that picks 8 bets can post
any Brier it likes. `brier_on_bets` is reported beside it and labelled
non-comparable.

### The two curves (Avg\*, primary; B365\* agrees throughout)

| | Avg\* | B365\* |
|---|---|---|
| **Brier minimised at** | **w = 0.00** (0.189960) | **w = 0.00** (0.190053) |
| Brier at w = 1.00 (production) | 0.197366 | 0.197366 |
| Exact unconstrained minimiser | **w\* = −0.2725** | **w\* = −0.2578** |
| **Yield maximised at** | **w = 0.60** (−8.90%, 1,015 bets) | **w = 0.65** (−9.10%, 977 bets) |
| Yield at w = 1.00 (production) | −10.23% (1,298 bets) | −9.78% (1,254 bets) |
| Flat ROI maximised at | w = 0.40 (−9.07%, 662 bets) | w = 0.60 (−9.84%, 928 bets) |

**They conflict, and neither is a licence to bet.** Calibration falls
monotonically as w falls: every step toward the market improves it, and the
minimum is at the boundary. Yield peaks in the middle. Every yield on the
grid is negative — there is no w at which this model makes money.

### Four things that decide the reading

**1. The Brier optimum is not just at zero, it is past it.** w\* is negative
on both books: the best available blend puts *negative* weight on the model,
i.e. the model's deviation from the market is on average pointed the wrong
way. That is not one season — per-season w\* is −0.39, −0.46, −0.21, +0.03,
−0.29, and per-band it is negative in every band with ≥300 selections except
band 2 (≈0). But the *size* is negligible: Brier at w\* is 0.189604 against
0.189960 at w = 0, a gain of 0.0004. The honest statement is "the model's
residual carries no usable information, and if anything is slightly
inverted", not "bet against the model".

**2. The yield peak is inside the noise.** Flat ROI at w = 0.60 beats w = 1.00
by +2.50pp against a combined standard error of ±7.10pp. The peak is a
wiggle: the neighbouring points are −9.26% (0.55) and −9.86% (0.65), and the
book-to-book optimum moves (0.60 vs 0.65) while the Brier optimum does not.

**3. Lowering w is a disguised EV-floor raise, and loses to the real one.**
Shrinking every probability toward the price shrinks every edge, so fewer
selections clear the +5% floor and the survivors are the largest raw
disagreements. That is a filter on edge size, which is what an EV floor
already is. `shrinkage_ev_floor_control.csv` runs the control — unshrunk
model, floor raised — and matched on bet count:

| | shrinkage | control (unshrunk, raised floor) |
|---|---|---|
| Avg\* ≈1,000 bets | w = 0.60: −8.90% (1,015) | 10% floor: −9.85% (1,056) |
| B365\* ≈1,000 bets | w = 0.65: −9.10% (977) | 10% floor: **−7.09%** (997) |

Shrinkage wins by 0.95pp on one book and loses by 2.01pp on the other. A
parameter whose sign flips between two highly correlated odds sources is
noise. **Shrinkage buys nothing the EV floor does not already buy.**

**4. w = 0 cannot bet at all, structurally.** Mean Avg\* overround is 1.04487,
so a de-vigged price staked against its own book has EV = 1/1.04487 − 1 =
**−4.29%**, which can never clear a +5% floor. Hence 0 bets at w = 0.00 and 8
at w = 0.05. The yield column does not exist at the end of the grid where
calibration is best — the two metrics are not measured on overlapping regions
of w, which is the sharpest way to say they do not trade off against each
other.

### Recommendation

Per the brief I am not picking w for you; both curves are in
`out/shrinkage.md` and the CSVs. What I will say is what the numbers rule
out:

- **Do not ship a mid-range w (0.5–0.7) as a fix.** It is the yield-curve
  argmax, and it is the weakest-supported number here: inside one standard
  error, unstable between books, and beaten on B365\* by the one-line
  alternative of raising `MIN_EV`.
- **If the goal is honest probabilities, w = 0 wins outright** — and w = 0 is
  the statement "publish the de-vigged price", which is not a model. That is
  the finding, not a parameter to tune. A shrinkage weight is the right tool
  when a model has genuine signal that needs damping toward a prior; the
  negative w\* says this one has no residual signal to damp. Shrinkage cannot
  repair that, and fitting w harder will only fit noise.
- **The decision this actually surfaces is about `MIN_EV`, not w.** If
  something must change now, the control table is the better place to argue
  from: it is one config value, it needs no new blend step in the pricing
  path, and on this sample it does at least as well as any w.
- **My recommendation is to ship no w at all** and treat section 1's
  calibration result as the finding to act on — the band model's
  probabilities are not competitive with the closing price, and no convex
  combination of the two rescues them.

### Caveats on the above, stated rather than buried

- **In-sample.** w is chosen on the same 1,900 matches it is scored on.
  `shrinkage_by_season.csv` pivots yield by season for every w so a w resting
  on one season is visible; 2024-25 is positive at every w and the other four
  seasons are negative at every w, which is the same pattern section 1 found
  and is not created by shrinkage.
- **De-vig method.** Proportional, as in section 1 — it under-corrects
  favourite-longshot bias, so `market_fair` is slightly too high on longshots
  and too low on favourites. Both the blend and the Brier inherit that. A
  Shin or power de-vig would move the numbers; it would have to move them a
  long way to make any w profitable.
- **Closing prices, not the prices production sees.** This is not a lookahead
  — the blend reads only that fixture's own closing odds, which the EV
  arithmetic at that same point already reads, and
  `test_shrinkage_has_no_lookahead_either` re-runs the truncation test at
  w = 0.5 to prove it. It is a transferability limit: production prices
  midweek against a softer, earlier line, so a w fitted against the closing
  line is fitted against a sharper opponent than the live pipeline faces.
- **No new dependencies, and nothing outside `backtest/` touched.** The only
  production-adjacent change is that `engine.run` gained a `shrinkage`
  keyword defaulting to the previous behaviour; `elo_calculator.py`,
  `pipeline/staking.py` and `tools/rebuild_elo.py` are untouched, which
  `test_production_is_not_written_to` verifies by content hash.
