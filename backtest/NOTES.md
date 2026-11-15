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

## 8. Lower leagues: same model, per-league chains (added 2026-11-15)

**Label correction for everything above.** `AvgH/AvgD/AvgA` are PRE-CLOSING
market averages (collected Friday / Tuesday afternoons), not closing odds;
`AvgC*` are the closing prices (`docs/Football-Data-Notes.md`). Sections 1-7
call Avg\* "closing". The figures are right; the label was wrong. The runner
now prints "pre-closing"; the committed `backtest/out/summary.md` still
carries the old wording until the PL run is next regenerated.

### Method

The model is unchanged: band-frequency 1x2, K=20, home advantage 100, MOV,
venue multipliers, MIN_EV 0.05, production staking. The league is the only
change.

- **Data**: `docs/historical/<code>/<endyear>.csv` via
  `data.load_league`. Aliases from `tools/output/team_alias_proposal.csv`
  applied as written (three `alias` rows).
- **Holdout**: 2025/26 (`2026.csv`) is never loaded. `data._assert_not_holdout`
  raises `AssertionError` before any such file is opened; tests cover all
  five leagues. The only exemption is the legacy flat PL file, reachable only
  through the argument-less `data.load_legacy_pl_flat()`, which exists to
  reproduce the section 1 baseline.
- **ELO**: one chain per league, built in-house from that league's earliest
  season (1993/94; EC 2005/06). Band table walk-forward from the same
  league's completed matches.
- **Bet seasons**: 2019/20 to 2024/25 only. Odds are read only in bet
  seasons, so BbAv\* is never touched.
- **Odds rules**: a missing odds column in a bet season is an error. A blank
  or <=1.0 price skips that match for that odds family (all three
  selections), is counted, and the result still feeds ELO.
- **Seeding policy `league`** (default), applied once per season before its
  first fixture, from ratings at the end of the previous season:
  - promoted in from below: bottom-four average of the destination league's
    previous season;
  - relegated in from above: average rating of the destination league's
    previous-season teams;
  - re-entrant after a gap (absent at least one full season) or first
    appearance: a new entrant by the direction it arrived from; stale rating
    replaced; direction unknown -> destination average.
  - Direction = where the team played the previous season in the adjacent
    league's file. EC arrivals not from E3, and E3 arrivals from below before
    2005/06 (no EC file), are "unknown" by construction.
  - Seasons are taken from the file, not `season_of(date)`: 2022/23 started
    29-30 July in E1/E2/E3.
- **Seeding policy `legacy`**: the original PL rule (returning teams keep
  their rating). Refused for anything but E0; used only by the regression.
- **Regression**: the legacy flat PL run reproduced 1,298 bets, -10.23%
  yield, -11.68% flat ROI after the loader change and again after the
  seeding change, with `backtest/out/` byte-identical both times.
- **95% CI**: flat-stake ROI, normal approximation on per-bet returns
  (mean +/- 1.96 sd / sqrt(n)), fixed before any league result was seen.
  Treats bets as independent.

Commands:

    python3 backtest/run_backtest.py --league E2 E3 --pool
    python3 backtest/run_backtest.py --league E1 EC E0
    python3 backtest/run_backtest.py --league E2 E3 --odds avgc --pool

### Reading rule (pre-registered)

PRIMARY: E2 + E3 pooled, flat stakes, Avg, 2019/20-2024/25. PASS = 95% CI on
pooled yield entirely above zero AND each of E2, E3 individually
non-negative; provisional until the holdout is checked. Anything else,
including a CI spanning zero, is not a pass. E0, E1, EC, AvgC, Pinnacle are
descriptive only. Slices are exploratory, never a headline.

### Result: NOT A PASS

Avg (pre-closing), 2019/20-2024/25:

| league | matches priced | bets | staked | profit | yield | flat ROI | strike | 95% CI flat ROI | rows skipped |
|---|---|---|---|---|---|---|---|---|---|
| E2 (primary) | 3,159 | 1,759 | 25,335.95 | −1,994.29 | −7.87% | −9.15% | 28.31% | −16.43 to −1.88 | 1 |
| E3 (primary) | 3,195 | 1,542 | 20,931.60 | −2,061.23 | −9.85% | −9.70% | 27.43% | −17.40 to −2.01 | 5 |
| **E2+E3 pooled (primary)** | 6,354 | 3,301 | 46,267.55 | −4,055.52 | −8.77% | **−9.41%** | 27.90% | **−14.70 to −4.13** | 6 |
| E1 (descriptive) | 3,312 | 1,716 | 23,449.64 | −1,248.89 | −5.33% | −5.05% | 28.32% | −12.69 to 2.58 | 0 |
| EC (descriptive) | 3,088 | 1,389 | 20,386.99 | −2,114.14 | −10.37% | −9.89% | 29.45% | −17.85 to −1.93 | 2 |
| E0 (descriptive) | 2,280 | 1,021 | 14,528.77 | −1,191.29 | −8.20% | −0.94% | 29.77% | −12.88 to 11.00 | 0 |

The E0 row uses league seeding, bets from 2019/20 and a warm-up from 1993/94.
It is **not comparable** to the 1,298-bet legacy baseline in section 1.

AvgC (closing), same seasons, descriptive:

| league | matches priced | bets | staked | profit | yield | flat ROI | strike | 95% CI flat ROI | rows skipped |
|---|---|---|---|---|---|---|---|---|---|
| E2 | 3,160 | 1,829 | 27,248.06 | −2,198.35 | −8.07% | −8.60% | 28.32% | −15.76 to −1.45 | 0 |
| E3 | 3,200 | 1,713 | 23,982.00 | −2,633.81 | −10.98% | −9.56% | 26.97% | −16.97 to −2.14 | 0 |
| E2+E3 pooled | 6,360 | 3,542 | 51,230.06 | −4,832.16 | −9.43% | −9.06% | 27.67% | −14.21 to −3.92 | 0 |

Rows skipped for a missing Avg price, by league-season: E2 2019-20 1/400;
E3 2019-20 4/440, 2022-23 1/552; EC 2020-21 1/477, 2023-24 1/552. All other
league-seasons, and every AvgC league-season, 0.

Pinnacle closing (PSC\*, 2012/13-2024/25) was not run.

### Unverified

- The 2025/26 holdout has not been looked at, by design.
- `band1_default_1500` fired in bet seasons: E2 x26, E3 x34, E1 x27. The
  check flags any pre-match rating of exactly 1500 and changes no price.
  Nothing in the league path assigns 1500 as a default, so these are
  presumed to be ordinary ratings landing on 1500; not checked one by one.
- EC priced one fixture from the empty-band 0.333/0.333/0.334 fallback.
- Entrant directions were spot-checked on four known movements, not audited
  across all seasons.
- 2019/20 (E2 400, E3 440, EC 451 rows) and EC 2020/21 (477) are curtailed
  seasons, run as they are; the bottom four come from the curtailed table.
- The CI assumes independent bets; same-day bets are not exactly independent.

## Beta test
dannyboy@pop-os:~/boolean/odds$ python3 backtest/diagnostic.py --league E2 E3 --pool
Odds: avg | seasons 2019-20 .. 2024-25 | model prob floor 0.01 | P(k) ~ exp(a ln market + b ln model)

E2: 3,159 matches
  log loss  uniform 1.0986 | market 1.0162 | model 1.0374
  market only: a = 1.195
  market + model: a = 1.251 (se 0.107), b = -0.070 (se 0.117), 95% CI on b -0.298 to 0.159, LR stat 0.36
  out of sample (1,503 fit, 1,656 scored): log loss market only 1.0137, market + model 1.0163, improvement -0.0026
  b reading: NO SIGNAL

E3: 3,195 matches
  log loss  uniform 1.0986 | market 1.0550 | model 1.0709
  market only: a = 1.009
  market + model: a = 1.067 (se 0.107), b = -0.085 (se 0.129), 95% CI on b -0.337 to 0.168, LR stat 0.43
  out of sample (1,540 fit, 1,655 scored): log loss market only 1.0543, market + model 1.0560, improvement -0.0017
  b reading: NO SIGNAL

POOLED E2+E3: 6,354 matches
  log loss  uniform 1.0986 | market 1.0357 | model 1.0542
  market only: a = 1.115
  market + model: a = 1.155 (se 0.076), b = -0.053 (se 0.086), 95% CI on b -0.221 to 0.115, LR stat 0.38
  out of sample (3,043 fit, 3,311 scored): log loss market only 1.0346, market + model 1.0370, improvement -0.0023
  b reading: NO SIGNAL

dannyboy@pop-os:~/boolean/odds$ python3 backtest/diagnostic.py --league E1 EC E0
Odds: avg | seasons 2019-20 .. 2024-25 | model prob floor 0.01 | P(k) ~ exp(a ln market + b ln model)

E1: 3,312 matches
  log loss  uniform 1.0986 | market 1.0379 | model 1.0560
  market only: a = 1.035
  market + model: a = 1.146 (se 0.107), b = -0.158 (se 0.131), 95% CI on b -0.414 to 0.099, LR stat 1.45
  out of sample (1,656 fit, 1,656 scored): log loss market only 1.0332, market + model 1.0329, improvement +0.0003
  b reading: NO SIGNAL

EC: 3,088 matches
  log loss  uniform 1.0986 | market 1.0227 | model 1.0394
  market only: a = 1.099
  market + model: a = 1.172 (se 0.113), b = -0.083 (se 0.113), 95% CI on b -0.306 to 0.139, LR stat 0.54
  out of sample (1,433 fit, 1,655 scored): log loss market only 1.0271, market + model 1.0273, improvement -0.0002
  b reading: NO SIGNAL

E0: 2,280 matches
  log loss  uniform 1.0986 | market 0.9597 | model 0.9823
  market only: a = 1.024
  market + model: a = 1.217 (se 0.116), b = -0.216 (se 0.119), 95% CI on b -0.450 to 0.018, LR stat 3.20
  out of sample (1,140 fit, 1,140 scored): log loss market only 0.9488, market + model 0.9483, improvement +0.0005
  b reading: NO SIGNAL

dannyboy@pop-os:~/boolean/odds$ python3 backtest/diagnostic.py --league E2 E3 --odds avgc --pool
Odds: avgc | seasons 2019-20 .. 2024-25 | model prob floor 0.01 | P(k) ~ exp(a ln market + b ln model)

E2: 3,160 matches
  log loss  uniform 1.0986 | market 1.0129 | model 1.0374
  market only: a = 1.194
  market + model: a = 1.227 (se 0.099), b = -0.043 (se 0.110), 95% CI on b -0.257 to 0.172, LR stat 0.15
  out of sample (1,504 fit, 1,656 scored): log loss market only 1.0129, market + model 1.0153, improvement -0.0024
  b reading: NO SIGNAL

E3: 3,200 matches
  log loss  uniform 1.0986 | market 1.0521 | model 1.0712
  market only: a = 1.023
  market + model: a = 1.097 (se 0.101), b = -0.113 (se 0.124), 95% CI on b -0.355 to 0.130, LR stat 0.83
  out of sample (1,544 fit, 1,656 scored): log loss market only 1.0512, market + model 1.0530, improvement -0.0018
  b reading: NO SIGNAL

POOLED E2+E3: 6,360 matches
  log loss  uniform 1.0986 | market 1.0326 | model 1.0544
  market only: a = 1.120
  market + model: a = 1.159 (se 0.071), b = -0.054 (se 0.081), 95% CI on b -0.213 to 0.106, LR stat 0.43
  out of sample (3,048 fit, 3,312 scored): log loss market only 1.0326, market + model 1.0349, improvement -0.0023
  b reading: NO SIGNAL

## 9. Line-movement proxy (added 2026-11-15)

Question: does the model's disagreement with the pre-closing price predict
the move to the closing price? Script: `backtest/line_movement.py`
(print-only; decisions in its header, fixed before any result).

### Method

- **Data**: same chains as section 8 (per-league ELO, league seeding,
  walk-forward bands), seasons 2019/20-2024/25, families `avg` and `avgc`
  loaded together. A match is used only if all three prices exist in both
  families; one-family matches are counted and excluded. The model
  probability must match across the two families' rows or the run stops.
- **Variables**, one row per match and selection: m = Avg fair, c = AvgC
  fair (proportional de-vig, the engine's `market_fair`), p = model
  probability (no floor), d = p - m, y = c - m.
- **Primary regression A**: OLS `y = a_home + a_draw + a_away + g*m +
  beta*d`. The selection and price-level controls stop a systematic drift
  (draws, longshots, or the de-vig of a lower-margin closing book) from
  reading as signal, since d is itself correlated with price level.
  Descriptive regression B: `y = a + beta*d`.
- **Errors**: CR1 cluster-robust SEs clustered by match (absorbs the exact
  dependence of a match's three rows, which sum to zero). Date-clustered
  SEs descriptive. 95% CI = beta +/- 1.96 se.
- **Tests**: no-signal, recovered signal (beta 0.3), a price-level drift
  that fools B but not A, CR1 = HC1 on singleton clusters, family pairing
  and exclusion counts, holdout unreachable.

Commands:

    python3 backtest/line_movement.py --league E2 E3 --pool
    python3 backtest/line_movement.py --league E1 EC E0

### Reading rule (pre-registered)

PRIMARY: E2 + E3 pooled, regression A, match clusters. SIGNAL = 95% CI on
beta entirely above zero. NO SIGNAL = anything else. E2 and E3 alone, E0,
E1, EC, regression B, date clusters, sign agreement and mean |y| are
descriptive only.

SIGNAL means the model's disagreement partly anticipates the close, so a
forward CLV log is worth running; it is not evidence of profit. NO SIGNAL
closes this proxy. beta is in probability units: 0.1 = the close moves 10%
of the way toward the model.

### Result: NO SIGNAL

Avg -> AvgC, 2019/20-2024/25. beta = slope of y on d; CIs are 95%.

| league | matches | excluded (AvgC only) | mean \|d\| | mean \|y\| | sign agree | A beta (se) | A 95% CI, match | A 95% CI, date | B beta, 95% CI | g on m (se) | A reading |
|---|---|---|---|---|---|---|---|---|---|---|---|
| E2 | 3,159 | 1 | 0.0420 | 0.0153 | 0.474 | −0.0228 (0.0079) | −0.0383 to −0.0072 | −0.0389 to −0.0066 | −0.0240, −0.0378 to −0.0102 | 0.0074 (0.0040) | NO SIGNAL |
| E3 | 3,195 | 5 | 0.0402 | 0.0145 | 0.486 | −0.0031 (0.0081) | −0.0189 to 0.0127 | −0.0219 to 0.0157 | −0.0075, −0.0195 to 0.0045 | 0.0071 (0.0057) | NO SIGNAL |
| **E2+E3 pooled (primary)** | 6,354 | 6 | 0.0411 | 0.0149 | 0.480 | **−0.0130 (0.0056)** | **−0.0239 to −0.0022** | −0.0258 to −0.0003 | −0.0159, −0.0251 to −0.0067 | 0.0073 (0.0033) | **NO SIGNAL** |
| E1 (descriptive) | 3,312 | 0 | 0.0416 | 0.0141 | 0.457 | −0.0117 (0.0083) | −0.0279 to 0.0045 | −0.0280 to 0.0046 | −0.0226, −0.0354 to −0.0099 | 0.0116 (0.0045) | NO SIGNAL |
| EC (descriptive) | 3,088 | 2 | 0.0392 | 0.0195 | 0.504 | 0.0432 (0.0099) | 0.0238 to 0.0626 | 0.0227 to 0.0637 | 0.0119, −0.0070 to 0.0308 | 0.0588 (0.0047) | (SIGNAL, descriptive) |
| E0 (descriptive) | 2,280 | 0 | 0.0428 | 0.0151 | 0.491 | −0.0303 (0.0088) | −0.0476 to −0.0130 | −0.0471 to −0.0135 | −0.0203, −0.0355 to −0.0050 | −0.0052 (0.0026) | NO SIGNAL |

No match was priced in Avg but not AvgC. The AvgC-only exclusions equal
the Avg rows skipped in section 8 (E2 1, E3 5, EC 2).

**Reading, applied as written.** The pooled 95% CI on beta lies entirely
below zero, so it is not above zero: NO SIGNAL. The proxy is closed.

Descriptive observations (decide nothing):

- The pooled slope is slightly negative, and the CI excludes zero under
  both clusterings. Between the Avg and AvgC snapshots the price moved
  slightly *away* from the model, by about 1% of the disagreement. That is
  tiny next to mean |d| of 0.041.
- Sign agreement is below 0.5 in E0, E1, E2 and E3: the price moved toward
  the model less often than a coin flip would.
- EC is the only league where A's CI is above zero. It is descriptive and
  outside the primary, so it carries no weight under the rule. It is also
  the only league with a large price-level term (g = 0.059, against
  0.007-0.012 elsewhere) and a larger mean |y| (0.0195), and its plain
  slope B spans zero. So its positive beta appears only once the
  price-level control is added. That looks like an EC-specific
  Avg-to-AvgC price-level effect interacting with the control, not
  match-level information. Not investigated; a follow-up would need its own
  pre-registration.

### Unverified

- The 2025/26 holdout has not been looked at, by design.
- Avg and AvgC are averages across books with possibly different book sets
  and margins; the move y includes any change in the averaging, not only
  price movement.
- The EC price-level effect above has not been explained.


## 10. Goals diagnostic: over/under 2.5 (added 2026-11-15)

Question: does the band table's over-2.5 rate know anything the O/U 2.5
market price does not? Script: `backtest/goals_diagnostic.py` (print-only;
decisions in its header, fixed before any result).

### What is being tested

The model's P(over 2.5) is the walk-forward band table's `over_25_pct` for
the match's ELO-difference band: one rate per band, no venue adjustment, no
team identity. Production prices and bets 1x2 only
(`pipeline/run_pipeline.py` `MARKETS`), so this tests the band table's goals
column, not anything production does. O/U 2.5 is the only goals line priced
in these files. The result is the baseline any later goals model has to
beat.

### Method

- **Data**: same chains as section 8, seasons 2019/20-2024/25. New loader
  families in `GOALS_FAMILIES` (kept out of `ODDS_FAMILIES` so the 1x2
  runners cannot be pointed at them): `ou25` = `Avg>2.5`/`Avg<2.5`
  (pre-closing), `ou25c` = `AvgC>2.5`/`AvgC<2.5` (closing). Same loader
  rules: a missing column is an error; a blank price skips the match for
  that family and is counted.
- **Engine**: `run()` also returns `goal_rates`, one row per priced match
  (band, band games, `over_25_pct` as of before the date, total goals). A
  separate key, not fixture-log columns, so `backtest/out/` is unchanged.
- **Inclusion**: a match is used if the engine priced it (band non-empty,
  not degenerate) and both O/U prices exist; the rest are counted.
- **Inputs**: m = O/U price de-vigged proportionally over two outcomes;
  p = `over_25_pct` clamped to [0.01, 0.99].
- **Model**: the two-outcome form of section 8's beta test, reusing
  `diagnostic.fit`/`analyse`/`verdict`: logit P(over) = a logit(m) +
  b logit(p), no intercept. Uniform log-loss baseline is ln 2.
- **Tests**: no signal on noise, real b recovered, clamp / de-vig /
  missing-price counts, new families load and a missing column errors,
  `goal_rates` equal a frozen band table and leave fixture rows unchanged,
  holdout unreachable.

Commands:

    python3 backtest/goals_diagnostic.py --league E2 E3 --pool
    python3 backtest/goals_diagnostic.py --league E1 EC E0
    python3 backtest/goals_diagnostic.py --league E2 E3 --odds ou25c --pool

### Reading rule (pre-registered)

PRIMARY: E2 + E3 pooled, `ou25` (Avg, pre-closing). SIGNAL = 95% CI on b
entirely above zero. NO SIGNAL = anything else. E2 and E3 alone, E0, E1, EC,
`ou25c`, the out-of-sample log-loss difference and mean p / mean m /
observed over rate are descriptive only.

NO SIGNAL: the band table's goals rate adds nothing to the market; goals
markets are closed for this model as it stands. SIGNAL: a goals betting
test is worth its own pre-registration and a holdout check; not evidence of
profit.

### Regression check

After the engine change, the legacy PL run left `fixtures.csv`, `bets.csv`,
`summary.csv`, `anomalies.csv` and `seeding.csv` byte-identical. The only
change was the `summary.md` heading moving from "closing" to "PRE-CLOSING":
the section 8 label correction, which was waiting for this regeneration.

### Result: NO SIGNAL

`ou25` (Avg, pre-closing), 2019/20-2024/25. Log-loss baseline: uniform
0.6931 (ln 2).

| league | matches | excl. | mean p | mean m | over rate | log loss market / model | a (market only) | b (se) | 95% CI on b | OOS improvement | reading |
|---|---|---|---|---|---|---|---|---|---|---|---|
| E2 | 3,159 | 1 | 0.4834 | 0.4882 | 0.4869 | 0.6855 / 0.6931 | 1.431 | −0.408 (0.481) | −1.351 to 0.535 | −0.0004 | NO SIGNAL |
| E3 | 3,195 | 5 | 0.4683 | 0.4727 | 0.4654 | 0.6818 / 0.6918 | 1.369 | 0.026 (0.215) | −0.395 to 0.447 | −0.0000 | NO SIGNAL |
| **E2+E3 pooled (primary)** | 6,354 | 6 | 0.4758 | 0.4804 | 0.4761 | 0.6836 / 0.6924 | 1.393 | **−0.052 (0.199)** | **−0.442 to 0.338** | −0.0000 | **NO SIGNAL** |
| E1 (descriptive) | 3,312 | 0 | 0.4770 | 0.4733 | 0.4629 | 0.6835 / 0.6903 | 1.152 | 0.317 (0.377) | −0.421 to 1.056 | −0.0001 | NO SIGNAL |
| EC (descriptive) | 3,088 | 2 | 0.5026 | 0.5162 | 0.5165 | 0.6872 / 0.6927 | 0.996 | −0.116 (0.403) | −0.907 to 0.675 | −0.0002 | NO SIGNAL |
| E0 (descriptive) | 2,280 | 0 | 0.5060 | 0.5510 | 0.5500 | 0.6720 / 0.6886 | 1.074 | −0.166 (0.271) | −0.697 to 0.365 | −0.0001 | NO SIGNAL |

`ou25c` (AvgC, closing), descriptive:

| league | matches | excl. | mean p | mean m | over rate | log loss market / model | a (market only) | b (se) | 95% CI on b | OOS improvement | reading |
|---|---|---|---|---|---|---|---|---|---|---|---|
| E2 | 3,160 | 0 | 0.4834 | 0.4854 | 0.4870 | 0.6845 / 0.6931 | 1.195 | −0.487 (0.482) | −1.432 to 0.459 | −0.0001 | NO SIGNAL |
| E3 | 3,200 | 0 | 0.4682 | 0.4671 | 0.4653 | 0.6793 / 0.6918 | 1.262 | −0.002 (0.224) | −0.441 to 0.436 | −0.0000 | NO SIGNAL |
| E2+E3 pooled | 6,360 | 0 | 0.4758 | 0.4762 | 0.4761 | 0.6819 / 0.6924 | 1.235 | −0.082 (0.208) | −0.490 to 0.325 | +0.0000 | NO SIGNAL |

The `ou25` exclusions equal the 1x2 Avg skips in section 8 (E2 1, E3 5,
EC 2); `ou25c` excluded none.

**Reading, applied as written.** The pooled 95% CI on b spans zero: NO
SIGNAL. The band table's over-2.5 rate adds nothing to the market; goals
markets are closed for this model as it stands.

Descriptive observations (decide nothing):

- The band rate is close to a coin flip: model log loss 0.6886-0.6931
  against 0.6931 uniform, with the market 0.0055-0.0166 better everywhere.
  One rate per ELO band carries almost no match-level goals information.
- Every league reads NO SIGNAL on both Avg and AvgC; no out-of-sample
  improvement exceeds 0.0001.
- E0: the band table's mean p (0.506) sits well under the observed over
  rate (0.550), which the market matched (0.551). The table pools evidence
  back to 1993/94, so it lags a change in the league's goal rate. Not
  checked by season.
- The market-only a is above 1 in E2/E3 (1.39 Avg, 1.24 AvgC): outcomes were
  more lopsided than the proportionally de-vigged O/U prices implied. This
  may be the de-vig method rather than the market; not tested.
- Any goals model built later should be compared with this baseline. A goals
  model needs per-team information (e.g. attack/defence rates); tweaking the
  band table will not supply it.

### Unverified

- The 2025/26 holdout has not been looked at, by design.
- The E0 goal-rate lag and the a > 1 pattern are not investigated.
