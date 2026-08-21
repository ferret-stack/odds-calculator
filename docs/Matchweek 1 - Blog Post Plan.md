# Matchweek 1 (2026/27) — Blog Post Plan

**Status:** Gathering complete, including the operator's pre-placement override of two selections
(§1.1). This document is a **brief for the writing agent**, not the post.
**Prepared:** 2026-08-21 · **Branch:** `claude/matchweek-1-blog-plan-6vo05u`
**Target:** Ghost post, same shape as `docs/Matchweek 27/29/31/32/33/34.md`
**Ledger state:** `data/bankroll.json` has been updated to the five-bet, £114.37 book — this reflects
the live betting record, not just a plan.

---

## 0. How to use this document

You are the **builder**. Everything below was extracted from the live repo state on 2026-08-21 and
cross-checked by re-running the model. **Every number in this document is real and verified — use
them verbatim.** If a number you want is not in here, it was not available; say so in the post or
leave it out. **Do not invent form narratives, injury news, transfer gossip, manager quotes or
match reports.** The qualitative layer (`data/qualitative/*`, `docs/August_2026_Team_News.md`,
`docs/Manager_Styles_*`) was deliberately **excluded from scope** for this post.

Three things are genuinely unresolved and need the operator's call before publication — see
**§10 Open questions**. Do not guess them.

---

## 1. The one-paragraph brief

Matchweek 1 of 2026/27. **Fresh bankroll: £1,000** (last season's £100 challenge ledger was archived
on 2026-08-21 and the new one opened at £1,000). This is the **first live run of the rebuilt
pipeline** — the automated one that prices fixtures, sizes stakes at Quarter-Kelly and writes to the
ledger itself. The model priced **seven** +EV selections; the operator then **overrode two before
placement** (see §1.1). The book that actually goes out is **five bets, all 1X2, £114.37 staked,
11.44% of bankroll**, stake-weighted expected value **+27.85%**.

**Recommended theme (strongest true angle):** *Band 1 was broken last season. This is the first week
we're betting on the fix — and the first week we've overruled it.* Five of the ten MW1 fixtures are
Band 1 (ELO gap ≤50). Band 1 is precisely where the model was inverted last season — it had the
**weaker** team winning 43.07% and the stronger 29.59%. After the Day 1 audit it reads 41.15%
stronger / 33.89% weaker, the right way round. Matchweek 1 hands us the maximum possible exposure to
the thing that was most wrong — and the two bets pulled by hand (§1.1) were both Band 1 away-side
picks, both already flagged by the Poisson cross-check as disagreeing with the band model (§5.3).
The override and the model's own cross-check point the same direction; that's worth saying plainly.
Secondary strand: **MW1 is the model's blindest week** — every "last 10 games" figure it holds is
from last season, so the honest position is that the ELO band structure is doing nearly all the work
and the Poisson layer is running on May's data.

### 1.1 Operator override — Everton/Palace and Brighton/Villa pulled before placement

Two of the seven model-selected bets were removed from the book **before either was placed with a
bookmaker** — this is a pre-placement edit to the plan, not a settled loss or a void:

- **Crystal Palace away win @ Everton (3.31, +20.19% EV, was £21.85)** — removed.
- **Aston Villa away win @ Brighton (3.09, +12.20% EV, was £14.59)** — removed.

**Operator's stated reasoning:** both are the **away** side in their fixture, and both are
higher-volatility picks than the rest of the portfolio — reducing exposure to that pair lowers the
week's variance without giving up the book's best edges. Both stay in the "bets we're not placing"
section rather than being silently dropped: the plan below documents them fully, including the
Poisson cross-check that had already flagged both as disagreeing with the band model (§5.3, §6).

Practical effect: total stake drops from £150.81 (15.08%) to **£114.37 (11.44%)**, and the
stake-weighted portfolio EV *improves* to +27.85% (from +25.22%) because the two removed bets carried
the two lowest model probabilities and the two Poisson disagreements in the portfolio.
`data/bankroll.json` has been updated to reflect this — the ledger now holds five bets (`00002`,
`00003`, `00005`, `00006`, `00007`); `00001` and `00004` were deleted outright, not marked void,
since neither was ever placed with a bookmaker.

---

## 2. Season and bankroll context

| Fact | Value | Source |
|---|---|---|
| Season | 2026/27, Matchweek 1 | — |
| Starting bankroll | **£1,000.00** | `data/bankroll.json` |
| Ledger opened | 2026-08-21 | `docs/Dev_Log_2026-27.md` |
| Bets priced/placed at | 2026-08-21 13:21:56 | `data/bankroll.json` |
| Model selections | 7 | recomputed this session |
| Bets actually on the book | **5** (two overridden pre-placement, §1.1) | `data/bankroll.json` |
| Committed to open bets | **£114.37** (11.44%) | `data/bankroll.json` |
| Remaining staking bankroll | £885.63 | `data/bankroll.json` |
| Settled bets / realised P&L | 0 / £0.00 | `data/bankroll.json` |
| Previous ledger | archived to `data/archive/bankroll-2026-05-24-test.json` (5 test bets, −£2.46) | Dev log |

**Important nuance for honesty:** all stakes were sized off the **full £1,000** in a single run (the
staking bankroll was £1,000 at that moment, nothing committed). If the same selections were priced
sequentially against a shrinking staking bankroll they would come out slightly smaller. Worth one
sentence; don't over-explain it.

**Exposure comparison:** last season's posts ran 4.0%–11.8% of bankroll. The model's raw seven-bet
output would have been 15.08% — the highest of the project so far. After the operator's override
(§1.1), the live book sits at **11.44%**, back inside last season's range and just above its top end.
Worth a line: the override didn't just cut variance, it pulled the number back into the band the
project has actually run at before.

---

## 3. THE FIVE BETS (verbatim from `data/bankroll.json` post-override, re-verified against the model)

All five are **1X2**, all **Quarter-Kelly (0.25)**, all confidence class **`standard`**. Bet IDs skip
`00001` and `00004` deliberately — those are the two overridden selections (§1.1), removed from the
ledger rather than renumbered, so the record shows exactly what happened.

| # | Fixture | KO | Selection | Odds | Implied | Model p | Fair odds | EV | Stake | % of £1,000 |
|---|---|---|---|---|---|---|---|---|---|---|
| 00002 | Nott'm Forest v Leeds | Sat 22, 14:00 | **Forest home win** | 2.30 | 43.48% | 45.72% | 2.19 | **+5.16%** | £9.92 | 0.99% |
| 00003 | Brentford v Spurs | Sat 22, 16:30 | **Brentford home win** | 2.42 | 41.32% | 54.04% | 1.85 | **+30.78%** | £54.18 | 5.42% |
| 00005 | Man City v Bournemouth | Sun 23, 13:00 | **Draw** | 4.79 | 20.88% | 22.20% | 4.50 | **+6.34%** | £4.18 | 0.42% |
| 00006 | Newcastle v Liverpool | Sun 23, 15:30 | **Newcastle home win** | 3.82 | 26.18% | 29.47% | 3.39 | **+12.58%** | £11.15 | 1.11% |
| 00007 | Fulham v Chelsea | Mon 24, 19:00 | **Fulham home win** | 3.66 | 27.32% | 37.48% | 2.67 | **+37.18%** | £34.94 | 3.49% |
| | **TOTAL** | | | | | | | | **£114.37** | **11.44%** |

**Overridden — priced by the model, not placed** (documented fully in §4 and §6):

| Fixture | Selection | Odds | Model p | EV | Would-be stake |
|---|---|---|---|---|---|
| Everton v Crystal Palace | Palace away win | 3.31 | 36.31% | +20.19% | £21.85 |
| Brighton v Aston Villa | Villa away win | 3.09 | 36.31% | +12.20% | £14.59 |

**Portfolio-level numbers, five-bet book (computed, safe to quote):**
- Stake-weighted EV: **+27.85%**
- Simple-average EV: **+18.41%**
- Expected profit if the model is right: **+£31.85**
- Expected number of winners: **1.89 of 5**
- P(at least one winner): **91.4%** (assumes independence — fixtures are separate matches, so this is fair)

**Suggested "type" labels** (matching house convention — PREMIUM / VALUE / SOLID / LONGSHOT):
- 🔥 PREMIUM: Fulham (+37.2%), Brentford (+30.8%)
- 💎 VALUE: Newcastle (+12.6%)
- 💰 SOLID: Man City draw (+6.3%), Forest (+5.2%)

---

## 4. Per-bet dossier

Everything here is verified. ELO ratings are post-offset (displayed) values from `data/current_elo.json`.
Form ratings and ΔELO come from `elo_history.json` via `calculate_form_metrics`; GF/GA are last-10 averages.
Includes the two overridden selections (§1.1) alongside the five live bets, clearly marked.

### 00003 — Brentford v Spurs · Brentford home @ 2.42 · +30.78% · £54.18 (biggest stake)
- **ELO:** Brentford 1819 (10th) v Spurs 1750 (16th) · diff **69** · **Band 2** (523-game sample: 49.33 / 24.47 / 26.20)
- **Model:** home 54.04% / draw 22.94% / away 23.01%. Market: 41.32 / 27.86 / 36.36 (overround 105.54%)
- **The edge:** the market has Spurs at 2.75 to win away at a side 69 ELO points better. The model has
  Brentford and Spurs' *win* probabilities more than double apart (54.0% vs 23.0%); the book has them
  nearly level (41.3% vs 36.4%).
- **Form:** Brentford 5.3/10 stable (ΔELO L5 +1, L10 −10), 1.1 GF / 1.2 GA. Spurs **6.9/10 improving**
  (ΔELO L5 +17, L10 −12), 1.0 GF / 1.4 GA. *Note honestly: form actually runs against this bet.*
- **H2H (10 meetings):** Spurs 5, draws 4, Brentford 1. Last: Brentford 0-0 Spurs (2026-01-01).
- **Poisson:** Brentford 1.178 xG, Spurs 0.751 xG → home 45.98% / draw 30.53% / away 23.49%. Poisson
  **softens but does not contradict** the bet (+11.3% EV rather than +30.8%). Modal score **1-0 (17.12%)**.
- **Advisory flag fired:** edge ≥20% (`implausible_edge`, non-blocking).

### 00007 — Fulham v Chelsea · Fulham home @ 3.66 · +37.18% · £34.94 (biggest edge)
- **ELO:** Fulham 1790 (12th) v Chelsea 1822 (9th) · diff **32** · **Band 1** (666 games: 40.99 / 25.08 / 33.93)
- **Model:** home 37.48% / draw 26.21% / away 36.31%. Market: 27.32 / 26.67 / 53.19 (**overround 107.18%** — the week's fattest)
- **The edge:** the book prices Chelsea at 1.88 — a 53% favourite — on a 32-point ELO gap. The model
  says a 32-point gap is a coin-flip with a slight nod to the visitor. That's a 17-point disagreement.
- **Form:** Fulham 5.4/10 stable (ΔELO L5 +4), 0.7 GF / 0.9 GA — the lowest scoring rate in the whole
  slate. Chelsea **3.6/10 declining**, ΔELO L5 **−25**, L10 **−53** — the worst 10-game ELO slide of
  any side in these ten fixtures. 0.9 GF / 1.9 GA.
- **H2H (10):** Chelsea 6, Fulham 3, 1 draw. Last: **Fulham 2-1 Chelsea (2026-01-07)** — Fulham did
  exactly this last season.
- **Poisson:** Fulham 1.017 xG, Chelsea 0.507 xG → home **47.39%** / draw 34.55% / away 18.06%.
  **Poisson agrees emphatically** (+73.4% EV on this price). Modal score **1-0 (22.16%)**, with 0-0 at
  21.79% right behind — the two lowest-scoring outcomes on the entire card.
- **Advisory flag fired:** edge ≥20%.

### ⛔ OVERRIDDEN — Everton v Crystal Palace · Palace away @ 3.31 · +20.19% · was £21.85 (bet_id 00001, removed)
- **Status: priced by the model, pulled by the operator before placement.** Higher-volatility,
  away-side pick — reduce exposure. Never staked with a bookmaker. See §1.1.
- **ELO:** Everton 1778 (14th) v Palace 1780 (13th) · diff **2** — the closest fixture of the week · **Band 1**
- **Model:** home 37.48% / draw 26.21% / away 36.31%. Market: 46.08 / 29.59 / 30.21 (overround 105.88%)
- **The edge:** two teams two ELO points apart, and the book gives the home side a 16-point
  probability advantage. The model gives home advantage far less than that.
- **Form:** Everton **2.9/10 declining** (ΔELO L5 −24) — worst form rating on the card. Palace 3.6/10
  declining (ΔELO L5 −16). Two teams in matched decline.
- **H2H (12):** Everton 7, draws 4, Palace 1. Last: Palace 2-2 Everton (2026-05-10). *H2H runs against this bet.*
- **Poisson: DISAGREES.** Everton 1.835 xG v Palace 1.276 xG → home 50.53% / draw 22.84% / away
  **26.63%**, which makes the bet −11.9% EV on the Poisson view. Modal score **1-1 (10.43%)**.
- **Advisory flag fired:** edge ≥20%.
- **Write this one honestly** — it's the clearest case in the portfolio where the two models fought,
  and it's exactly the fixture the operator chose to pull. The override and the cross-check agree.

### 00006 — Newcastle v Liverpool · Newcastle home @ 3.82 · +12.58% · £11.15
- **ELO:** Newcastle 1826 (7th) v Liverpool 1895 (3rd) · diff **69** · **Band 2**
- **Model:** home 29.47% / draw 26.04% / away 44.49%. Market: 26.18 / 26.04 / 53.48 (overround 105.70%)
- **The edge:** modest and clean — the model agrees Liverpool are favourites, it just doesn't think
  they're 53% favourites at St James'. Note the draw is priced *dead on* the model (−0.01% EV): a nice
  detail showing the book gets things right more often than not.
- **Form:** Newcastle 5.6/10 stable. Liverpool **3.5/10 declining**, ΔELO L5 −22, L10 −36. 1.6 GF / 1.6 GA.
- **H2H (12):** Liverpool 9, draws 3, **Newcastle 0**. Last: Liverpool 4-1 Newcastle (2026-01-31).
  *H2H is brutal against this bet — say so.*
- **Poisson: agrees strongly.** Newcastle 1.591 v Liverpool 1.301 → home **44.18%** / draw 24.53% /
  away 31.29% (+68.8% EV on this price). Modal score **1-1 (11.48%)**.

### ⛔ OVERRIDDEN — Brighton v Aston Villa · Villa away @ 3.09 · +12.20% · was £14.59 (bet_id 00004, removed)
- **Status: priced by the model, pulled by the operator before placement.** Higher-volatility,
  away-side pick — reduce exposure. Never staked with a bookmaker. See §1.1.
- **ELO:** Brighton 1823 (8th) v Villa 1859 (5th) · diff **36** · **Band 1**
- **Model:** home 37.48% / draw 26.21% / away 36.31%. Market: 45.45 / 28.09 / 32.36 (overround 105.91%)
- **Form:** Brighton **3.3/10 declining** (ΔELO L5 −18). Villa 5.4/10 stable (ΔELO L5 +1, L10 −17).
  1.8 GF / 1.9 GA — Villa score and concede freely.
- **H2H (11):** **Villa 8**, Brighton 2, 1 draw. Last: Villa 1-0 Brighton (2026-02-11). *H2H supports this bet
  — the one dissent from the pattern; the override held anyway on volatility grounds, not on this data.*
- **Poisson: DISAGREES, hardest of the seven.** Brighton 2.034 xG (highest on the card) v Villa 1.239
  → home 55.77% / draw 21.33% / away **22.90%** (−29.2% EV) — the single largest band/Poisson gap in
  the whole slate. Modal score **2-1 (9.71%)**; the Super 6 tie-break rule pulls the pick down to
  1-1 (9.55%).

### 00005 — Man City v Bournemouth · Draw @ 4.79 · +6.34% · £4.18 (smallest stake)
- **ELO:** Man City 1987 (2nd) v Bournemouth 1830 (6th) · diff **157** · **Band 4** (282 games: 57.80 / 24.11 / 18.09)
- **Model:** home 62.19% / draw 22.20% / away 15.61%. Market: 68.49 / 20.88 / 16.69 (overround 106.06%)
- **The edge:** thin, and the smallest stake of the week reflects it. The model doesn't dispute City
  win this — it disputes that they win it 68% of the time.
- **Form:** City **3.9/10 stable** (ΔELO L5 −11) — a notably unremarkable rating for the second-ranked
  side. Bournemouth **6.0/10 improving** (ΔELO L5 +17, L10 **+35**), 1.4 GF / **0.8 GA** (best defensive
  10-game record in the slate).
- **H2H (8):** City 7, 1 draw, Bournemouth 0. Last: **Bournemouth 1-1 Man City (2026-05-19)** — the
  one draw is the most recent meeting. Good detail.
- **Poisson: agrees strongly.** City 1.101 xG v Bournemouth 0.876 → home 40.52% / **draw 30.79%** /
  away 28.69% (+47.5% EV on the draw). Modal score **1-0 (15.25%)**.
  **Caveat to state:** a 1.10 xG for Man City is Poisson reading Bournemouth's 0.8 GA and City's
  end-of-last-season output; treat it as a directional nudge, not a forecast.

### 00002 — Nott'm Forest v Leeds · Forest home @ 2.30 · +5.16% · £9.92 (thinnest edge)
- **ELO:** Forest 1798 (11th) v Leeds 1752 (15th) · diff **46** · **Band 1** *(and the only Band 1 bet
  where the stronger side is at home — hence a different probability triplet)*
- **Model:** home 45.72% / draw 23.94% / away 30.34%. Market: 43.48 / 29.94 / 32.26 (overround 105.68%)
- **The edge:** barely clears the +5% floor. This is the "the book is nearly right" bet of the week.
- **Form:** Forest **6.7/10 improving** — best form rating in the slate — ΔELO L5 **+28**, L10 **+57**
  (biggest 10-game gain of anyone here), 2.2 GF / 1.0 GA (best attacking record in the slate).
  Leeds 5.3/10 stable (ΔELO L5 +7, L10 +22), 1.2 GF / 0.9 GA.
- **H2H (4):** 2-2, no draws. Last: **Leeds 3-1 Forest (2026-02-06)**.
- **Poisson: agrees strongly.** Forest 1.514 xG v Leeds 0.751 → home **55.48%** / draw 26.00% / away
  18.52% (+27.6% EV). Modal score **1-0 (15.72%)**.
- **Nice line for the post:** this is the bet with the thinnest model edge and the strongest
  fundamentals behind it. The band model can't see that Forest are the form team; Poisson can.

---

## 5. The Poisson section (the post must include this)

### 5.1 How it works — explain it plainly, once

For each side the model takes its **last 10 Premier League matches** and forms two indices against a
league baseline of **1.4384 goals per team per game** (from 2,215 matches, 2020-09-12 → 2026-05-24):

- attack index = (team's L10 goals **for** ÷ 1.4384)
- defence index = (team's L10 goals **against** ÷ 1.4384)

Then expected goals for the fixture:

```
home λ = home_attack × away_defence × 1.4384 × 1.1
away λ = away_attack × home_defence × 1.4384 × 0.9
```

The two λ values generate an independent-Poisson **16×16 score grid (0–15 goals a side, 6 decimal
places)**. Summing the cells gives every market: below the diagonal is a home win, the diagonal is
the draw, above it the away win; the anti-diagonals give over/under; everything from 1-1 outward
gives BTTS.

**Worth a sentence in the post:** that grid used to be 6×6, which silently threw away up to **7.5%**
of the probability mass on high-scoring fixtures. It was widened and revalidated this month — worst
deviation from 1.0 across the ten real MW1 fixtures is now **0.000004**. (`tools/validate_poisson.py`,
dev log 2026-08-21.)

### 5.2 The honest framing — do not skip this

**Poisson does not price the bets.** The +EV engine that produced all seven selections is the
**ELO-band + venue-adjustment model** — band base rates from ~2,200 historical matches, multiplied by
HOME ×1.11 / AWAY ×0.89 (draws ×0.95 home-stronger / ×1.05 away-stronger), then normalised.
`calculate_poisson()` currently has **no callers in the pricing path**; it exists for Super 6 and for
exactly the kind of cross-check this post is doing. Presenting Poisson as the source of the bets
would be false.

Two further caveats the post should carry:

1. **At Matchweek 1 every "last 10" is last season's.** All ten fixtures' Poisson inputs end
   2026-05-24. There is no current-season signal in the numbers at all. Ipswich are worse still —
   their last 10 PL matches end **2025-05-25**, because they weren't in the division last season.
2. **Two teams have no Poisson at all.** Hull City and Coventry City have no Premier League match
   history in the dataset, so no attack/defence index exists. They're carried on a **seeded ELO of
   1685** — the average current rating of last season's bottom four (Spurs 1744, West Ham 1733,
   Wolves 1648, Burnley 1615). The band model can price them; Poisson can't.

### 5.3 The cross-check table — the centrepiece of the section

The five bets actually on the book:

| Bet | Odds | Band model p | Band EV | Poisson p | Poisson EV | Verdict |
|---|---|---|---|---|---|---|
| Forest home v Leeds | 2.30 | 45.72% | +5.16% | 55.48% | **+27.60%** | ✅ Reinforces |
| Brentford home v Spurs | 2.42 | 54.04% | +30.78% | 45.98% | **+11.27%** | ⚠️ Softens |
| Draw, City v Bournemouth | 4.79 | 22.20% | +6.34% | 30.79% | **+47.48%** | ✅ Reinforces |
| Newcastle home v Liverpool | 3.82 | 29.47% | +12.58% | 44.18% | **+68.77%** | ✅ Reinforces |
| Fulham home v Chelsea | 3.66 | 37.48% | +37.18% | 47.39% | **+73.44%** | ✅ Reinforces |

**Scoreboard on the live book: Poisson reinforces 4, softens 1, contradicts 0.** Every bet still on
the ledger has Poisson agreeing with the direction of the band model, even where it disagrees on the
size of the edge (Brentford).

The two selections the operator pulled before placement are exactly the two the cross-check disagreed
with:

| Bet (overridden, not placed) | Odds | Band model p | Band EV | Poisson p | Poisson EV | Verdict |
|---|---|---|---|---|---|---|
| Palace away @ Everton | 3.31 | 36.31% | +20.19% | 26.63% | **−11.85%** | ❌ Conflict |
| Villa away @ Brighton | 3.09 | 36.31% | +12.20% | 22.90% | **−29.23%** | ❌ Conflict |

Worth stating plainly: **the operator's override and the model's own cross-check landed on the same
two fixtures independently.** The override was made on a volatility/exposure basis (§1.1), not by
reading this table — but the table is a good after-the-fact sanity check on that call, and the post
should say so rather than claim more foresight than there was. A useful framing for why these two, and
not others, disagree: both are **Band 1 away-team bets where the home side has the better recent
scoring record**. The band model can't see that, by construction — it prices the band, not the teams.

Optional extra column if you want it — a 50/50 blend of the two models, which is *not* a live model
output but is arithmetically honest if labelled as an illustration:
Forest +16.38%, Brentford +21.03%, City draw +26.91%, Newcastle +40.67%, Fulham +55.31%
(and, for the overridden pair: Palace +4.17%, Villa **−8.52%**).

### 5.4 Poisson expected goals and modal scorelines — full slate

| Fixture | Home xG | Away xG | Modal score | P(modal) | Runner-up |
|---|---|---|---|---|---|
| Everton v Crystal Palace | 1.835 | 1.276 | 1-1 | 10.43% | 2-1 (9.57%) |
| Ipswich v Sunderland | 1.071 | 1.952 | 1-1 | 10.17% | 1-2 (9.93%) |
| Nott'm Forest v Leeds | 1.514 | 0.751 | 1-0 | 15.72% | 2-0 (11.90%) |
| Brentford v Spurs | 1.178 | 0.751 | 1-0 | 17.12% | 0-0 (14.54%) |
| Brighton v Aston Villa | 2.034 | 1.239 | 2-1 | 9.71% | 1-1 (9.55%) |
| Man City v Bournemouth | 1.101 | 0.876 | 1-0 | 15.25% | 0-0 (13.85%) |
| Newcastle v Liverpool | 1.591 | 1.301 | 1-1 | 11.48% | 2-1 (9.13%) |
| Fulham v Chelsea | 1.017 | 0.507 | 1-0 | 22.16% | 0-0 (21.79%) |
| Arsenal v Coventry City | — | — | — | — | no PL history (promoted) |
| Hull City v Man Utd | — | — | — | — | no PL history (promoted) |

### 5.5 Goals and BTTS markets — flagged, NOT bet

The pipeline currently prices **1X2 only** (`MARKETS` in `pipeline/run_pipeline.py` covers home, draw,
away — nothing else). So the over/under and BTTS numbers below are the Poisson grid held up against
the book's prices; **they are not bets and must not be presented as bets.** They are, however, a good
"here's what the model can see but isn't yet allowed to act on" section.

| Fixture | Market | Poisson p | Book | Notional EV |
|---|---|---|---|---|
| Fulham v Chelsea | Under 2.5 | 80.28% | 2.27 | +82.2% |
| Fulham v Chelsea | BTTS No | 74.62% | 2.38 | +77.6% |
| Man City v Bournemouth | Under 2.5 | 68.28% | 2.79 | +90.5% |
| Brentford v Spurs | Under 2.5 | 69.60% | 2.19 | +52.4% |
| Brentford v Spurs | BTTS No | 63.46% | 2.44 | +54.8% |
| Man City v Bournemouth | BTTS No | 61.05% | 2.39 | +45.9% |
| Nott'm Forest v Leeds | BTTS No | 58.81% | 2.09 | +22.9% |
| Newcastle v Liverpool | Under 2.5 | 44.78% | 2.54 | +13.7% |
| Everton v Crystal Palace | Over 2.5 | 60.14% | 1.88 | +13.1% |

**Mandatory caveat if this table is used:** these are enormous notional edges, and enormous notional
edges from a model running on May's goal data are a warning sign, not a windfall. The Fulham/Chelsea
numbers in particular come out of Fulham averaging 0.7 goals and Chelsea 0.9 over their last 10 —
a 0.507 xG for Chelsea is almost certainly too low. Last season's posts *did* bet goals markets;
this season the automated pipeline doesn't price them yet. Say that plainly.

---

## 6. Bets we're avoiding / no-bet fixtures

Five of the ten fixtures produced no live bet: two the model itself refused or fell short on, one the
market priced correctly, and **two the operator pulled by hand after the model cleared them.** The
variety is a good section, and the override pair deserves its own subsection rather than being folded
in with the model's own no-bets — the reasoning is different in kind (a risk-management call, not a
pricing one).

### 🔻 Overridden by the operator — Everton/Palace and Brighton/Villa

Both cleared the model's +5% floor, both were priced and would have been placed by the pipeline —
these are not "the model said no" fixtures. They were pulled **before either bet was placed with a
bookmaker**, on the operator's judgement that they were the two highest-volatility, away-side picks in
the portfolio and reducing exposure to that pair was worth more than keeping their edges.

- **Crystal Palace away win @ Everton, 3.31, +20.19% EV, would-be £21.85.** Poisson disagreed with
  this one outright (−11.9% EV on the Poisson view; see §5.3), and Everton's H2H record over Palace
  (7 wins to 1 in 12 meetings) ran against it too.
- **Aston Villa away win @ Brighton, 3.09, +12.20% EV, would-be £14.59.** The largest single
  band-vs-Poisson gap in the whole slate (−29.2% EV on the Poisson view), though notably H2H (Villa 8
  wins to Brighton's 2) actually supported the bet — the override held on volatility grounds, not
  because every signal agreed.

Both are documented in full in §4, both remain visible in this plan as **priced-but-not-placed**, and
`data/bankroll.json` has been edited to remove them outright — since neither reached a bookmaker, they
are deleted from the ledger rather than marked void. This is a deliberate departure from a settled
result: the post should be clear these are not losses, pushes, or bets that were later cashed out —
they simply never happened.

**Editorial note for the write-up:** this is worth a genuinely reflective paragraph, not a footnote.
It's the first week the operator has overridden the model pre-placement rather than just skipping a
model-rejected fixture, and it happens to line up with what the model's own Poisson cross-check found
independently. Say both things: the override was a risk call, not a forecast, and it turned out the
two picks pulled were also the two the cross-check most disagreed with.

### Fixtures the model itself found nothing in

### ❌ Hull City v Man Utd — blocked by a sanity check (best story of the three)
- Sat 22, 11:30. Hull City 1685 (seeded, 20th) v Man Utd 1888 (4th) · diff 203 · **Band 5** (163-game sample)
- Model: home 14.98% / draw 24.97% / away 60.06%. Market: 11.49 / 20.20 / 73.53 (overround 105.23%)
- **Hull home came out at +30.33% EV and the draw at +23.60% EV — both clear the floor.**
- The pipeline **refused both**. Two +EV selections in the same market on the same fixture are
  mutually exclusive outcomes priced against one overround; the model cannot genuinely favour both.
  The rule (`check_same_market_conflicts` in `pipeline/staking.py`) treats that as a signal to
  re-check the model, not a pair of bets. Both were blocked, stake zeroed, and left visible in the
  report rather than quietly dropped.
- Underlying cause, worth naming: the book has Man Utd at **1.36** (73.5% implied) away at a promoted
  side; the model's Band 5 base rate says 60.1%. That 13-point gap spills value into *both* other
  outcomes. Add that Hull have **no PL history at all** — they're priced entirely off a seeded 1685 —
  and refusing is clearly right.
- Man Utd context that *is* real: 7.2/10 form, ΔELO L5 **+28**, 1.9 GF / 1.2 GA.
- **This is the post's best demonstration that the system says no to itself.** Very on-brand for the
  anti-tipster positioning.

### ❌ Ipswich v Sunderland — below the floor
- Sat 22, 14:00. Ipswich 1685 (seeded, 19th) v Sunderland 1728 (18th) · diff 43 · **Band 1**
- Model: home 37.48% / draw 26.21% / away 36.31%. Market: 36.76 / 30.30 / 38.31
- Ipswich home is **+1.95% EV** — real, positive, and **below the +5% floor**, so no bet. Away −5.23%,
  draw −13.51%.
- Good line: the floor exists so that a 2% edge measured by a model that admits its own error bars
  doesn't become a stake.
- Poisson *is* available here (Ipswich have 2024-25 PL data) and reads Ipswich 1.071 v Sunderland
  1.952 — a 57.87% away win. Their attack index is the lowest in the slate (0.695) and their defence
  index the worst (1.669: 2.4 goals conceded per game). Sunderland 5.4/10 stable, ΔELO L10 +26.

### ❌ Arsenal v Coventry City — the market got it right
- Fri 21, 19:00 (**kicks off before publication — check before writing about it in future tense**)
- Arsenal 2013 (1st) v Coventry City 1685 (seeded, 21st) · diff 328 · **Band 7** — only a **50-game**
  historical sample, the thinnest band on the card
- Model: home 80.92% / draw 12.43% / away 6.65%. Market: 83.33 / 14.68 / 6.94 (overround **104.96%** — the tightest of the week)
- Every selection negative: home −2.90%, draw −15.35%, away −4.17%. Nothing to do.
- Good line: the book's sharpest price of the week is on its most predictable fixture. That's not a
  failure of the model — it's the model correctly finding nothing.

---

## 7. Method / transparency section (recommended — this is the differentiator)

Three things the post should disclose. All are true, all are already documented in the repo, and
disclosing them is exactly the anti-tipster positioning the project is built on.

### 7.1 The band model prices every fixture in a band identically
The 1X2 model is **band + venue**, not fixture-specific. Look at three of this week's bets:

| Fixture | Band | Model home | Model draw | Model away |
|---|---|---|---|---|
| Everton v Crystal Palace | 1 | 37.48% | 26.21% | 36.31% |
| Brighton v Aston Villa | 1 | 37.48% | 26.21% | 36.31% |
| Fulham v Chelsea | 1 | 37.48% | 26.21% | 36.31% |

**Identical.** All three are Band 1 fixtures with the stronger side away, so all three get the same
probability triplet — every difference in EV comes purely from the price. (Forest v Leeds is Band 1
too but has the stronger team at *home*, hence 45.72 / 23.94 / 30.34.) Worth noting: two of these
three identical-probability picks (Everton/Palace, Brighton/Villa) are the pair the operator pulled
before placement (§1.1) — only Fulham stayed on the book, and it's the one of the three where Poisson
agreed most emphatically (§5.3).

This is the known coarseness of the model, flagged in `docs/Odds_Calculator_Executable_Plan.md` as an
open Day 3+ decision. It is why Poisson is worth running as a cross-check, and it is why the
`implausible_edge` advisory exists.

### 7.2 The advisory flag fired five times this week
Any edge ≥ **+20%** raises a non-blocking advisory: against a sharp market, an edge that large is more
often a stale price or a too-coarse band than genuine value. It fired on:
Fulham home +37.2%, Brentford home +30.8%, Hull home +30.3%, Hull draw +23.6%, Palace away +20.2%.
**Two of those (Fulham, Brentford) are on the live book.** Hull's pair was blocked by a separate
sanity check regardless (§6); Palace's was one of the two the operator pulled (§1.1) — so every
fixture that tripped this advisory ended up either blocked, overridden, or is Fulham/Brentford, both
of which the Poisson cross-check also backs (§5.3). Report the flag as-is; it's a useful marker that
the two overridden bets weren't the only aggressive-looking numbers in the slate, just the two that
also lost the cross-check.

### 7.3 The staking rule, stated once
- **+5% EV minimum.** Below that, no action.
- **Quarter-Kelly (0.25 × full Kelly) as a ceiling** for standard plays. **Eighth-Kelly (0.125)** for
  hedges and low-confidence plays (Bands 9 and 10, which hold only 6 and 4 matches respectively).
- Not Half-Kelly. Older repo documents say Half-Kelly; they're superseded.
- All five live bets this week are standard-confidence Quarter-Kelly (as were the two overridden
  selections before they were pulled — the override was a judgement call layered on top of the
  staking rule, not a change to it).
- Full Kelly f* = (p·d − 1) / (d − 1). Stake = bankroll × multiplier × f*.

---

## 8. Fixture reference table (all 10, for the writer's convenience)

| Fixture | Date/KO | Home ELO | Away ELO | Diff | Band | Book H/D/A | Overround | Outcome |
|---|---|---|---|---|---|---|---|---|
| Arsenal v Coventry City | Fri 21, 19:00 | 2013 | 1685 | 328 | 7 | 1.20 / 6.81 / 14.41 | 104.96% | No edge |
| Hull City v Man Utd | Sat 22, 11:30 | 1685 | 1888 | 203 | 5 | 8.70 / 4.95 / 1.36 | 105.23% | **Blocked** |
| Everton v Crystal Palace | Sat 22, 14:00 | 1778 | 1780 | 2 | 1 | 2.17 / 3.38 / 3.31 | 105.88% | **Overridden** (was BET away) |
| Ipswich v Sunderland | Sat 22, 14:00 | 1685 | 1728 | 43 | 1 | 2.72 / 3.30 / 2.61 | 105.38% | Below floor |
| Nott'm Forest v Leeds | Sat 22, 14:00 | 1798 | 1752 | 46 | 1 | 2.30 / 3.34 / 3.10 | 105.68% | **BET home** |
| Brentford v Spurs | Sat 22, 16:30 | 1819 | 1750 | 69 | 2 | 2.42 / 3.59 / 2.75 | 105.54% | **BET home** |
| Brighton v Aston Villa | Sun 23, 13:00 | 1823 | 1859 | 36 | 1 | 2.20 / 3.56 / 3.09 | 105.91% | **Overridden** (was BET away) |
| Man City v Bournemouth | Sun 23, 13:00 | 1987 | 1830 | 157 | 4 | 1.46 / 4.79 / 5.99 | 106.06% | **BET draw** |
| Newcastle v Liverpool | Sun 23, 15:30 | 1826 | 1895 | 69 | 2 | 3.82 / 3.84 / 1.87 | 105.70% | **BET home** |
| Fulham v Chelsea | Mon 24, 19:00 | 1790 | 1822 | 32 | 1 | 3.66 / 3.75 / 1.88 | 107.18% | **BET home** |

**Band composition of the slate:** Band 1 ×5, Band 2 ×2, Band 4 ×1, Band 5 ×1, Band 7 ×1.

### Band base rates used this week (from `data/elo_bands.json`)

| Band | Range | Sample | Stronger | Draw | Weaker | O2.5 | BTTS |
|---|---|---|---|---|---|---|---|
| 1 | 0–50 | 666 (11 even) | 40.99% | 25.08% | 33.93% | 54.65% | 55.83% |
| 2 | 51–100 | 523 | 49.33% | 24.47% | 26.20% | 51.43% | 52.39% |
| 4 | 151–200 | 282 | 57.80% | 24.11% | 18.09% | 54.60% | 53.20% |
| 5 | 201–250 | 163 | 64.42% | 22.70% | 12.88% | 61.40% | 55.80% |
| 7 | 301–350 | 50 | 78.00% | 14.00% | 8.00% | 64.00% | 52.00% |

Venue adjustment applied on top: stronger-team-home ×1.11, stronger-team-away ×0.89, draw ×0.95 (home
stronger) / ×1.05 (away stronger), then renormalised.

### Full form table (last 10 matches, all from 2025/26 unless noted)

| Team | GF | GA | Form | Trend | ΔELO L5 | ΔELO L10 | Note |
|---|---|---|---|---|---|---|---|
| Nott'm Forest | 2.2 | 1.0 | 6.7 | improving | +28 | +57 | |
| Man Utd | 1.9 | 1.2 | 7.2 | improving | +28 | +28 | |
| Spurs | 1.0 | 1.4 | 6.9 | improving | +17 | −12 | |
| Arsenal | 1.5 | 0.7 | 6.4 | stable | +14 | +1 | |
| Bournemouth | 1.4 | 0.8 | 6.0 | improving | +17 | +35 | |
| Newcastle | 1.3 | 1.3 | 5.6 | stable | +4 | −8 | |
| Fulham | 0.7 | 0.9 | 5.4 | stable | +4 | −1 | |
| Aston Villa | 1.8 | 1.9 | 5.4 | stable | +1 | −17 | |
| Sunderland | 1.3 | 1.4 | 5.4 | stable | +9 | +26 | |
| Brentford | 1.1 | 1.2 | 5.3 | stable | +1 | −10 | |
| Leeds | 1.2 | 0.9 | 5.3 | stable | +7 | +22 | |
| Man City | 1.8 | 1.0 | 3.9 | stable | −11 | −1 | |
| Ipswich | 1.0 | 2.4 | 3.7 | declining | −17 | −18 | 2024/25 data |
| Crystal Palace | 1.2 | 1.6 | 3.6 | declining | −16 | −10 | |
| Chelsea | 0.9 | 1.9 | 3.6 | declining | −25 | −53 | |
| Liverpool | 1.6 | 1.6 | 3.5 | declining | −22 | −36 | |
| Brighton | 1.4 | 1.1 | 3.3 | declining | −18 | −3 | |
| Everton | 1.5 | 1.7 | 2.9 | declining | −24 | −13 | |
| Hull City / Coventry City | — | — | — | — | — | — | no PL history |

---

## 9. Format spec (derived from `docs/Matchweek 27/29/31/32/33/34.md`)

**Voice:** first-person plural ("we're backing", "we're skipping"), dry, self-deprecating, numerate.
Occasional extended joke simile is on-brand (MW34's "if Fulham were a spice, they'd be flour"), but
one or two per post, not per section. Never hype. Losses are reported as flatly as wins. The
recurring refrain is *the framework working as intended*.

**Section order** (the mature template, MW29/31/32):

1. `# Matchweek 1` — optional Ghost front-matter line, as MW27 used:
   `## layout: post title: Matchweek 1 - <subtitle> date: 2026-08-21 ferret: boolean author: Ferret Stack`
2. `## 🧠 This week's best bets` — blockquote **TL;DR** paragraph, then a second blockquote line:
   `> 💰 **Total Portfolio EV:** … 📊 **Bankroll Stake:** … ⚡ **Theme:** …`
3. Boilerplate block (reuse verbatim):
   ```
   **See for yourself:** [Odds Calculator](https://ferret-stack.github.io/boolean/odds-calculator/)

   Consider **following me on X** for live-play updates and betting thoughts

   - Click [here](https://x.com/intent/post?text=@Ferret_Stack%20I%20still%20call%20it%20Twitter) if you still call it "Twitter"
   - Click [here](https://x.com/intent/post?text=@Ferret_Stack%20I%20call%20it%20X) if you call it "X"
   ```
4. `## 📊 Matchweek N−1 review` — **replace for MW1** with a short season-opener / last-season-recap.
   See §10.
5. `## 📈 The bigger picture` — the running table. See §10 for what it should contain this week.
6. `## 🎯 This week's theme: <name>`
7. Per-bet sections, one each, headed with a type emoji:
   `## 🔥 PREMIUM PICK: …` / `## 💎 VALUE PICK: …` / `## 💰 SOLID PICK: …` / `## 🎰 LONGSHOT: …`
   Each opens with the stat block, on one line, bolded labels:
   `**The Bet:** <selection> @ **X.XX** (NN.N% implied) **Model Probability:** NN.N% (fair odds: X.XX) **Expected Value:** **+NN.N%** **Stake:** Quarter-Kelly **Confidence:** High`
   then 2–5 short paragraphs of reasoning.
8. **NEW for this post:** `## 🎲 The Poisson cross-check` — §5 above. Place it after the individual
   picks and before the skips, so readers have the bets in mind when the disagreements land.
9. `## ❌ Bets we're skipping` — §6 above.
10. `## 🎯 Super 6 predictions` — optional, see §10.
11. `## 📋 Portfolio summary` — the table below.
12. `## Final thoughts`
13. Footer, verbatim:
    `_All probabilities calculated using 5+ seasons of Premier League data (2,000+ matches). ELO ratings updated weekly. Model performance tracked publicly at [ferret-stack.github.io](https://ferret-stack.github.io)._`

**Portfolio summary table — ready to paste:**

| Bet | Odds | EV% | Sizing | % of Bankroll | Type |
| --- | --- | --- | --- | --- | --- |
| 🔥 Fulham Win vs Chelsea | 3.66 | +37.2% | Quarter-Kelly | 3.5% | PREMIUM |
| 🔥 Brentford Win vs Spurs | 2.42 | +30.8% | Quarter-Kelly | 5.4% | PREMIUM |
| 💎 Newcastle Win vs Liverpool | 3.82 | +12.6% | Quarter-Kelly | 1.1% | VALUE |
| 💰 Man City / Bournemouth Draw | 4.79 | +6.3% | Quarter-Kelly | 0.4% | SOLID |
| 💰 Nott'm Forest Win vs Leeds | 2.30 | +5.2% | Quarter-Kelly | 1.0% | SOLID |
| **TOTAL** | — | **+27.9% (wtd)** | — | **11.4%** | — |

Consider a short note beneath the table naming the two overridden picks and their would-be sizing
(Crystal Palace Win @ Everton, 3.31, +20.2%, would-be 2.2%; Aston Villa Win @ Brighton, 3.09, +12.2%,
would-be 1.5%) so the table is legible against the "seven bets" the model actually found, not just
the five that made the book.

**Length:** MW32 (~2,100 words) is the right target. MW33/34 are shorter, MW29 longer. The override
section (§1.1/§6) adds genuine new material this week, so err toward the longer end.

---

## 10. Open questions — operator input needed before publication

These cannot be resolved from the repo. **Do not fabricate answers.**

1. **Last season's closing result.** The matchweek docs stop at MW34 (cumulative: 80 bets, 26 wins,
   ~£138 staked, **+£0.88**, bankroll £100.88 heading into MW34). MW35–38 aren't in the repo and the
   old ledger was archived as a *test* file, not the real season record. The post needs either
   (a) the real closing figure from the operator, or (b) a deliberate choice not to give one.
   **Recommendation:** open with "last season's £100 challenge finished roughly where it started —
   the point of it was the process, not the pot" only if the operator confirms; otherwise skip the
   recap entirely and open on the reset.
2. **The bigger-picture table.** Last season's table can't continue — different bankroll, different
   base. **Recommendation:** start a fresh 2026/27 table with a single MW1 row
   (`MW1 | £1,000.00 | 5 | — | £114.37 | — | —`) and one line pointing back at last season. If the
   table format allows a notes column, use it to record the two overridden selections rather than
   showing "7" bets and confusing the reader about what was actually staked.
3. **Super 6.** Sky picks the six fixtures and we don't know which. §5.4 has modal scorelines for the
   eight fixtures where Poisson is available; Arsenal v Coventry and Hull v Man Utd cannot be given
   one. **Recommendation:** include the section only if the operator confirms the slate, and note the
   tie-break rule (any scoreline within 0.5 percentage points of the argmax counts as tied; fewest
   total goals wins, then highest probability, then lowest home/away goals). It fired twice this week:
   Brighton v Villa 2-1 → **1-1**, Fulham v Chelsea 1-0 → **0-0**.
4. **Free-bet / promo income.** Previous posts carried a recurring note about bookmaker free spins
   muddying the bankroll arithmetic. Unknown whether that continues at £1,000. Omit unless told.
5. **Publication timing.** Arsenal v Coventry (Fri 21, 19:00) may already have been played. Write it
   in a tense that survives either way, or confirm the publish time.

---

## 11. Do-not-do list

- ❌ Don't present Poisson as the source of the bets. It isn't. (§5.2)
- ❌ Don't bet or imply bets on over/under or BTTS. The pipeline doesn't price them. (§5.5)
- ❌ Don't use any card/booking-point figures. The card data is known-unreliable — 380 fabricated
  zero-card rows were purged this month and the historical rows still on disk were never repaired.
  Several L10 booking averages in the current data are visibly implausible.
- ❌ Don't invent team news, injuries, transfers, manager quotes or pre-season results. Not in scope,
  not in the data.
- ❌ Don't describe the promoted sides' quality. Hull City, Coventry City and Ipswich are on a **seeded
  rating**, and the post should say so rather than characterise them.
- ❌ Don't claim last season's results beyond MW34 (see §10.1).
- ❌ Don't round away the awkward numbers. Two Poisson conflicts, five advisory flags, the model's
  raw 15.08% exposure before the override — those are the post's credibility.
- ❌ Don't call the two overridden bets "losses," "voids," or present them as settled in any way.
  They were never placed. Say "overridden," "pulled," or "not placed" — not "we lost on."
- ❌ Don't imply the override was model-driven. It was the operator's risk-management judgement
  (higher volatility, away-side exposure), made independently of the Poisson cross-check — the fact
  that the cross-check agrees afterward is a good observation, not the stated reason for the call.

---

## 12. Source index

| Claim type | File |
|---|---|
| The five live bets (and the two overridden ones), stakes, EV, bankroll | `data/bankroll.json` |
| Fixtures, kick-off times, bookmaker odds | `data/upcoming_fixtures.json` |
| ELO ratings and ranks | `data/current_elo.json` |
| Band base rates and sample sizes | `data/elo_bands.json` |
| Venue multipliers | `elo_calculator.py` (1.11 / 0.89 / 0.95 / 1.05); `data/venue_adjustment.json` holds the empirical 1.099 / 0.897 |
| Form ratings, ΔELO | `data/elo_history.json` via `calculate_form_metrics` |
| H2H records | `data/h2h_records.json` |
| Poisson method | `odds_calculator.py::calculate_poisson`; validated by `tools/validate_poisson.py` |
| Super 6 tie-break rule | `tools/super6_picks.py`; dev log 2026-08-21 |
| Staking rule, sanity checks | `pipeline/staking.py` |
| Pipeline behaviour, duplicate guard | `pipeline/run_pipeline.py` |
| Band 1 bug history, promoted-team seeding | `docs/Odds_Calculator_System_Source_of_Truth.md`, `docs/Dev_Log_2026-27.md` |
| Bankroll reset | `docs/Dev_Log_2026-27.md` (2026-08-21) |
| Format and voice | `docs/Matchweek 27/29/31/32/33/34.md` |

All model figures in this document were regenerated on 2026-08-21 by re-running
`python3 -m pipeline.run_pipeline --dry-run` and an independent Poisson recomputation against the
same data files; the dry run reproduced all seven model selections and their EVs exactly. Two of
those seven (Everton/Palace, Brighton/Villa) were subsequently removed from `data/bankroll.json` by
the operator, pre-placement, per §1.1 — the five-bet portfolio totals in §3 were recomputed directly
from the edited ledger and cross-checked by hand (£114.37 staked, +27.85% stake-weighted EV,
1.89 expected winners, 91.4% P(≥1 winner)).
