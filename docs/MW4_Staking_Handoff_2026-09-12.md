# Matchweek 4 — Staking & Portfolio Handoff

**Prepared:** 2026-09-10 · **Pipeline run:** 2026-09-08 15:02 (commit `a9095f5` "MW4 run")
**Fixtures covered:** 2026-09-12 → 2026-09-14 · **Repo:** `ferret-stack/odds-calculator`

> **Purpose of this document.** It is a self-contained quantitative briefing for a
> separate chat with no access to this repository. Everything needed to argue about
> final staking and portfolio construction is below: the model's numbers, the
> independent cross-check, the bankroll's realised record, and the four material
> problems with the run as it currently stands. Nothing here has been rounded away
> or softened.

---

## 0. Read this first — four flags before any staking discussion

These are not stylistic quibbles. Each one changes the numbers you would discuss.

### FLAG 1 (critical) — the MW4 run executed against stale code; the exposure caps never fired

The five pending bets were sized by a build that predates the staking-caps work.
Verified from git, not inferred:

| Check | Result |
| --- | --- |
| `a9095f5` ("MW4 run") parent | `16c949f` ("MW3 run", 2026-08-31) — **not** the caps merge |
| Caps merge `19f06bb` landed on `main` | 2026-09-01, i.e. **7 days before the run** |
| `pipeline/confidence.py` present at `a9095f5` | **No** |
| Occurrences of `MAX_STAKE_FRACTION` in `pipeline/staking.py` at `a9095f5` | **0** |

The operator ran the pipeline from a local checkout that had never pulled the
caps commit. Three rules were therefore silently absent from this week's sizing:

1. **Per-bet cap** — no stake above 3% of the staking bankroll.
2. **Weekly cap flag** — a week's total above 12% is surfaced for an operator decision.
3. **Implausible-edge downgrade** — an edge ≥ +20% EV is sized at Eighth-Kelly (0.125),
   not Quarter-Kelly (0.25), because such an edge is far more often a stale price or a
   model artefact than genuine value.

**Quantified impact.** Re-pricing the identical five selections with the current
code, at the same staking bankroll that was live at placement time (£923.68 —
settled balance, nothing yet committed):

| | As placed | Rule-compliant | Delta |
| --- | ---: | ---: | ---: |
| Total staked | **£145.88** | **£99.85** | **−£46.03 (−31.6%)** |
| % of staking bankroll | **15.79%** | **10.81%** | −4.98 pts |
| Weekly cap (12% = £110.84) | **breached by £35.04** | within cap | — |
| Stakes exceeding the 3% per-bet cap (£27.71) | **3 of 5** | 0 | — |

The £145.88 figure is 46.1% larger than the rule permits. This is the single most
important number in this document.

### FLAG 2 (critical) — Spurs v Everton is 5.6% of bankroll on the exact edge profile the rules exist to shrink

Bet `00018` (Everton away @ 3.59) carries a **+58.14% modelled edge** and was staked
at **£51.84 — 5.61% of the £923.68 bankroll, 1.87× the per-bet cap.** It is the
largest stake on the card.

Under the current rules that edge triggers the `large_edge` class, halving the
multiplier from 0.25 to 0.125 → **£25.92**. An edge of +58% against a mainstream
1x2 price on a Premier League fixture is, on its face, not credible; it is
precisely the signal the downgrade was written for. Instead it received the
biggest stake of the week.

### FLAG 3 (significant) — 57% of the money sits on one unhedged model hypothesis

Two of the five bets are not merely correlated — they are **structurally the same bet**:

| Bet | Fixture | Band | Structure | Model p |
| --- | --- | --- | --- | --- |
| `00018` | Spurs (1736) v Everton (1790) | 2 | stronger side **away**, 54-pt gap | 0.4405 |
| `00020` | Leeds (1766) v Newcastle (1833) | 2 | stronger side **away**, 67-pt gap | 0.4405 |

Identical model probability to four decimal places, because both resolve to the
same cell of the band table: Band 2 `stronger_win_pct` 0.4887, × away multiplier
0.898, normalised. Combined stake **£83.34 = 57.1% of the week's exposure.**

The pipeline's correlation check operates **within a single fixture only**. It
cannot see this, and it did not flag it. If "Band 2 stronger-side-away is
underpriced by the market" is wrong this week, over half the portfolio fails
together for one reason.

Historical exposure to this same structure (model p ≈ 0.4405–0.4449): bets
`00008` (won), `00009` (lost), `00011` (lost) — **1W/2L, £111.48 staked,
£126.12 returned, +£14.64.** Sample of three. Directionally nothing, but it is
the third consecutive week the ledger has leaned on this cell.

### FLAG 4 (data quality) — the Poisson cross-check is degenerate on two fixtures, and empty of qualitative input on all ten

- **Coventry City** `last_10_avg_goals_for = 0.0` and **Hull City**
  `last_10_avg_goals_against = 0.0`. A zero in either index collapses the Poisson
  grid: `Chelsea v Hull City` returns **P(Chelsea win) = 0.0000** and
  **BTTS = 0.0000**; `Coventry v Brighton` returns **P(Coventry win) = 0.0000**.
  Both are nonsense outputs, not low probabilities. Neither fixture is staked, so
  no money is at risk — but any Poisson reading of those two rows must be discarded,
  and this is the same unguarded-input class as the NaN fix in `8cc1134`.
- **`data/qualitative/team_news.json` and `formations.json` are both empty**
  (`"teams": {}`). Team news is documented in-repo as *"the input that goes stale
  fastest"* and it was not refreshed. There is **no injury, suspension or
  availability overlay on any of these ten fixtures.**
- **No referee assignment exists per fixture.** `referee_stats.json` is a lookup
  table only. Booking-points context is unavailable this week.
- **Prices are ~2 days stale.** Odds were captured 2026-09-08; this briefing is
  dated 2026-09-10 with the first kick-off 2026-09-12 14:00. Re-check every price
  before staking, most urgently the three edges above +20%.

---

## 1. Bankroll state

Source: `data/bankroll.json`, `updated_at` 2026-09-08 15:02:16.

| Metric | Value |
| --- | ---: |
| Starting bankroll (season, opened 2026-08-21) | £1,000.00 |
| Current bankroll | **£923.68** |
| Committed to open bets | £145.88 |
| **Staking bankroll** (settled less committed) | **£777.80** |
| Realised P/L | **−£76.32** |
| Bankroll growth | **−7.63%** |
| Bets total / settled / open / void | 20 / 15 / 5 / 0 |
| Settled record | **3W – 12L** |
| Strike rate | **20.0%** |
| Total staked (settled) | £368.86 |
| **Realised ROI** | **−20.69%** |

### Calibration: model claim vs. realised outcome

This is the most important diagnostic for the discussion.

| Metric (15 settled bets) | Model expected | Actual | Gap |
| --- | ---: | ---: | ---: |
| Wins | **5.71** | **3** | −2.71 |
| P/L | **+£101.98** | **−£76.32** | **−£178.30** |
| ROI on £368.86 staked | **+27.65%** | **−20.69%** | **−48.34 pts** |

**P(3 or fewer wins | model probabilities) = 0.1086** — exact Poisson-binomial over
the 15 individual probabilities.

Read this carefully. A ~11% tail event is **not** statistical proof the model is
broken; roughly one season-start in nine looks like this from pure variance. But
neither is it comfortable, and it is the second data point in the same direction:
the model has claimed a stake-weighted +27.65% edge and delivered −20.69%. Fifteen
bets cannot separate "unlucky" from "systematically overconfident". **The correct
posture is that stake sizing should be treated as the lever, not the edge estimate**
— which is exactly what the caps in Flag 1 do, and exactly what was bypassed.

Supporting cuts (small samples, stated as such):

| Cut | Bets | Staked | Exp. wins | Actual | Returned | P/L | ROI |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Edge ≥ +20% EV | 8 | £306.35 | 3.38 | 2 | £257.24 | −£49.11 | −16.0% |
| Edge < +20% EV | 7 | £62.51 | 2.33 | 1 | £35.30 | −£27.21 | −43.5% |
| Selection = home | 6 | — | — | 2W/4L | — | — | — |
| Selection = draw | 2 | — | — | 0W/2L | — | — | — |
| Selection = away | 7 | — | — | **1W/6L** | — | — | — |

Both edge buckets underperformed expectation. The away-selection record (1W/6L) is
worth noting given **three of this week's five bets are away selections.**

### Week-by-week ledger

| Placed | Bets | Staked | % of then-bankroll | Wtd EV | Wins | Settled P/L |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-08-21 (MW1) | 5 | £114.37 | 11.44% | +27.85% | 1 | **+£16.75** |
| 2026-08-26 (MW2) | 6 | £153.76 | 15.12% | +28.18% | 1 | **−£27.64** |
| 2026-08-31 (MW3) | 4 | £100.73 | 10.18% | +26.62% | 1 | **−£65.43** |
| 2026-09-08 (MW4) | 5 | £145.88 | 15.79% | +31.67% | — | *pending* |

MW2 (15.12%) and MW4 (15.79%) both breach the 12% weekly cap. MW4 also carries the
**highest weighted EV of the season (+31.67%)** — which, given the table above, is a
warning sign rather than a selling point.

---

## 2. The five open bets

Staking bankroll at sizing: **£923.68.** Per-bet cap £27.71 (3%). Weekly cap £110.84 (12%).

| # | Fixture | KO | Sel | Odds | Implied | Model p | Fair | EV | Full Kelly | **Placed** | **Should be** | Class |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 00016 | Aston Villa v Nott'm Forest | 12th 14:00 | home | 2.17 | 46.08% | 0.5357 | 1.87 | **+16.25%** | 0.1389 | £32.07 | **£27.71** | standard, *capped* |
| 00017 | Bournemouth v Brentford | 12th 14:00 | home | 2.47 | 40.49% | 0.4540 | 2.20 | **+12.14%** | 0.0826 | £19.07 | **£19.07** | standard |
| 00018 | Spurs v Everton | 12th 16:30 | away | 3.59 | 27.86% | 0.4405 | 2.27 | **+58.14%** | 0.2245 | £51.84 | **£25.92** | **large_edge** |
| 00019 | Man Utd v Man City | 13th 15:30 | away | 2.11 | 47.39% | 0.4999 | 2.00 | **+5.48%** | 0.0494 | £11.40 | **£11.40** | standard |
| 00020 | Leeds v Newcastle | 14th 19:00 | away | 2.84 | 35.21% | 0.4405 | 2.27 | **+25.10%** | 0.1364 | £31.50 | **£15.75** | **large_edge** |
| | | | | | | | | | **Total** | **£145.88** | **£99.85** | |

Sizing rule: `stake = bankroll × multiplier × full_kelly`, then clamped to the
per-bet cap. `multiplier` = 0.25 (standard) or 0.125 (hedge / low-confidence /
large-edge). EV floor +5%. There is deliberately no 0.5 multiplier anywhere —
this is **not** Half-Kelly, and older repo documents saying otherwise are superseded.

### Statistical robustness of each edge

The band probabilities come from finite historical samples. Below, each edge is
re-tested at the **lower bound of its 95% Wilson interval** — i.e. "if the band's
true rate is at the pessimistic end of what the sample supports, does this bet
still have positive expectation?"

| # | Bet | Band | n | Wilson 95% | Break-even p (1/odds) | Model p | **EV at lower bound** | Survives? |
| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | --- |
| 00016 | Villa home | 2 | 532 | [0.4894, 0.5822] | 0.4608 | 0.5357 | **+6.20%** | ✅ |
| 00017 | Bournemouth home | 1 | 676 | [0.4134, 0.4959] | 0.4049 | 0.4540 | **+2.11%** | ⚠️ marginal |
| 00018 | Everton away | 2 | 532 | [0.4024, 0.4787] | 0.2786 | 0.4405 | **+44.46%** | ✅ comfortably |
| 00019 | Man City away | 3 | 391 | [0.4546, 0.5443] | **0.4739** | 0.4999 | **−4.08%** | ❌ **fails** |
| 00020 | Newcastle away | 2 | 532 | [0.4024, 0.4787] | 0.3521 | 0.4405 | **+14.28%** | ✅ |

**`00019` (Man City away) is the weakest position on the card**: thinnest edge
(+5.48%, barely over the +5% floor), the only edge that inverts at the interval's
lower bound, and — see below — negative on the Poisson cross-check too. It is the
obvious first candidate to drop.

---

## 3. Independent Poisson cross-check

**Important framing:** the Poisson layer **does not price these bets.**
`calculate_poisson()` has no caller in the staking pipeline. It is an independent
second opinion built from a different input (last-10 goals for/against) than the
band model (ELO gap + venue), and exists to cross-check and to drive Super 6 picks.
Disagreement is information, not an error.

Method: attack/defence indices against a league baseline of **1.4381 goals per team
per game** (from 2,245 matches in `matches_data.json`), × 1.1 home / × 0.9 away, then
a 16×16 independent-Poisson grid. Probability mass sums to 1.000000 ± 0.000006 on
every fixture — no truncation error.

| Bet | Band model p | **Poisson p** | xG (H–A) | Modal score | **EV on bet price (Poisson)** | Verdict |
| --- | ---: | ---: | --- | --- | ---: | --- |
| Villa home @2.17 | 0.5357 | **0.2177** | 1.178 – 2.021 | 1-2 (9.82%) | **−52.76%** | ❌ **contradicts** |
| Bournemouth home @2.47 | 0.4540 | **0.4885** | 1.469 – 0.964 | 1-0 (12.90%) | **+20.66%** | ✅ agrees, stronger |
| Everton away @3.59 | 0.4405 | **0.3617** | 1.101 – 1.126 | 1-1 (13.37%) | **+29.85%** | ⚠️ agrees, **half the edge** |
| Man City away @2.11 | 0.4999 | **0.4539** | 1.377 – 1.709 | 1-1 (10.75%) | **−4.23%** | ❌ contradicts |
| Newcastle away @2.84 | 0.4405 | **0.2203** | 1.606 – 0.939 | 1-0 (12.60%) | **−37.45%** | ❌ **contradicts** |

**Three of five bets — £74.97, or 51.4% of the money — are negative-EV on the
Poisson view.** Two of those three (Villa, Newcastle) are not marginal
disagreements: Poisson points at the **opposite outcome**, and in both cases the
band model is reading a small ELO gap while Poisson is reading the goal record.

Two specific tensions worth resolving in the discussion:

- **Villa v Forest (`00016`, £32.07).** Band model: Villa 51 ELO points better, home,
  53.57%. Poisson: Forest xG 2.021 vs Villa 1.178, Forest win 56.87%, modal score
  **1-2 to Forest**. Underlying: Forest GF10 **1.9** / GA10 1.1 versus Villa GF10 1.4
  / GA10 **1.7**. This is the largest model-vs-model disagreement on the card and it
  carries the second-largest stake.
- **Leeds v Newcastle (`00020`, £31.50).** Band model: Newcastle away, 44.05%.
  Poisson: Leeds 53.01%, Newcastle 22.03%, modal **1-0 Leeds**. Leeds are ΔELO **+40
  over ten games** (second-best on the card) with GF10 1.5 / GA10 1.0; Newcastle are
  ΔELO −5, GF10 1.5 / GA10 1.4. The band model cannot see form; Poisson can, and it
  says the home side.

Only **Bournemouth v Brentford (`00017`)** has both models pointing the same way with
Poisson the more enthusiastic of the two.

Poisson output for the two rejected fixtures — `Chelsea v Hull City` and
`Coventry v Brighton` — is **invalid** (Flag 4) and excluded here.

---

## 4. Full slate — all 10 fixtures, all 30 selections

Model probabilities are band-based; the ELO band is set by the absolute ELO gap.

| Fixture | Date/KO | ELO H | ELO A | Gap | Band | Odds H/D/A | Overround |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| Aston Villa v Nott'm Forest | 12th 14:00 | 1840 | 1789 | 51 | 2 | 2.17 / 3.39 / 3.24 | 106.45% |
| Bournemouth v Brentford | 12th 14:00 | 1827 | 1822 | 5 | 1 | 2.47 / 3.54 / 2.65 | 106.47% |
| Chelsea v Hull City | 12th 14:00 | 1833 | 1710 | 123 | 3 | 1.22 / 6.36 / 11.93 | 106.07% |
| Crystal Palace v Ipswich | 12th 14:00 | 1772 | 1680 | 92 | 2 | 1.86 / 3.70 / 3.88 | 106.56% |
| Liverpool v Fulham | 12th 14:00 | 1898 | 1767 | 131 | 3 | 1.42 / 4.88 / 6.29 | 106.81% |
| Spurs v Everton | 12th 16:30 | 1736 | 1790 | 54 | 2 | 2.01 / 3.49 / 3.59 | 106.26% |
| Sunderland v Arsenal | 12th 19:00 | 1733 | 2022 | 289 | 6 | 6.93 / 4.14 / 1.47 | 106.61% |
| Coventry City v Brighton | 13th 13:00 | 1674 | 1826 | 152 | 4 | 4.01 / 3.67 / 1.84 | 106.53% |
| Man Utd v Man City | 13th 15:30 | 1876 | 1999 | 123 | 3 | 3.04 / 3.81 / 2.11 | 106.53% |
| Leeds v Newcastle | 14th 19:00 | 1766 | 1833 | 67 | 2 | 2.37 / 3.45 / 2.84 | 106.39% |

Overrounds are tight and uniform (106.07%–106.81%). No fixture shows the fat,
inefficient market that produced MW1's largest edge — worth holding in mind when
assessing a claimed +58% edge into a 106.26% book.

### Near-misses and blocks (bets not taken)

| Fixture | Sel | EV | Outcome |
| --- | --- | ---: | --- |
| Crystal Palace v Ipswich | home | **−0.36%** | below +5% floor — closest miss on the card |
| Sunderland v Arsenal | away | −0.63% | below floor |
| Coventry City v Brighton | away | −1.14% | below floor |
| Man Utd v Man City | draw | **+3.94%** | **positive EV but below the +5% floor** |
| Sunderland v Arsenal | home | −5.47% | below floor |
| Coventry City v Brighton | draw | −5.28% | below floor |
| **Chelsea v Hull City** | draw / away | **+49.1% / +106.5%** | 🚫 **BLOCKED** — two +EV selections in one market |
| **Liverpool v Fulham** | draw / away | (both +EV) | 🚫 **BLOCKED** — two +EV selections in one market |

The two blocks are the sanity checker working correctly, and they are a signal in
their own right. Mutually exclusive outcomes cannot both be value against a single
106% overround; the model producing **+106.5% EV on Hull City away @ 11.93** means
the band model is materially mispricing heavy-favourite fixtures, not that the
market has left a fortune on the table. That defect lives in Band 3 — **the same
band `00019` (Man City away) is priced from.**

---

## 5. Model reference — how the numbers are produced

**Band model (prices and stakes everything).** ELO gap → band → historical base
rates for stronger/draw/weaker → venue adjustment → normalise.

Venue multipliers (`venue_adjustment.json`, updated 2026-09-08, n=2,234):
home **×1.099**, away **×0.898**; home win rate 57.31%, away 46.81%.

Band table (bands in play this week):

| Band | ELO gap | n | Stronger win | Draw | Weaker win | O2.5 | BTTS |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0–50 | 676 | 40.68% | 25.00% | 34.32% | 54.73% | 55.90% |
| 2 | 51–100 | 532 | 48.87% | 25.19% | 25.94% | 51.13% | 52.44% |
| 3 | 101–150 | 391 | 54.73% | 25.32% | 19.95% | 56.01% | 56.78% |
| 4 | 151–200 | 286 | 58.39% | 23.78% | 17.83% | 54.90% | 53.50% |
| 6 | 251–300 | 95 | 71.58%* | 16.84%* | 11.58%* | — | — |

\* Band 6 n=95; the dry run reports Wilson widths of 0.15–0.17 there — roughly
double Band 2's. Bands **9 and 10** (n=6 and n=4) are configured as
`low_confidence` and size at Eighth-Kelly. **None of this week's fixtures fall in
a low-confidence band**, so every bet was classed `standard` before the
`large_edge` downgrade would have applied.

**Poisson layer (cross-check only, never stakes).** See §3.

**Staking rule, in full.** Quarter-Kelly (0.25) is a **ceiling** for standard +EV
plays. Eighth-Kelly (0.125) for hedges, low-confidence bands, and edges ≥ +20% EV.
Act only on edges ≥ +5% EV. No single stake above 3% of staking bankroll (clamped
automatically, always logged). A week above 12% is **flagged, never silently
rescaled** — which bet to drop is explicitly an operator decision, and that is the
decision in front of you.

---

## 6. Portfolio outcome distributions

Exact enumeration of all 2⁵ = 32 outcomes, using model probabilities and assuming
independence. **The independence assumption is questionable** given Flag 3 — the two
Band 2 stronger-away bets share a common driver, so the real distribution has fatter
tails than shown on both sides.

### As placed (£145.88)

| Metric | Value |
| --- | ---: |
| Stake-weighted EV | **+31.67%** |
| Expected wins | 2.37 of 5 |
| E[P/L] | **+£46.20** |
| Median P/L | +£40.23 |
| **P(P/L ≤ 0)** | **40.15%** |
| 5th percentile | **−£121.83** |
| 95th percentile | +£223.33 |
| Max win / max loss | +£270.43 / **−£145.88** |

### Rule-compliant (£99.85)

| Metric | Value |
| --- | ---: |
| Stake-weighted EV | +26.50% |
| E[P/L] | +£26.46 |
| Median P/L | +£29.06 |
| **P(P/L ≤ 0)** | **35.66%** |
| 5th percentile | **−£75.80** |
| 95th percentile | +£124.49 |
| Max win / max loss | +£169.22 / **−£99.85** |

**The trade.** Complying with the caps gives up £19.74 of expected value and
£98.84 of upside tail, in exchange for cutting the 5th-percentile loss by
**£46.03** and the worst case by the same. Against a bankroll already **−7.63%**
with a model that has delivered **−48 points below its own EV claim** over 15 bets,
that is not obviously a bad trade — and it is the trade the system was explicitly
designed to make.

Downside in bankroll terms, from £923.68:

| Scenario | Ending bankroll | Growth from £1,000 |
| --- | ---: | ---: |
| As placed, all five lose | **£777.80** | **−22.22%** |
| Rule-compliant, all five lose | **£823.83** | **−17.62%** |
| As placed, 5th percentile | £801.85 | −19.82% |
| Rule-compliant, 5th percentile | £847.88 | −15.21% |

---

## 7. Supporting per-team data

### Form and goal record (last 10 matches)

| Team | GF/10 | GA/10 | ΔELO 5 | ΔELO 10 | Trend | Form |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| Arsenal | 1.6 | 0.6 | +14 | −1 | stable | 6.4 |
| Aston Villa | 1.4 | 1.7 | +4 | −15 | stable | 5.7 |
| Bournemouth | 1.6 | 1.1 | +1 | **+37** | stable | 4.4 |
| Brentford | 1.4 | 1.2 | +4 | −3 | stable | 5.5 |
| Brighton | 1.9 | 1.4 | **−20** | −8 | declining | 3.2 |
| Chelsea | 1.3 | 2.1 | +7 | **−36** | stable | 6.4 |
| Coventry City | **0.0** ⚠️ | 1.7 | −9 | −9 | stable | 4.3 |
| Crystal Palace | 1.2 | 2.1 | −8 | −22 | stable | 4.6 |
| Everton | 1.5 | 1.8 | −11 | −16 | stable | 4.2 |
| Fulham | 0.8 | 1.4 | −14 | −20 | stable | 4.0 |
| Hull City | 1.0 | **0.0** ⚠️ | +9 | +9 | stable | 5.7 |
| Ipswich | 0.9 | 2.5 | **+64** ⚠️ | **+65** ⚠️ | improving | **10.0** ⚠️ |
| Leeds | 1.5 | 1.0 | +10 | **+40** | stable | 5.2 |
| Liverpool | 1.9 | 1.5 | −13 | −7 | stable | 3.8 |
| Man City | 2.1 | 0.9 | −2 | +19 | stable | 4.4 |
| Man Utd | 2.0 | 1.3 | +8 | +13 | stable | 5.5 |
| Newcastle | 1.5 | 1.4 | +2 | −5 | stable | 5.3 |
| Nott'm Forest | 1.9 | 1.1 | −15 | **+24** | stable | 3.0 |
| Spurs | 0.8 | 1.2 | −12 | −2 | stable | 3.8 |
| Sunderland | 1.3 | 1.5 | **+30** | +16 | improving | 7.7 |

⚠️ **Ipswich carry a form rating of exactly 10.0 with +64/+65 ELO swings** — an
order of magnitude beyond any other side. This is a promoted-team artefact (thin
match history inflating the ELO delta), not a real signal. Crystal Palace v Ipswich
came in at −0.36% EV, the closest near-miss on the card; had the artefact pushed it
1 point the other way it would have been staked. Treat that fixture as untradeable
regardless of what the number says next week.

### Head-to-head (all-time in `matches_data`)

| Fixture | Record | Last meeting |
| --- | --- | --- |
| Aston Villa v Nott'm Forest | 8 games — Villa 4, Forest 2, 2 drawn | Forest 1-1 Villa (2026-04-12) |
| Bournemouth v Brentford | 9 games — **Bournemouth 0**, Brentford 6, 3 drawn | Bournemouth 0-0 Brentford (2026-03-03) |
| Spurs v Everton | 11 games — Spurs 6, Everton 1, 4 drawn | Spurs 1-0 Everton (2026-05-24) |
| Man Utd v Man City | 12 games — City 6, Utd 4, 2 drawn | Man Utd 2-0 Man City (2026-01-17) |
| Leeds v Newcastle | 8 games — Leeds 2, Newcastle 2, 4 drawn | Newcastle 4-3 Leeds (2026-01-07) |
| Crystal Palace v Ipswich | 3 games — Palace 3, Ipswich 0 | Palace 1-0 Ipswich (2025-03-08) |
| Liverpool v Fulham | 10 games — Liverpool 4, Fulham 2, 4 drawn | Liverpool 2-0 Fulham (2026-04-11) |
| Sunderland v Arsenal | 2 games — Arsenal 1, 1 drawn | Arsenal 3-0 Sunderland (2026-02-07) |
| Chelsea v Hull City | **no record** | — |
| Coventry City v Brighton | **no record** | — |

H2H is **not** an input to either model. It is context only. Two are notable against
current positions: **Bournemouth have never beaten Brentford in 9 attempts** (bet
`00017` is Bournemouth to win), and **Everton have won once in 11 against Spurs**
(bet `00018` is Everton away, the largest stake).

### Fixture congestion (PL-only proxy, trailing 14 days)

All ten fixtures show 1–2 Premier League games per side in the prior fortnight — no
outlier. `cup_fixtures.json` is empty for all tracked competitions, so European and
domestic-cup load is **not** captured. This is a known incomplete measure.

---

## 8. Decisions to make

Ranked by the size of the number attached.

1. **Do the caps get applied retrospectively to bets already on the book?**
   The bets were placed 2026-09-08 against a build that could not enforce them.
   Reducing to £99.85 requires cashing out or laying off £46.03 across three
   positions, at whatever the market now offers. Alternative: accept this week at
   15.79% and pull the checkout up to date before MW5. The cost of the second
   option is a 5th-percentile loss £46 worse.

2. **`00019` Man Utd v Man City away (£11.40) — drop?**
   Fails on all three independent tests: +5.48% EV (barely clears the floor),
   −4.08% at the Wilson lower bound, −4.23% on Poisson. It also sits in Band 3, the
   band that produced the absurd +106.5% Chelsea v Hull figure. Smallest stake on
   the card, so dropping it costs little; the argument for keeping it is only that
   it is the least-correlated position in the portfolio.

3. **`00018` Spurs v Everton (£51.84) — halve to £25.92?**
   The +58.14% edge is the least credible number in the run, into a 106.26%
   overround. Poisson agrees on direction but at half the magnitude. Under current
   rules it is automatically Eighth-Kelly. Keeping it at £51.84 is an active
   decision to override the rule, not a default.

4. **Flag 3 concentration — take one of `00018` / `00020`, not both?**
   £83.34 on a single model hypothesis. If the answer to (3) is "halve it", the
   combined figure falls to £41.67 and the concern largely resolves itself.

5. **`00016` Villa home (£32.07) — reconcile the model split.**
   Band model +16.25%, Poisson −52.76%, the largest disagreement on the card,
   second-largest stake. Poisson has the better underlying case here (Forest's
   1.9 GF / 1.1 GA against Villa's 1.4 / 1.7). At minimum this is capped to £27.71.

6. **Re-verify all five prices before staking.** Two days stale, and three of the
   edges exceed +20% — the range where a stale price is the most likely explanation.

7. **Process fix, before MW5.** `git pull` before every run. Worth considering a
   guard in `run_pipeline.py` that refuses to write to the ledger when the working
   tree is behind `origin/main` — this failure was silent, and it cost real money
   in position sizing.

---

## 9. Provenance

| Item | Source |
| --- | --- |
| Ledger, open bets, realised record | `data/bankroll.json` @ 2026-09-08 15:02:16 |
| Fixtures and bookmaker odds | `data/upcoming_fixtures.json` |
| ELO ratings and ranks | `data/current_elo.json` |
| Band base rates | `data/elo_bands.json` |
| Venue multipliers | `data/venue_adjustment.json` (2026-09-08, n=2,234) |
| Team form / goal record | `data/team_stats.json` |
| Head-to-head | `data/h2h_records.json` |
| Staking rule and caps | `pipeline/staking.py`, `data/pipeline_config.json` |
| Re-priced stakes, Wilson intervals, cap and exposure figures | `python3 -m pipeline.run_pipeline --data data --dry-run` at HEAD, plus a re-price at the £923.68 placement-time bankroll |
| Poisson figures | `OddsCalculator.calculate_poisson()` at HEAD, 16×16 grid |
| Calibration, Poisson-binomial, portfolio distributions | Exact enumeration over the ledger and the 5 open bets |

**Not available this week:** team news, formations, referee assignments, cup/European
fixture load. All four are empty or absent in the data as run.
