# Odds Calculator — System Source of Truth (v1)

**Purpose:** Single reference for the system as it stands going into this season. Written to seed Claude Code prompts for a week-long build sprint. Supersedes conflicting sections of older project files (see bottom). Does not cover PnL/bankroll state — this is system, not results.

**Headline:** The maths core (ELO + venue adjustment + Poisson) is sound and stable. What's actually unresolved is (1) one confirmed calculation bug, (2) three genuinely undecided design questions, and (3) a pile of stale reference data. This doc exists to stop those three categories from getting blended into one undifferentiated backlog.

---

## 1. Model Core (ELO + Poisson)

**Stable, in production, not up for debate this week:**
- Independent ELO system (no external API dependency). K=20. Standard update formula.
- Home advantage: +100 points applied when calculating expected score.
- Margin-of-victory multiplier (log-based, with upset bonus). See `Elo_Approach.md` for the exact function.
- Venue adjustment: HOME ×1.11 / AWAY ×0.89 (league average; band-level table exists for finer-grained use).
- +284 cosmetic offset applied to all ratings post-calculation, to bring the scale in line with conventional ELO expectations (top team ~1750 raw → ~2030 displayed).
- 10 ELO bands (0–50 through 450+), per `An_Odd_Prompt.md`.
- Poisson score-matrix generation already exists and backs O/U and BTTS markets — this is the same infrastructure Super 6 will reuse (§4).
- Minimum edge to act on any bet: +5% EV. Not in dispute.

**Known bug — ELO Band 1:**
- Confirmed real. Affects non-promoted teams (not a promoted-team artifact).
- Symptom: weaker team wins more than the model expects in Band 1 fixtures; some teams show a suspicious default of 1500.
- **Leading hypothesis, unconfirmed:** a fallback path in the codebase still hardcodes `1500` as a default rating, written before the +284 scaling offset existed. On the current scale that would land as an artificially low/miscalibrated value — concentrated exactly where small ELO differences (Band 1) would expose it. This needs a codebase audit, not a documentation decision. Flag for Claude Code: search for hardcoded `1500` and confirm every fallback path applies the same offset as the rest of the system.

**Open question, explicitly parked (not blocking this week):**
- Long-horizon ELO (full 2020+ history) vs. short-horizon/rolling ELO (e.g. 2-year window) vs. running both in parallel. Raised in `Preparing_for_Fable` (never sent to Fable — that session didn't happen) and repeated in this week's rattle. Still open. Not deciding it this week.

---

## 2. New Teams This Season

Hull City, Coventry City, Ipswich Town — promoted from the Championship.

**Data available:** Premier League matches only, 2020–present. No Championship data. No usable recent PL history for these three (last PL spells predate the 2020 dataset).

**Recommendation (pending your confirm/override):** Seed each promoted team's initial ELO at the average current (post-offset) rating of last season's bottom 4 finishers, rather than a flat 1500. Rationale: promoted teams statistically perform like bottom-of-table sides, not average sides, and a flat 1500 default is exactly the value implicated in the Band 1 bug above — using it again here just recreates the same miscalibration on purpose. This uses only data you already have.

If you'd rather just flat-seed at league-average or something else, override this line — everything downstream (Band 1 fix, promoted-team seeding) should use one consistent, deliberate default, not two different accidental ones.

---

## 3. Staking Logic

**Formalized standard (supersedes Half-Kelly references elsewhere):**
- Quarter-Kelly ceiling for standard +EV plays.
- Eighth-Kelly for hedge positions / lower-confidence plays.
- This matches actual practice (MW34 and others), not the Half-Kelly currently written into `An_Odd_Prompt.md` and `odds-calculator-weekly_workflow.md` — those need updating to match this, not the other way round.
- Existing sanity checks stay as-is: no correlated bets without explaining the correlation; two +EV outcomes in the same market = re-check the model, don't bet both.

---

## 4. Super 6 (Hybrid Architecture)

- Not part of the betting/staking pipeline — separate output, separate purpose (jackpot exact-score game, not value betting).
- **Coded baseline:** reuse the existing Poisson score-matrix per fixture. Select the highest-probability exact scoreline for each of the six Super 6 fixtures.
- **LLM layer:** generates a one-line rationale per pick, in house voice.
- Supersedes the old approach (an LLM agent freeform-guessing off Poisson, unsystematized).

---

## 5. Distribution Channels

**Live, in scope:** Twitter/X, Mastodon, Discord, Ghost newsletter.

**Explicitly out of scope for now:** YouTube, Reddit. Older docs (`An_Odd_Roadmap.md`, `odds-calculator-weekly_workflow.md`) describing YouTube/Reddit workflows and a video-editing partner are archived, not merged into this system — they describe a different plan from a different point in time.

- **Ghost:** site exists and is live; this cycle's work is updates + rebuilding the newsletter specifically (prior newsletter approach didn't work — cause not diagnosed in project knowledge; treat as a fresh build rather than a fix unless you know otherwise).
- **GitHub Pages odds-calculator page:** mobile/responsive view is the most pressing front-end fix.

---

## 6. Automated Pipeline

- **Infra:** local machine only, for now. No VM/cloud hosting in scope this week.
- **Implication:** build as a locally-triggered script (you run it), not an unattended service — but avoid hardcoding local-only assumptions where it costs nothing, so it's portable to a VM later if you decide to host it.
- **Scope:** bankroll/PnL tracking, running the ELO/Poisson model, identifying +EV games, generating the matchweek write-up (quant + qual layers), Super 6 picks, posting to Twitter/Discord/Mastodon/Ghost.
- **Blocking sub-bug:** advanced stats scrape currently throws an exception ("additional statistics not scraped") on cards and some other metrics. This needs fixing before the qualitative layer can rely on it — flag as a priority fix, not a nice-to-have.

---

## 7. Reference / Qualitative Layer

- **Manager styles DB:** stale for promoted teams' managers, and for any change since the Feb-26 addendum. Needs a refresh before MW1.
- **Team news:** stale — last update January 2026. Needs a fresh pull before season start.
- **Formation analysis:** started, not systematized (per `Preparing_for_Fable`).

---

## 8. Explicitly Parked — Not This Week

- Long vs. short ELO horizon question (§1).
- Alternative markets/leagues (WSL, Championship, Saudi, European).
- Monte Carlo season simulation.
- OLS regression enhancement (V6.2).
- Any growth/marketing plan beyond the four confirmed channels.

---

## Superseded / Archived Documents

These aren't deleted — they're still useful for history and narrative — but should not be treated as current on the points this doc covers:

| Document | Superseded on |
|---|---|
| `odds-calculator_README.md` | Current-state section (version history stays valid) |
| `odds-calculator-weekly_workflow.md` | Channel list, staking language, YouTube/Reddit/partner workflow |
| `An_Odd_Roadmap.md` | Channel plan and timeline — archived in full |
| `An_Odd_Prompt.md` | Staking rule (§3), Super 6 section (§4) — needs editing to match this doc before it's used to generate content again |

---

*Open items requiring your confirm before this is fully locked: §2 (promoted-team ELO seeding method).*
