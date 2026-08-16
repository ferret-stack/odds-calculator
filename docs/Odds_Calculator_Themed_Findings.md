# Odds Calculator — Themed Findings (v1, amended after Day 1)

**Headline:** All 4 themes are now build-ready. Model & Maths Core's one real gate — the Band 1 root cause — is closed: audited, fixed, and validated on `claude/odds-calc-band1-audit-su1rn4`.

**Resolved since v1:** promoted-team ELO seeding (bottom-4-average, locked and implemented) and game congestion (split into a buildable backward-looking signal + a separate fixture-occurrence scrape, with the forward-looking rotation question correctly parked rather than built half-baked).

**Resolved on Day 1:** the Band 1 bug itself. See §Theme 1 below and the System Source of Truth doc for the root-cause writeup and validation numbers.

---

## North Star (not a numbered theme — see reasoning from prior turn)

Small, humble, engaged community over monetization this season. Anti-tipster positioning: systematised transparency, not hype (established voice — see `ELO_Betting_System_Explained.md`, `Twitter_Style_Guide.md`, both already consistent with this). Every theme below is built to serve this, not the other way round.

---

## Theme 1: Model & Maths Core

**Status: `[Specified]` — both former gates closed on Day 1**

| Item | Status | Note |
|---|---|---|
| ELO formula, MOV multiplier, venue adjustment, band structure | `[Specified]` | Stable, documented, in production |
| **ELO Band 1 bug** | `[Resolved]` | Four compounding defects, not one: `current_elo` never loaded at scrape time (fallback fired for every team, not just unknown ones); the fallback literal was 284 points below scale; exact ties mislabelled as weaker-team wins; the repair path had silently become a permanent no-op. Fixed and validated — Band 1 stronger/weaker went from 29.59%/43.07% (inverted) to 41.15%/33.89% (correct direction). Also surfaced and repaired 387 corrupt match dates (17.5% of the dataset) needed to replay the chain correctly. One residual flagged, not fixed: a band-boundary off-by-one affecting 2.2% of matches — needs a decision since it reassigns matches at every band edge. |
| Promoted-team ELO seeding (Hull, Coventry, Ipswich) | `[Resolved]` | Implemented: seeded at 1685, the average current (post-offset) ELO of 2025-26's bottom 4 (Spurs, West Ham, Wolves, Burnley). Runs through its own seeding path, not the fallback fixed above. Validated: promoted teams land 18th–20th of the 2026-27 field, bands 1–7. |
| Long vs. short ELO horizon | `[Parked]` | Doesn't block anything this week |
| Monte Carlo season simulation | `[Parked]` | Explicit stretch goal, not this week's work |
| OLS regression (V6.2) | `[Parked]` | Deferred, unchanged from source-of-truth doc |
| Alternative markets/leagues | `[Parked]` | Carried from `Preparing_for_Fable`, not raised again this rattle |

**Why this mattered:** Matchweek 1 will contain Band 1 fixtures — there was no way to avoid that in week 1. Pipeline and Distribution output was not trustworthy until this closed; it now is.

---

## Theme 2: Automated Pipeline

**Status: `[Specified]` with one blocking bug**

| Item | Status | Note |
|---|---|---|
| Algorithm execution, locally-triggered | `[Specified]` | Local machine only, portable-later design |
| PnL tracking | `[Specified]` | |
| +EV identification | `[Specified]` | Depends on Theme 1 output being correct |
| **Scraper exception** (cards/advanced stats) | `[Needs decision — verify expertise first]` | Exact error unknown to you ("something like additional statistics not scraped") — needs reproduction before it can be fixed |
| Live feed / live match updates | `[Parked]` | Raised as "I wonder if," not a commitment — logging it so it isn't lost, not building it this week |

---

## Theme 3: Qualitative Reference Layer

**Status: `[Specified]` execution tasks, one new design decision**

| Item | Status | Note |
|---|---|---|
| Manager styles refresh (promoted teams + anything since Feb-26) | `[Specified]` | Known task, just needs doing before MW1 |
| Team news refresh (stale since Jan-26) | `[Specified]` | Same — execution, not judgment |
| Formations | `[Specified]` | Already started per `Preparing_for_Fable` |
| Game congestion — backward-looking fatigue signal | `[Specified]` | PL games played in trailing N days (7/14), numeric. Explicitly labelled as a PL-only proxy, not a complete fatigue measure |
| Game congestion — fixture-occurrence scrape (FA Cup, Carabao Cup, European) | `[Specified]` | Date/competition/opponent only, no lineup or rotation analysis. Used as narrative context in write-ups, not folded into the numeric signal — domestic cup lineups are too rotation-prone to count at face value. Separate scrape target from the existing broken advanced-stats scraper (Theme 2) — don't let debugging one block building the other |
| Game congestion — forward-looking rotation risk (resting players ahead of a big European tie) | `[Parked]` | Real phenomenon, but manager-dependent and needs calibration against actual past rotation patterns. Folds into Manager Styles as a future enrichment, not a standalone system |

**Note:** none of these block Pipeline from running for MW1 — they enrich the qualitative layer but aren't load-bearing for +EV calculation itself. Lower urgency than Theme 1's Band 1 gate.

---

## Theme 4: Site & Distribution

**Status: `[Specified]`**

| Item | Status | Note |
|---|---|---|
| Twitter, Mastodon, Discord posting | `[Specified]` | Channels locked this rattle |
| Ghost updates | `[Specified]` | Site exists, live |
| Ghost newsletter rebuild | `[Specified]` | Treated as fresh build — prior failure cause undiagnosed, not blocking |
| **Ghost public performance/results page** | `[Specified]` | Confirmed this rattle: lives here, *built from* Theme 2 — see dependency map |
| GitHub Pages mobile fix | `[Specified]` | Flagged priority, not a blocker for other themes |
| Super 6 (hybrid: coded Poisson baseline + LLM narrative) | `[Specified]` | Reuses existing Poisson infrastructure |

---

## Dependency Map

- **Theme 1 → Theme 2 (gate, now cleared):** Band 1 bug and promoted-team seeding resolved on Day 1 — Pipeline output is trustworthy for MW1. This was the critical path; Theme 2 can now proceed.
- **Theme 3 → Theme 2 (partial gate):** game congestion definition gates only the congestion-scoring piece of the qualitative layer, not the pipeline as a whole — Pipeline can run without it, just without that one input.
- **Theme 2 → Theme 4 (handoff, not a gate):** Ghost results page and Distribution copy are both *built from* Pipeline output. Distribution has nothing to publish until Pipeline runs, but Distribution itself has no open decisions of its own.

**Resolution order:** Theme 1's two gates first (they block the most and are on the critical path for MW1) → Theme 3's game congestion definition (parallel-safe, non-blocking) → then Pipeline and Distribution build straightforwardly.

---

## Handoff Contracts

**Theme 3 → Theme 2:** Reference layer guarantees structured qualitative inputs per matchweek (manager style tags, injury/suspension list, formation notes, congestion score once defined). It does *not* guarantee interpretation or analysis — that's Pipeline's job, using these as raw inputs.

**Theme 2 → Theme 4:** Pipeline guarantees, per matchweek: bankroll figure, per-bet EV/stake/odds/result in a structured format, +EV fixture list, Super 6 picks with Poisson-derived scorelines. It does *not* guarantee narrative copy, blog prose, or social posts — that's Theme 4's (LLM-assisted) job to generate from the structured output.

---

## What This Document Does Not Do

- Band 1 root cause and fix are resolved as of Day 1 (see Theme 1 above) — this doc no longer tracks them as open.
- Does not sequence any of this into a build order — that's Step 6, next.
- Does not account for your personal time allocation around United Mortgages this week — real constraint, but it's a Step 6 scheduling input, not a system theme.
- Does not revisit YouTube, Reddit, Monte Carlo, OLS, or alternative markets beyond confirming they're parked.

---

## Dump Trace (4a — mechanical, line by line)

| Dump content | Mapped to |
|---|---|
| Summer/timing framing, "wasn't in the right place before" | Context only — no theme |
| One-week timebox | Step 6 input, not a theme |
| Fitting around United Mortgages | Does-not-do (Step 6 input) |
| Monte Carlo simulation + technical uncertainty | Theme 1, parked |
| Opus Prompt / Preparing_for_Fable reference | Cross-checked, no new content — Fable session never ran |
| GitHub Pages mobile view | Theme 4 |
| "check the maths... ELO Band 1 needs resolving... confirm statistics" | Theme 1 (Band 1 gate + general audit folded into `[Specified]` maths core) |
| Short vs. long ELO | Theme 1, parked |
| New promoted teams | Theme 1, open gate |
| Ghost updates + newsletter | Theme 4 |
| "tracking/logging the performance" | Theme 4, dependency on Theme 2 |
| Automated pipeline: PnL, algorithm run, VM question | Theme 2 — VM question resolved (local only) |
| Qualitative analysis: formations, managerial styles, game congestion, injuries | Theme 3 (congestion = open item) |
| Scraper exception error | Theme 2, gate |
| OLS regression | Theme 1, parked |
| Twitter/Discord posting, live feed idea | Theme 4 (posting) / Theme 2 (live feed, parked) |
| Anti-tipster philosophy paragraph | North Star |
| Season goals (community, not monetizing) | North Star |
| Super 6 / "£1M" framing | Theme 4 |

Every line accounted for. Nothing silently dropped.
