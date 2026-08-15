# Odds Calculator — Themed Findings (v1)

**Headline:** 3 of 4 themes are fully build-ready. Model & Maths Core has one real gate left — the Band 1 root cause — which needs a codebase audit, not more conversation. Everything else that was open has been resolved in-session.

**Resolved since v1:** promoted-team ELO seeding (bottom-4-average, locked) and game congestion (split into a buildable backward-looking signal + a separate fixture-occurrence scrape, with the forward-looking rotation question correctly parked rather than built half-baked).

---

## North Star (not a numbered theme — see reasoning from prior turn)

Small, humble, engaged community over monetization this season. Anti-tipster positioning: systematised transparency, not hype (established voice — see `ELO_Betting_System_Explained.md`, `Twitter_Style_Guide.md`, both already consistent with this). Every theme below is built to serve this, not the other way round.

---

## Theme 1: Model & Maths Core

**Status: `[Mixed]` — core is `[Specified]`, two items are genuine gates**

| Item | Status | Note |
|---|---|---|
| ELO formula, MOV multiplier, venue adjustment, band structure | `[Specified]` | Stable, documented, in production |
| **ELO Band 1 bug** | `[Needs decision — verify expertise first]` | Root cause unconfirmed (leading hypothesis: pre-offset 1500 fallback). Needs a codebase audit before anyone can decide how to fix it — not a preference call. **This is the one remaining gate.** |
| Promoted-team ELO seeding (Hull, Coventry, Ipswich) | `[Specified]` | **Locked:** seed at average current (post-offset) ELO of last season's bottom 4 finishers |
| Long vs. short ELO horizon | `[Parked]` | Doesn't block anything this week |
| Monte Carlo season simulation | `[Parked]` | Explicit stretch goal, not this week's work |
| OLS regression (V6.2) | `[Parked]` | Deferred, unchanged from source-of-truth doc |
| Alternative markets/leagues | `[Parked]` | Carried from `Preparing_for_Fable`, not raised again this rattle |

**Why this blocks:** Matchweek 1 will contain Band 1 fixtures — there's no way to avoid that in week 1. Everything downstream (Pipeline output, Distribution content) inherits whatever's wrong here until it's fixed.

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

- **Theme 1 → Theme 2 (gate):** Band 1 bug and promoted-team seeding must resolve before Pipeline output is trustworthy for MW1. This is the actual critical path.
- **Theme 3 → Theme 2 (partial gate):** game congestion definition gates only the congestion-scoring piece of the qualitative layer, not the pipeline as a whole — Pipeline can run without it, just without that one input.
- **Theme 2 → Theme 4 (handoff, not a gate):** Ghost results page and Distribution copy are both *built from* Pipeline output. Distribution has nothing to publish until Pipeline runs, but Distribution itself has no open decisions of its own.

**Resolution order:** Theme 1's two gates first (they block the most and are on the critical path for MW1) → Theme 3's game congestion definition (parallel-safe, non-blocking) → then Pipeline and Distribution build straightforwardly.

---

## Handoff Contracts

**Theme 3 → Theme 2:** Reference layer guarantees structured qualitative inputs per matchweek (manager style tags, injury/suspension list, formation notes, congestion score once defined). It does *not* guarantee interpretation or analysis — that's Pipeline's job, using these as raw inputs.

**Theme 2 → Theme 4:** Pipeline guarantees, per matchweek: bankroll figure, per-bet EV/stake/odds/result in a structured format, +EV fixture list, Super 6 picks with Poisson-derived scorelines. It does *not* guarantee narrative copy, blog prose, or social posts — that's Theme 4's (LLM-assisted) job to generate from the structured output.

---

## What This Document Does Not Do

- Does not resolve the Band 1 root cause — needs a codebase audit, not more conversation.
- Does not resolve the Band 1 *fix* even once the cause is found — diagnosis and fix are two separate steps.
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
