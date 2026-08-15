# Odds Calculator — Executable Plan (v1)

**This is Step 6 of the Rattle — the first document that earns the word "plan."** Sequenced against the actual calendar (today Sat 15 Aug), not abstract theme order, but the underlying order still follows the Themed Findings dependency map: Model & Maths Core gates Pipeline; Pipeline feeds Distribution.

---

## Today (Saturday) — light, momentum only

Today was analysis. Keep today's build load light.

- [ ] Set up the dedicated Odds Calculator project. Attach: full existing project knowledge, `Odds_Calculator_System_Source_of_Truth.md`, `Odds_Calculator_Themed_Findings.md`, this Executable Plan.
- [ ] **Optional, only if desk time remains and energy allows:** kick off the Claude Code session on the Band 1 root-cause audit. This is the single highest-leverage thing you could do today — it's the one item every other theme is waiting on. Not mandatory today; fine to start fresh next week if you'd rather stop here.

---

## Tomorrow (Sunday) — voice, no desk

Everything here is deliberately conversation-shaped, not screen/code-shaped.

- [ ] **Draft the actual Claude Code prompts** for Day 1–4 below, so Monday starts with execution, not prompt-writing. This is the highest-leverage use of a voice session — talk the prompts into existence now.
- [ ] Talk through and refresh Manager Styles + Team News content verbally — Claude can search and summarize for the promoted teams' managers and anything since Feb-26, you review and correct by ear.
- [ ] Settle the last small parameter: trailing window for the congestion signal — 7 days or 14? Worth deciding out loud rather than defaulting silently.
- [ ] Review this plan itself — adjust day boundaries if anything below feels wrong once you say it out loud.

---

## Next Week — 4 dedicated build days

### Day 1 — Model & Maths Core — DONE
*Gated everything else — completed first, as planned.*
- [x] Band 1 root-cause audit — found four compounding defects (unloaded `current_elo`, off-scale fallback, mislabelled ties, a silently-dead repair path), not the single hypothesized cause alone
- [x] Fix, then validated against historical data (before/after: Band 1 stronger/weaker 29.59%/43.07% → 41.15%/33.89%; swings >50pts 775 → 0)
- [x] Implemented promoted-team ELO seeding: bottom-4-average (1685), on its own code path, locked
- [x] Validated promoted teams produce sane band assignments once seeded (18th–20th of field, bands 1–7)
- Bonus: found and repaired 387 corrupt match dates (17.5% of the dataset) blocking a trustworthy chain replay
- Flagged, not fixed: band-boundary off-by-one (2.2% of matches) — needs a decision, out of Day 1 scope since it touches every band

### Day 2 — Automated Pipeline
*Depended on Day 1 being correct — Day 1 is validated, so this is unblocked.*
- [ ] Fix the scraper exception (cards/advanced stats) — reproduce the error, diagnose, fix
- [ ] Build the fixture-occurrence scraper (FA Cup, Carabao Cup, European — date/competition/opponent only)
- [ ] Build the local-triggered pipeline script: run model → identify +EV fixtures → apply Quarter/Eighth-Kelly staking → PnL tracking
- [ ] Wire in Theme 3 qualitative inputs (manager styles, team news, formations, congestion signal)

### Day 3 — Site & Distribution
*Consumes Day 2's output — build the shape now, content flows once Day 2 is live.*
- [ ] GitHub Pages mobile/responsive fix
- [ ] Ghost updates + newsletter rebuild
- [ ] Ghost public performance/results page, built from Pipeline output
- [ ] Twitter, Mastodon, Discord posting integration
- [ ] Super 6: wire the Poisson baseline to existing score-matrix infra, add LLM narrative layer

### Day 4 — Integration, QA, buffer
- [ ] End-to-end dry run: full pipeline against a real or dummy matchweek, start to finish
- [ ] Update `An_Odd_Prompt.md` to match this week's decisions: staking rule (Quarter/Eighth-Kelly, not Half-Kelly), Super 6 section
- [ ] Fix whatever broke in the dry run
- [ ] Buffer for slippage from Days 1–3 — there will be some, this is the day that absorbs it

---

## Explicitly Not This Sprint

Carried straight from the Themed Findings doc — don't let build momentum pull these forward:
- Long vs. short ELO horizon
- Monte Carlo season simulation
- OLS regression (V6.2)
- Alternative markets/leagues
- YouTube, Reddit
- Forward-looking rotation-risk congestion signal (parked into Manager Styles as a future enrichment)
