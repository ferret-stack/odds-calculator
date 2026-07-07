# B1 — Data-layer overhaul & multi-league architecture

> **Status: SPEC STUB — to be written by Fable (Brief Part B), implemented later by Opus.**
> Priority: **next up after foundation.**

## Objective
Replace the brittle Selenium PL-website scraper (absolute XPaths, already broke once, silent
single point of failure) with **football-data.co.uk** CSV ingestion, and parameterise the
pipeline by league so adding a competition is config, not code. PL-only this cycle; then
Championship + one European league. See `docs/ROADMAP.md` for the benefit analysis.

## Fable to fill in
- Files touched
- Concrete step-by-step (CSV schema → mapping → team-name normalisation across leagues →
  league config object → ELO/bands per league)
- Data contracts (raw-facts JSON shape; per-league output layout)
- Verification method (row counts vs source; PL parity with current data; no regressions)
