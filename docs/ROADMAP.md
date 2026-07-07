# Ferret Stack — Off-Season Roadmap

Companion to `docs/FABLE_BRIEF.md`. This is the owner-facing prioritisation and the
reference the Part B specs (`docs/roadmap/B1..B5.md`) expand on.

## Division of labour

- **Fable (usage 1, the surgeon-architect):** fixes the foundation (Brief Part A) **and**
  writes the sequenced work-order specs (Brief Part B). Fable implements Part A only.
- **Opus (later usages, the builder):** implements B1 → B5 from Fable's specs.

## Prioritisation

| When | Model | Work |
|------|-------|------|
| Usage 1 (now, ~1 hr) | Fable | Part A foundation fixes + write B1–B5 specs |
| Usage 2 | Opus | **B1 — data-layer / multi-league** (top growth priority) |
| Then, owner-selected | Opus | B2 blog/mobile → B3 edge engine → B4 Monte Carlo → B5 AI/marketing |

Rationale: the foundation feeds everything, and only the strongest model should perform the
subtle, interlocking data-integrity surgery. New leagues are prioritised ahead of Monte
Carlo because the data-layer overhaul also retires a live single point of failure (the
scraper) and unlocks richer backtesting data.

## The ELO decision (locked)

**Dual ELO.** A continuous **Long ELO** (since 2020, all start 1500 — "class") and a 2-year
**Rolling ELO** that re-baselines to 1500 ("current form"). Which one drives the published
band tables / fair odds is decided by **backtest calibration**, not preference — Fable
reports Brier/log-loss for both. Both remain available for the "class vs form" content angle.

## New-leagues decision (owner to confirm) — benefit analysis

The current PL data comes from a Selenium scraper against the Premier League website using
absolute XPaths. It already broke once (see commit history: "Fixed XPATHs …"). It is a live
single point of failure: a site redesign can silently kill a matchweek's update.

- **Architect now, PL-only — RECOMMENDED.** Biggest resilience win, least risk. Retires the
  Selenium scraper. Adopts **football-data.co.uk**: 20+ years of stable, free CSVs with
  results, cards, shots, referee, *and bookmaker closing odds* (the last is a gift for
  backtesting edge and closing-line value). Makes the pipeline league-parameterised so a new
  competition is a config line, not a rewrite — but keep only the PL switched on while the
  foundation stabilises.
- **Add 1–2 leagues now.** Same data-source win, plus immediate marketing surface (more
  leagues → more value bets → more content → more audience). But each league needs its own
  ELO calibration and enough matches for stable bands (lower divisions are noisier), and you
  validate brand-new maths on several fronts at once, on the same clock. Higher risk.
- **Design doc only.** Cheapest; keeps the brittle scraper as a live single point of failure.
  Weakest — the scraper risk alone argues against it.

**Recommendation:** architect now, PL-only; then flip on the **Championship + one European
league** once the foundation is proven — because with the right architecture and a
multi-league feed, each additional league is nearly free.

## Growth backlog (specs live in `docs/roadmap/` — all execution-ready as of July 2026)

- **B1** Data-layer overhaul & multi-league architecture *(next up)*
- **B2** Blog / mobile presentation overhaul
- **B3** Betting edge engine (EV, Kelly staking, value-bet JSON, public PnL ledger)
- **B4** Monte Carlo season simulator (title / top-4 / relegation odds)
- **B5** AI analysis + marketing workflow (Opus prompts + content/distribution plan)

Part A (the foundation) is done — see `docs/PART_A_REPORT.md` for the
before/after numbers and the calibration-locked model decisions the specs
build on.
