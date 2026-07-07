# Fable Brief — Ferret Stack ELO System Overhaul

> **Paste this whole file into a fresh Fable session.** It is self-contained.

---

## Who you are

You are the **architect-surgeon** for the Ferret Stack ELO football betting model — a
4-year-old system that rates Premier League teams with ELO, converts rating gaps into
Win/Draw/Loss + goals probabilities via historical "ELO bands", compares those to
bookmaker odds to find value, and publishes everything to a Jekyll/GitHub-Pages blog as a
transparent "anti-tipster".

Two repositories, both already checked out on branch
`claude/elo-system-fable-plan-2jd9lm` — **do all work on that branch in both:**

- **`odds-calculator`** — the Python pipeline (`odds_calculator.py`, `elo_calculator.py`)
  and the generated `data/*.json` that the site reads.
- **`ferret-stack.github.io`** — the Jekyll blog (`odds-calculator.html`, `assets/css/`,
  `_layouts/`, `_posts/`). It carries a copy of the data under `assets/data/`.

It is the off-season (July; next season starts late-August). This is the window to fix the
foundation before go-live.

Your job has **two parts**: **Part A you implement in code now.** **Part B you only write
as specs** — a downstream Opus session will implement them.

---

## The philosophy (read this — it shapes every decision)

The owner is a "True EV" believer (poker background): the aim is not to be flashy but to
be *right*, and to be *seen to be right* through radical transparency. "We're scientists
here, but it's also an art." Every change to the maths must be **proven** better (via
backtest/calibration), not merely different. Documentation must match the code — the brand
depends on it. When in doubt, favour correctness, reproducibility and explainability over
cleverness. **Do not over-engineer** — resist adding parameters the data can't support.

---

## PART A — FIX THE FOUNDATION (implement in `odds-calculator`)

The public numbers are currently computed on sand. There is one root-cause knot of bugs
that contaminates the **ELO band probability tables** — the heart of the model — and thus
every downstream number. Here is the confirmed diagnosis; fix it at the root.

### The confirmed root-cause knot

1. **`current_elo` is never loaded before scraping.** `OddsCalculator.load_existing_data()`
   (`odds_calculator.py`) loads only `matches_data.json`. So `scrape_matches()` reads
   `self.current_elo.get(team, 1500)` → **1500 for both teams** → `elo_diff = 0` → match
   filed into **Band 1**. (Confirmed in data: 74 matches carry a 1500; 72 sit in Band 1.)
2. **Equal ELO mislabels the `winner` field.** With `home_elo == away_elo`, the logic
   `winner = "stronger" if home_elo > away_elo else "weaker"` labels **every decisive
   result `"weaker"`**. (Confirmed: 50 matches — 28 real home wins + 22 real away wins —
   all stamped `"weaker"` in Band 1.) This is the direct cause of Band 1 showing the
   *weaker* team winning 43% vs the *stronger* 30% — an artifact, not football.
3. **Bands are computed from POST-match ELO, and `winner` is never recomputed.**
   `update_elo_ratings()` overwrites `home_elo/away_elo/elo_diff/elo_band` with ratings
   *after* the result is known, but never re-derives `winner`. Matches are filed under a
   look-ahead-leaked rating. Every band is subtly distorted.

**The fix that dissolves 1–3 at once:** separate immutable match *facts* from ELO-*derived*
fields, and derive all ELO fields from **one deterministic chronological replay** that
stamps **pre-match** ELO.

### Part A tasks

1. **Re-architect the data model: facts vs derived.**
   - `matches_data.json` holds only immutable facts: `match_id`, `date`, `home_team`,
     `away_team`, `home_goals`, `away_goals`, cards, `referee`, `xg`, `possession`.
     **Nothing ELO-derived.**
   - A single `rebuild()`/derive step replays matches in date order and produces, per match,
     the **pre-match** `home_elo`, `away_elo`, `elo_diff`, `elo_band`, and `winner`. Never
     write post-match ELO onto a match's band fields.

2. **Stamp PRE-match ELO.** `elo_band` and `winner` come from the ratings each team held
   *before* kickoff.

3. **Kill the 1500 default by construction.** Scraper/importer records only raw facts; ELO
   is always (re)derived. No code path bakes `1500` into a stored match. Remove the
   `current_elo`-not-loaded footgun entirely.

4. **Fix `winner` labelling.** No decisive result may be `"weaker"` because of an ELO tie.
   Pick and document an explicit rule for exact equality (recommended: treat the home side
   as de-facto stronger when raw ELO is equal, since home advantage breaks the tie — or
   exclude exact ties from band WDL stats). Recompute `winner` inside the single derive step.

5. **Rebuild `elo_bands.json` from clean pre-match data.** Assert sanity: for every band
   `stronger_win_pct >= weaker_win_pct`; Band 1 must no longer be inverted. Print a
   before/after Band-1 WDL table.

6. **Dual ELO** (owner's chosen philosophy — "class vs form"):
   - **Long ELO:** continuous chronological replay since 2020, all teams start 1500.
   - **Rolling ELO:** 2-year window that re-baselines to 1500 (absorbs promotion/relegation
     churn naturally).
   Store both in `current_elo.json` and history. **Backtest both** and decide, by
   calibration, which drives the band tables / fair odds — report the numbers, don't guess.
   Keep both available for the blog narrative.

7. **Wire venue adjustment to real data.** `calculate_home_advantage_multipliers()` writes
   `venue_adjustment.json`, but `adjust_probability_for_venue()` ignores it and uses
   hardcoded constants (1.11/0.89/0.95/1.05). Either apply the computed multipliers (prefer
   this — ideally per-band, since the V6 analysis shows band-varying home effects) or
   consciously delete the dead computation and document the fixed constants.

8. **Reconcile the MOV formula.** The code (`elo_calculator.py`
   `margin_of_victory_multiplier`) does `goal_factor * dampening`; the V6 blog post
   (`ferret-stack.github.io/_posts/2025-12-15-V6-Update.md`) documents `0.7 + base*0.5` with
   an upset bonus. Pick ONE, implement it, and update the post so docs match code. State the
   final formula explicitly.

9. **Secrets.** Remove the three hardcoded `the-odds-api` keys from `odds_calculator.py`;
   read from environment variables / GitHub Actions secrets. **Tell the owner to rotate the
   keys** — they are already public in git history.

10. **Remove rot.** De-duplicate the two `import_excel` and two `calculate_fair_odds`
    definitions; fix `get_band_probabilities` (it reads `home_win_pct`, which only exists on
    empty bands → `KeyError` on real bands; real bands use `stronger_win_pct`/
    `weaker_win_pct`); either implement or delete the phantom `value_bets` reference in the
    summary.

11. **Add a calibration/backtest harness.** Prove the rebuild is better, don't just change
    it. Over historical matches compute reliability (predicted vs actual buckets), **Brier
    score** and **log-loss** — for old-vs-new pipeline and long-vs-rolling ELO. This is both
    scientific due diligence and marketing gold.

**When Part A is done:** run the full pipeline, regenerate every JSON (in `odds-calculator/
data/` and mirror to `ferret-stack.github.io/assets/data/`), and paste into your final
summary: (a) the before/after Band-1 WDL, (b) the calibration metrics, (c) confirmation that
no stored match carries a baked 1500 and no decisive result is mislabelled `"weaker"`,
(d) the chosen equality rule, venue approach, MOV formula, and which ELO drives the bands.

---

## PART B — WRITE THE WORK-ORDER FOR OPUS (specs only — DO NOT implement)

Write each as a standalone, execution-ready spec under `odds-calculator/docs/roadmap/`
(skeleton files already exist). Each spec must list: **objective, files touched, concrete
step-by-step, data contracts (JSON shapes), and a verification method** — precise enough
that an Opus session can implement without re-deriving. Sequence reflects owner priority
(**leagues before Monte Carlo**):

- **B1 — Data-layer overhaul & multi-league architecture** *(highest priority)*. Replace the
  brittle Selenium PL-website scraper (absolute XPaths like
  `/html/body/main/div[1]/div[2]/…`, which already broke once and can silently kill a
  matchweek) with **football-data.co.uk** CSV ingestion (results, cards, shots, referee,
  bookmaker closing odds). Parameterise the pipeline by league so adding a competition is
  config, not code. Recommend PL-only this cycle, then Championship + one European league.
  Pull in the benefit analysis from `docs/ROADMAP.md`.
- **B2 — Blog / mobile presentation overhaul.** `ferret-stack.github.io/odds-calculator.html`
  is a 902-line page over a 2923-line `style.css` with essentially no page-level
  responsiveness; the Poisson table overflows on mobile. Spec a mobile-first responsive
  redesign; surface the dual ELO; keep it theme-consistent. (Use the `dataviz` /
  `artifact-design` guidance for any charts.)
- **B3 — Betting edge engine.** Actually compute model-prob vs book-odds → EV → Kelly /
  half-Kelly staking → populate the `value_bets` JSON the UI already expects. Add a public
  PnL ledger (the anti-tipster track record) with closing-line-value tracking.
- **B4 — Monte Carlo season simulator.** Simulate N seasons off the rebuilt ELO →
  title / top-4 / relegation probabilities; the showpiece for the late-August launch.
- **B5 — AI analysis + marketing workflow.** A library of Opus analysis prompts grounded in
  the model's data + the owner's qualitative docs (formations, managerial playing-style
  references); a content calendar and distribution/marketing gameplan. Study the example
  match-week and analysis posts under `_posts/` for the house voice before writing.

---

## Guardrails

- Branch `claude/elo-system-fable-plan-2jd9lm` in both repos. Commit with clear messages.
  Do **not** open a PR unless the owner asks.
- Keep `odds-calculator/data/*.json` and `ferret-stack.github.io/assets/data/*.json` in sync.
- Do not print, commit, or invent secrets. Flag the exposed API keys for rotation.
- Prioritise correctness and explainability. Prove maths changes with the calibration
  harness. Don't over-engineer.
