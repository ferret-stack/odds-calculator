# B4 — Monte Carlo season simulator

> **Status: SPEC — execution-ready.** Written by Fable (Brief Part B), to be
> implemented by Opus. Depends on B1 for the season fixture list
> (`season_fixtures.json`); the model inputs all exist after Part A.
> Target: publish with the **late-August season launch** post.

## Objective

Simulate the 2026-27 season N times off the rebuilt ELO — per-match
probabilities from the venue-adjusted band model, in-simulation rating updates
so early results shape later ones — and publish title / top-4 / top-6 /
relegation / full position-distribution probabilities per team. The launch
showpiece, refreshed weekly in season ("title race odds after MW5").

## Files touched

| File | Change |
|---|---|
| `monte_carlo.py` | NEW — simulator + aggregator |
| `odds_calculator.py` | optional `--simulate` step after rebuild |
| `data/E0/season_simulation.json` | NEW — output (mirrored to the site) |
| `tests/test_monte_carlo.py` | NEW |
| `ferret-stack.github.io` | (with B2) a simulation section: probability bars + position heat strip per team |

## Inputs (all existing contracts)

- `season_fixtures.json` (B1): the 380 fixtures with dates.
- `current_elo.json`: driver (`elo`) ratings are the starting ratings.
- `elo_bands.json` + `venue_adjustment.json`: per-match probability model via
  `elo_calculator.get_venue_adjusted_probabilities(home_elo, away_elo, bands,
  venue)` — the same function the odds page uses; the simulator must not
  carry its own probability model.
- **Promoted teams** have no PL rating. Prior: `1450` flat (slightly below
  the 1500 debut convention — promoted sides underperform it historically).
  Parameterise (`PROMOTED_PRIOR`) and revisit with Championship data once B1
  multi-league lands; document the chosen value in the output JSON.

## Simulation procedure

Per iteration (fix `seed = iteration_index` for reproducibility):

1. Copy starting ratings.
2. Fixtures in date order. Per fixture:
   - probabilities `p = get_venue_adjusted_probabilities(...)` from CURRENT
     in-sim ratings;
   - sample outcome W/D/L;
   - sample a scoreline **conditioned on the outcome** for goal difference
     (needed for tie-breaks and for realistic MOV-driven rating moves):
     draw a home/away goal pair from a Poisson grid (means: league-average
     goals ± the band's over/under profile is over-engineering — use plain
     `home_mean = 1.5, away_mean = 1.2` scaled to the sampled outcome by
     rejection: resample the pair until it matches the sampled W/D/L;
     6×6 grid, cheap);
   - update both teams' in-sim ratings with `ELOCalculator.process_match`
     (same K/MOV parameters as production — import, don't re-implement).
3. League table: 3/1/0 points, tie-break goal difference then goals scored.
4. Record each team's final position.

Aggregate over N = 10,000 iterations (~4M match sims; the Part A replay does
~2,200 in 2s, so budget minutes, not hours — chunk with `multiprocessing` if
it drags).

## Output contract — `season_simulation.json`

```json
{
  "as_of": "2026-08-10",
  "season": "2627",
  "iterations": 10000,
  "seed_base": 0,
  "promoted_prior": 1450,
  "model": {"driver": "long", "mov": "v6blog", "venue": "global"},
  "teams": {
    "Arsenal": {
      "start_elo": 1767,
      "title": 0.31, "top4": 0.78, "top6": 0.91,
      "relegation": 0.0,
      "expected_points": 74.2,
      "position_dist": [0.31, 0.22, 0.14, "... 20 entries summing to 1"]
    }
  }
}
```

## Step-by-step

1. `monte_carlo.py` with `simulate_season(fixtures, ratings, bands, venue,
   seed) -> dict[team, position]` (pure, testable) and
   `run(n, ...) -> aggregate`.
2. Scoreline sampler with outcome-conditioning by rejection; unit-test that
   the sampled outcome distribution matches the input probabilities within
   Monte-Carlo error.
3. Aggregator + JSON writer; wire `--simulate` into the pipeline main.
4. Convergence harness: run N=5,000 twice (different seed bases) and N=10,000;
   report `max |Δp|` across all published probabilities. Ship N such that
   `max |Δp| < 0.005`. Record the check in the JSON (`convergence` block).
5. Weekly refresh mode: mid-season, starting ratings = current ratings and
   fixtures = remaining fixtures; already-played matches contribute their
   real points (read the season's facts to seed the table).
6. Site block (with B2): horizontal probability bars (title/top4/relegation)
   and a 20-cell position heat strip per team — load `dataviz` skill for the
   sequential palette and accessibility rules.

## Verification method

- `pytest tests/test_monte_carlo.py`:
  - toy league of 2 teams, fixed probabilities → analytic position
    probabilities match simulation within 3σ;
  - Σ `position_dist` = 1 per team; Σ over teams of P(position k) = 1 for
    every k; Σ title = 1; 3 relegation slots: Σ relegation = 3;
  - determinism: same seed base → identical output.
- Convergence report as in step 4.
- Sanity vs market: compare title/top-4/relegation probabilities against
  bookmaker futures prices (de-vig by proportional normalisation) at publish
  time; differences are content, not necessarily bugs — but a team we rate
  10x away from the market needs a written explanation before publishing.
- Expected points across all teams sums to ≈ 380 × 2.72 (the historical
  points-per-match yield; assert within 2%).
