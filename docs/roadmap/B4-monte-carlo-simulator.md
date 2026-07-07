# B4 — Monte Carlo season simulator

> **Status: SPEC STUB — to be written by Fable (Brief Part B), implemented later by Opus.**

## Objective
Simulate N full seasons off the rebuilt ELO (per-match WDL from the band model + venue
adjustment) → title / top-4 / relegation / final-table-position probabilities. The
showpiece to publish at the late-August season launch.

## Fable to fill in
- Files touched
- Concrete step-by-step (fixture list source, per-match sampling from band probabilities,
  points accumulation, N iterations, aggregation, convergence check)
- Data contracts (simulation output JSON: per-team outcome probabilities)
- Verification method (probabilities sum sanely; sanity vs market futures; N-convergence)
