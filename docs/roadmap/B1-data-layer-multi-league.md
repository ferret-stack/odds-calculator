# B1 — Data-layer overhaul & multi-league architecture

> **Status: SPEC — execution-ready.** Written by Fable (Brief Part B), to be
> implemented by Opus. Priority: **next up after foundation.**
> Prerequisite reading: `docs/PART_A_REPORT.md` (the facts/derived split this
> builds on), `docs/ROADMAP.md` (benefit analysis).

## Objective

Retire the Selenium scraper (absolute XPaths against premierleague.com; already
broke once; a silent single point of failure that has cost us ~90 matches — see
per-season gaps in `data/reference/repair_report.json`) and replace it with
**football-data.co.uk CSV ingestion**. Parameterise the pipeline by league so a
new competition is a config entry, not code. **PL-only switched on this
cycle**; Championship + one European league once the foundation is proven.

Bonus that motivates the priority: football-data CSVs carry **bookmaker closing
odds**, which B3 needs for closing-line-value tracking, and shots/corners data
that enriches future markets.

## Files touched

| File | Change |
|---|---|
| `ingest.py` | NEW — football-data.co.uk downloader + normaliser + upserter |
| `leagues.py` | NEW — league configuration registry |
| `rebuild.py` | parameterise by league directory; otherwise unchanged |
| `calibration.py` | accept `--league`; default E0 |
| `odds_calculator.py` | delete `scrape_matches` + selenium imports; call `ingest` instead; loop enabled leagues |
| `tools/repair_facts.py` | freeze — one-time migration, keep for the record |
| `data/` | becomes per-league: `data/E0/…` (layout below) |
| `tests/test_ingest.py` | NEW — schema mapping, name normalisation, upsert idempotency |
| GitHub Actions workflow (if/when one runs the pipeline) | no browser setup needed; `ODDS_API_KEYS` as secret |

## Source of truth: football-data.co.uk

- URL pattern: `https://www.football-data.co.uk/mmz4281/{season}/{div}.csv`
  where `season` is e.g. `2526` for 2025-26 and `div` is `E0` (Premier
  League), `E1` (Championship), `D1` (Bundesliga), `SP1` (La Liga), `I1`
  (Serie A), `F1` (Ligue 1).
- Update cadence: roughly twice weekly in season. One GET per league per run —
  no scraping, no browser, no XPaths.
- Columns to ingest (all present in current-era files):
  - identity: `Div, Date (dd/mm/yyyy), Time, HomeTeam, AwayTeam, Referee`
  - result: `FTHG, FTAG, FTR, HTHG, HTAG`
  - discipline: `HY, AY, HR, AR, HF, AF`
  - shots/corners: `HS, AS, HST, AST, HC, AC`
  - closing odds (for B3 CLV): `PSCH, PSCD, PSCA` (Pinnacle closing),
    `AvgCH, AvgCD, AvgCA` (market-average closing), `AvgC>2.5, AvgC<2.5`
  - pre-close reference odds: `B365H, B365D, B365A, Avg>2.5, Avg<2.5`
- **Parse dates with `dayfirst=True` — non-negotiable.** The original Excel
  import parsed d/m/Y month-first and corrupted 396 stored dates (see
  `tools/repair_facts.py`). Add an ingest-time assertion: no parsed date may
  fall in June or July.
- Cache every downloaded CSV under
  `data/reference/football-data/{div}_{season}.csv` and **commit it**, so every
  rebuild is reproducible offline and source changes are visible in review.
  (Note: some sandboxed environments block this host — run the ingest where
  normal egress exists: owner's machine or GitHub Actions.)

## League configuration (data contract)

`leagues.py` exposes `LEAGUES: dict[str, League]`:

```python
LEAGUES = {
    'E0': League(
        code='E0',
        name='Premier League',
        seasons=['2021', '2122', '2223', '2324', '2425', '2526', '2627'],
        enabled=True,
        team_name_map={              # football-data name -> house name
            'Man United': 'Man Utd',
            'Tottenham': 'Spurs',
            'Sheffield United': 'Sheffield Utd',
            # ... complete the map by diffing ingested names against
            # sorted({m['home_team'] for m in existing facts})
        },
    ),
    'E1': League(code='E1', name='Championship', seasons=[...], enabled=False),
}
```

Adding a league = adding an entry and flipping `enabled`. Nothing else.

## Facts schema v2 (data contract)

`data/{code}/matches_data.json` — list of fact rows, a superset of today's
15-field schema. `rebuild.load_facts`'s assertion (no ELO-derived fields in
the facts store) still applies:

```json
{
  "match_id": "E0-2025-08-16-Arsenal-Wolves",
  "legacy_match_id": 2561890,
  "league": "E0",
  "season": "2526",
  "date": "2025-08-16",
  "kickoff_time": "15:00",
  "home_team": "Arsenal", "away_team": "Wolves",
  "home_goals": 2, "away_goals": 0,
  "ht_home_goals": 1, "ht_away_goals": 0,
  "home_yellow": 1, "away_yellow": 3, "home_red": 0, "away_red": 0,
  "home_fouls": 9, "away_fouls": 14,
  "home_shots": 17, "away_shots": 6,
  "home_shots_target": 8, "away_shots_target": 2,
  "home_corners": 7, "away_corners": 3,
  "referee": "Michael Oliver",
  "home_xg": null, "away_xg": null,
  "home_possession": null, "away_possession": null,
  "closing_odds": {"home": 1.44, "draw": 4.75, "away": 7.90,
                    "over_25": 1.85, "under_25": 2.02,
                    "source": "AvgC",
                    "pinnacle": {"home": 1.45, "draw": 4.80, "away": 8.00}},
  "source": "football-data.co.uk"
}
```

- `match_id` is the natural key `"{league}-{date}-{home_team}-{away_team}"` —
  stable across re-downloads. Old integer ids survive on legacy PL rows as
  `legacy_match_id` only.
- Fields football-data doesn't carry (`xg`, `possession`) stay `null` on new
  rows; legacy PL rows keep their existing values.
- Referee names: football-data uses `M Oliver`, existing data `Michael
  Oliver`. Ship a `referee_name_map` in `leagues.py` for the ~45 known PL
  referees (build it by matching surname + initial against
  `data/referee_stats.json` keys) so referee stats don't fork into two
  spellings. Unmapped new referees pass through in source format.

## Per-league data layout

```
data/
  E0/
    matches_data.json      matches_derived.json   elo_bands.json
    current_elo.json       elo_history.json       venue_adjustment.json
    referee_stats.json     team_stats.json        h2h_records.json
    upcoming_fixtures.json season_fixtures.json
  reference/football-data/E0_2526.csv ...
```

Until B2 re-points the site, **mirror `data/E0/*` to the legacy flat paths**
(`data/*.json` and `ferret-stack.github.io/assets/data/*.json`) so the
published page keeps working unchanged. Remove the mirror when B2 lands.

## Step-by-step

1. **Ingest module.** `ingest.py` with:
   - `fetch_csv(league, season) -> Path` — download + cache + commit
   - `parse_csv(path, league) -> list[FactRow]` — dayfirst dates, name maps,
     type coercion, closing-odds block assembly
   - `upsert(facts_path, rows) -> ChangeReport` — keyed on `match_id`; new
     rows appended; existing rows: **facts are immutable** — if a re-download
     disagrees on goals/cards, print a loud diff and refuse unless
     `--accept-source` is passed. Second run with no source change is a no-op.
2. **Historical backfill + reconciliation (one-time; the acceptance test).**
   Ingest E0 seasons 2020-21 → 2025-26 into a scratch dir. Join against the
   repaired facts on `(date, home_team, away_team)` after name mapping:
   - expect ≈ 2,190 rows to match on goals **exactly** — investigate ANY goal
     mismatch by hand before accepting either side (this doubles as the full-
     population audit of the A1 date repair, which was only spot-checked);
   - expect ≈ 90 rows present only in football-data (the silently killed
     matchweeks: 2023-24 ≈ 25, 2024-25 ≈ 51, 2025-26 ≈ 12) — backfill them;
   - merge per row: football-data wins on date/cards/referee-format; legacy
     wins on xg/possession; goals must agree.
3. **Parameterise the derive.** `rebuild.rebuild(data_dir)` already takes a
   directory — move PL data under `data/E0/`, add the legacy mirror, and loop
   `for league in enabled_leagues:` in `odds_calculator.py`. ELO replays,
   bands and venue multipliers are strictly per-league (no cross-league
   rating flow; promoted-team priors are a B4 concern).
4. **Re-run calibration** on the backfilled data
   (`python calibration.py --league E0`). Acceptance gate: log-loss ≤ 0.9917
   + 0.005 (the extra 90 matches should help, not hurt; if it degrades, stop
   and investigate — do not ship).
5. **Retire the scraper.** Delete `scrape_matches`, the selenium/webdriver
   imports and dependencies. Pipeline main becomes: ingest enabled leagues →
   rebuild each → fetch bookmaker odds (PL only for now) → regenerate
   presentation JSONs → mirror.
6. **Season fixture list** (B4 and the odds page need it): football-data
   publishes `fixtures.csv` (upcoming fixtures, all leagues). Add
   `fetch_fixtures()` → `data/{code}/season_fixtures.json`:
   `[{"date", "kickoff_time", "home_team", "away_team", "league"}]`.
7. **Second league dry-run (do not publish).** Flip `E1` on in a branch,
   ingest three seasons, rebuild, eyeball band monotonicity. This proves
   "config, not code". Leave disabled on main until the owner green-lights —
   recommendation stays PL-only this cycle.

## Verification method

- `pytest tests/test_ingest.py`: name mapping is total (every team in every
  cached CSV maps), no Jun/Jul dates, upsert idempotency, immutability
  refusal on conflicting goals.
- Reconciliation report from step 2 committed as
  `data/reference/backfill_report.json` (counts matched / backfilled /
  conflicted, every conflict enumerated). Target: 0 unexplained goal
  conflicts.
- Per-season row counts after backfill: 380 × 6.
- `python rebuild.py --data-dir data/E0` passes its sanity asserts.
- Calibration acceptance gate from step 4; paste the numbers into the PR.
- Legacy mirror byte-identical to the `data/E0` outputs.
