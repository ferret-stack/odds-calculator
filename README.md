# Premier League Odds Calculator

ELO-based model that produces the JSON data consumed by the betting calculator
on the website (`ferret-stack.github.io/boolean/odds-calculator.html`). It
scrapes match results, maintains independent ELO ratings, builds empirical
probability tables by ELO band, and compares model fair odds against bookmaker
odds to surface value bets.

## Layout

- `elo_calculator.py` — pure ELO/odds maths (ratings, margin-of-victory,
  band probabilities with venue adjustment, form metrics). No heavy deps.
- `odds_calculator.py` — the pipeline: scraping, ELO rebuild, stats, bookmaker
  odds + value bets, and JSON generation.
- `data/*.json` — generated outputs served to the website.

## Setup

```bash
pip install -r requirements.txt          # numpy + requests always; pandas/selenium optional
export ODDS_API_KEYS="key1,key2,key3"    # the-odds-api.com keys (never commit these)
```

`pandas` is only required for the one-off historical import (`import_excel`);
`selenium` + `webdriver-manager` (and Firefox) are only required for live
scraping. The ELO/stats/odds pipeline runs without them.

## Running

```bash
python odds_calculator.py
```

This scrapes the next matchweek, **rebuilds all ELO ratings deterministically
from results** (pre-match ratings, chronological), then regenerates every JSON
file. Commit and push `data/` to update the website.

### Verifying scraped cards

Yellow/red card XPATHs are absolute paths into the PL "Stats" tab. Only
`home_red` is confirmed; the other three are derived by convention. On the
first live run set `CARD_XPATH_DEBUG=1` to print the stats panel so you can
confirm/adjust the indices in `CARD_XPATHS`:

```bash
CARD_XPATH_DEBUG=1 python odds_calculator.py
```

## Key design notes

- **Full ELO rebuild every run.** Each match record stores the *pre-match* ELO
  of both teams — the ratings that actually predicted the game — so the ELO
  band tables aren't biased by post-match ratings. Rebuilding from scratch also
  removes fragile incremental "new match" detection.
- **Data-driven venue adjustment** (`venue_adjustment.json`) recomputes home/away
  multipliers from actual results each run.
- **Value bets**: `upcoming_fixtures.json` carries `model_odds` and `value_bets`
  (edge = model_prob × bookmaker_odds − 1, as a %).
