# Runbook: running and deploying the calculator

Everything here is run by hand from a terminal, from the **repository root**
(the folder holding `odds_calculator.py`). Nothing runs itself: there is no
scheduler, no daemon, no CI job. If a command was not typed, it did not run.

---

## 0. One-time setup

```bash
cd /path/to/odds-calculator          # repo root -- all commands assume this
python3 -m venv .venv                # optional but recommended
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                 # then edit .env and paste your API key
```

`.env` needs at least `ODDS_API_KEY` (from the-odds-api.com).
`ODDS_API_KEY_BACKUP` is optional; it is used automatically if the primary key
hits its quota mid-run. `.env` is gitignored -- never commit it.

Check the install worked:

```bash
python3 -m pytest tests/ -q
```

---

## 1. The command to run the pipeline

`pipeline.run_pipeline` on its own is not a command -- it is a module path.
Python needs `-m` to run it:

```bash
python3 -m pipeline.run_pipeline --dry-run     # safe: writes no bets
python3 -m pipeline.run_pipeline               # real: writes bets to the ledger
```

Always run `--dry-run` first. The ledger (`data/bankroll.json`) is the one
piece of state a mistaken run is awkward to unwind.

Options:

| Flag | What it does |
|---|---|
| `--dry-run` | Price, report, print -- but write nothing to the ledger |
| `--force` | Re-price a selection already on the book, superseding the standing bet. Never touches a settled bet. Logged in the report |
| `--bankroll N` | Override the starting bankroll (first run only; after that the ledger is the source of truth) |
| `--data DIR` | Point at a different data directory (default `data`) |

See every flag straight from the script:

```bash
python3 -m pipeline.run_pipeline --help
```

**Re-running is safe.** Before writing a bet the pipeline checks the ledger for
one already standing on the same fixture + market + selection + match date, in
any status. A hit is SKIPPED, not duplicated. So running twice in a row prices,
reports, and writes nothing new.

---

## 2. The weekly cycle, in order

```bash
# a. Scrape last matchweek's results + refresh ELO and the JSON data files
python3 odds_calculator.py

# b. Sanity-check the model before staking anything against it
python3 tools/validate_elo.py
python3 tools/validate_poisson.py

# c. Price this week's fixtures -- look first, then commit
python3 -m pipeline.run_pipeline --dry-run
python3 -m pipeline.run_pipeline

# d. AFTER the matches finish: re-scrape (step a) so the results are in
#    matches_data.json, then settle straight off it -- see section 5a
python3 -m pipeline.settle_results --from-matches --dry-run
python3 -m pipeline.settle_results --from-matches

# e. Deploy: commit the updated data files and push
git add data/ docs/
git commit -m "matchweek N: results, ELO refresh, priced bets"
git push -u origin main
```

Step (a) is the only step that opens a browser and calls the odds API; it takes
minutes, the rest take seconds. It needs Firefox installed (geckodriver is
downloaded automatically if it is not on PATH). Env vars if your setup differs:

```bash
ODDS_BROWSER=chrome        # firefox (default) | chrome
ODDS_HEADLESS=0            # run headed, so you can watch it work
ODDS_DRIVER_PATH=/path/to/geckodriver
ODDS_BROWSER_BINARY=/path/to/firefox
```

`odds_calculator.py` auto-detects which matches to scrape: it takes the highest
match ID already in `matches_data.json` and scrapes the next 10. So it wants
running once per matchweek, in order -- not twice, and not skipped.

---

## 3. What a run actually prints

Real output from `--dry-run` against the current data, trimmed. This is the
shape to expect:

```
Pricing 10 fixture(s) against a staking bankroll of 885.63   [DRY RUN]

======================================================================
BANKROLL
======================================================================
  starting bankroll   : 1000.00
  open bets           : 5 (114.37 committed)
  staking bankroll    : 885.63

======================================================================
SELECTIONS
======================================================================
  · Arsenal v Coventry City   home: NO BET -- edge -2.90% below the 5% floor
  · Hull City v Man Utd       home: NO BET -- blocked by sanity check:
                                    same_market_multiple_ev
  ✓ Brentford v Spurs         home @ 2.42 | p=0.5404 EV=+0.3078 (+30.78%)
                                | full Kelly=0.2167 x 0.25 (standard)
                                = 0.0542 | stake 47.99

======================================================================
DUPLICATE CHECK
======================================================================
  ✗ SKIPPED  Brentford v Spurs / 1x2 / home (2026-08-22)
      already on the book as 00003 (pending); re-run with --force to re-price
```

Reading it:

- `·` priced, no bet. `✓` a selection that cleared the +5% EV floor.
- A `✓` in SELECTIONS is **not** a bet placed. Check DUPLICATE CHECK: a `✓` that
  reappears there as SKIPPED is already on the book and was not written again.
- `blocked by sanity check: same_market_multiple_ev` means two +EV selections
  landed in the same market on one fixture. That is deliberately unblockable --
  it is a signal to re-check the model, not a bet.
- The closing line is the one that matters: `N bet(s) written to
  data/bankroll.json`, or `--dry-run: ledger untouched`.

---

## 4. How to know the bankroll was updated

Three checks, cheapest first.

**The run prints it.** Every `run_pipeline` run opens with a BANKROLL block and
closes with a line naming the file it wrote:

```
  5 bet(s) written to data/bankroll.json
```

If you see `--dry-run: ledger untouched` instead, nothing was written -- that is
`--dry-run` doing its job, not a failure.

**Read the ledger summary directly** at any time:

```bash
python3 -c "from pipeline.bankroll import Ledger; print(Ledger().format_summary())"
```

```
  starting bankroll   : 1000.00
  current bankroll    : 1000.00
  open bets           : 5 (114.37 committed)
  staking bankroll    : 885.63
  realised PnL        : +0.00
  settled             : 0 (0W / 0L / 0V)
  bankroll growth     : +0.00%
```

**Read the file** -- it is plain JSON, append-only, and git-tracked:

```bash
python3 -m json.tool data/bankroll.json | head -30
git diff data/bankroll.json          # exactly what a run changed
```

What the numbers mean:

- `bankroll` -- starting bankroll plus **realised** profit. It only moves when a
  bet is **settled**, never when one is placed.
- `staking_bankroll` -- what Kelly sizes against: bankroll minus stake committed
  to open bets. This is the number that drops the moment a bet is placed.
- `realised_pnl`, `strike_rate`, `roi` -- stay at zero/`null` until you run
  settlement. That is expected, not a bug.

So: **placing** bets moves `staking_bankroll` and `bets_open`.
**Settling** them moves `bankroll` and `realised_pnl`.

---

## 5. Settling results (closing the loop)

Settlement is a separate command on purpose -- pricing happens before kickoff,
grading after full time.

Most of this is now filled in for you -- see 5a. What follows is the format,
which still matters for the entries the scrape cannot supply (an abandonment,
a matchweek not scraped yet) and for correcting one it got wrong.

Edit `data/results.json` and append one entry per finished fixture to the
`results` list. The fixture name must be the one the ledger recorded
(`Home v Away`); case and spacing do not matter:

```json
{
  "results": [
    {"fixture": "Nott'm Forest v Leeds", "date": "2026-08-22", "score": "2-1",
     "source": "ESPN gameId 740968"},
    {"fixture": "Spurs v Everton", "date": "2026-08-22", "status": "void",
     "notes": "abandoned"}
  ]
}
```

`source` is not read by the code -- it is there so a scoreline can be traced
back to where you got it. `score` may also be given as `home_goals` /
`away_goals`.

Then:

```bash
python3 -m pipeline.settle_results --dry-run     # grade and print, write nothing
python3 -m pipeline.settle_results               # write the ledger
```

Other options:

| Flag | What it does |
|---|---|
| `--from-matches` | Also resolve results from the scraped `data/matches_data.json` store, so you type up less |
| `--set BET_ID=STATUS` | Settle one bet by hand, e.g. `--set 00003=void` |
| `--results FILE` | Use a results file other than `data/results.json` |

A bet with no result in either source is **left pending and reported**.
Settlement never infers a result from silence.

### 5a. Settling without typing anything up

Most weeks you do not need to touch `results.json` at all.
`odds_calculator.py` already scrapes every finished fixture, scoreline
included, into `data/matches_data.json`. `--from-matches` tells settlement to
read that store, so the scorelines never get retyped:

```bash
python3 -m pipeline.settle_results --from-matches --dry-run
python3 -m pipeline.settle_results --from-matches
```

Each bet is matched to the first scrape of its fixture **on or after the date
the bet was struck** -- so last season's meeting of the same two teams can
never settle this season's bet. A fixture with no such scrape stays pending
and is named in the report.

Order matters: this reads `matches_data.json`, so run step (a) for the
finished matchweek first, or there is nothing to read.

**The results file still wins where it has an entry.** Settlement checks it
before the scrape, and without a date check, so `results.json` is where you
overrule the scrape -- an abandonment to void, a fixture the scraper got
wrong. It is also where a stale entry hides: one typed in with the wrong
scoreline beats the correct scraped one, silently and permanently. If you are
not deliberately overruling something, leave the file empty of that fixture
and let `--from-matches` do it.

That is a real risk, not a hypothetical: the `Newcastle v Liverpool` entry sat
in this file as `1-1` when the match finished `2-2`. It cost nothing on a 1x2
bet -- a draw either way -- but the same typo on an over/under would have
graded the bet backwards (1-1 under 2.5, 2-2 over).

Find a bet's ID:

```bash
python3 -c "import json;[print(b['bet_id'],b['status'],b['fixture'],b['selection']) for b in json.load(open('data/bankroll.json'))['bets']]"
```

---

## 6. Supporting tools

```bash
python3 tools/validate_elo.py            # ELO output vs recorded results
python3 tools/validate_poisson.py        # Poisson probabilities sum/consistency
python3 tools/rebuild_elo.py --dry-run   # rebuild ratings from match history
python3 tools/rebuild_elo.py             # ...for real (writes data/)
python3 tools/repair_card_data.py --dry-run
python3 tools/super6_picks.py            # Super 6 picks (needs a local Ollama)
```

Every one of these takes `--help`, and the destructive ones take `--dry-run`.

`validate_poisson.py` and `odds_calculator.py` import pandas/numpy/scipy, so
they need the full `requirements.txt` install. `validate_elo.py`,
`rebuild_elo.py`, `repair_card_data.py` and the whole `pipeline/` package run on
the standard library alone -- useful to know when you are on a machine where the
heavy install failed: you can still price, settle and read the ledger.

---

## 7. Deploying

There is no deploy server, no CI workflow and no GitHub Pages branch in *this*
repository. Pushing here commits the record -- the ledger, the refreshed data
files, the write-up -- and that is the part this repo owns.

The public calculator is served from a **different** repository
(`ferret-stack/boolean`, at ferret-stack.github.io/boolean/odds-calculator/).
How the JSON in `data/` gets from here to there is not described anywhere in
this repo, so it is not written down here either rather than guessed at. If it
is a manual copy, that step belongs in this section -- worth adding the next
time you do it.

```bash
git status                    # see what the run changed
git diff data/bankroll.json   # read the ledger change before committing it
git add data/ docs/
git commit -m "matchweek N: priced bets and settled results"
git push -u origin main
```

Commit the ledger with the run that produced it -- `data/bankroll.json` is the
audit trail, and an uncommitted one is a run nobody can reconstruct. Never
commit `.env`.

---

## 8. Habits worth keeping

- `--dry-run` first, every time, on both `run_pipeline` and `settle_results`.
- Both scripts **exit non-zero on failure**, so a red error line means nothing
  was written. Check `echo $?` if you are unsure.
- Never hand-edit `data/bankroll.json`. Use `--set` for a one-off correction --
  it keeps the file's history honest.
- `--force` is for a deliberate re-price only. It supersedes the standing bet
  rather than adding a second one, and the report says it did.
- Settle before pricing the next week: Kelly stakes as a fraction of *current*
  bankroll, so pricing against a stale balance silently changes your stake size.
