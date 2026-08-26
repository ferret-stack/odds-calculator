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

## 1. The command you actually asked about

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

# d. AFTER the matches finish: type up results, then settle
#    (edit data/results.json first -- see section 4)
python3 -m pipeline.settle_results --dry-run
python3 -m pipeline.settle_results

# e. Deploy: commit the updated data files and push
git add data/ docs/
git commit -m "matchweek N: results, ELO refresh, priced bets"
git push -u origin main
```

Step (a) is the only step that opens a browser and calls the odds API; it takes
minutes, the rest take seconds.

---

## 3. How to know the bankroll was updated

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

## 4. Settling results (closing the loop)

Settlement is a separate command on purpose -- pricing happens before kickoff,
grading after full time.

Edit `data/results.json` and add one entry per finished fixture. The fixture
name must be the one the ledger recorded (`Home v Away`); case and spacing do
not matter:

```json
{"fixture": "Nott'm Forest v Leeds", "score": "2-1"}
{"fixture": "Spurs v Everton", "status": "void", "notes": "abandoned"}
```

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

Find a bet's ID:

```bash
python3 -c "import json;[print(b['bet_id'],b['status'],b['fixture'],b['selection']) for b in json.load(open('data/bankroll.json'))['bets']]"
```

---

## 5. Supporting tools

```bash
python3 tools/validate_elo.py            # ELO output vs recorded results
python3 tools/validate_poisson.py        # Poisson probabilities sum/consistency
python3 tools/rebuild_elo.py --dry-run   # rebuild ratings from match history
python3 tools/rebuild_elo.py             # ...for real (writes data/)
python3 tools/repair_card_data.py --dry-run
python3 tools/super6_picks.py            # Super 6 picks (needs a local Ollama)
```

Every one of these takes `--help`, and the destructive ones take `--dry-run`.

---

## 6. Deploying

There is no deploy server and no CI workflow. "Deploy" here means: the JSON in
`data/` is the published artefact, so pushing it to GitHub is the deployment.

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

## 7. Habits worth keeping

- `--dry-run` first, every time, on both `run_pipeline` and `settle_results`.
- Both scripts **exit non-zero on failure**, so a red error line means nothing
  was written. Check `echo $?` if you are unsure.
- Never hand-edit `data/bankroll.json`. Use `--set` for a one-off correction --
  it keeps the file's history honest.
- `--force` is for a deliberate re-price only. It supersedes the standing bet
  rather than adding a second one, and the report says it did.
- Settle before pricing the next week: Kelly stakes as a fraction of *current*
  bankroll, so pricing against a stale balance silently changes your stake size.
