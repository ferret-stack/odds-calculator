"""
Autofill data/results.json from the scraped match store.

    python3 tools/fill_results.py [--data DIR] [--results FILE]
                                  [--all] [--overwrite] [--dry-run]

`odds_calculator.py` already scrapes every finished fixture into
`data/matches_data.json`, scoreline included. Typing those same scorelines
into `data/results.json` by hand before settling is therefore transcription,
not judgement -- and a mistyped scoreline silently restates the PnL of a
settled bet.

This writes the entries instead. By default it fills in exactly the fixtures
the ledger is waiting on: every PENDING bet, matched to the first scrape of
that fixture ON OR AFTER the date the bet was struck (the same rule
`pipeline/settle_results.py` uses, so what lands in the file is what
settlement would have graded against anyway). `--all` widens that to every
scraped fixture from the pending bets' date onwards.

WHAT IT WILL NOT DO
-------------------
  * Overwrite an entry that is already in the file. A hand-typed scoreline,
    or a void, is the operator's ruling and outranks the scrape; those are
    reported as kept, and only `--overwrite` replaces them.
  * Invent a result. A fixture with no scrape on or after the bet date is
    reported as still needing a manual entry, and nothing is written for it.
  * Mark anything void. An abandoned or postponed match has no scrape to
    read, so it stays a manual entry by design.

Every entry it writes is stamped with the match_id it came from, so a
settled bet still traces back to the record it was graded on.

If you would rather not keep the document at all, settlement can read the
scrape directly:

    python3 -m pipeline.settle_results --from-matches

That grades from `matches_data.json` without a results file. This tool is for
when you want the written record too -- one you can read, correct, and commit
alongside the ledger.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.bankroll import PENDING, Ledger, normalise_fixture
from pipeline.settle_results import index_matches, match_for_bet


def load_document(path):
    """
    The results document as (payload, entries).

    Accepts both shapes settlement accepts -- a bare list or
    {'results': [...]} -- and hands back the payload so the file's own
    surrounding keys (its `_comment` block) survive a rewrite.
    """
    path = Path(path)
    if not path.exists():
        payload = {'results': []}
        return payload, payload['results']

    payload = json.loads(path.read_text())
    if isinstance(payload, list):
        return payload, payload
    entries = payload.setdefault('results', [])
    if not isinstance(entries, list):
        raise ValueError(f"{path}: 'results' is not a list")
    return payload, entries


def entry_fixture(entry):
    """The fixture an existing entry names, normalised, or None."""
    if 'fixture' in entry:
        return normalise_fixture(entry['fixture'])
    if 'home_team' in entry and 'away_team' in entry:
        return normalise_fixture(f"{entry['home_team']} v {entry['away_team']}")
    return None


def entry_for_match(fixture, match):
    """A results-document entry for one scraped match."""
    return {
        'fixture': fixture,
        'date': match.get('date', ''),
        'score': f"{match['home_goals']}-{match['away_goals']}",
        'source': f"matches_data.json match_id {match.get('match_id', '?')}",
    }


def pending_fixtures(ledger):
    """
    Fixture name -> earliest placement date, over the ledger's pending bets.

    Keyed on the name the ledger recorded, because that is the name
    settlement will look up. Several bets on one fixture collapse to one
    entry, dated from the earliest of them.
    """
    wanted = {}
    for bet in ledger.bets:
        if bet.status != PENDING:
            continue
        key = normalise_fixture(bet.fixture)
        placed = bet.placed_at[:10]
        if key not in wanted or placed < wanted[key][1]:
            wanted[key] = (bet.fixture, placed)
    return wanted


def collect(ledger, match_index, include_all=False):
    """
    (new_entries, missing): what to add, and what has no scrape to add.

    `missing` is a list of (fixture, placed_on) the operator still has to
    type up -- an abandonment, or a matchweek not scraped yet.
    """
    wanted = pending_fixtures(ledger)
    new_entries, missing = [], []

    for key, (fixture, placed_on) in sorted(wanted.items(),
                                            key=lambda kv: kv[1][0]):
        records = match_index.get(key) or []
        match = next((m for m in records if (m.get('date') or '') >= placed_on),
                     None)
        if match is None:
            missing.append((fixture, placed_on))
            continue
        new_entries.append((key, entry_for_match(fixture, match)))

    if include_all and wanted:
        # Everything scraped from the earliest pending bet onwards, so a
        # fixture bet on later in the week is already written up.
        floor = min(placed for _, placed in wanted.values())
        for key, records in sorted(match_index.items()):
            for match in records:
                if (match.get('date') or '') < floor:
                    continue
                fixture = f"{match['home_team']} v {match['away_team']}"
                new_entries.append((key, entry_for_match(fixture, match)))
                break

    return new_entries, missing


def merge(entries, new_entries, overwrite=False):
    """
    Fold new entries into the document's list in place.

    Returns (added, kept, replaced) for the report. An entry already in the
    file wins unless `overwrite` -- see the module docstring.
    """
    by_fixture = {}
    for entry in entries:
        key = entry_fixture(entry)
        if key is not None:
            by_fixture[key] = entry

    added, kept, replaced = [], [], []
    seen = set()
    for key, entry in new_entries:
        if key in seen:
            continue
        seen.add(key)

        existing = by_fixture.get(key)
        if existing is None:
            entries.append(entry)
            by_fixture[key] = entry
            added.append(entry)
        elif overwrite:
            entries[entries.index(existing)] = entry
            by_fixture[key] = entry
            replaced.append((existing, entry))
        else:
            kept.append((existing, entry))

    return added, kept, replaced


def print_report(added, kept, replaced, missing, path, dry_run):
    print('\n' + '=' * 72)
    print('RESULTS AUTOFILL')
    print('=' * 72)

    if not added and not replaced:
        print('  nothing to write')
    for entry in added:
        print(f"  + {entry['fixture']:<32} {entry['score']:<6} "
              f"{entry['date']}")
    for old, new in replaced:
        was = old.get('score', f"{old.get('home_goals')}-{old.get('away_goals')}")
        print(f"  ~ {new['fixture']:<32} {new['score']:<6} {new['date']}   "
              f"(replaced {was})")

    if kept:
        print('\n  already in the file, left as they are '
              '(--overwrite to replace):')
        for old, new in kept:
            was = old.get('score',
                          f"{old.get('home_goals')}-{old.get('away_goals')}")
            note = '' if str(was) == new['score'] \
                else f'   << scrape says {new["score"]}'
            print(f"    · {new['fixture']:<32} {was}{note}")

    if missing:
        print('\n  no scraped result -- still needs typing up by hand:')
        for fixture, placed_on in missing:
            print(f'    · {fixture:<32} (bet placed {placed_on})')

    if dry_run:
        print(f'\n  --dry-run: {path} untouched')
    else:
        print(f'\n  {len(added) + len(replaced)} entr(y/ies) written to {path}')


def run(data_dir='data', results_path=None, include_all=False,
        overwrite=False, dry_run=False):
    data_dir = Path(data_dir)
    results_path = Path(results_path or data_dir / 'results.json')

    ledger = Ledger(path=data_dir / 'bankroll.json')
    pending = sum(1 for b in ledger.bets if b.status == PENDING)
    if not pending:
        print(f'No pending bets in {ledger.path} -- nothing to fill in.')
        return [], [], [], []

    match_index = index_matches(data_dir / 'matches_data.json')
    payload, entries = load_document(results_path)

    print(f'{pending} pending bet(s); '
          f'{len(match_index)} fixture(s) in the scraped store'
          + ('   [DRY RUN]' if dry_run else ''))

    new_entries, missing = collect(ledger, match_index, include_all=include_all)
    added, kept, replaced = merge(entries, new_entries, overwrite=overwrite)

    print_report(added, kept, replaced, missing, results_path, dry_run)

    if not dry_run and (added or replaced):
        results_path.write_text(json.dumps(payload, indent=2) + '\n')

    return added, kept, replaced, missing


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--data', default='data')
    ap.add_argument('--results', default=None,
                    help='results file to fill in (default: DATA/results.json)')
    ap.add_argument('--all', dest='include_all', action='store_true',
                    help='write every scraped fixture from the pending bets '
                         'onwards, not just the ones a bet is waiting on')
    ap.add_argument('--overwrite', action='store_true',
                    help='replace entries already in the file with the '
                         'scraped scoreline')
    ap.add_argument('--dry-run', action='store_true',
                    help='report what would be written, write nothing')
    args = ap.parse_args(argv)

    try:
        run(data_dir=args.data, results_path=args.results,
            include_all=args.include_all, overwrite=args.overwrite,
            dry_run=args.dry_run)
    except FileNotFoundError as exc:
        print(f'ERROR: required data file missing: {exc}', file=sys.stderr)
        return 1
    except Exception as exc:
        print(f'ERROR: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
