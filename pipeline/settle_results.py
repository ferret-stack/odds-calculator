"""
Settlement: resolve pending bets in the ledger against real match results.

    python3 -m pipeline.settle_results [--results FILE] [--from-matches]
                                       [--set BET_ID=STATUS] [--dry-run]

`Ledger.settle()` has existed since the ledger was written but nothing ever
called it, so every bet the pipeline struck sat at `pending` forever and
`realised_pnl`, `strike_rate` and `roi` stayed at zero/None. This is the
trigger that closes the loop.

RESULTS COME FROM THE OPERATOR, NOT FROM A GUESS
------------------------------------------------
Two sources, both of them recorded fact:

  1. A results file (default `data/results.json`) the operator types up after
     the matches finish. One entry per fixture, a scoreline or an explicit
     void.
  2. `--from-matches`: the repository's own scraped match store
     (`matches_data.json`). No new scraper -- this reads the record the
     existing scrapers already maintain, and matches a bet to the first
     occurrence of its fixture ON OR AFTER the date the bet was struck.

A bet with no result in either source is LEFT PENDING and reported. Settlement
never infers a result from silence, and never grades a selection it does not
understand -- both are reported as unresolved so the operator can see exactly
what is outstanding.

Separate script from run_pipeline deliberately: pricing runs before kickoff
against odds, settlement runs after full time against results. Folding them
into one command would mean a pricing run could not be repeated without also
re-grading the book, and a settlement run would need fixtures and odds it has
no use for.

--dry-run grades everything and prints it without touching bankroll.json.
"""

import argparse
import json
import re
import sys
from pathlib import Path

from pipeline.bankroll import LOST, PENDING, SETTLED, VOID, WON, Ledger

# Goal-line selections: 'over_25' -> 2.5. The ledger stores the selection key
# the pipeline priced against, so grading reads the same vocabulary.
_LINE = re.compile(r'^(over|under)_(\d)(\d)$')

_SCORE = re.compile(r'^\s*(\d+)\s*[-:]\s*(\d+)\s*$')


class Unresolved(Exception):
    """No usable result for this bet. It stays pending."""


# --- grading --------------------------------------------------------------

def grade_selection(selection, home_goals, away_goals):
    """
    WON or LOST for one selection against a final scoreline.

    Raises ValueError for a selection this does not know how to grade -- an
    ungraded bet must never be silently resolved as a loss.
    """
    home_goals, away_goals = int(home_goals), int(away_goals)
    if home_goals < 0 or away_goals < 0:
        raise ValueError(f'negative scoreline {home_goals}-{away_goals}')

    total = home_goals + away_goals

    if selection == 'home':
        return WON if home_goals > away_goals else LOST
    if selection == 'away':
        return WON if away_goals > home_goals else LOST
    if selection == 'draw':
        return WON if home_goals == away_goals else LOST
    if selection == 'btts_yes':
        return WON if home_goals and away_goals else LOST
    if selection == 'btts_no':
        return LOST if home_goals and away_goals else WON

    line = _LINE.match(selection)
    if line:
        side, whole, half = line.groups()
        # Half-goal lines only, so a push is not possible.
        threshold = float(f'{whole}.{half}')
        over = total > threshold
        return WON if (over if side == 'over' else not over) else LOST

    raise ValueError(f'no grading rule for selection {selection!r}')


# --- results input --------------------------------------------------------

def normalise_fixture(name):
    return ' '.join(str(name).split()).casefold()


def parse_score(entry):
    """(home_goals, away_goals) from a result entry, or None if it is a void."""
    status = str(entry.get('status', '')).strip().lower()
    if status == VOID or entry.get('void'):
        return None

    if 'score' in entry:
        match = _SCORE.match(str(entry['score']))
        if not match:
            raise ValueError(f'cannot read score {entry["score"]!r}; '
                             f'expected "2-1"')
        return int(match.group(1)), int(match.group(2))

    if 'home_goals' in entry and 'away_goals' in entry:
        return int(entry['home_goals']), int(entry['away_goals'])

    raise ValueError(f'result entry has no score and is not marked void: '
                     f'{entry!r}')


def load_results(path):
    """
    Manual results keyed by normalised fixture name.

    Accepts a bare list or {'results': [...]}. An entry names its fixture
    either as 'fixture': 'Home v Away' or as home_team/away_team.
    """
    path = Path(path)
    if not path.exists():
        return {}

    data = json.loads(path.read_text())
    entries = data.get('results', []) if isinstance(data, dict) else data

    results = {}
    for entry in entries:
        if 'fixture' in entry:
            name = entry['fixture']
        elif 'home_team' in entry and 'away_team' in entry:
            name = f"{entry['home_team']} v {entry['away_team']}"
        else:
            raise ValueError(f'result entry names no fixture: {entry!r}')

        key = normalise_fixture(name)
        if key in results:
            raise ValueError(f'duplicate result for fixture {name!r}')
        results[key] = entry
    return results


def index_matches(path):
    """
    Scraped match records grouped by normalised fixture name, date-ordered.

    Same fixture recurs every season, so the caller disambiguates by date.
    """
    path = Path(path)
    if not path.exists():
        return {}

    index = {}
    for match in json.loads(path.read_text()):
        if match.get('home_goals') is None or match.get('away_goals') is None:
            continue
        key = normalise_fixture(f"{match['home_team']} v {match['away_team']}")
        index.setdefault(key, []).append(match)

    for records in index.values():
        records.sort(key=lambda m: m.get('date') or '')
    return index


def match_for_bet(bet, index):
    """
    The scraped match a bet was struck on: the first occurrence of that
    fixture on or after the placement date.

    Anything earlier is a previous season's meeting, and grading a bet against
    it would settle it on a match that had already been played when the bet
    was written.
    """
    records = index.get(normalise_fixture(bet.fixture))
    if not records:
        return None
    placed_on = bet.placed_at[:10]
    return next((m for m in records if (m.get('date') or '') >= placed_on),
                None)


# --- the settlement pass --------------------------------------------------

def resolve(bet, results, match_index, overrides):
    """
    (status, notes) for one pending bet.

    Raises Unresolved when no source has a result for it -- the bet stays
    pending rather than being graded on an assumption.
    """
    if bet.bet_id in overrides:
        return overrides[bet.bet_id], 'settled by hand (--set)'

    entry = results.get(normalise_fixture(bet.fixture))
    if entry is not None:
        score = parse_score(entry)
        if score is None:
            return VOID, entry.get('notes') or 'void (results file)'
        home_goals, away_goals = score
        status = grade_selection(bet.selection, home_goals, away_goals)
        return status, f'{home_goals}-{away_goals} (results file)'

    if match_index:
        match = match_for_bet(bet, match_index)
        if match is not None:
            status = grade_selection(
                bet.selection, match['home_goals'], match['away_goals'])
            return status, (f"{match['home_goals']}-{match['away_goals']} "
                            f"(matches_data {match.get('date', '?')})")

    raise Unresolved(f'no result for {bet.fixture}')


def settle_pending(ledger, results=None, match_index=None, overrides=None):
    """
    Settle every pending bet that has a result.

    Returns (settled, unresolved): a list of (bet, status) and a list of
    (bet, reason). Does not save -- the caller decides, so --dry-run can grade
    the whole book without writing.
    """
    results = results or {}
    match_index = match_index or {}
    overrides = overrides or {}

    pending_ids = {b.bet_id for b in ledger.bets if b.status == PENDING}
    unknown = sorted(set(overrides) - pending_ids)
    if unknown:
        raise KeyError(f'--set names no pending bet: {", ".join(unknown)}')

    settled, unresolved = [], []
    for bet in list(ledger.bets):
        if bet.status != PENDING:
            continue
        try:
            status, notes = resolve(bet, results, match_index, overrides)
        except Unresolved as exc:
            unresolved.append((bet, str(exc)))
            continue
        except (ValueError, KeyError, TypeError) as exc:
            unresolved.append((bet, f'{type(exc).__name__}: {exc}'))
            continue

        ledger.settle(bet.bet_id, status, notes=notes)
        settled.append((bet, status))

    return settled, unresolved


# --- CLI ------------------------------------------------------------------

def parse_override(text):
    """'00003=void' -> ('00003', 'void')."""
    bet_id, _, status = text.partition('=')
    status = status.strip().lower()
    if not bet_id or status not in SETTLED:
        raise argparse.ArgumentTypeError(
            f'--set expects BET_ID=STATUS with STATUS in '
            f'{sorted(SETTLED)}, got {text!r}')
    return bet_id.strip(), status


def print_outcome(settled, unresolved, ledger):
    print('\n' + '=' * 72)
    print('SETTLEMENT')
    print('=' * 72)
    if not settled:
        print('  nothing settled')
    for bet, status in settled:
        marker = {WON: '✓', LOST: '✗', VOID: '–'}[status]
        print(f'  {marker} {bet.bet_id}  {bet.fixture:<28} '
              f'{bet.selection:<9} @ {bet.odds:.2f}  '
              f'{status.upper():<5} {bet.profit:+.2f}   {bet.notes}')

    if unresolved:
        print('\n  still pending (no result):')
        for bet, reason in unresolved:
            print(f'    · {bet.bet_id}  {bet.fixture:<28} '
                  f'{bet.selection:<9} -- {reason}')

    print('\n' + '=' * 72)
    print('BANKROLL')
    print('=' * 72)
    print(ledger.format_summary())

    summary = ledger.summary()
    if summary['strike_rate'] is None:
        print('\n  strike rate / ROI still unavailable: no non-void bet has '
              'settled yet')


def run(data_dir='data', results_path=None, from_matches=False,
        overrides=None, dry_run=False):
    data_dir = Path(data_dir)
    # The ledger file carries its own starting_bankroll; settlement never
    # sets one, because a bankroll typed in here would silently restate the
    # PnL of every bet already on the book.
    ledger = Ledger(path=data_dir / 'bankroll.json')

    if not ledger.bets:
        print(f'No bets in {ledger.path} -- nothing to settle.')
        return ledger, [], []

    results = load_results(results_path or data_dir / 'results.json')
    match_index = index_matches(data_dir / 'matches_data.json') \
        if from_matches else {}

    print(f'{sum(1 for b in ledger.bets if b.status == PENDING)} pending '
          f'bet(s); {len(results)} manual result(s) loaded'
          + (f'; {len(match_index)} fixture(s) in the scraped store'
             if from_matches else '')
          + ('   [DRY RUN]' if dry_run else ''))

    settled, unresolved = settle_pending(
        ledger, results, match_index, dict(overrides or []))

    print_outcome(settled, unresolved, ledger)

    if dry_run:
        print(f'\n  --dry-run: {ledger.path} untouched')
    else:
        ledger.save()
        print(f'\n  {len(settled)} bet(s) settled, ledger written to '
              f'{ledger.path}')

    return ledger, settled, unresolved


def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--data', default='data')
    ap.add_argument('--results', default=None,
                    help='manual results file (default: DATA/results.json)')
    ap.add_argument('--from-matches', action='store_true',
                    help='also resolve results from the scraped '
                         'matches_data.json store')
    ap.add_argument('--set', dest='overrides', action='append', default=[],
                    type=parse_override, metavar='BET_ID=STATUS',
                    help='settle one bet by hand, e.g. --set 00003=void')
    ap.add_argument('--dry-run', action='store_true',
                    help='grade and report without writing the ledger')
    args = ap.parse_args(argv)

    try:
        run(data_dir=args.data, results_path=args.results,
            from_matches=args.from_matches, overrides=args.overrides,
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
