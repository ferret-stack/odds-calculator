"""
update_elo_ratings() must never crash on a scoreless record.

Reported bug: a live run scraped its matches fine, then update_elo_ratings()
raised `KeyError: 'home_goals'`. new_matches is every record missing the
elo_processed flag -- that set is not guaranteed to be goals-complete, since
a record can pick up the flag's absence from any source (a partial import, a
fixture appended before full-time, a hand-edited file) without necessarily
carrying a result. The fix: split those records out, report them by match
id/date/teams instead of raising, and still process every match that does
have a result.

odds_calculator imports pandas/numpy/scipy/selenium at module level, so this
module is skipped rather than failed where those are not installed.
"""

import sys
from pathlib import Path

import pytest

pytest.importorskip('pandas')
pytest.importorskip('scipy')
pytest.importorskip('selenium')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from odds_calculator import OddsCalculator  # noqa: E402


def make_match(match_id, home, away, home_goals=None, away_goals=None,
               date='2026-08-16'):
    match = {
        'match_id': match_id,
        'date': date,
        'home_team': home,
        'away_team': away,
    }
    if home_goals is not None:
        match['home_goals'] = home_goals
    if away_goals is not None:
        match['away_goals'] = away_goals
    return match


@pytest.fixture
def calc(tmp_path):
    return OddsCalculator(data_dir=str(tmp_path))


def test_scoreless_record_is_skipped_not_raised(calc, capsys):
    calc.matches_data = [
        make_match(1, 'Arsenal', 'Chelsea', home_goals=2, away_goals=1),
        # No 'home_goals' / 'away_goals' at all -- the reported crash.
        make_match(2, 'Man City', 'Liverpool'),
    ]

    calc.update_elo_ratings()  # must not raise

    processed = {m['match_id']: m for m in calc.matches_data}
    assert processed[1].get('elo_processed') is True
    assert 'elo_processed' not in processed[2]

    out = capsys.readouterr().out
    assert 'Skipping 1 unprocessed match' in out
    assert 'match 2' in out
    assert 'Man City vs Liverpool' in out


def test_all_scoreless_leaves_nothing_to_process(calc, capsys):
    calc.matches_data = [make_match(1, 'Arsenal', 'Chelsea')]

    calc.update_elo_ratings()  # must not raise

    out = capsys.readouterr().out
    assert 'Skipping 1 unprocessed match' in out
    assert 'No new matches to process' in out


def test_fully_scored_matches_process_normally(calc):
    calc.matches_data = [
        make_match(1, 'Arsenal', 'Chelsea', home_goals=2, away_goals=1),
        make_match(2, 'Man City', 'Liverpool', home_goals=1, away_goals=1,
                   date='2026-08-17'),
    ]

    calc.update_elo_ratings()

    assert all(m.get('elo_processed') for m in calc.matches_data)
