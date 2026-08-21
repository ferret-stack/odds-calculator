"""
Team-name standardisation: every spelling a feed might emit must land on the
key the ELO ratings are actually stored under.

The failure this guards against is not a crash. pipeline.run_pipeline's
price_fixture() looks the rating up by exact key and returns None on a miss,
so a name that standardises to something current_elo.json does not hold is
reported as "no ELO rating for one or both teams" and the fixture is dropped
unpriced -- for a team that does have a rating.

odds_calculator imports pandas/numpy/scipy/selenium at module level, so this
module is skipped rather than failed where those are not installed; the rest
of the suite deliberately runs without them.
"""

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip('pandas')
pytest.importorskip('scipy')
pytest.importorskip('selenium')

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from odds_calculator import OddsCalculator  # noqa: E402

REPO = Path(__file__).resolve().parent.parent


@pytest.fixture(scope='module')
def standardise(tmp_path_factory):
    # data_dir is only used for file IO the constructor does not perform;
    # point it at a temp dir so constructing this never touches data/.
    calc = OddsCalculator(data_dir=str(tmp_path_factory.mktemp('data')))
    return calc.standardize_team_name


@pytest.fixture(scope='module')
def seeded_keys():
    return set(json.loads((REPO / 'data' / 'current_elo.json').read_text()))


# Every spelling on the left is one a fixture feed plausibly emits.
@pytest.mark.parametrize('raw,expected', [
    ('Hull', 'Hull City'),
    ('Hull City', 'Hull City'),
    ('Hull City AFC', 'Hull City'),
    ('Coventry', 'Coventry City'),
    ('Coventry City', 'Coventry City'),
    ('Coventry City FC', 'Coventry City'),
    # The three promoted sides are seeded together; Ipswich shortens instead,
    # so pin the direction of all three rather than only the two new rules.
    ('Ipswich Town', 'Ipswich'),
    ('Ipswich', 'Ipswich'),
])
def test_promoted_sides_reach_their_seeded_key(raw, expected, standardise,
                                               seeded_keys):
    assert standardise(raw) == expected
    assert expected in seeded_keys


# Regression: 'Hull' and 'Coventry' are matched as substrings, so they must
# not swallow a different club. These are every side currently rated.
def test_new_rules_do_not_capture_other_clubs(standardise, seeded_keys):
    for team in seeded_keys - {'Hull City', 'Coventry City'}:
        result = standardise(team)
        assert result not in ('Hull City', 'Coventry City'), (
            f'{team!r} standardised to {result!r}')


def test_every_rated_team_standardises_to_a_rated_key(standardise, seeded_keys):
    """
    No stored key may standardise to something outside the ratings file.

    A name already in canonical form has to survive the table unchanged --
    otherwise a fixture named exactly as the ratings file names it would still
    fail to price.
    """
    for team in seeded_keys:
        assert standardise(team) in seeded_keys, (
            f'{team!r} -> {standardise(team)!r}, not a rated key')
