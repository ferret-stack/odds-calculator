"""Unit checks for the derive step. Run: python -m pytest tests/ -q"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from elo_calculator import ELOCalculator
from rebuild import band_of, band_range_label, derive_winner, derive_matches


def test_band_edges():
    assert band_of(0) == 1
    assert band_of(49) == 1
    assert band_of(50) == 2
    assert band_of(99) == 2
    assert band_of(449) == 9
    assert band_of(450) == 10
    assert band_of(2000) == 10


def test_band_labels_match_maths():
    assert band_range_label(1) == '0-49'
    assert band_range_label(2) == '50-99'
    assert band_range_label(10) == '450+'


def test_winner_from_pre_match_ratings():
    assert derive_winner(2, 1, 1600, 1500) == 'stronger'   # favourite wins at home
    assert derive_winner(0, 1, 1600, 1500) == 'weaker'     # underdog wins away
    assert derive_winner(1, 1, 1600, 1500) == 'draw'


def test_equality_rule_home_is_stronger():
    # On an exact pre-match tie the home side is the de-facto stronger team.
    assert derive_winner(1, 0, 1500, 1500) == 'stronger'
    assert derive_winner(0, 1, 1500, 1500) == 'weaker'
    assert derive_winner(2, 2, 1500, 1500) == 'draw'


def test_mov_formulas():
    fte = ELOCalculator(mov_formula='fte')
    blog = ELOCalculator(mov_formula='v6blog')
    assert fte.margin_of_victory_multiplier(0, 0) == 1.0
    assert blog.margin_of_victory_multiplier(0, 0) == 1.0
    # fte: bigger margins move ELO more; winning as favourite is dampened
    assert (fte.margin_of_victory_multiplier(3, 0)
            > fte.margin_of_victory_multiplier(1, 0))
    assert (fte.margin_of_victory_multiplier(2, 300)
            < fte.margin_of_victory_multiplier(2, -300))
    # blog variant: documented anchor points from the V6 post
    assert abs(blog.margin_of_victory_multiplier(1, 0) - 1.0466) < 1e-3
    assert blog.margin_of_victory_multiplier(3, -200) > \
        blog.margin_of_victory_multiplier(3, 0)


def test_derive_stamps_pre_match_elo():
    facts = [
        {'match_id': 1, 'date': '2023-01-01', 'home_team': 'A', 'away_team': 'B',
         'home_goals': 3, 'away_goals': 0},
        {'match_id': 2, 'date': '2023-01-08', 'home_team': 'B', 'away_team': 'A',
         'home_goals': 1, 'away_goals': 1},
    ]
    derived, _ = derive_matches(facts, driver='long')
    m1, m2 = derived
    # first-ever match: both genuinely unrated -> 1500 pre-match, band 1
    assert m1['home_elo'] == m1['away_elo'] == 1500
    assert m1['elo_band'] == 1 and m1['winner'] == 'stronger'
    # second match must see the ratings that existed AFTER match 1
    assert m2['away_elo'] == m1['long_home_elo_post'] > 1500
    assert m2['home_elo'] == m1['long_away_elo_post'] < 1500
    assert m2['winner'] == 'draw'
    # pre-match stamping: the post-match rating never appears as the match's own
    assert m1['long_home_elo_post'] != m1['home_elo']


def test_replay_is_deterministic():
    facts = [
        {'match_id': i, 'date': f'2023-01-{i:02d}', 'home_team': 'A',
         'away_team': 'B', 'home_goals': i % 3, 'away_goals': (i + 1) % 2}
        for i in range(1, 20)
    ]
    a, _ = derive_matches(facts)
    b, _ = derive_matches(facts)
    assert a == b
