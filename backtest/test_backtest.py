#!/usr/bin/env python3
"""
Self-checks for the backtest harness.

pytest-compatible (matching tests/), and runnable directly with
`python3 backtest/test_backtest.py` where pytest is not installed.

These check the properties the ROI numbers depend on: that the EV arithmetic
is production's, that settlement is right, and -- the one that matters most --
that the walk-forward genuinely does not see the future.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtest import report                                    # noqa: E402
from backtest import data                                      # noqa: E402
from backtest.data import DataError, load_league, load_legacy_pl_flat  # noqa: E402
from backtest import engine                                    # noqa: E402
from backtest.engine import MARKETS, SETTLES, band_is_degenerate  # noqa: E402
from tools.rebuild_elo import league_table                     # noqa: E402


def run(matches, **kwargs):
    """The original PL tests exercise the legacy-seeded PL chain."""
    kwargs.setdefault('seeding', 'legacy')
    kwargs.setdefault('league', 'E0')
    return engine.run(matches, **kwargs)

from elo_calculator import calculate_elo_bands, elo_band       # noqa: E402
from pipeline.staking import expected_value                    # noqa: E402


def load_all():
    """The legacy flat PL matches, as the original tests consumed them."""
    return load_legacy_pl_flat()[0]


def test_dataset_is_complete():
    matches = load_all()
    assert len(matches) == 2280, 'six PL seasons, 380 matches each'
    assert sorted({m['season'] for m in matches}) == [
        '2020-21', '2021-22', '2022-23', '2023-24', '2024-25', '2025-26']
    assert matches == sorted(matches, key=lambda m: (m['date'], m['time'],
                                                     m['home_team']))


def test_edge_matches_production_ev_formula():
    """edge_pct must be exactly staking.expected_value, not a re-derivation."""
    result = run(load_all())
    for row in result['fixtures'][:2000]:
        expected = expected_value(row['model_prob'], row['odds']) * 100
        assert abs(row['edge_pct'] - expected) < 1e-6, row


def test_settlement_follows_the_result():
    result = run(load_all())
    for row in result['fixtures']:
        assert row['won'] == (row['ftr'] == SETTLES[row['selection']])
        if not row['bet']:
            assert row['profit'] == 0.0
        elif row['won']:
            assert row['profit'] > 0
        else:
            assert row['profit'] == -row['stake']


def test_no_lookahead_in_the_band_table():
    """
    The strongest statement of the property: truncating the future must not
    change the past.

    A fixture priced on 2023-01-01 must get the same probability, edge and
    stake whether or not the 2024-25 and 2025-26 seasons are present in the
    input. If any future match leaked into the band table -- or into the ELO
    chain, or the promoted-team seeding -- the truncated run would disagree.
    """
    matches = load_all()
    cutoff = '2024-06-01'
    full = run(matches)
    truncated = run([m for m in matches if m['date'] < cutoff])

    def key(row):
        return (row['date'], row['home_team'], row['book'], row['selection'])

    full_rows = {key(r): r for r in full['fixtures'] if r['date'] < cutoff}
    truncated_rows = {key(r): r for r in truncated['fixtures']}

    assert full_rows, 'expected priced fixtures before the cutoff'
    assert set(full_rows) == set(truncated_rows), (
        'truncating the future changed WHICH selections were priced')
    for row_key, row in full_rows.items():
        assert row == truncated_rows[row_key], (
            f'truncating the future changed {row_key}')


def test_chain_is_deterministic():
    matches = load_all()
    first = run(matches)
    second = run(matches)
    assert first['fixtures'] == second['fixtures']
    assert first['final_elo'] == second['final_elo']


def test_degenerate_band_is_detected():
    """An all-even band prices to 0/0/0 and must not reach calculate_fair_odds."""
    assert band_is_degenerate({})
    assert band_is_degenerate({'stronger_win_pct': 0.0, 'draw_pct': 0.0,
                               'weaker_win_pct': 0.0})
    assert not band_is_degenerate({'stronger_win_pct': 0.4, 'draw_pct': 0.0,
                                   'weaker_win_pct': 0.6})


def test_probabilities_are_a_normalised_1x2():
    result = run(load_all())
    by_fixture = {}
    for row in result['fixtures']:
        if row['book'] != 'avg':
            continue
        by_fixture.setdefault((row['date'], row['home_team']), {})[
            row['selection']] = row['model_prob']
    complete = [p for p in by_fixture.values() if len(p) == len(MARKETS)]
    assert complete, 'expected fixtures priced on all three selections'
    for probs in complete:
        # calculate_fair_odds rounds each leg to 4dp, so the sum can miss 1.0
        # by up to 1.5e-4. Anything larger means the normalisation is wrong.
        assert abs(sum(probs.values()) - 1.0) < 2e-4, probs


def test_yield_and_flat_roi_agree_on_sign_for_a_known_set():
    rows = [
        {'stake': 10.0, 'profit': 10.0, 'won': True, 'odds': 2.0,
         'model_prob': 0.6, 'market_fair': 0.5, 'edge_pct': 20.0},
        {'stake': 10.0, 'profit': -10.0, 'won': False, 'odds': 2.0,
         'model_prob': 0.6, 'market_fair': 0.5, 'edge_pct': 20.0},
    ]
    summary = report.summarise(rows, 'test')
    assert summary['bets'] == 2
    assert summary['staked'] == 20.0
    assert summary['profit'] == 0.0
    assert summary['yield_pct'] == 0.0
    assert summary['flat_roi_pct'] == 0.0
    assert summary['strike_rate_pct'] == 50.0


def test_edge_buckets_partition_the_floor():
    assert report.edge_bucket(5.0) == '5-10%'
    assert report.edge_bucket(9.999) == '5-10%'
    assert report.edge_bucket(10.0) == '10-20%'
    assert report.edge_bucket(19.999) == '10-20%'
    assert report.edge_bucket(20.0) == '20%+'
    assert report.edge_bucket(4.99) == 'below floor'


def test_production_is_not_written_to():
    """
    A full run must leave every production file byte-identical.

    Checked by content hash rather than by reading the source for suspicious
    strings, so it holds however the harness is refactored.
    """
    import hashlib

    watched = sorted(
        [p for p in (REPO_ROOT / 'data').glob('*.json')]
        + [p for p in (REPO_ROOT / 'pipeline').glob('*.py')]
        + [p for p in (REPO_ROOT / 'tools').glob('*.py')]
        + [REPO_ROOT / 'elo_calculator.py']
        + [p for p in (REPO_ROOT / 'docs' / 'historical').glob('*.csv')])
    assert watched, 'expected production files to watch'

    def digest():
        return {p: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in watched if p.exists()}

    before = digest()
    run(load_all())
    assert digest() == before, 'a production file changed during a run'


# --- generalised loader ---------------------------------------------------

HEADER = 'Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,AvgH,AvgD,AvgA'


def _write_league(root, code, year, lines, header=HEADER):
    directory = Path(root) / code
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f'{year}.csv').write_text(
        header + '\n' + '\n'.join(lines) + '\n', encoding='utf-8')


def _raises(exc_type, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc_type as exc:
        return exc
    raise AssertionError(f'expected {exc_type.__name__}')


def test_holdout_is_never_loaded_through_the_normal_loader():
    """E2/2026.csv and E3/2026.csv (and every other league's) must raise."""
    for code in ('E2', 'E3', 'E0', 'E1', 'EC'):
        path = data.DEFAULT_DIR / code / '2026.csv'
        assert path.exists(), f'{path} should exist for this test to mean anything'
        exc = _raises(AssertionError, load_league, code, seasons=[2026])
        assert 'HOLDOUT' in str(exc)
        _raises(AssertionError, load_league, code,
                seasons=range(2020, 2027), bet_seasons=range(2020, 2027))
        # Named only as a betting season: still refused, before any read.
        _raises(AssertionError, load_league, code, seasons=[2025],
                bet_seasons=[2026])
        # Even the lowest-level reader refuses, with or without the flag
        # the legacy loader uses.
        _raises(AssertionError, data._read_rows, path)
        _raises(AssertionError, data._read_rows, path, legacy_regression=True)
        assert 2026 not in data.available_seasons(code)
    # The flat PL holdout file is refused too unless the legacy flag is set.
    _raises(AssertionError, data._read_rows, data.DEFAULT_DIR / '2026.csv')


def test_legacy_loader_takes_no_arguments():
    import inspect
    assert not inspect.signature(load_legacy_pl_flat).parameters


def test_missing_odds_column_is_an_error():
    import tempfile
    with tempfile.TemporaryDirectory() as root:
        _write_league(root, 'E2', 2020, ['E2,03/08/2019,A,B,1,0,H'],
                      header='Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR')
        # Fine as a warm-up season: no odds are read.
        matches, skips = load_league('E2', seasons=[2020], root=root)
        assert len(matches) == 1 and matches[0]['odds'] == {} and skips == []
        exc = _raises(DataError, load_league, 'E2', seasons=[2020],
                      bet_seasons=[2020], root=root)
        assert 'AvgH' in str(exc)


def test_blank_price_is_skipped_and_counted():
    import tempfile
    with tempfile.TemporaryDirectory() as root:
        _write_league(root, 'EC', 2020, [
            'EC,03/08/2019,A,B,1,0,H,2.0,3.4,3.6',
            'EC,03/08/2019,C,D,0,0,D,2.0,,3.6',      # blank draw price
            'EC,04/08/2019,E,F,0,2,A,1.0,3.4,3.6',   # unusable price
        ])
        matches, skips = load_league('EC', seasons=[2020], bet_seasons=[2020],
                                     root=root)
        assert len(matches) == 3, 'skipped rows still feed the ELO chain'
        assert [bool(m['odds']) for m in matches] == [True, False, False]
        assert matches[0]['odds']['avg'] == {'home': 2.0, 'draw': 3.4,
                                            'away': 3.6}
        assert skips == [{'league': 'EC', 'season': '2019-20',
                          'family': 'avg', 'rows': 3, 'skipped': 2}]


def test_aliases_are_applied_and_distinct_names_are_not():
    import tempfile
    aliases = data.load_aliases()
    assert aliases == {'AFC Telford United': 'Telford United',
                       'Wimbledon': 'Milton Keynes Dons',
                       'Boston': 'Boston Utd'}
    with tempfile.TemporaryDirectory() as root:
        _write_league(root, 'E3', 2003, [
            'E3,10/08/2002,Boston,Oxford City,1,0,H,2.0,3.4,3.6',
            'E3,10/08/2002,AFC Wimbledon,Northwich,1,1,D,2.0,3.4,3.6',
        ])
        matches, _ = load_league('E3', seasons=[2003], root=root)
        names = {t for m in matches for t in (m['home_team'], m['away_team'])}
        assert names == {'Boston Utd', 'Oxford City', 'AFC Wimbledon',
                         'Northwich'}


def test_unknown_league_and_family_are_errors():
    _raises(DataError, load_league, 'E9')
    _raises(DataError, load_league, 'E2', families=('bbav',))


def test_real_league_files_load_from_the_earliest_season():
    """Structure only: seasons present, order, and odds confined to bet seasons."""
    matches, skips = load_league('E2', bet_seasons=range(2020, 2026))
    seasons = sorted({m['season'] for m in matches})
    assert seasons[0] == '1993-94' and seasons[-1] == '2024-25'
    assert matches == sorted(matches, key=lambda m: (m['date'], m['time'],
                                                     m['home_team']))
    assert all(not m['odds'] for m in matches if m['season'] < '2019-20')
    assert [s['season'] for s in skips] == [
        '2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']
    assert 'Wimbledon' not in {m['home_team'] for m in matches}



# --- per-league chain and entrant seeding ----------------------------------

def _season(label, year, teams):
    """A double round robin with scores that give a strict table order."""
    out, day = [], 0
    for i, home in enumerate(teams):
        for j, away in enumerate(teams):
            if home == away:
                continue
            day += 1
            out.append({
                'season': label, 'date': f'{year}-{9 + day // 28:02d}-{1 + day % 28:02d}',
                'time': '', 'home_team': home, 'away_team': away,
                # Earlier in `teams` = stronger: wins by a margin that grows
                # with the gap, so the final table is teams[] in order.
                'home_goals': max(0, j - i) + (1 if i < j else 0),
                'away_goals': max(0, i - j),
                'ftr': 'H' if i < j else 'A', 'odds': {},
                'total_booking_points': None})
    return out


def _mean(values):
    values = list(values)
    return round(sum(values) / len(values))


def test_legacy_seeding_is_refused_outside_e0():
    matches = _season('2001-02', 2001, list('ABCD'))
    for code in ('E1', 'E2', 'E3', 'EC', None):
        exc = _raises(ValueError, engine.run, matches, seeding='legacy',
                      league=code)
        assert 'E0' in str(exc)
    _raises(ValueError, engine.run, matches, seeding='nope', league='E2')
    assert engine.run.__defaults__ is not None
    import inspect
    assert inspect.signature(engine.run).parameters['seeding'].default == 'league'


def test_league_seeding_rules():
    one = _season('2001-02', 2001, list('ABCDEF'))
    # E and F leave; P comes up, R comes down, U arrives from nowhere known.
    two = _season('2002-03', 2002, ['A', 'B', 'C', 'D', 'P', 'R', 'U'])
    # E returns after a one-season gap, by relegation from above; F returns
    # by promotion. Both must be re-seeded, not carried.
    three = _season('2003-04', 2003, ['A', 'B', 'C', 'D', 'P', 'E', 'F'])
    directions = {('2002-03', 'P'): 'promoted', ('2002-03', 'R'): 'relegated',
                  ('2003-04', 'E'): 'relegated', ('2003-04', 'F'): 'promoted'}

    def go(matches):
        return engine.run(matches, seeding='league', league='E2',
                          directions=directions, bet_seasons=())

    after_one = go(one)['final_elo']
    after_two = go(one + two)['final_elo']
    full = go(one + two + three)
    seeds = {(s['season'], s['team']): s for s in full['seeding']}

    # Opening season: everyone at SEED_ELO.
    for team in 'ABCDEF':
        assert seeds[('2001-02', team)]['elo'] == engine.SEED_ELO

    # Season two, from ratings as they stood at the end of season one.
    bottom = [t for t, _ in league_table(one)[-4:]]
    assert bottom == ['C', 'D', 'E', 'F']
    bottom_avg = _mean(after_one[t] for t in bottom)
    league_avg = _mean(after_one[t] for t in 'ABCDEF')
    assert bottom_avg != league_avg, 'the test must distinguish the rules'
    assert seeds[('2002-03', 'P')]['elo'] == bottom_avg       # promoted in
    assert seeds[('2002-03', 'R')]['elo'] == league_avg       # relegated in
    assert seeds[('2002-03', 'U')]['elo'] == league_avg       # unknown
    assert 'direction unknown' in seeds[('2002-03', 'U')]['note']
    # Continuing teams are never re-seeded.
    assert ('2002-03', 'A') not in seeds

    # Season three: re-entrants after a gap are new entrants.
    members_two = ['A', 'B', 'C', 'D', 'P', 'R', 'U']
    bottom_two = [t for t, _ in league_table(two)[-4:]]
    e_seed, f_seed = seeds[('2003-04', 'E')], seeds[('2003-04', 'F')]
    assert e_seed['elo'] == _mean(after_two[t] for t in members_two)
    assert f_seed['elo'] == _mean(after_two[t] for t in bottom_two)
    assert f'stale {after_one["E"]} replaced' in e_seed['note']
    assert f'stale {after_one["F"]} replaced' in f_seed['note']
    assert len(full['seeding']) == 6 + 3 + 2


def test_bet_seasons_restrict_pricing():
    one = _season('2001-02', 2001, list('ABCDEF'))
    two = _season('2002-03', 2002, list('ABCDEF'))
    for m in two:
        m['odds'] = {'avg': {'home': 2.5, 'draw': 3.3, 'away': 2.9}}
    result = engine.run(one + two, seeding='league', league='E2',
                        bet_seasons=['2002-03'], books=('avg',))
    assert result['fixtures']
    assert {r['season'] for r in result['fixtures']} == {'2002-03'}
    assert all(a['date'] >= two[0]['date'] for a in result['anomalies'])
    _raises(ValueError, engine.run, one, seeding='league', league='E2',
            bet_seasons=['2010-11'])


def test_entrant_directions_from_adjacent_leagues():
    """Names only. Known movements, read from the previous season's files."""
    e2 = data.entrant_directions('E2')
    assert e2[('2004-05', 'Milton Keynes Dons')] == 'relegated'   # via alias
    assert e2[('2024-25', 'Birmingham')] == 'relegated'
    assert e2[('2024-25', 'Stockport')] == 'promoted'
    assert not any(season >= '2025-26' for season, _team in e2)
    ec = data.entrant_directions('EC')
    assert ec[('2024-25', 'Boston Utd')] == 'unknown'
    assert set(ec.values()) <= {'relegated', 'unknown'}
    assert set(data.entrant_directions('E0').values()) <= {'promoted', 'unknown'}
    _raises(AssertionError, data.league_teams, 'E2', 2026)


def test_league_chain_has_no_lookahead():
    """Truncating the future must not change the past, league policy too."""
    matches, _ = load_league('E3', bet_seasons=range(2020, 2026))
    directions = data.entrant_directions('E3')
    bet = ['2019-20', '2020-21', '2021-22', '2022-23', '2023-24', '2024-25']
    cutoff = '2023-06-01'

    def go(rows, seasons):
        return engine.run(rows, seeding='league', league='E3',
                          directions=directions, bet_seasons=seasons,
                          books=('avg',))

    full = go(matches, bet)
    truncated = go([m for m in matches if m['date'] < cutoff], bet[:4])

    def key(row):
        return (row['date'], row['home_team'], row['book'], row['selection'])

    full_rows = {key(r): r for r in full['fixtures'] if r['date'] < cutoff}
    truncated_rows = {key(r): r for r in truncated['fixtures']}
    assert full_rows and full_rows == truncated_rows
    assert ([s for s in full['seeding'] if s['season'] <= '2022-23']
            == truncated['seeding'])


def test_flat_roi_ci_is_the_normal_approximation():
    rows = [{'odds': 2.0, 'won': True}, {'odds': 2.0, 'won': False}]
    # returns +1, -1: mean 0, sample sd sqrt(2), se 1 -> +/- 196%.
    assert report.flat_roi_ci(rows) == (-196.0, 196.0)
    assert report.flat_roi_ci(rows[:1]) == (None, None)
    rows = [{'odds': 3.0, 'won': i % 3 == 0} for i in range(300)]
    low, high = report.flat_roi_ci(rows)
    assert low < 0.0 < high and abs((low + high) / 2) < 0.01


def test_league_runner_cannot_reach_the_holdout():
    from backtest import run_backtest
    for year in ('2026', '2027'):
        _raises(AssertionError, run_backtest.main,
                ['--league', 'E2', '--bet-to', year])


if __name__ == '__main__':
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith('test_') or not callable(fn):
            continue
        try:
            fn()
            print(f'  PASS  {name}')
        except Exception as exc:  # noqa: BLE001 -- report, keep going
            failures += 1
            print(f'  FAIL  {name}: {type(exc).__name__}: {exc}')
    print(f'\n{failures} failure(s)')
    raise SystemExit(1 if failures else 0)
