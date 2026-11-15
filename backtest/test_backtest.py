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


# --- diagnostic: does the model add to the market? --------------------------

def _synthetic_obs(n, true_b, seed):
    """Matches whose true probabilities are market^1 * model^true_b."""
    import math
    import random
    from backtest import diagnostic
    rng = random.Random(seed)
    obs = []
    for i in range(n):
        def simplex():
            raw = [rng.uniform(0.15, 1.0) for _ in range(3)]
            return [x / sum(raw) for x in raw]
        market, model = simplex(), simplex()
        features = [(math.log(m), math.log(p)) for m, p in zip(market, model)]
        truth = diagnostic._probabilities(features, [1.0, true_b], (0, 1))
        winner = rng.choices(range(3), weights=truth)[0]
        obs.append(('2019-20' if i % 2 else '2022-23', features, winner))
    return obs


def test_diagnostic_finds_no_signal_when_the_model_is_noise():
    from backtest import diagnostic
    result = diagnostic.analyse(_synthetic_obs(4000, 0.0, seed=1))
    assert abs(result['a'] - 1.0) < 0.1
    assert result['b_ci'][0] < 0.0 < result['b_ci'][1]
    assert diagnostic.verdict(result) == 'NO SIGNAL'


def test_diagnostic_recovers_a_real_signal():
    from backtest import diagnostic
    result = diagnostic.analyse(_synthetic_obs(4000, 0.5, seed=2))
    assert abs(result['b'] - 0.5) < 0.1 and abs(result['a'] - 1.0) < 0.1
    assert diagnostic.verdict(result) == 'SIGNAL'
    assert result['oos']['improvement'] > 0


def test_diagnostic_observations_floor_a_zero_model_probability():
    import math
    from backtest import diagnostic
    rows = [{'book': 'avg', 'date': '2020-01-01', 'home_team': 'A',
             'away_team': 'B', 'season': '2019-20', 'ftr': 'D',
             'selection': sel, 'market_fair': fair, 'model_prob': prob}
            for sel, fair, prob in (('home', 0.5, 0.7), ('draw', 0.3, 0.3),
                                    ('away', 0.2, 0.0))]
    (season, features, winner), = diagnostic.observations(rows, 'avg')
    assert (season, winner) == ('2019-20', 1)
    assert features[2] == (math.log(0.2), math.log(diagnostic.MODEL_PROB_FLOOR))
    _raises(ValueError, diagnostic.observations, rows[:2], 'avg')


# --- line-movement proxy -----------------------------------------------------

def _synthetic_moves(n, seed, true_beta=0.0, drift=0.0, model_sharpen=1.0):
    """
    Matches with y = true_beta * d + drift * (m - 1/3) + noise, every
    quantity summing to zero within a match as the real ones do.
    model_sharpen > 1 makes the model lean toward favourites, so d is
    correlated with m.
    """
    import math
    import random
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        raw = [rng.uniform(0.15, 1.0) for _ in range(3)]
        m = [x / sum(raw) for x in raw]
        raw = [mk ** model_sharpen * math.exp(rng.gauss(0, 0.3)) for mk in m]
        p = [x / sum(raw) for x in raw]
        noise = [rng.gauss(0, 0.02) for _ in range(3)]
        noise = [e - sum(noise) / 3 for e in noise]
        for k, selection in enumerate(('home', 'draw', 'away')):
            d = p[k] - m[k]
            y = true_beta * d + drift * (m[k] - 1 / 3) + noise[k]
            rows.append({'league': 'E2', 'season': '2019-20',
                         'date': f'd{i // 10}', 'match': ('E2', i),
                         'selection': selection, 'm': m[k], 'c': m[k] + y,
                         'p': p[k], 'd': d, 'y': y})
    return rows


def test_line_movement_finds_no_signal_when_moves_ignore_the_model():
    from backtest import line_movement as lm
    result = lm.analyse(_synthetic_moves(3000, seed=11))
    assert result['beta_ci'][0] < 0.0 < result['beta_ci'][1]
    assert lm.verdict(result) == 'NO SIGNAL'


def test_line_movement_recovers_a_real_signal():
    from backtest import line_movement as lm
    result = lm.analyse(_synthetic_moves(3000, seed=12, true_beta=0.3))
    low, high = result['beta_ci']
    assert low < 0.3 < high and low > 0
    assert lm.verdict(result) == 'SIGNAL'


def test_line_movement_controls_absorb_a_price_level_drift():
    # Moves depend only on price level; the model leans to favourites, so
    # the simple slope (B) is fooled and the primary (A) is not.
    from backtest import line_movement as lm
    result = lm.analyse(_synthetic_moves(3000, seed=13, drift=0.05,
                                         model_sharpen=1.5))
    assert result['beta_simple_ci'][0] > 0
    assert result['beta_ci'][0] < 0.0 < result['beta_ci'][1]
    assert lm.verdict(result) == 'NO SIGNAL'


def test_line_movement_cluster_se_matches_hc1_with_singleton_clusters():
    # With one row per cluster, CR1 reduces to HC1:
    # var(b) = n/(n-k) * sum((x - xbar)^2 e^2) / sum((x - xbar)^2)^2
    import math
    from backtest import line_movement as lm
    xs = [0.1, -0.2, 0.05, 0.3, -0.1, 0.0]
    ys = [0.02, -0.01, 0.0, 0.05, -0.03, 0.01]
    rows = [{'d': x, 'y': y, 'm': 0.3, 'selection': 'home', 'match': i,
             'date': i} for i, (x, y) in enumerate(zip(xs, ys))]
    fit = lm.ols_clustered(rows, lm.REGRESSION_B, lm.by_match)
    n, k = len(xs), 2
    xbar, ybar = sum(xs) / n, sum(ys) / n
    sxx = sum((x - xbar) ** 2 for x in xs)
    b = sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys)) / sxx
    a = ybar - b * xbar
    resid = [y - a - b * x for x, y in zip(xs, ys)]
    var = n / (n - k) * sum((x - xbar) ** 2 * e ** 2
                            for x, e in zip(xs, resid)) / sxx ** 2
    assert abs(fit['coef']['d'] - b) < 1e-12
    assert abs(fit['se']['d'] - math.sqrt(var)) < 1e-12


def test_line_movement_observations_pair_families_and_count_exclusions():
    from backtest import line_movement as lm

    def legs(book, home, fairs, probs=(0.5, 0.3, 0.2)):
        return [{'book': book, 'date': '2020-01-01', 'home_team': home,
                 'away_team': 'Z', 'season': '2019-20', 'selection': sel,
                 'market_fair': f, 'model_prob': p}
                for sel, f, p in zip(('home', 'draw', 'away'), fairs, probs)]

    fixtures = (legs('avg', 'A', (0.4, 0.3, 0.3))
                + legs('avgc', 'A', (0.45, 0.3, 0.25))
                + legs('avg', 'B', (0.4, 0.3, 0.3))
                + legs('avgc', 'C', (0.4, 0.3, 0.3))
                + legs('b365', 'A', (0.1, 0.1, 0.8)))
    rows, excluded = lm.observations(fixtures, 'E2')
    assert excluded == {'avg_only': 1, 'avgc_only': 1}
    assert len(rows) == 3
    home = rows[0]
    assert home['selection'] == 'home' and home['match'][2] == 'A'
    assert abs(home['d'] - 0.1) < 1e-12 and abs(home['y'] - 0.05) < 1e-12

    mismatch = (legs('avg', 'A', (0.4, 0.3, 0.3))
                + legs('avgc', 'A', (0.45, 0.3, 0.25), probs=(0.6, 0.2, 0.2)))
    _raises(ValueError, lm.observations, mismatch, 'E2')


def test_line_movement_cannot_reach_the_holdout():
    from backtest import line_movement as lm
    for year in (2026, 2027):
        _raises(AssertionError, lm.league_rows, 'E2', (year,))


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


# --- goals diagnostic: does the band over-2.5 rate add to the market? -------

def _synthetic_goal_obs(n, true_b, seed):
    """Two-outcome matches whose true P(over) is market^1 * model^true_b."""
    import math
    import random
    from backtest import diagnostic
    rng = random.Random(seed)
    obs = []
    for i in range(n):
        m, p = rng.uniform(0.3, 0.7), rng.uniform(0.3, 0.7)
        features = [(math.log(m), math.log(p)),
                    (math.log(1 - m), math.log(1 - p))]
        truth = diagnostic._probabilities(features, [1.0, true_b], (0, 1))
        winner = rng.choices(range(2), weights=truth)[0]
        obs.append(('2019-20' if i % 2 else '2022-23', features, winner))
    return obs


def test_goals_diagnostic_finds_no_signal_when_the_model_is_noise():
    import math
    from backtest import goals_diagnostic as gd
    result = gd.analyse(_synthetic_goal_obs(6000, 0.0, seed=21))
    assert result['b_ci'][0] < 0.0 < result['b_ci'][1]
    assert abs(result['logloss_uniform'] - math.log(2)) < 1e-12
    assert gd.verdict(result) == 'NO SIGNAL'


def test_goals_diagnostic_recovers_a_real_signal():
    from backtest import goals_diagnostic as gd
    result = gd.analyse(_synthetic_goal_obs(6000, 1.0, seed=22))
    low, high = result['b_ci']
    assert low < 1.0 < high and low > 0
    assert gd.verdict(result) == 'SIGNAL'


def test_goals_observations_clamp_devig_and_count_missing_prices():
    import math
    from backtest import goals_diagnostic as gd

    def rate(home, pct, goals):
        return {'date': '2020-01-01', 'season': '2019-20', 'home_team': home,
                'away_team': 'Z', 'band': 1, 'band_games': 5,
                'over_25_pct': pct, 'total_goals': goals}

    matches = [
        {'date': '2020-01-01', 'home_team': 'A', 'away_team': 'Z',
         'odds': {'ou25': {'over': 2.0, 'under': 2.0}}},
        {'date': '2020-01-01', 'home_team': 'B', 'away_team': 'Z',
         'odds': {'ou25': {'over': 1.5, 'under': 3.0}}},
        {'date': '2020-01-01', 'home_team': 'C', 'away_team': 'Z',
         'odds': {}},
    ]
    rates = [rate('A', 1.0, 3), rate('B', 0.0, 2), rate('C', 0.5, 4)]
    obs, summary = gd.observations(rates, matches, 'ou25')
    assert summary['missing_price'] == 1 and len(obs) == 2
    (_, fa, wa), (_, fb, wb) = obs
    assert (wa, wb) == (0, 1), 'outcome 0 is over 2.5'
    assert abs(fa[0][0] - math.log(0.5)) < 1e-12
    assert abs(fa[0][1] - math.log(0.99)) < 1e-12
    assert abs(fa[1][1] - math.log(0.01)) < 1e-12
    assert abs(fb[0][0] - math.log(2 / 3)) < 1e-12
    assert abs(fb[0][1] - math.log(0.01)) < 1e-12
    _raises(ValueError, gd.observations, rates, matches[:2], 'ou25')


def test_goals_families_load_and_a_missing_column_is_an_error():
    import tempfile
    header = 'Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,Avg>2.5,Avg<2.5'
    with tempfile.TemporaryDirectory() as root:
        _write_league(root, 'E2', 2020, ['E2,03/08/2019,A,B,2,1,H,1.9,1.95',
                                         'E2,03/08/2019,C,D,0,0,D,,1.95'],
                      header=header)
        matches, skips = load_league('E2', seasons=[2020], bet_seasons=[2020],
                                     families=('ou25',), root=root)
        assert matches[0]['odds']['ou25'] == {'over': 1.9, 'under': 1.95}
        assert matches[1]['odds'] == {}
        assert skips[0]['skipped'] == 1
        exc = _raises(DataError, load_league, 'E2', seasons=[2020],
                      bet_seasons=[2020], families=('ou25c',), root=root)
        assert 'AvgC>2.5' in str(exc)
    assert 'ou25' not in data.ODDS_FAMILIES, '1x2 runners must not offer it'


def test_goal_rates_match_the_band_table_and_leave_fixtures_alone():
    matches = load_all()
    frozen = [{'band': b, 'range': '', 'total_games': 100,
               'evenly_rated_games': 0, 'stronger_win_pct': 0.45,
               'draw_pct': 0.27, 'weaker_win_pct': 0.28,
               'over_25_pct': round(0.40 + b / 100, 4)}
              for b in range(1, 11)]
    result = run(matches, bands_mode='frozen', frozen_bands=frozen,
                 books=('avg',))
    rates = result['goal_rates']
    assert rates
    for r in rates:
        assert r['over_25_pct'] == round(0.40 + r['band'] / 100, 4)
        assert r['band_games'] == 100
    keys = {(r['date'], r['home_team'], r['away_team']) for r in rates}
    assert {(r['date'], r['home_team'], r['away_team'])
            for r in result['fixtures']} <= keys
    assert all('over_25_pct' not in r for r in result['fixtures'])
    plain = run(matches, bands_mode='frozen', frozen_bands=frozen, books=())
    assert plain['goal_rates'] == rates, 'goal rates do not depend on books'


def test_goals_diagnostic_cannot_reach_the_holdout():
    from backtest import goals_diagnostic as gd
    for year in (2026, 2027):
        _raises(AssertionError, gd.league_obs, 'E2', 'ou25', (year,))
