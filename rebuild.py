"""
The single derive step: facts in, every ELO-derived number out.

matches_data.json stores only immutable match facts. This module replays those
facts once, in strict chronological order, and derives everything else:

- per-match PRE-match ratings (the rating each team held before kickoff — the
  only rating a forecast could have known), for two ELO variants:
    * LONG ELO    — continuous replay since 2020-09, every team starts at 1500
                    ("class")
    * ROLLING ELO — for each match, a fresh replay of only the previous
                    ROLLING_WINDOW_DAYS of matches, all teams re-baselined to
                    1500 ("current form"; absorbs promotion/relegation churn)
- elo_diff / elo_band / winner stamped from PRE-match ratings of the driver
  variant. Exact-equality rule: when raw pre-match ratings are equal, the HOME
  side is the de-facto stronger team (home advantage breaks the tie — the same
  +100 the update formula already grants). A decisive result can therefore
  never be labelled "weaker" by accident of a tie: home win on a tie is
  "stronger", away win on a tie is "weaker", by documented rule.
- the band probability tables (elo_bands.json)
- venue-adjustment multipliers measured from the data (venue_adjustment.json)
- current ratings + history (current_elo.json, elo_history.json)

No code path bakes a default 1500 into a stored match: facts carry no ratings
at all, and every derived rating is recomputed from the replay on every run.

DRIVER selects which variant stamps the canonical band fields and tables.
It is set by backtest calibration (see calibration.py), not preference.
"""

import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from elo_calculator import ELOCalculator

# Calibration-chosen defaults (see calibration.py and docs/PART_A_REPORT.md).
DRIVER = 'long'
MOV_FORMULA = 'fte'
K_FACTOR = 20
HOME_ADVANTAGE = 100
ROLLING_WINDOW_DAYS = 730
NUM_BANDS = 10
BAND_WIDTH = 50


def parse_date(ds):
    y, m, d = map(int, ds.split('-'))
    return date(y, m, d)


def band_of(elo_diff):
    return min(int(elo_diff // BAND_WIDTH) + 1, NUM_BANDS)


def band_range_label(band):
    if band == NUM_BANDS:
        return f"{(NUM_BANDS - 1) * BAND_WIDTH}+"
    return f"{(band - 1) * BAND_WIDTH}-{band * BAND_WIDTH - 1}"


def load_facts(path='data/matches_data.json'):
    with open(path) as f:
        facts = json.load(f)
    derived_keys = {'home_elo', 'away_elo', 'elo_diff', 'elo_band', 'winner'}
    for m in facts:
        leaked = derived_keys & set(m)
        assert not leaked, (
            f"matches_data.json must be facts-only; match {m['match_id']} "
            f"carries derived fields {leaked} — run tools/repair_facts.py")
    return sorted(facts, key=lambda m: (m['date'], m['match_id']))


def derive_winner(home_goals, away_goals, home_elo, away_elo):
    """Winner label from PRE-match ratings; home is stronger on an exact tie."""
    if home_goals == away_goals:
        return 'draw'
    stronger_is_home = home_elo >= away_elo
    home_won = home_goals > away_goals
    return 'stronger' if home_won == stronger_is_home else 'weaker'


def replay_long(facts, mov_formula=MOV_FORMULA):
    """One continuous chronological replay. Returns (per-match list of
    {pre/post ratings}, calculator with final state)."""
    calc = ELOCalculator(k_factor=K_FACTOR, home_advantage=HOME_ADVANTAGE,
                         use_mov=True, mov_formula=mov_formula)
    rows = []
    for m in facts:
        pre_home = calc.get_current_elo(m['home_team'])
        pre_away = calc.get_current_elo(m['away_team'])
        post_home, post_away, _, _ = calc.process_match(
            m['home_team'], m['away_team'],
            m['home_goals'], m['away_goals'], m['date'])
        rows.append({'pre_home': pre_home, 'pre_away': pre_away,
                     'post_home': post_home, 'post_away': post_away})
    return rows, calc


def rolling_pre_ratings(facts, window_days=ROLLING_WINDOW_DAYS,
                        mov_formula=MOV_FORMULA):
    """PRE-match rolling ratings per match: for each kickoff date, replay only
    the window of matches played in the preceding `window_days`, all teams
    starting from 1500. Grouped by date so the window replay runs once per
    matchday, not once per match."""
    by_date = defaultdict(list)
    for i, m in enumerate(facts):
        by_date[m['date']].append(i)

    out = [None] * len(facts)
    for ds, idxs in sorted(by_date.items()):
        day = parse_date(ds)
        start = day - timedelta(days=window_days)
        calc = ELOCalculator(k_factor=K_FACTOR, home_advantage=HOME_ADVANTAGE,
                             use_mov=True, mov_formula=mov_formula)
        for m in facts:
            d = parse_date(m['date'])
            if start <= d < day:
                calc.process_match(m['home_team'], m['away_team'],
                                   m['home_goals'], m['away_goals'],
                                   m['date'], update_history=False)
        for i in idxs:
            out[i] = {'pre_home': calc.get_current_elo(facts[i]['home_team']),
                      'pre_away': calc.get_current_elo(facts[i]['away_team'])}
    return out


def current_rolling(facts, window_days=ROLLING_WINDOW_DAYS,
                    mov_formula=MOV_FORMULA):
    """Rolling ratings as of the day after the last recorded match."""
    as_of = max(parse_date(m['date']) for m in facts) + timedelta(days=1)
    start = as_of - timedelta(days=window_days)
    calc = ELOCalculator(k_factor=K_FACTOR, home_advantage=HOME_ADVANTAGE,
                         use_mov=True, mov_formula=mov_formula)
    for m in facts:
        if start <= parse_date(m['date']) < as_of:
            calc.process_match(m['home_team'], m['away_team'],
                               m['home_goals'], m['away_goals'],
                               m['date'], update_history=False)
    return dict(calc.current_elo)


def derive_matches(facts, driver=DRIVER, mov_formula=MOV_FORMULA):
    """Facts + one replay -> fully derived match rows."""
    long_rows, long_calc = replay_long(facts, mov_formula)
    roll_rows = rolling_pre_ratings(facts, mov_formula=mov_formula)

    derived = []
    for m, lr, rr in zip(facts, long_rows, roll_rows):
        total_goals = m['home_goals'] + m['away_goals']
        booking = ((m.get('home_yellow') or 0) * 10 + (m.get('home_red') or 0) * 25
                   + (m.get('away_yellow') or 0) * 10 + (m.get('away_red') or 0) * 25)
        row = dict(m)
        row.update({
            'total_booking_points': booking,
            'over_05': total_goals > 0, 'over_15': total_goals > 1,
            'over_25': total_goals > 2, 'over_35': total_goals > 3,
            'over_45': total_goals > 4,
            'btts': m['home_goals'] > 0 and m['away_goals'] > 0,
            # long ELO, pre- and post-match
            'long_home_elo': lr['pre_home'], 'long_away_elo': lr['pre_away'],
            'long_home_elo_post': lr['post_home'],
            'long_away_elo_post': lr['post_away'],
            # rolling ELO, pre-match
            'rolling_home_elo': rr['pre_home'],
            'rolling_away_elo': rr['pre_away'],
        })
        pre_h, pre_a = ((lr['pre_home'], lr['pre_away']) if driver == 'long'
                        else (rr['pre_home'], rr['pre_away']))
        diff = abs(pre_h - pre_a)
        row.update({
            'home_elo': pre_h, 'away_elo': pre_a,          # PRE-match, driver
            'elo_diff': diff,
            'elo_band': band_of(diff),
            'winner': derive_winner(m['home_goals'], m['away_goals'],
                                    pre_h, pre_a),
        })
        derived.append(row)
    return derived, long_calc


def calculate_elo_bands(derived):
    """Band WDL + market tables from PRE-match-stamped matches."""
    bands = []
    for band in range(1, NUM_BANDS + 1):
        rows = [m for m in derived if m['elo_band'] == band]
        entry = {'band': band, 'range': band_range_label(band),
                 'total_games': len(rows)}
        if rows:
            n = len(rows)
            booking = [m['total_booking_points'] for m in rows]
            entry.update({
                'stronger_win_pct': round(sum(m['winner'] == 'stronger' for m in rows) / n, 4),
                'draw_pct': round(sum(m['winner'] == 'draw' for m in rows) / n, 4),
                'weaker_win_pct': round(sum(m['winner'] == 'weaker' for m in rows) / n, 4),
                'avg_booking_points': round(sum(booking) / n, 1),
                'over_05_pct': round(sum(m['over_05'] for m in rows) / n, 4),
                'over_15_pct': round(sum(m['over_15'] for m in rows) / n, 4),
                'over_25_pct': round(sum(m['over_25'] for m in rows) / n, 4),
                'over_35_pct': round(sum(m['over_35'] for m in rows) / n, 4),
                'over_45_pct': round(sum(m['over_45'] for m in rows) / n, 4),
                'btts_pct': round(sum(m['btts'] for m in rows) / n, 4),
            })
        else:
            # neutral placeholders, same keys as populated bands
            entry.update({
                'stronger_win_pct': 0.333, 'draw_pct': 0.333,
                'weaker_win_pct': 0.334, 'avg_booking_points': 40,
                'over_05_pct': 0.9, 'over_15_pct': 0.75, 'over_25_pct': 0.5,
                'over_35_pct': 0.25, 'over_45_pct': 0.1, 'btts_pct': 0.5,
            })
        bands.append(entry)
    return bands


def calculate_venue_adjustment(derived, as_of=None):
    """Venue multipliers measured from pre-match-stamped data.

    For each outcome, multiplier = P(outcome | stronger team at home or away)
    divided by P(outcome overall). Ties (equal raw pre-match ELO) are excluded
    from the measurement — by rule they are "stronger at home", which would
    contaminate the venue split with the tie convention.
    """
    rows = [m for m in derived if m['home_elo'] != m['away_elo']]
    home = [m for m in rows if m['home_elo'] > m['away_elo']]    # stronger at home
    away = [m for m in rows if m['home_elo'] < m['away_elo']]    # stronger away
    if not home or not away:
        raise ValueError('not enough data to measure venue adjustment')

    def rates(subset):
        n = len(subset)
        return {w: sum(m['winner'] == w for m in subset) / n
                for w in ('stronger', 'draw', 'weaker')}

    overall, at_home, at_away = rates(rows), rates(home), rates(away)

    def mult(cond, w):
        return round(cond[w] / overall[w], 3) if overall[w] else 1.0

    return {
        # stronger-team win multipliers (keys the site already reads)
        'home_multiplier': mult(at_home, 'stronger'),
        'away_multiplier': mult(at_away, 'stronger'),
        'draw_home_multiplier': mult(at_home, 'draw'),
        'draw_away_multiplier': mult(at_away, 'draw'),
        'weaker_home_multiplier': mult(at_away, 'weaker'),   # weaker home = stronger away
        'weaker_away_multiplier': mult(at_home, 'weaker'),
        'home_win_rate': round(at_home['stronger'], 4),
        'away_win_rate': round(at_away['stronger'], 4),
        'combined_rate': round(overall['stronger'], 4),
        'sample_size': len(rows),
        'stronger_home_games': len(home),
        'stronger_away_games': len(away),
        'excluded_elo_ties': len(derived) - len(rows),
        'last_updated': as_of or date.today().isoformat(),
    }


def sanity_check(derived, bands):
    """The invariants the V6 data violated. Raises on regression."""
    for m in derived:
        assert m['home_elo'] == (m['long_home_elo'] if DRIVER == 'long'
                                 else m['rolling_home_elo'])
        expected = derive_winner(m['home_goals'], m['away_goals'],
                                 m['home_elo'], m['away_elo'])
        assert m['winner'] == expected, f"winner mislabelled: {m['match_id']}"
        if m['home_elo'] == m['away_elo'] and m['home_goals'] > m['away_goals']:
            assert m['winner'] == 'stronger', (
                f"home win on ELO tie must be 'stronger': {m['match_id']}")
    for b in bands:
        if b['total_games'] > 0:
            assert b['stronger_win_pct'] >= b['weaker_win_pct'], (
                f"band {b['band']} inverted: stronger {b['stronger_win_pct']} "
                f"< weaker {b['weaker_win_pct']} over {b['total_games']} games")


def rebuild(data_dir='data', driver=DRIVER, mov_formula=MOV_FORMULA,
            verbose=True):
    """Full derive: facts -> derived matches + every derived JSON."""
    data_dir = Path(data_dir)
    facts = load_facts(data_dir / 'matches_data.json')
    derived, long_calc = derive_matches(facts, driver, mov_formula)
    bands = calculate_elo_bands(derived)
    sanity_check(derived, bands)

    last_date = max(m['date'] for m in facts)
    venue = calculate_venue_adjustment(derived, as_of=last_date)
    rolling_now = current_rolling(facts, mov_formula=mov_formula)

    # current_elo.json — 'elo'/'rank' are the driver's numbers (site contract);
    # both variants ship alongside for the class-vs-form narrative.
    long_now = dict(long_calc.current_elo)
    driver_now = long_now if driver == 'long' else rolling_now
    ranked = sorted(driver_now.items(), key=lambda kv: kv[1], reverse=True)
    roll_ranked = {t: r for r, (t, _) in enumerate(
        sorted(rolling_now.items(), key=lambda kv: kv[1], reverse=True), 1)}
    long_ranked = {t: r for r, (t, _) in enumerate(
        sorted(long_now.items(), key=lambda kv: kv[1], reverse=True), 1)}
    current = {}
    for rank, (team, elo) in enumerate(ranked, 1):
        current[team] = {
            'elo': elo, 'rank': rank,
            'long_elo': long_now.get(team),
            'long_rank': long_ranked.get(team),
            'rolling_elo': rolling_now.get(team),
            'rolling_rank': roll_ranked.get(team),
        }

    history = {team: sorted(h, key=lambda x: x['date'])
               for team, h in long_calc.elo_history.items()}

    outputs = {
        'matches_derived.json': derived,
        'elo_bands.json': bands,
        'current_elo.json': current,
        'elo_history.json': history,
        'venue_adjustment.json': venue,
    }
    for name, payload in outputs.items():
        with open(data_dir / name, 'w') as f:
            json.dump(payload, f, indent=2)

    if verbose:
        print(f"rebuild: {len(facts)} facts -> {len(derived)} derived matches "
              f"(driver={driver}, mov={mov_formula})")
        b1 = bands[0]
        print(f"  Band 1 ({b1['range']}): n={b1['total_games']} "
              f"stronger {b1['stronger_win_pct']:.1%} / draw {b1['draw_pct']:.1%}"
              f" / weaker {b1['weaker_win_pct']:.1%}")
        print(f"  venue: stronger-home x{venue['home_multiplier']}, "
              f"stronger-away x{venue['away_multiplier']}, "
              f"draw {venue['draw_home_multiplier']}/{venue['draw_away_multiplier']}")
        print("  wrote:", ', '.join(outputs))
    return derived, bands, current, venue


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument('--data-dir', default='data')
    ap.add_argument('--driver', default=DRIVER, choices=['long', 'rolling'])
    ap.add_argument('--mov', default=MOV_FORMULA, choices=['fte', 'v6blog'])
    args = ap.parse_args()
    rebuild(args.data_dir, args.driver, args.mov)
