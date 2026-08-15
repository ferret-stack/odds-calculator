"""
One-shot repair + rebuild of the ELO chain.

Recomputes every match's pre-match ratings, ELO difference, band and winner
label from scratch, chronologically, and rewrites the derived JSON files.

Why a full rebuild rather than an incremental patch: the stored ratings were
produced by a pipeline whose repair path (update_elo_ratings) had become a
permanent no-op, so the corruption is spread across the whole file and cannot
be localised. Replaying the chain from the results -- which were never in
doubt -- is the only way to get an internally consistent set of ratings.

Scale: every team starts at SEED_ELO (1784), not 1500. The ELO update is
translation-invariant, so seeding at 1500 + 284 reproduces the raw chain
shifted by exactly the cosmetic offset. The offset therefore stays a single
named constant instead of being implicit in whatever was last written to
current_elo.json.

Run:  python3 tools/rebuild_elo.py [--data DIR] [--out DIR] [--dry-run]
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from elo_calculator import (
    ELOCalculator,
    SEED_ELO,
    classify_winner,
    elo_band,
    calculate_elo_bands,
    calculate_home_advantage_multipliers,
)

# Six matches carry 2026-12-30, a date six months after the season ended. The
# surrounding match IDs (2562074-2562085) run 2025-12-27 -> 2026-01-01, so the
# day and month are right and the year rolled over early -- almost certainly a
# scraper parsing "30 Dec" against the matchweek's January fixtures. This is
# the date that poisoned update_elo_ratings' "newer than last history entry"
# filter and froze the whole pipeline.
DATE_CORRECTIONS = {'2026-12-30': '2025-12-30'}

# A second, larger date defect: ~10% of matches have the day and month
# transposed (DD/MM parsed as MM/DD), so they only affect dates where the day
# is <= 12 and are invisible otherwise. Confirmed against reality -- Aston
# Villa 7-2 Liverpool is stored as 2020-04-10 but was played on 4 Oct 2020.
#
# Detection leans on match_id ordering: IDs are assigned per matchweek,
# alphabetically by home team, so ID order is matchweek order. A match whose
# date is far from its ID-neighbours' dates, and which lands back among them
# once day and month are swapped, is a transposition. Both conditions must
# hold, so an unambiguous date (day > 12) or one already consistent with its
# neighbours is never touched.
#
# This matters here because the ELO chain is order-dependent: processing a
# match out of sequence applies the update against the wrong prior ratings.
BACKWARD_PENALTY = 3.0     # a backward step is worse than a long gap
PLAUSIBLE_GAP_DAYS = 21    # free forward gap; longer is penalised (winter/summer breaks)

# Promoted into the upcoming season. Seeded deliberately, NOT defaulted --
# see seed_for_season() and the note in main().
PROMOTED_TEAMS = ['Hull City', 'Coventry City', 'Ipswich Town']

SEASON_START_MONTH = 8  # August


def swapped_date(date_str):
    """The day/month-transposed reading of a date, or None if unambiguous."""
    year, month, day = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
    if day > 12:
        return None
    try:
        return date(year, day, month).isoformat()
    except ValueError:
        return None


def repair_transposed_dates(matches):
    """
    Correct day/month transpositions, using match_id order as ground truth.

    Each ambiguous date has exactly two readings (as stored, or transposed).
    Choosing per-match against a local median fails when a whole matchweek is
    transposed together -- the local median moves with it and nothing looks
    like an outlier. So this picks the readings for the whole sequence at once,
    by dynamic programming over match_id order, minimising:

      * backward steps  -- a later match dated before an earlier one, and
      * implausible forward jumps -- gaps longer than a plausible break.

    Unambiguous dates (day > 12) have a single reading and act as fixed anchors
    that pin the rest of the sequence.

    Returns the list of (match_id, old, new) corrections applied.
    """
    ordered = sorted(matches, key=lambda x: x['match_id'])

    options = []
    for match in ordered:
        as_stored = date.fromisoformat(match['date'])
        transposed = swapped_date(match['date'])
        if transposed is None or transposed == match['date']:
            options.append([as_stored])
        else:
            options.append([as_stored, date.fromisoformat(transposed)])

    def step_cost(previous, current):
        delta = (current - previous).days
        if delta < 0:
            return -delta * BACKWARD_PENALTY
        return max(0, delta - PLAUSIBLE_GAP_DAYS)

    # Forward pass: best[i][k] = cheapest cost to reach option k of match i.
    best = [[0.0] * len(opts) for opts in options]
    back = [[0] * len(opts) for opts in options]
    for i in range(1, len(options)):
        for k, current in enumerate(options[i]):
            costs = [best[i - 1][j] + step_cost(previous, current)
                     for j, previous in enumerate(options[i - 1])]
            back[i][k] = min(range(len(costs)), key=costs.__getitem__)
            best[i][k] = costs[back[i][k]]

    # Backtrack the cheapest path.
    last = len(options) - 1
    choice = min(range(len(options[last])), key=lambda k: best[last][k])
    chosen = [0] * len(options)
    for i in range(last, -1, -1):
        chosen[i] = choice
        if i:
            choice = back[i][choice]

    corrections = []
    for match, opts, k in zip(ordered, options, chosen):
        picked = opts[k].isoformat()
        if picked != match['date']:
            corrections.append((match['match_id'], match['date'], picked))
            match['date'] = picked

    return corrections


def season_of(date_str):
    """Season label for a date, e.g. '2025-26'. Season runs Aug -> May."""
    year, month = int(date_str[:4]), int(date_str[5:7])
    start = year if month >= SEASON_START_MONTH else year - 1
    return f'{start}-{str(start + 1)[2:]}'


def league_table(matches):
    """Points table for a set of matches, best first."""
    table = defaultdict(lambda: dict(P=0, W=0, D=0, L=0, GF=0, GA=0, Pts=0))
    for m in matches:
        pairs = ((m['home_team'], m['home_goals'], m['away_goals']),
                 (m['away_team'], m['away_goals'], m['home_goals']))
        for team, gf, ga in pairs:
            row = table[team]
            row['P'] += 1
            row['GF'] += gf
            row['GA'] += ga
            if gf > ga:
                row['W'] += 1
                row['Pts'] += 3
            elif gf == ga:
                row['D'] += 1
                row['Pts'] += 1
            else:
                row['L'] += 1
    return sorted(table.items(),
                  key=lambda kv: (-kv[1]['Pts'],
                                  -(kv[1]['GF'] - kv[1]['GA']),
                                  -kv[1]['GF']))


def bottom_four(matches, season):
    """The four lowest finishers of a given season."""
    season_matches = [m for m in matches if season_of(m['date']) == season]
    if not season_matches:
        return []
    return [team for team, _ in league_table(season_matches)[-4:]]


def bottom_four_average(matches, season, ratings):
    """
    Average rating of a season's bottom four, on the displayed scale.

    This is the locked seeding rule for promoted teams: they perform like
    bottom-of-table sides, not average ones. It is a deliberate modelling
    decision with its own code path -- it deliberately does NOT reuse the
    generic fallback, so a seed can never be confused with a default that
    quietly fired.
    """
    teams = bottom_four(matches, season)
    values = [ratings[t] for t in teams if t in ratings]
    if not values:
        return None, teams
    return round(sum(values) / len(values)), teams


def rebuild(matches, verbose=True):
    """
    Replay the full chain chronologically, stamping pre-match state.

    Returns (calc, seeding_log).
    """
    # Ordered by match_id, not date. IDs are issued per matchweek in fixture
    # order, so ID order is the true match order and -- unlike the dates -- it
    # is not corrupted. The ELO chain is order-dependent: replaying a match out
    # of sequence applies its update against the wrong prior ratings.
    ordered = sorted(matches, key=lambda x: x['match_id'])
    calc = ELOCalculator(k_factor=20, home_advantage=100, use_mov=True,
                         default_elo=SEED_ELO)

    first_season = season_of(ordered[0]['date'])
    seeding_log = []

    for match in ordered:
        season = season_of(match['date'])

        # A team appearing for the first time after the opening season is a
        # promotion. Seed it by the same locked rule used for the upcoming
        # season's promoted sides -- the bottom-four average of the season
        # just gone -- rather than dropping it on league average, which would
        # be the same mistake in a different place.
        for team in (match['home_team'], match['away_team']):
            if team in calc.current_elo:
                continue
            if season == first_season:
                calc.seed_team(team, SEED_ELO)
                seeding_log.append((team, season, SEED_ELO, 'opening-season baseline'))
                continue
            prev = f'{int(season[:4]) - 1}-{season[:4][2:]}'
            avg, teams = bottom_four_average(matches, prev, calc.current_elo)
            if avg is None:
                avg = SEED_ELO
                note = 'no prior season in data; baseline'
            else:
                note = f'bottom-4 of {prev}: {", ".join(teams)}'
            calc.seed_team(team, avg)
            seeding_log.append((team, season, avg, note))

        result = calc.process_match(
            home_team=match['home_team'],
            away_team=match['away_team'],
            home_goals=match['home_goals'],
            away_goals=match['away_goals'],
            match_date=match['date'],
        )

        # Pre-match state is what the fixture was gradeable on before kickoff.
        match['home_elo'] = result['pre_home']
        match['away_elo'] = result['pre_away']
        match['elo_diff'] = abs(result['pre_home'] - result['pre_away'])
        match['elo_band'] = elo_band(match['elo_diff'])
        match['winner'] = classify_winner(
            match['home_goals'], match['away_goals'],
            match['home_elo'], match['away_elo'])
        match['elo_processed'] = True

    return calc, seeding_log


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data', default='data')
    ap.add_argument('--out', default=None, help='defaults to --data (in place)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    data_dir = Path(args.data)
    out_dir = Path(args.out or args.data)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(data_dir / 'matches_data.json') as f:
        matches = json.load(f)

    print(f'Loaded {len(matches)} matches from {data_dir}')

    # 1a. Repair the corrupt-year dates
    fixed = 0
    for m in matches:
        if m['date'] in DATE_CORRECTIONS:
            m['date'] = DATE_CORRECTIONS[m['date']]
            fixed += 1
    print(f'  repaired {fixed} corrupt-year dates')

    # 1b. Repair day/month transpositions
    corrections = repair_transposed_dates(matches)
    print(f'  repaired {len(corrections)} day/month-transposed dates '
          f'({len(corrections) / len(matches):.1%} of the dataset)')
    if corrections:
        by_year = Counter(old[:4] for _, old, _ in corrections)
        print(f'    by year: {dict(sorted(by_year.items()))}')
        for mid, old, new in corrections[:5]:
            print(f'    e.g. match {mid}: {old} -> {new}')

    # 2. Replay the chain
    calc, seeding_log = rebuild(matches)
    print(f'  rebuilt {len(matches)} matches across {len(calc.current_elo)} teams')

    print('\nSeeding decisions taken during the rebuild:')
    for team, season, value, note in seeding_log:
        print(f'  {team:<16s} {season}  ->  {value}   ({note})')

    # 3. Seed the upcoming season's promoted teams
    last_season = season_of(max(m['date'] for m in matches))
    avg, bottom = bottom_four_average(matches, last_season, calc.current_elo)
    print(f'\n{last_season} bottom four: {", ".join(bottom)}')
    for team in bottom:
        print(f'    {team:<16s} {calc.current_elo[team]}')
    print(f'  average -> {avg}')

    print('\nSeeding promoted teams (deliberate seed, not a fallback):')
    for team in PROMOTED_TEAMS:
        # Ipswich Town normalises to 'Ipswich' and already carries a stale
        # rating from an earlier PL spell. The locked rule says seed all three
        # promoted sides at the bottom-four average, so the stale value is
        # replaced rather than carried forward.
        key = 'Ipswich' if team == 'Ipswich Town' else team
        previous = calc.current_elo.get(key)
        calc.current_elo[key] = avg
        calc.seeded_teams[key] = avg
        was = f' (replacing stale {previous})' if previous is not None else ''
        print(f'  {key:<16s} -> {avg}{was}')

    if args.dry_run:
        print('\n[dry run] nothing written')
        return

    # 4. Write out
    with open(out_dir / 'matches_data.json', 'w') as f:
        json.dump(matches, f, indent=2)

    rankings = calc.get_rankings()
    with open(out_dir / 'current_elo.json', 'w') as f:
        json.dump({t: {'elo': e, 'rank': r} for t, e, r in rankings}, f, indent=2)

    with open(out_dir / 'elo_history.json', 'w') as f:
        json.dump(calc.export_elo_history(), f, indent=2)

    with open(out_dir / 'venue_adjustment.json', 'w') as f:
        json.dump(calculate_home_advantage_multipliers(matches), f, indent=2)

    with open(out_dir / 'elo_bands.json', 'w') as f:
        json.dump(calculate_elo_bands(matches), f, indent=2)

    print(f'\nWrote matches_data.json, current_elo.json, elo_history.json, '
          f'elo_bands.json, venue_adjustment.json to {out_dir}/')


if __name__ == '__main__':
    main()
