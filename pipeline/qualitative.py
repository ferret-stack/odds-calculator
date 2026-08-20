"""
Qualitative inputs: manager styles, team news, formations, game congestion.

These attach CONTEXT to a fixture. None of them moves a probability, a price
or a stake -- the model output and the staking rule are untouched by anything
in this file. They exist so a write-up can explain a bet, and so a human can
overrule one before it is placed.

THE CONGESTION SIGNAL
---------------------
    congestion = number of PREMIER LEAGUE games the team played in the
                 trailing 14 days, counted backwards from the fixture date.

It is numeric, and it is a PL-ONLY PROXY. It is not a complete fatigue
measure. It does not include FA Cup, Carabao Cup or European fixtures --
those are scraped separately (scrapers/fixture_occurrence.py), stay in
cup_fixtures.json, and remain narrative-only. Every consumer of this number
gets the caveat attached to it, so it cannot travel without its label.

Forward-looking rotation risk is NOT modelled here. It is parked under
Manager Styles and is deliberately not a standalone system.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

CONGESTION_WINDOW_DAYS = 14

CONGESTION_LABEL = 'PL-only proxy'
CONGESTION_CAVEAT = (
    'Premier League games in the trailing 14 days. PL-only proxy, not a '
    'complete fatigue measure -- excludes FA Cup, Carabao Cup and European '
    'fixtures, which are tracked separately in cup_fixtures.json and are '
    'narrative-only.'
)


def _parse(date_str):
    return datetime.strptime(date_str[:10], '%Y-%m-%d')


def congestion_signal(team, matches, as_of, window_days=CONGESTION_WINDOW_DAYS):
    """
    Count PL games played by `team` in the `window_days` before `as_of`.

    The window is half-open: (as_of - window_days, as_of). A match played on
    the fixture date itself is not "in the trailing window", and the boundary
    day is excluded so a 14-day window means 14 days, not 15.

    `matches` is matches_data.json -- Premier League only by construction,
    which is exactly why this is a PL-only proxy.
    """
    as_of_date = _parse(as_of) if isinstance(as_of, str) else as_of
    cutoff = as_of_date - timedelta(days=window_days)

    count = 0
    played = []
    for m in matches:
        if team not in (m.get('home_team'), m.get('away_team')):
            continue
        try:
            played_on = _parse(m['date'])
        except (KeyError, ValueError, TypeError):
            continue
        if cutoff < played_on < as_of_date:
            count += 1
            opponent = (m['away_team'] if m['home_team'] == team
                        else m['home_team'])
            played.append({'date': m['date'], 'opponent': opponent})

    played.sort(key=lambda x: x['date'])
    return {
        'value': count,
        'window_days': window_days,
        'basis': CONGESTION_LABEL,
        'caveat': CONGESTION_CAVEAT,
        'matches': played,
    }


def load_qualitative(data_dir='data'):
    """
    Load the hand-maintained qualitative files.

    Missing files are not an error -- they are the normal state early in a
    season. Each returns {} and the fixture context says so explicitly rather
    than inventing a default.
    """
    base = Path(data_dir) / 'qualitative'
    out = {}
    for name in ('manager_styles', 'team_news', 'formations'):
        path = base / f'{name}.json'
        if path.exists():
            data = json.loads(path.read_text())
            # Tolerate both {"teams": {...}} and a bare {team: ...} mapping.
            out[name] = data.get('teams', data)
        else:
            out[name] = {}
    return out


def cup_context(team, cup_fixtures, as_of, window_days=CONGESTION_WINDOW_DAYS,
                lookahead_days=7):
    """
    Recent and imminent cup/European ties, for narrative only.

    Explicitly NOT folded into the congestion number. Kept beside it so a
    write-up can say "two PL games in 14 days, plus a Thursday in Rome"
    without that Thursday silently entering a numeric signal.
    """
    as_of_date = _parse(as_of) if isinstance(as_of, str) else as_of
    back = as_of_date - timedelta(days=window_days)
    forward = as_of_date + timedelta(days=lookahead_days)

    recent, upcoming = [], []
    for fixture in cup_fixtures.get('teams', {}).get(team, []):
        try:
            when = _parse(fixture['date'])
        except (KeyError, ValueError, TypeError):
            continue
        if back < when < as_of_date:
            recent.append(fixture)
        elif as_of_date <= when <= forward:
            upcoming.append(fixture)

    return {
        'recent': sorted(recent, key=lambda f: f['date']),
        'upcoming': sorted(upcoming, key=lambda f: f['date']),
        'note': 'narrative context only -- excluded from the congestion signal',
    }


def team_context(team, matches, qualitative, cup_fixtures, as_of):
    """Assemble every qualitative input for one team."""
    return {
        'manager_style': qualitative.get('manager_styles', {}).get(team),
        'team_news': qualitative.get('team_news', {}).get(team),
        'formation': qualitative.get('formations', {}).get(team),
        'congestion': congestion_signal(team, matches, as_of),
        'cup_fixtures': cup_context(team, cup_fixtures, as_of),
    }


def fixture_context(fixture, matches, qualitative, cup_fixtures):
    """
    Qualitative context for both sides of a fixture.

    Returned alongside the pricing output, never merged into it.
    """
    home, away = fixture['home_team'], fixture['away_team']
    as_of = fixture['date']

    return {
        'congestion_signal_definition': CONGESTION_CAVEAT,
        'home': team_context(home, matches, qualitative, cup_fixtures, as_of),
        'away': team_context(away, matches, qualitative, cup_fixtures, as_of),
    }


def summarise(context, side):
    """One-line, human-readable summary for a write-up."""
    ctx = context[side]
    congestion = ctx['congestion']
    bits = [f"congestion {congestion['value']} PL game(s)/"
            f"{congestion['window_days']}d ({congestion['basis']})"]

    if ctx['formation']:
        bits.append(f"formation {ctx['formation']}")
    if ctx['manager_style']:
        style = ctx['manager_style']
        bits.append(style if isinstance(style, str)
                    else style.get('style', 'style on file'))
    if ctx['team_news']:
        news = ctx['team_news']
        bits.append(news if isinstance(news, str) else 'team news on file')

    cups = ctx['cup_fixtures']
    if cups['recent']:
        latest = cups['recent'][-1]
        bits.append(f"recent cup tie {latest['competition']} v "
                    f"{latest['opponent']} ({latest['date']}) [narrative only]")
    if cups['upcoming']:
        nxt = cups['upcoming'][0]
        bits.append(f"cup tie next {nxt['competition']} v {nxt['opponent']} "
                    f"({nxt['date']}) [narrative only]")

    return '; '.join(bits)
