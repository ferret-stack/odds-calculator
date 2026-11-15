"""
Load football-data.co.uk season CSVs into the shape the ELO chain wants.

TWO ENTRY POINTS, ON PURPOSE
----------------------------
`load_league(code, ...)` is the normal loader. It reads
docs/historical/<code>/<endyear>.csv (E0, E1, E2, E3, EC), where <endyear> is
the calendar year the season ENDS in (2020.csv = 2019/20).

`load_legacy_pl_flat()` is the regression-only loader. It takes no arguments
and reads the six flat Premier League files docs/historical/2021..2026.csv
that the original harness was built on, so the original PL figures (1,298
bets on Avg*) can be reproduced through the same row parser. Nothing else may
use it.

HOLDOUT
-------
2025/26 (any file named 2026.csv, or any season ending 2026 or later) is a
holdout. `load_league` can never load, read or list it: `_assert_not_holdout`
raises AssertionError before a file is opened. The single exemption is the
exact legacy flat file docs/historical/2026.csv, and only when reached through
`load_legacy_pl_flat` -- that file is part of the pre-existing PL baseline
being reproduced, not of the lower-league study.

ODDS
----
Odds families, column names from docs/Football-Data-Notes.md:

    avg   AvgH/AvgD/AvgA      market average, PRE-CLOSING (collected Friday /
                              Tuesday afternoons). The original harness
                              called these "closing"; they are not.
    avgc  AvgCH/AvgCD/AvgCA   market average, closing
    b365  B365H/B365D/B365A   Bet365, pre-closing
    psc   PSCH/PSCD/PSCA      Pinnacle, closing

Odds are read ONLY for the seasons named in `bet_seasons`. Warm-up seasons
load fixtures and results and nothing else, so the older BbAv* averages (a
different bookmaker set) are never touched.

  * A requested family whose COLUMN is absent from a betting-season file is
    an error (`DataError`). It never silently becomes None.
  * A blank or unusable PRICE in a column that exists skips that match for
    that family -- all three selections, since a 1x2 with a leg missing can
    neither be de-vigged nor sanity-checked -- and is counted per season and
    family. The match's result still feeds the ELO chain.

TEAM NAMES
----------
Aliases come from tools/output/team_alias_proposal.csv (rows whose decision
is 'alias'), applied as written. Rows marked 'distinct' are left alone.
"""

import csv
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DIR = REPO_ROOT / 'docs' / 'historical'
ALIAS_FILE = REPO_ROOT / 'tools' / 'output' / 'team_alias_proposal.csv'

LEAGUES = ('E0', 'E1', 'E2', 'E3', 'EC')

# First season end-year that is held out. 2026 = 2025/26.
HOLDOUT_END_YEAR = 2026

# The flat PL files the original harness was built on, by season end-year.
_LEGACY_PL_END_YEARS = (2021, 2022, 2023, 2024, 2025, 2026)
_LEGACY_PL_FAMILIES = ('avg', 'b365')

ODDS_FAMILIES = {
    'avg': {'home': 'AvgH', 'draw': 'AvgD', 'away': 'AvgA'},
    'avgc': {'home': 'AvgCH', 'draw': 'AvgCD', 'away': 'AvgCA'},
    'b365': {'home': 'B365H', 'draw': 'B365D', 'away': 'B365A'},
    'psc': {'home': 'PSCH', 'draw': 'PSCD', 'away': 'PSCA'},
}

REQUIRED = ('Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR')


class DataError(Exception):
    pass


def season_label(end_year):
    """2020 -> '2019-20'. Sorts chronologically as a string."""
    return f'{end_year - 1}-{str(end_year)[2:]}'


def _assert_not_holdout(path, legacy_regression=False):
    """
    Fail loudly before a holdout file is opened.

    Raised explicitly rather than with an `assert` statement so that running
    under `python -O` cannot strip it.
    """
    path = Path(path)
    stem = path.stem
    held_out = stem.isdigit() and int(stem) >= HOLDOUT_END_YEAR
    if not held_out:
        return
    legacy_file = (DEFAULT_DIR / f'{HOLDOUT_END_YEAR}.csv').resolve()
    if legacy_regression and path.resolve() == legacy_file:
        return
    raise AssertionError(
        f'HOLDOUT VIOLATION: refusing to read {path}. Season '
        f'{season_label(int(stem))} is held out and must never be loaded, '
        f'read or summarised.')


def load_aliases(path=None):
    """{alias: canonical} for every row decided as 'alias'."""
    aliases = {}
    with open(path or ALIAS_FILE, encoding='utf-8', newline='') as handle:
        for row in csv.DictReader(handle):
            if row['decision'].strip() == 'alias':
                aliases[row['alias'].strip()] = row['canonical_name'].strip()
    return aliases


def _parse_date(value):
    """football-data writes DD/MM/YY or DD/MM/YYYY. Both appear in the wild."""
    for fmt in ('%d/%m/%Y', '%d/%m/%y'):
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    raise DataError(f'unparseable date {value!r}')


def _price(row, column):
    """A usable price, or None for a blank / malformed / <=1.0 cell."""
    raw = (row.get(column) or '').strip()
    if not raw:
        return None
    try:
        price = float(raw)
    except ValueError:
        return None
    # size_bet rejects odds <= 1.0 with a ValueError rather than a no-bet.
    return price if price > 1.0 else None


def _read_rows(path, legacy_regression=False):
    """(header, rows). Strict UTF-8 first, cp1252 fallback (8 old files)."""
    _assert_not_holdout(path, legacy_regression=legacy_regression)
    for encoding in ('utf-8-sig', 'cp1252'):
        try:
            with open(path, encoding=encoding, newline='') as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)
                return [h.strip() for h in (reader.fieldnames or []) if h], rows
        except UnicodeDecodeError:
            continue
    raise DataError(f'{path}: neither utf-8 nor cp1252')


def _load_season(path, season, families, aliases, legacy_regression=False):
    """
    One season's matches plus {family: skipped-row count}.

    `families` is the odds to read for this season; empty for a warm-up
    season, in which case no odds column is looked at.
    """
    header, rows = _read_rows(path, legacy_regression=legacy_regression)

    absent = [c for c in REQUIRED if c not in header]
    for family in families:
        absent += [c for c in ODDS_FAMILIES[family].values()
                   if c not in header]
    if absent:
        raise DataError(f'{path}: missing column(s) {", ".join(absent)}')

    matches = []
    skipped = {family: 0 for family in families}
    for line_no, row in enumerate(rows, start=2):
        if not (row.get('HomeTeam') or '').strip():
            continue  # trailing blank rows are common in these files
        missing = [c for c in REQUIRED if not (row.get(c) or '').strip()]
        if missing:
            raise DataError(
                f'{path}:{line_no} missing {", ".join(missing)}')

        odds = {}
        for family in families:
            prices = {sel: _price(row, col)
                      for sel, col in ODDS_FAMILIES[family].items()}
            if all(prices.values()):
                odds[family] = prices
            else:
                skipped[family] += 1

        home = row['HomeTeam'].strip()
        away = row['AwayTeam'].strip()
        home_goals, away_goals = int(row['FTHG']), int(row['FTAG'])
        match = {
            'season': season,
            'date': _parse_date(row['Date']),
            'time': (row.get('Time') or '').strip(),
            'home_team': aliases.get(home, home),
            'away_team': aliases.get(away, away),
            'home_goals': home_goals,
            'away_goals': away_goals,
            'ftr': row['FTR'].strip().upper(),
            # Only families with all three prices present. A family missing
            # here was either not requested for this season or skipped.
            'odds': odds,
        }

        # Goal/BTTS flags, so calculate_elo_bands() builds the same rows
        # it builds in production rather than defaulting them to False.
        # The 1x2 backtest does not read them; the band table does.
        total = home_goals + away_goals
        for line in (0.5, 1.5, 2.5, 3.5, 4.5):
            match[f'over_{str(line).replace(".", "")[:2]}'] = total > line
        match['btts'] = home_goals > 0 and away_goals > 0
        # Not in these CSVs in booking-point form; left absent so the
        # band table reports 0 rather than a fabricated average.
        match['total_booking_points'] = None

        matches.append(match)
    return matches, skipped


def _ordered(matches):
    """
    Kickoff order: (date, time, home_team).

    The ELO chain is order-dependent. These dates are the source's own and
    need no repair (the audit found none unparseable or out of window). Time
    breaks same-day ties where the file has it; home_team breaks the rest
    deterministically so a rerun reproduces a run.
    """
    return sorted(matches, key=lambda m: (m['date'], m['time'], m['home_team']))


def available_seasons(code, root=None):
    """End-years with a file for `code`, holdout excluded, ascending."""
    if code not in LEAGUES:
        raise DataError(f'unknown league code {code!r}; expected one of '
                        f'{", ".join(LEAGUES)}')
    directory = Path(root or DEFAULT_DIR) / code
    years = sorted(int(p.stem) for p in directory.glob('*.csv')
                   if p.stem.isdigit())
    return [y for y in years if y < HOLDOUT_END_YEAR]


def load_league(code, seasons=None, bet_seasons=(), families=('avg',),
                root=None):
    """
    One league's matches in kickoff order, plus the skipped-price counts.

    code         -- 'E0', 'E1', 'E2', 'E3' or 'EC'
    seasons      -- season end-years to load (2020 = 2019/20). None loads
                    every available season from the league's earliest, which
                    is what the per-league ELO warm-up wants.
    bet_seasons  -- end-years whose odds are read. Must be within `seasons`.
    families     -- odds families to read in those seasons.

    Returns (matches, skips). `skips` is a list of
    {'league', 'season', 'family', 'rows', 'skipped'}, one per betting
    season and family, zeros included.
    """
    unknown = [f for f in families if f not in ODDS_FAMILIES]
    if unknown:
        raise DataError(f'unknown odds family {unknown}; expected one of '
                        f'{", ".join(ODDS_FAMILIES)}')

    available = available_seasons(code, root)
    years = sorted(available if seasons is None else set(seasons))
    bet_years = set(bet_seasons)

    directory = Path(root or DEFAULT_DIR) / code
    # Guard first, over everything requested, so a holdout season in the
    # arguments fails before any other file is read.
    for year in sorted(set(years) | bet_years):
        _assert_not_holdout(directory / f'{year}.csv')
    stray = sorted(bet_years - set(years))
    if stray:
        raise DataError(f'{code}: bet seasons {stray} are not among the '
                        f'seasons being loaded')

    aliases = load_aliases()
    matches, skips = [], []
    for year in years:
        path = directory / f'{year}.csv'
        if not path.exists():
            raise DataError(f'missing season file {path}')
        season = season_label(year)
        wanted = tuple(families) if year in bet_years else ()
        rows, skipped = _load_season(path, season, wanted, aliases)
        matches.extend(rows)
        for family in wanted:
            skips.append({'league': code, 'season': season, 'family': family,
                          'rows': len(rows), 'skipped': skipped[family]})

    if not matches:
        raise DataError(f'no matches loaded for {code} from {directory}')
    return _ordered(matches), skips


def load_legacy_pl_flat():
    """
    REGRESSION ONLY. The original harness's six flat Premier League files.

    Hard-coded path, no arguments: docs/historical/2021.csv .. 2026.csv,
    Avg* and B365* read in every season, exactly the input the original
    1,298-bet PL result was computed from. It goes through the same row
    parser as `load_league`, which is the point of the regression.

    This is the only route by which a 2026.csv can be opened, and only the
    flat PL one.
    """
    aliases = load_aliases()
    matches, skips = [], []
    for year in _LEGACY_PL_END_YEARS:
        path = DEFAULT_DIR / f'{year}.csv'
        if not path.exists():
            raise DataError(f'missing season file {path}')
        season = season_label(year)
        rows, skipped = _load_season(path, season, _LEGACY_PL_FAMILIES,
                                     aliases, legacy_regression=True)
        matches.extend(rows)
        for family in _LEGACY_PL_FAMILIES:
            skips.append({'league': 'E0-legacy-flat', 'season': season,
                          'family': family, 'rows': len(rows),
                          'skipped': skipped[family]})
    return _ordered(matches), skips


def league_teams(code, year, root=None):
    """
    Team names (aliased) in one league season, or None if there is no file.

    Reads names only. Goes through the guarded reader, so a holdout season
    raises rather than returning a list.
    """
    path = Path(root or DEFAULT_DIR) / code / f'{year}.csv'
    _assert_not_holdout(path)
    if not path.exists():
        return None
    aliases = load_aliases()
    _header, rows = _read_rows(path)
    teams = set()
    for row in rows:
        for column in ('HomeTeam', 'AwayTeam'):
            name = (row.get(column) or '').strip()
            if name:
                teams.add(aliases.get(name, name))
    return teams


def entrant_directions(code, root=None):
    """
    {(season, team): 'promoted' | 'relegated' | 'unknown'} for every team
    that enters `code` -- i.e. plays a season there without having played
    the previous one there.

    Direction is where the team played the PREVIOUS season: in the league
    above -> relegated in; in the league below -> promoted in; in neither
    file (or there is no such file) -> unknown. EC has nothing below it in
    the data and nothing exists below E3 before 2005/06, so those arrivals
    are 'unknown' by construction.
    """
    index = LEAGUES.index(code)
    above = LEAGUES[index - 1] if index > 0 else None
    below = LEAGUES[index + 1] if index + 1 < len(LEAGUES) else None

    years = available_seasons(code, root)
    directions = {}
    previous_members = None
    for year in years:
        members = league_teams(code, year, root)
        if previous_members is not None:
            came_down = (league_teams(above, year - 1, root) if above else None) or set()
            came_up = (league_teams(below, year - 1, root) if below else None) or set()
            for team in members - previous_members:
                if team in came_down:
                    direction = 'relegated'
                elif team in came_up:
                    direction = 'promoted'
                else:
                    direction = 'unknown'
                directions[(season_label(year), team)] = direction
        previous_members = members
    return directions
