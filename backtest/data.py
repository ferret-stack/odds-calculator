"""
Load the football-data.co.uk season CSVs into the shape the ELO chain wants.

The six files in docs/historical/ are one Premier League season each,
2020-21 through 2025-26, 380 rows apiece. Columns used:

    Date, Time, HomeTeam, AwayTeam, FTHG, FTAG, FTR   -- fixture and result
    AvgH/AvgD/AvgA                                    -- market-average close
    B365H/B365D/B365A                                 -- single-book close

Nothing else is read. The files also carry shot/card/handicap columns and a
second closing block (AvgC*), which are deliberately ignored: the brief names
Avg* as the primary benchmark and B365* as the secondary, and silently
switching to a different column set would change what the ROI figures mean.

Team naming needs no aliasing. All six seasons use one convention (28
distinct names across the six files, e.g. 'Man United' and "Nott'm Forest"
throughout), so a team's ELO chain is continuous across seasons on the raw
CSV name. Those names differ from production's (`data/current_elo.json`),
but the backtest never joins to production state, so no mapping is needed --
and adding one would be the only place a name could silently split a chain.
"""

import csv
from datetime import datetime
from pathlib import Path

DEFAULT_DIR = Path(__file__).resolve().parent.parent / 'docs' / 'historical'

# Season file -> the season it holds. football-data names a file for the
# calendar year the season ENDS in.
SEASON_FILES = {
    '2021.csv': '2020-21',
    '2022.csv': '2021-22',
    '2023.csv': '2022-23',
    '2024.csv': '2023-24',
    '2025.csv': '2024-25',
    '2026.csv': '2025-26',
}

# Model selection -> (home column suffix). Mirrors run_pipeline.MARKETS.
ODDS_COLUMNS = {
    'avg': {'home': 'AvgH', 'draw': 'AvgD', 'away': 'AvgA'},
    'b365': {'home': 'B365H', 'draw': 'B365D', 'away': 'B365A'},
}

REQUIRED = ('Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR')


class DataError(Exception):
    pass


def _parse_date(value):
    """football-data writes DD/MM/YY or DD/MM/YYYY. Both appear in the wild."""
    for fmt in ('%d/%m/%Y', '%d/%m/%y'):
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    raise DataError(f'unparseable date {value!r}')


def _odds(row, column):
    """A price, or None when the book did not quote it."""
    raw = (row.get(column) or '').strip()
    if not raw:
        return None
    try:
        price = float(raw)
    except ValueError:
        return None
    # size_bet rejects odds <= 1.0 with a ValueError rather than a no-bet, so
    # a malformed price is dropped here rather than crashing the walk.
    return price if price > 1.0 else None


def load_season(path, season):
    """One season's matches, ordered as the file has them (validated later)."""
    matches = []
    with open(path, encoding='utf-8-sig', newline='') as handle:
        for line_no, row in enumerate(csv.DictReader(handle), start=2):
            if not (row.get('HomeTeam') or '').strip():
                continue  # trailing blank rows are common in these files
            missing = [c for c in REQUIRED if not (row.get(c) or '').strip()]
            if missing:
                raise DataError(
                    f'{path.name}:{line_no} missing {", ".join(missing)}')

            home_goals, away_goals = int(row['FTHG']), int(row['FTAG'])
            match = {
                'season': season,
                'date': _parse_date(row['Date']),
                'time': (row.get('Time') or '').strip(),
                'home_team': row['HomeTeam'].strip(),
                'away_team': row['AwayTeam'].strip(),
                'home_goals': home_goals,
                'away_goals': away_goals,
                'ftr': row['FTR'].strip().upper(),
                'odds': {
                    book: {sel: _odds(row, col) for sel, col in cols.items()}
                    for book, cols in ODDS_COLUMNS.items()
                },
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
    return matches


def load_all(directory=None):
    """
    Every season, in kickoff order.

    Order is (date, time, home_team). The ELO chain is order-dependent, and
    unlike production's scraped data -- where `tools/rebuild_elo.py` has to
    repair 17.5% corrupt dates and fall back on match_id ordering -- these
    dates are the source's own and need no repair. Time breaks same-day ties;
    home_team breaks the rest deterministically so a rerun reproduces a run.
    """
    directory = Path(directory or DEFAULT_DIR)
    matches = []
    for filename, season in sorted(SEASON_FILES.items()):
        path = directory / filename
        if not path.exists():
            raise DataError(f'missing season file {path}')
        matches.extend(load_season(path, season))

    if not matches:
        raise DataError(f'no matches loaded from {directory}')

    matches.sort(key=lambda m: (m['date'], m['time'], m['home_team']))
    return matches
