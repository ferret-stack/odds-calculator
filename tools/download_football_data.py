"""
Download football-data.co.uk season CSVs for the five English leagues.

Inputs only: this fetches raw files and does not parse columns or touch
backtest/ or production code. For the column key, see
docs/Football-Data-Notes.md.

Layout. The existing Premier League files in docs/historical/ are flat
(2021.csv ... 2026.csv) and backtest/data.py reads them by those exact names,
so they are left alone. New files go one folder per league with the same
year-only naming:

    docs/historical/<code>/<endyear>.csv      e.g. docs/historical/E1/1994.csv

<endyear> is the calendar year the season ENDS in, which matches the existing
files (2021.csv = 2020-21). E0 is downloaded again into E0/ so every league
has the same shape; the flat copies stay as they are.

Source URL: https://www.football-data.co.uk/mmz4281/<YYZZ>/<code>.csv,
where YYZZ is the two-digit start and end year (9394, 9900, 0001, 2526).

Availability. A 404 or 300 (the site's answer for a file it doesn't have),
an empty body, or an HTML page in place of a CSV is
recorded as "n/a" (not available) and never fails the run. A network error
or another HTTP status is recorded as "ERR". It is reported separately so a
connection problem isn't mistaken for a season that doesn't exist. It also
sets exit status 1, so you know to re-run.

Idempotent. A file that is already present and non-empty is skipped unless
--force is given. Downloads are written to a .part file and renamed into
place, so an interrupted run never leaves a truncated CSV that a later run
would skip as "present".

Run:  python3 tools/download_football_data.py [--force] [--leagues E0 E1 ...]
                                               [--from 1993] [--to 2025]
                                               [--out DIR] [--delay SECONDS]
"""

import argparse
import sys
import time
from pathlib import Path

import requests

BASE_URL = 'https://www.football-data.co.uk/mmz4281/{yyzz}/{code}.csv'
DEFAULT_OUT = Path(__file__).resolve().parent.parent / 'docs' / 'historical'

LEAGUES = ('E0', 'E1', 'E2', 'E3', 'EC')
FIRST_START_YEAR = 1993   # 1993-94
LAST_START_YEAR = 2025    # 2025-26

# Generic client name: no personal or project details sent to the site.
HEADERS = {'User-Agent': 'football-data-downloader/1.0'}
TIMEOUT = 30

NEW, PRESENT, MISSING, ERROR = 'new', 'have', 'n/a', 'ERR'


def season_code(start_year):
    """1993 -> '9394', 1999 -> '9900', 2025 -> '2526'."""
    return f'{start_year % 100:02d}{(start_year + 1) % 100:02d}'


def season_label(start_year):
    """1993 -> '1993-94'."""
    return f'{start_year}-{(start_year + 1) % 100:02d}'


def looks_unavailable(body):
    """True when a 200 response is not really a season file."""
    text = body.lstrip(b'\xef\xbb\xbf').strip()   # drop a UTF-8 BOM, whitespace
    if not text:
        return True
    head = text[:200].lower()
    return head.startswith(b'<') or b'<html' in head


def fetch_one(session, code, start_year, dest, force):
    """Returns (status, bytes_on_disk, note)."""
    if dest.exists() and dest.stat().st_size > 0 and not force:
        return PRESENT, dest.stat().st_size, ''

    url = BASE_URL.format(yyzz=season_code(start_year), code=code)
    try:
        resp = session.get(url, timeout=TIMEOUT)
    except requests.RequestException as exc:
        return ERROR, 0, f'{url}: {type(exc).__name__}'

    # football-data answers 300 Multiple Choices (not 404) for a season file
    # it does not have, e.g. EC before 2005-06.
    if resp.status_code in (300, 404):
        return MISSING, 0, ''
    if resp.status_code != 200:
        return ERROR, 0, f'{url}: HTTP {resp.status_code}'
    if looks_unavailable(resp.content):
        return MISSING, 0, ''

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix('.csv.part')
    tmp.write_bytes(resp.content)
    tmp.replace(dest)
    return NEW, len(resp.content), ''


def fmt_cell(status, size):
    if status in (NEW, PRESENT):
        return f'{status} {size / 1024:.0f}K'
    return status


def print_summary(leagues, years, results):
    """Grid of season rows x league columns, then per-league totals."""
    width = 11
    print()
    print('Season   ' + ''.join(f'{c:>{width}}' for c in leagues))
    print('-' * (9 + width * len(leagues)))
    for y in years:
        cells = (fmt_cell(*results[(c, y)][:2]) for c in leagues)
        print(f'{season_label(y):<9}' + ''.join(f'{x:>{width}}' for x in cells))

    print()
    print(f'{"League":<8}{"new":>6}{"have":>6}{"n/a":>6}{"ERR":>6}{"MB on disk":>12}')
    grand = {NEW: 0, PRESENT: 0, MISSING: 0, ERROR: 0, 'bytes': 0}
    for c in leagues:
        counts = {NEW: 0, PRESENT: 0, MISSING: 0, ERROR: 0}
        total = 0
        for y in years:
            status, size, _ = results[(c, y)]
            counts[status] += 1
            total += size
        for k, v in counts.items():
            grand[k] += v
        grand['bytes'] += total
        print(f'{c:<8}{counts[NEW]:>6}{counts[PRESENT]:>6}{counts[MISSING]:>6}'
              f'{counts[ERROR]:>6}{total / 1_048_576:>12.2f}')
    print(f'{"All":<8}{grand[NEW]:>6}{grand[PRESENT]:>6}{grand[MISSING]:>6}'
          f'{grand[ERROR]:>6}{grand["bytes"] / 1_048_576:>12.2f}')

    errors = [(c, y, note) for (c, y), (s, _, note) in results.items() if s == ERROR]
    if errors:
        print('\nErrors (re-run to retry; these are NOT confirmed missing):')
        for c, y, note in sorted(errors):
            print(f'  {c} {season_label(y)}  {note}')


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--leagues', nargs='+', default=list(LEAGUES), metavar='CODE')
    ap.add_argument('--from', dest='first', type=int, default=FIRST_START_YEAR,
                    help='first season START year (default 1993 = 1993-94)')
    ap.add_argument('--to', dest='last', type=int, default=LAST_START_YEAR,
                    help='last season START year (default 2025 = 2025-26)')
    ap.add_argument('--out', type=Path, default=DEFAULT_OUT)
    ap.add_argument('--force', action='store_true',
                    help='re-download files that are already present')
    ap.add_argument('--delay', type=float, default=0.5,
                    help='seconds between requests (default 0.5)')
    args = ap.parse_args()

    years = list(range(args.first, args.last + 1))
    results = {}
    with requests.Session() as session:
        session.headers.update(HEADERS)
        for code in args.leagues:
            for y in years:
                dest = args.out / code / f'{y + 1}.csv'
                status, size, note = fetch_one(session, code, y, dest, args.force)
                results[(code, y)] = (status, size, note)
                print(f'{code} {season_label(y)}  {fmt_cell(status, size)}'
                      + (f'  {note}' if note else ''), flush=True)
                if status != PRESENT:   # only sleep after a real request
                    time.sleep(args.delay)

    print_summary(args.leagues, years, results)
    return 1 if any(s == ERROR for s, _, _ in results.values()) else 0


if __name__ == '__main__':
    sys.exit(main())
