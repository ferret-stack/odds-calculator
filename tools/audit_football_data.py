#!/usr/bin/env python3
"""Read-only column audit of football-data.co.uk season files.

Scans docs/historical/{E0,E1,E2,E3,EC}/<endyear>.csv (endyear 2020 = 2019/20)
and reports one row per league x season:

  * rows (blank padding rows excluded and counted separately)
  * distinct teams, expected matches = teams * (teams - 1), shortfall
    (expected - rows), and a flag when the team count differs from the
    division's usual size (E0 20, others 24) -- flagged, not corrected
  * date format actually seen per row (dd/mm/yy vs dd/mm/yyyy), unparseable
    and out-of-season-window counts, first/last date
  * FTHG / FTAG / FTR presence and blank counts
  * odds groups, column names taken from docs/Football-Data-Notes.md:
      avg_pre   AvgH/AvgD/AvgA, else the older BbAvH/BbAvD/BbAvA
      avg_close AvgCH/AvgCD/AvgCA  (notes: closing = code + "C")
      ps_pre    PSH/PSD/PSA, else the documented alias PH/PD/PA
      ps_close  PSCH/PSCD/PSCA
      b365      B365H/B365D/B365A
    each with present yes/no and % of rows where all three cells are non-blank

A second table lists, per league, team names seen in only one season and
pairs of names that look like spelling variants, plus cross-league variant
pairs (names that never share a league). Nothing is merged.

Holdout: the 2025/26 file (2026.csv) is reported for structure only. The
script never reads a cell's value there beyond "blank or not" (plus the Date
and team-name columns, which are structure). No odds values, results
summaries or win rates are computed for ANY season; odds and results cells
are only ever tested for blankness.

Writes nothing except tools/output/football_data_audit.csv and
tools/output/football_data_team_names.csv.

Usage:  python3 tools/audit_football_data.py [--root docs/historical]
"""

import argparse
import csv
import difflib
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = REPO / 'docs' / 'historical'
OUT_DIR = REPO / 'tools' / 'output'

LEAGUES = ['E0', 'E1', 'E2', 'E3', 'EC']
USUAL_TEAMS = {'E0': 20, 'E1': 24, 'E2': 24, 'E3': 24, 'EC': 24}
HOLDOUT_FILE = '2026.csv'

RESULT_COLS = ['FTHG', 'FTAG', 'FTR']

# group -> list of (label, [H, D, A]) candidates in preference order.
# All names copied from docs/Football-Data-Notes.md.
ODDS_GROUPS = {
    'avg_pre':   [('Avg',  ['AvgH', 'AvgD', 'AvgA']),
                  ('BbAv', ['BbAvH', 'BbAvD', 'BbAvA'])],
    'avg_close': [('AvgC', ['AvgCH', 'AvgCD', 'AvgCA'])],
    'ps_pre':    [('PS',   ['PSH', 'PSD', 'PSA']),
                  ('P',    ['PH', 'PD', 'PA'])],
    'ps_close':  [('PSC',  ['PSCH', 'PSCD', 'PSCA'])],
    'b365':      [('B365', ['B365H', 'B365D', 'B365A'])],
}

RE_YY = re.compile(r'^\d{1,2}/\d{1,2}/\d{2}$')
RE_YYYY = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}$')


def season_label(end_year):
    return f'{end_year - 1}/{str(end_year)[2:]}'


def read_rows(path):
    """Return (header, data_rows, blank_rows, encoding). Strict UTF-8 first,
    cp1252 fallback; the encoding used is reported, not assumed."""
    raw = path.read_bytes()
    for enc in ('utf-8-sig', 'cp1252', 'latin-1'):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    # csv handles \n, \r\n and bare \r line endings when given a str.
    rows = list(csv.reader(text.splitlines()))
    if not rows:
        return [], [], 0, enc
    header = [h.strip().lstrip('﻿') for h in rows[0]]
    data, blank = [], 0
    for r in rows[1:]:
        if all(not c.strip() for c in r):
            blank += 1
        else:
            data.append(r)
    return header, data, blank, enc


def col_index(header):
    idx = {}
    for i, h in enumerate(header):
        if h and h not in idx:          # first occurrence wins
            idx[h] = i
    return idx


def cell(row, i):
    return row[i].strip() if i is not None and i < len(row) else ''


def audit_dates(rows, di, end_year):
    n_yy = n_yyyy = n_blank = n_unparsed = n_window = 0
    parsed = []
    lo, hi = date(end_year - 1, 6, 1), date(end_year, 8, 31)
    for r in rows:
        s = cell(r, di)
        if not s:
            n_blank += 1
            continue
        if RE_YY.match(s):
            fmt = '%d/%m/%y'
            n_yy += 1
        elif RE_YYYY.match(s):
            fmt = '%d/%m/%Y'
            n_yyyy += 1
        else:
            n_unparsed += 1
            continue
        try:
            d = datetime.strptime(s, fmt).date()
        except ValueError:
            n_unparsed += 1
            continue
        if not lo <= d <= hi:
            n_window += 1
        parsed.append(d)
    if n_yy and n_yyyy:
        fmt_label = 'mixed'
    elif n_yy:
        fmt_label = 'dd/mm/yy'
    elif n_yyyy:
        fmt_label = 'dd/mm/yyyy'
    else:
        fmt_label = 'none'
    clean = (n_blank == 0 and n_unparsed == 0 and n_window == 0
             and fmt_label in ('dd/mm/yy', 'dd/mm/yyyy'))
    return {
        'date_format': fmt_label,
        'dates_yy': n_yy,
        'dates_yyyy': n_yyyy,
        'dates_blank': n_blank,
        'dates_unparsed': n_unparsed,
        'dates_out_of_window': n_window,
        'dates_clean': 'yes' if clean else 'no',
        'first_date': min(parsed).isoformat() if parsed else '',
        'last_date': max(parsed).isoformat() if parsed else '',
    }


def audit_file(league, path):
    end_year = int(path.stem)
    holdout = path.name == HOLDOUT_FILE
    header, rows, blank_rows, enc = read_rows(path)
    idx = col_index(header)
    n = len(rows)

    out = {
        'league': league,
        'season': season_label(end_year),
        'file': str(path.relative_to(REPO)) if path.is_relative_to(REPO) else str(path),
        'holdout': 'yes' if holdout else 'no',
        'encoding': enc,
        'rows': n,
        'blank_rows_skipped': blank_rows,
    }

    # Teams: names only (structure), used for counts and expected matches.
    hi_, ai_ = idx.get('HomeTeam'), idx.get('AwayTeam')
    teams = set()
    for r in rows:
        for i in (hi_, ai_):
            t = cell(r, i)
            if t:
                teams.add(t)
    usual = USUAL_TEAMS[league]
    k = len(teams)
    expected = k * (k - 1)
    out.update({
        'teams': k,
        'usual_teams': usual,
        'team_count_flag': 'CHECK' if k != usual else '',
        'expected_matches': expected,
        'shortfall': expected - n,
    })

    out.update(audit_dates(rows, idx.get('Date'), end_year))

    # Results: presence + blank counts only. No values read.
    for c in RESULT_COLS:
        i = idx.get(c)
        out[f'{c}_present'] = 'yes' if i is not None else 'no'
        out[f'{c}_blank'] = sum(1 for r in rows if not cell(r, i)) if i is not None else ''

    # Odds groups: presence + % rows with all three cells non-blank.
    for g, candidates in ODDS_GROUPS.items():
        found = [(lab, cols) for lab, cols in candidates
                 if all(c in idx for c in cols)]
        if found:
            lab, cols = found[0]
            ii = [idx[c] for c in cols]
            full = sum(1 for r in rows if all(cell(r, i) for i in ii))
            out[f'{g}_present'] = 'yes'
            out[f'{g}_cols'] = '+'.join(l for l, _ in found)  # e.g. "Avg+BbAv" if both
            out[f'{g}_pct_full'] = f'{100 * full / n:.1f}' if n else ''
        else:
            # note partial groups (1-2 of the 3 columns) rather than hide them
            partial = [c for _, cols in candidates for c in cols if c in idx]
            out[f'{g}_present'] = 'partial' if partial else 'no'
            out[f'{g}_cols'] = '+'.join(partial)
            out[f'{g}_pct_full'] = ''

    flags = []
    if out['team_count_flag']:
        flags.append(f'teams {k}!={usual}')
    if out['shortfall'] != 0:
        # Report the number as-is; don't assume the holdout season is unfinished.
        flags.append(f"shortfall {out['shortfall']}" + (' (holdout file)' if holdout else ''))
    if out['dates_clean'] != 'yes':
        flags.append('dates not clean')
    if hi_ is None or ai_ is None:
        flags.append('missing team column')
    out['flags'] = '; '.join(flags)
    return out, teams


# ---------- team-name checks ----------

STOP = {'fc', 'afc', 'cf'}


def norm(name):
    s = name.lower().replace('&', ' and ')
    s = re.sub(r"[^a-z0-9 ]", ' ', s)
    return ' '.join(w for w in s.split() if w not in STOP)


def initials_match(a, b):
    """'mk dons' vs 'milton keynes dons': one token in a is the initials of
    two-or-more consecutive tokens in b, rest identical."""
    ta, tb = a.split(), b.split()
    for i, tok in enumerate(ta):
        for j in range(len(tb)):
            for L in range(2, len(tb) - j + 1):
                if ''.join(w[0] for w in tb[j:j + L]) == tok:
                    if ta[:i] + ta[i + 1:] == tb[:j] + tb[j + L:]:
                        return True
    return False


def variant_reason(a, b):
    na, nb = norm(a), norm(b)
    if na == nb:
        return 'same after normalising case/punctuation/FC'
    if na.replace(' ', '') == nb.replace(' ', ''):
        return 'differs only in spacing'
    if initials_match(na, nb) or initials_match(nb, na):
        return 'abbreviation/initials'
    short, long_ = sorted((na, nb), key=len)
    if len(short) >= 4 and (long_.startswith(short + ' ') or long_.endswith(' ' + short)):
        return 'one name contains the other'
    r = difflib.SequenceMatcher(None, na, nb).ratio()
    if r >= 0.85:
        return f'similar spelling ({r:.2f})'
    return None


def team_name_report(league, seasons_by_team):
    out = []
    for t, seasons in sorted(seasons_by_team.items()):
        if len(seasons) == 1:
            out.append({'league': league, 'check': 'one season only', 'name': t,
                        'other_name': '', 'reason': '',
                        'seasons': ', '.join(sorted(seasons))})
    names = sorted(seasons_by_team)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            why = variant_reason(a, b)
            if why:
                out.append({'league': league, 'check': 'possible variant', 'name': a,
                            'other_name': b, 'reason': why,
                            'seasons': f"{a}: {', '.join(sorted(seasons_by_team[a]))} | "
                                       f"{b}: {', '.join(sorted(seasons_by_team[b]))}"})
    return out


def cross_league_report(where_by_team):
    """Variant pairs whose two names never share a league, so the per-league
    check can't see them (e.g. 'Boston' in E3 vs 'Boston Utd' in EC).
    where_by_team: name -> {league: set(seasons)}."""
    def fmt(t):
        return '; '.join(f"{lg} {', '.join(sorted(s))}"
                         for lg, s in sorted(where_by_team[t].items()))
    out = []
    names = sorted(where_by_team)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            if set(where_by_team[a]) & set(where_by_team[b]):
                continue        # share a league: already covered per league
            why = variant_reason(a, b)
            if why:
                out.append({'league': 'cross-league', 'check': 'possible variant',
                            'name': a, 'other_name': b, 'reason': why,
                            'seasons': f'{a}: {fmt(a)} | {b}: {fmt(b)}'})
    return out


# ---------- output ----------

def print_table(rows, cols):
    widths = {c: max(len(c), *(len(str(r.get(c, ''))) for r in rows)) for c in cols}
    print('  '.join(c.ljust(widths[c]) for c in cols))
    print('  '.join('-' * widths[c] for c in cols))
    for r in rows:
        print('  '.join(str(r.get(c, '')).ljust(widths[c]) for c in cols))


def write_csv(path, rows, cols):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    args = ap.parse_args()

    season_rows, name_rows = [], []
    where_by_team = defaultdict(lambda: defaultdict(set))
    for lg in LEAGUES:
        d = args.root / lg
        if not d.is_dir():
            print(f'warning: {d} missing', file=sys.stderr)
            continue
        seasons_by_team = defaultdict(set)
        for path in sorted(d.glob('*.csv'), key=lambda p: int(p.stem)):
            row, teams = audit_file(lg, path)
            season_rows.append(row)
            for t in teams:
                seasons_by_team[t].add(row['season'])
                where_by_team[t][lg].add(row['season'])
        name_rows.extend(team_name_report(lg, seasons_by_team))
    name_rows.extend(cross_league_report(where_by_team))

    season_cols = list(season_rows[0].keys())
    name_cols = ['league', 'check', 'name', 'other_name', 'reason', 'seasons']

    print('== Season audit (one row per league x season) ==')
    print_table(season_rows, season_cols)
    print()
    print('== Team names: single-season and possible spelling variants (not merged) ==')
    print_table(name_rows, name_cols)

    a = OUT_DIR / 'football_data_audit.csv'
    b = OUT_DIR / 'football_data_team_names.csv'
    write_csv(a, season_rows, season_cols)
    write_csv(b, name_rows, name_cols)
    print(f'\nwrote {a.relative_to(REPO)} ({len(season_rows)} rows)')
    print(f'wrote {b.relative_to(REPO)} ({len(name_rows)} rows)')


if __name__ == '__main__':
    main()
