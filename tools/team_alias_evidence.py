#!/usr/bin/env python3
"""Read-only evidence for candidate team-name aliases in docs/historical/.

For each name: every league and season it appears in.
For each pair:
  (1) seasons where both names appear (any league)
  (2) fixtures where one name plays the other (date, league, home v away only)
  (3) last season of each name vs first season of the other

Reads only Date, HomeTeam and AwayTeam. No scores or odds are read for any
season, so the 2025/26 holdout stays structure-only. Prints to stdout and
writes nothing.

Usage:  python3 tools/team_alias_evidence.py
"""

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import audit_football_data as A  # noqa: E402  (reuse the audit's CSV reader)

PAIRS = [
    ('Telford United', 'AFC Telford United'),
    ('Boston', 'Boston Utd'),
    ('Oxford', 'Oxford City'),
    ('Wimbledon', 'AFC Wimbledon'),
    ('Wimbledon', 'Milton Keynes Dons'),   # data spells it in full; no "MK Dons"
    ('Northwich', 'Norwich'),
]


def load():
    seen = defaultdict(set)          # name -> {(end_year, league)}
    fixtures = []                    # (end_year, league, date, home, away)
    for lg in A.LEAGUES:
        for p in sorted((A.DEFAULT_ROOT / lg).glob('*.csv'), key=lambda p: int(p.stem)):
            y = int(p.stem)
            header, rows, _, _ = A.read_rows(p)
            idx = A.col_index(header)
            hi, ai, di = idx.get('HomeTeam'), idx.get('AwayTeam'), idx.get('Date')
            for r in rows:
                h, a = A.cell(r, hi), A.cell(r, ai)
                for t in (h, a):
                    if t:
                        seen[t].add((y, lg))
                fixtures.append((y, lg, A.cell(r, di), h, a))
    return seen, fixtures


def appearances(seen, name):
    by_lg = defaultdict(list)
    for y, lg in sorted(seen.get(name, ())):
        by_lg[lg].append(A.season_label(y))
    return '; '.join(f"{lg}: {', '.join(s)}" for lg, s in sorted(by_lg.items())) or 'NOT FOUND'


def main():
    seen, fixtures = load()
    names = sorted({n for p in PAIRS for n in p})
    print('== Appearances ==')
    for n in names:
        print(f'{n}\n  {appearances(seen, n)}')

    for a, b in PAIRS:
        print(f'\n== {a}  /  {b} ==')
        ya = {y for y, _ in seen.get(a, ())}
        yb = {y for y, _ in seen.get(b, ())}
        both = sorted(ya & yb)
        print('(1) seasons with both names:',
              ', '.join(A.season_label(y) for y in both) or 'none')
        if both:
            for y in both:
                la = sorted(lg for yy, lg in seen[a] if yy == y)
                lb = sorted(lg for yy, lg in seen[b] if yy == y)
                print(f'      {A.season_label(y)}: {a} in {la}, {b} in {lb}')
        meet = [f for f in fixtures if {f[3], f[4]} == {a, b}]
        print(f'(2) fixtures between them: {len(meet)}')
        for y, lg, d, h, aw in meet:
            print(f'      {lg} {A.season_label(y)} {d}  {h} v {aw}')
        if ya and yb:
            print(f'(3) {a}: {A.season_label(min(ya))} .. {A.season_label(max(ya))}   '
                  f'{b}: {A.season_label(min(yb))} .. {A.season_label(max(yb))}')
        else:
            print('(3) n/a (a name was not found)')


if __name__ == '__main__':
    main()
