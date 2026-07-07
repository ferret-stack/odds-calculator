"""
One-time (idempotent) migration: turn the legacy matches_data.json into a clean,
facts-only matches file.

What it fixes, and why:

1. DUPLICATE ROWS — a few matchweeks were scraped twice (e.g. ids 1584-88 vs
   1589-93). Rule: group rows by (date, home_team, away_team); keep the row with
   the most non-null facts; on a tie keep the higher match_id (a re-scrape is
   assumed to be a correction — verified by eye: the re-scraped 2025-03 batch has
   the correct per-match referees while the first batch wrongly repeats one name).

2. DAY/MONTH-SWAPPED DATES (Excel era, match_id < 1,000,000) — the original Excel
   import parsed d/m/Y strings month-first when the day was <= 12, so a subset of
   those rows carries a swapped date (match_id 1 "2020-12-09" is really 12 Sep
   2020, the opening weekend of 2020-21). Rows with stored day > 12 cannot have
   been swapped and are trusted anchors. For each ambiguous row we choose between
   the stored and the swapped date using, in order:
     a. hard elimination: no Premier League match happens in June or July, and
        the dataset starts with the 2020-21 season (nothing before 2020-09-01);
     b. fixture uniqueness: a given (home, away) pairing occurs exactly once per
        season, so a candidate that collides with a trusted row is impossible;
     c. team-schedule check: a team cannot play two matches less than 2 days
        apart, nor twice on one day;
     d. matchweek-neighbour vote: Excel match_ids are grouped by scheduled
        matchweek, so the median date of the nearest trusted neighbours by id is
        a strong prior. We only act on it when it is decisive: one candidate must
        sit at least DECISIVE_MARGIN_DAYS closer to the vote than the other
        (postponements are real, especially in 2020-21, so near-ties stay open).
   Undecidable rows keep their stored date and are flagged in the report.

3. WRONG-YEAR DATES (scraped era, match_id >= 1,000,000) — the Selenium scraper's
   date fallback appended datetime.now().year when the page omitted the year, so
   matches scraped just after New Year could be stamped a year late (six fixtures
   played 30 Dec 2025 are stored as 2026-12-30). Rule: every scraped id cohort
   belongs to a known season window; if the stored date falls outside it and a
   +/-1 year shift lands inside, shift.

4. DERIVED-FIELD REMOVAL — everything ELO-derived (home_elo, away_elo, elo_diff,
   elo_band, winner) and everything recomputable from facts (over_XX, btts,
   total_booking_points) is dropped. rebuild.py re-derives all of it from one
   chronological replay. Facts are the only thing this file stores.

Usage:  python tools/repair_facts.py [--in data/matches_data.json] [--out same]
"""

import argparse
import json
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

FACT_FIELDS = [
    'match_id', 'date', 'home_team', 'away_team',
    'home_goals', 'away_goals',
    'home_yellow', 'away_yellow', 'home_red', 'away_red',
    'referee',
    'home_xg', 'away_xg', 'home_possession', 'away_possession',
]

EXCEL_ERA_MAX_ID = 1_000_000          # below: sequential Excel ids; above: PL site ids
CLOSE_SEASON_MONTHS = (6, 7)          # no PL football in June/July
MIN_REST_DAYS = 2                     # a team never plays twice within <2 days
NEIGHBOUR_COUNT = 8                   # trusted id-neighbours consulted for the vote
DECISIVE_MARGIN_DAYS = 10             # vote acts only with at least this margin
DATASET_START = date(2020, 9, 1)      # first season on record is 2020-21
DATASET_END = date(2026, 6, 10)

# Season windows for scraped-era id cohorts (PL website ids).
SCRAPED_SEASON_WINDOWS = [
    (2444000, 2445000, date(2024, 8, 1), date(2025, 6, 10)),   # 2024-25
    (2561000, 2563000, date(2025, 8, 1), date(2026, 6, 10)),   # 2025-26
]


def parse(ds):
    y, m, d = map(int, ds.split('-'))
    return date(y, m, d)


def fmt(d):
    return d.strftime('%Y-%m-%d')


def swapped(d):
    """Day/month transposed, or None if impossible."""
    if d.day <= 12:
        try:
            return date(d.year, d.day, d.month)
        except ValueError:
            return None
    return None


def dedupe(matches, report):
    groups = defaultdict(list)
    for m in matches:
        groups[(m['date'], m['home_team'], m['away_team'])].append(m)
    kept = []
    for key, rows in groups.items():
        if len(rows) > 1:
            rows.sort(key=lambda r: (sum(v is not None for v in r.values()),
                                     r['match_id']))
            for dropped in rows[:-1]:
                report['duplicates_dropped'].append(
                    {'kept': rows[-1]['match_id'], 'dropped': dropped['match_id'],
                     'fixture': f"{key[1]} v {key[2]} {key[0]}"})
            kept.append(rows[-1])
        else:
            kept.append(rows[0])
    return kept


def repair_scraped_dates(matches, report):
    for m in matches:
        if m['match_id'] < EXCEL_ERA_MAX_ID:
            continue
        d = parse(m['date'])
        for lo, hi, start, end in SCRAPED_SEASON_WINDOWS:
            if lo <= m['match_id'] <= hi:
                if not (start <= d <= end):
                    for shift in (-1, 1):
                        cand = date(d.year + shift, d.month, d.day)
                        if start <= cand <= end:
                            report['year_shifted'].append(
                                {'match_id': m['match_id'], 'from': m['date'],
                                 'to': fmt(cand)})
                            m['date'] = fmt(cand)
                            break
                    else:
                        report['unresolved'].append(
                            {'match_id': m['match_id'], 'date': m['date'],
                             'reason': 'outside season window, no year shift fits'})
                break
        else:
            report['unresolved'].append(
                {'match_id': m['match_id'], 'date': m['date'],
                 'reason': 'scraped id outside known season cohorts'})


def repair_excel_dates(matches, report):
    excel = sorted((m for m in matches if m['match_id'] < EXCEL_ERA_MAX_ID),
                   key=lambda m: m['match_id'])

    trusted = {}      # match_id -> date, for anchor rows and decided rows
    ambiguous = []
    for m in excel:
        d = parse(m['date'])
        if swapped(d) is None or swapped(d) == d:
            trusted[m['match_id']] = d
        else:
            ambiguous.append(m)

    def neighbour_vote(mid):
        """Median date of the nearest trusted rows by id (matchweek prior)."""
        near = sorted(trusted.items(), key=lambda kv: abs(kv[0] - mid))
        dates = sorted(d for _, d in near[:NEIGHBOUR_COUNT])
        return dates[len(dates) // 2] if dates else None

    def season_of(d):
        return d.year if d.month >= 8 else d.year - 1

    def context(all_matches):
        """Trusted dates per team, and trusted (season, home, away) fixture keys."""
        tdates = defaultdict(set)
        fixtures = set()
        for m in all_matches:
            if m['match_id'] in trusted:
                d = trusted[m['match_id']]
                tdates[m['home_team']].add(d)
                tdates[m['away_team']].add(d)
                fixtures.add((season_of(d), m['home_team'], m['away_team']))
        return tdates, fixtures

    def candidate_ok(m, cand, tdates, fixtures):
        if cand.month in CLOSE_SEASON_MONTHS:
            return False
        if not (DATASET_START <= cand <= DATASET_END):
            return False
        if (season_of(cand), m['home_team'], m['away_team']) in fixtures:
            return False
        for team in (m['home_team'], m['away_team']):
            for other in tdates[team]:
                if abs((cand - other).days) < MIN_REST_DAYS:
                    return False
        return True

    def commit(m, d, tdates, fixtures):
        trusted[m['match_id']] = d
        tdates[m['home_team']].add(d)
        tdates[m['away_team']].add(d)
        fixtures.add((season_of(d), m['home_team'], m['away_team']))

    # Multi-pass: decide the easy rows first, let them anchor the harder ones.
    # Context updates as each row is decided so same-pass decisions see each other.
    pending = list(ambiguous)
    for _ in range(8):
        if not pending:
            break
        tdates, fixtures = context(excel)
        still = []
        for m in pending:
            stored = parse(m['date'])
            alt = swapped(stored)
            cands = [c for c in (stored, alt)
                     if candidate_ok(m, c, tdates, fixtures)]
            if len(cands) == 1:
                choice, rule = cands[0], 'elimination'
            elif len(cands) == 2:
                vote = neighbour_vote(m['match_id'])
                if vote is None:
                    still.append(m)
                    continue
                d0, d1 = (abs((c - vote).days) for c in cands)
                if abs(d0 - d1) >= DECISIVE_MARGIN_DAYS:
                    choice, rule = cands[0] if d0 < d1 else cands[1], 'matchweek-vote'
                else:
                    still.append(m)
                    continue
            else:
                # both candidates eliminated — keep stored, flag loudly
                report['unresolved'].append(
                    {'match_id': m['match_id'], 'date': m['date'],
                     'reason': 'both date interpretations conflict with schedule'})
                commit(m, stored, tdates, fixtures)
                continue
            if choice != stored:
                report['dates_swapped'].append(
                    {'match_id': m['match_id'], 'from': fmt(stored),
                     'to': fmt(choice), 'rule': rule})
            m['date'] = fmt(choice)
            commit(m, choice, tdates, fixtures)
        pending = still

    tdates, fixtures = context(excel)
    for m in pending:   # nothing decided them — keep stored, flag
        report['unresolved'].append(
            {'match_id': m['match_id'], 'date': m['date'],
             'reason': 'ambiguous day/month, no rule discriminates'})
        commit(m, parse(m['date']), tdates, fixtures)


def validate(matches, report):
    """Global sanity of the repaired facts."""
    problems = []
    by_team = defaultdict(list)
    for m in matches:
        d = parse(m['date'])
        if d.month in CLOSE_SEASON_MONTHS:
            problems.append(f"close-season date: {m['match_id']} {m['date']}")
        by_team[m['home_team']].append(d)
        by_team[m['away_team']].append(d)
    for team, ds in by_team.items():
        ds.sort()
        for a, b in zip(ds, ds[1:]):
            if (b - a).days < MIN_REST_DAYS:
                problems.append(f"{team} plays twice within {(b - a).days}d of {a}")
    # one meeting per (season, home, away)
    seen = Counter()
    for m in matches:
        d = parse(m['date'])
        season = d.year if d.month >= 8 else d.year - 1
        seen[(season, m['home_team'], m['away_team'])] += 1
    for key, n in seen.items():
        if n > 1:
            problems.append(f"fixture appears {n}x in one season: {key}")
    report['validation_problems'] = problems

    # per-season match counts (a full PL season has 380)
    per_season = Counter()
    for m in matches:
        d = parse(m['date'])
        per_season[d.year if d.month >= 8 else d.year - 1] += 1
    report['matches_per_season'] = {f"{y}-{str(y + 1)[2:]}": n
                                    for y, n in sorted(per_season.items())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--infile', default='data/matches_data.json')
    ap.add_argument('--out', default=None, help='default: overwrite --infile')
    args = ap.parse_args()

    with open(args.infile) as f:
        raw = json.load(f)

    report = {'duplicates_dropped': [], 'dates_swapped': [],
              'year_shifted': [], 'unresolved': []}

    matches = dedupe(raw, report)
    repair_scraped_dates(matches, report)
    repair_excel_dates(matches, report)

    facts = [{k: m.get(k) for k in FACT_FIELDS} for m in matches]
    facts.sort(key=lambda m: (m['date'], m['match_id']))
    validate(facts, report)

    out = args.out or args.infile
    with open(out, 'w') as f:
        json.dump(facts, f, indent=2)

    print(f"in: {len(raw)} rows  ->  out: {len(facts)} facts rows ({out})")
    print(f"duplicates dropped : {len(report['duplicates_dropped'])}")
    print(f"dates day/month-fixed : {len(report['dates_swapped'])}"
          f" ({Counter(r['rule'] for r in report['dates_swapped'])})")
    print(f"dates year-shifted : {len(report['year_shifted'])}")
    print(f"unresolved (kept stored date, flagged): {len(report['unresolved'])}")
    for r in report['unresolved']:
        print(f"  ! {r}")
    print(f"validation problems: {len(report['validation_problems'])}")
    for p in report['validation_problems'][:20]:
        print(f"  ! {p}")
    print("matches per season:", report['matches_per_season'])

    report_path = Path(out).parent / 'reference' / 'repair_report.json'
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"full report: {report_path}")


if __name__ == '__main__':
    main()
