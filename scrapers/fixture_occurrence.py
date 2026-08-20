"""
Cup and European fixture OCCURRENCE.

Scope, deliberately narrow: date, competition, opponent. Nothing else.

This exists to answer one question in a write-up -- "did this side have a
midweek cup tie, and against whom?" -- and it feeds narrative context only.
It produces no score, no weighting and no adjustment to any price. Venue,
round, kick-off time, lineups and rotation are all out of scope; rotation in
particular is parked under Manager Styles and is not a system here.

Competitions covered:
    FA Cup
    Carabao Cup (EFL Cup)
    UEFA Champions League / Europa League / Conference League

Deliberately NOT covered: Premier League (that is matches_data.json), Community
Shield, friendlies, UEFA Super Cup, Club World Cup.

Sources
-------
Primary is a team fixture-list page (BBC Sport by default, configurable via
FIXTURE_SOURCE_URL). Because this is narrative-only, low-volume data, a
hand-maintained override file is a first-class input rather than a fallback
hack: data/cup_fixtures_manual.json is merged over anything scraped, so a
broken selector degrades to "stale but correct" rather than to wrong.

NOTE ON VERIFICATION: the parsing layer below is unit-tested against captured
markup. The live selectors have NOT been run against the source site from this
environment -- outbound access to it is blocked here -- so the first local run
should be `--dry-run` to confirm the rows come back as expected.
"""

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

# --- competition normalisation -------------------------------------------
# Left side is matched case-insensitively as a substring of the raw label the
# source prints; right side is the canonical name we store.
COMPETITION_ALIASES = [
    ('fa cup', 'FA Cup'),
    ('emirates fa cup', 'FA Cup'),
    ('carabao', 'Carabao Cup'),
    ('efl cup', 'Carabao Cup'),
    ('league cup', 'Carabao Cup'),
    ('champions league', 'UEFA Champions League'),
    ('ucl', 'UEFA Champions League'),
    ('europa league', 'UEFA Europa League'),
    ('uel', 'UEFA Europa League'),
    ('conference league', 'UEFA Conference League'),
    ('uecl', 'UEFA Conference League'),
]

EUROPEAN = {
    'UEFA Champions League',
    'UEFA Europa League',
    'UEFA Conference League',
}

# Explicitly excluded even though a source may list them on the same page.
EXCLUDED = [
    'premier league', 'community shield', 'friendly', 'club world cup',
    'super cup', 'championship', 'league one', 'league two', 'efl trophy',
]

DEFAULT_SOURCE = 'https://www.bbc.co.uk/sport/football/teams/{slug}/scores-fixtures'


def normalise_competition(raw):
    """
    Map a source's competition label to a canonical name.

    Returns None when the competition is out of scope -- which includes the
    Premier League, since PL occurrence already lives in matches_data.json and
    duplicating it here would double-count in any downstream reader.
    """
    if not raw:
        return None
    text = re.sub(r'\s+', ' ', raw).strip().lower()

    for token in EXCLUDED:
        if token in text:
            return None

    for token, canonical in COMPETITION_ALIASES:
        if token in text:
            return canonical
    return None


def parse_date(raw, season_start_year=None):
    """
    Parse a fixture date into ISO form.

    Handles the common source formats. When the year is absent it is inferred
    from the season: months Aug-Dec belong to season_start_year, Jan-Jul to the
    following year. Returns None if nothing parses -- an unparseable date is
    dropped rather than guessed, because a wrong date puts a cup tie in the
    wrong week and that is exactly the claim this data is used to make.
    """
    if not raw:
        return None
    text = re.sub(r'\s+', ' ', str(raw)).strip()
    # Strip a leading weekday and any ordinal suffixes: "Sat 3rd Jan" -> "3 Jan"
    text = re.sub(r'^[A-Za-z]{3,9},?\s+', '', text)
    text = re.sub(r'(\d+)(st|nd|rd|th)\b', r'\1', text, flags=re.I)

    for fmt in ('%d %B %Y', '%d %b %Y', '%Y-%m-%d', '%d/%m/%Y', '%d/%m/%y'):
        try:
            return datetime.strptime(text, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass

    if season_start_year:
        for fmt in ('%d %B', '%d %b'):
            try:
                parsed = datetime.strptime(text, fmt)
            except ValueError:
                continue
            year = season_start_year if parsed.month >= 8 else season_start_year + 1
            return parsed.replace(year=year).strftime('%Y-%m-%d')
    return None


def parse_fixture_rows(rows, team, season_start_year=None, standardise=None):
    """
    Turn (date_text, competition_text, opponent_text) triples into records.

    Pure function -- the testable core. Out-of-scope competitions and
    unparseable dates are dropped. Output carries exactly three fields per
    fixture: date, competition, opponent.
    """
    standardise = standardise or (lambda name: name)
    out = []
    seen = set()

    for date_text, comp_text, opponent_text in rows:
        competition = normalise_competition(comp_text)
        if competition is None:
            continue

        date = parse_date(date_text, season_start_year)
        if date is None:
            continue

        opponent = re.sub(r'\s+', ' ', (opponent_text or '')).strip()
        # Drop any venue marker the source appends; venue is out of scope.
        opponent = re.sub(r'\s*\((?:H|A|N)\)\s*$', '', opponent, flags=re.I).strip()
        if not opponent or opponent.lower() == team.lower():
            continue
        opponent = standardise(opponent)

        key = (date, competition, opponent)
        if key in seen:
            continue
        seen.add(key)

        out.append({
            'date': date,
            'competition': competition,
            'opponent': opponent,
        })

    out.sort(key=lambda f: (f['date'], f['competition'], f['opponent']))
    return out


def is_european(competition):
    return competition in EUROPEAN


# --- Selenium layer -------------------------------------------------------

def _team_slug(team):
    return re.sub(r'[^a-z0-9]+', '-', team.lower()).strip('-')


def _extract_rows(driver):
    """
    Pull (date, competition, opponent) triples off a team fixture page.

    Reads by structure and label rather than absolute position: fixture blocks
    are grouped under a competition heading, with a date heading above each
    group of ties.
    """
    from selenium.webdriver.common.by import By

    triples = []
    for block in driver.find_elements(
            By.XPATH, "//*[self::li or self::article or self::div]"
                      "[.//*[contains(@class,'fixture') or contains(@class,'Fixture')]]"):
        text = (block.text or '').strip()
        if not text:
            continue
        parts = [p.strip() for p in text.split('\n') if p.strip()]
        if len(parts) < 3:
            continue
        # Heuristic layout: a date line, a competition line, and team names.
        date_line = next((p for p in parts if re.search(r'\d', p)), None)
        comp_line = next((p for p in parts if normalise_competition(p)), None)
        if not date_line or not comp_line:
            continue
        opponents = [p for p in parts if p not in (date_line, comp_line)]
        if not opponents:
            continue
        triples.append((date_line, comp_line, opponents[-1]))
    return triples


def scrape_team(driver, team, source_template=None, season_start_year=None,
                standardise=None):
    """Scrape one team's in-scope cup/European fixtures. Never raises."""
    from scrapers.browser import accept_cookies

    template = source_template or os.environ.get('FIXTURE_SOURCE_URL', DEFAULT_SOURCE)
    url = template.format(slug=_team_slug(team), team=team)

    try:
        driver.get(url)
        accept_cookies(driver)
        rows = _extract_rows(driver)
        return parse_fixture_rows(rows, team, season_start_year, standardise), None
    except Exception as exc:
        return [], f'{type(exc).__name__}: {exc}'


def load_manual_overrides(path):
    """Hand-maintained fixtures, merged over anything scraped."""
    path = Path(path)
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return data.get('teams', data)


def merge(scraped, manual, team, standardise=None):
    """Union of scraped and manual fixtures, deduped on the three fields."""
    combined = list(scraped)
    seen = {(f['date'], f['competition'], f['opponent']) for f in scraped}
    for entry in manual.get(team, []):
        competition = normalise_competition(entry.get('competition')) \
            or entry.get('competition')
        date = parse_date(entry.get('date')) or entry.get('date')
        opponent = entry.get('opponent')
        if not (date and competition and opponent):
            continue
        if standardise:
            opponent = standardise(opponent)
        key = (date, competition, opponent)
        if key in seen:
            continue
        seen.add(key)
        combined.append({'date': date, 'competition': competition,
                         'opponent': opponent})
    combined.sort(key=lambda f: (f['date'], f['competition'], f['opponent']))
    return combined


def build(teams, data_dir='data', season_start_year=None, dry_run=False,
          standardise=None):
    """
    Scrape every team and write data/cup_fixtures.json.

    Returns the payload. Teams that fail are reported in `errors` rather than
    aborting the run -- a single broken page must not cost the whole set.
    """
    data_dir = Path(data_dir)
    manual = load_manual_overrides(data_dir / 'cup_fixtures_manual.json')

    payload = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'scope': 'date, competition, opponent -- narrative context only',
        'competitions': ['FA Cup', 'Carabao Cup'] + sorted(EUROPEAN),
        'teams': {},
        'errors': {},
    }

    driver = None
    try:
        for team in teams:
            fixtures, error = [], None
            if not dry_run:
                if driver is None:
                    from scrapers.browser import make_driver
                    driver = make_driver()
                fixtures, error = scrape_team(
                    driver, team, season_start_year=season_start_year,
                    standardise=standardise)
            merged = merge(fixtures, manual, team, standardise)
            payload['teams'][team] = merged
            if error:
                payload['errors'][team] = error
            print(f'  {team:<18} {len(merged):>2} fixture(s)'
                  + (f'   [{error}]' if error else ''))
    finally:
        if driver is not None:
            driver.quit()

    out = data_dir / 'cup_fixtures.json'
    out.write_text(json.dumps(payload, indent=2))
    print(f'\n  written to {out}')
    return payload


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--data', default='data')
    ap.add_argument('--season-start-year', type=int, default=None,
                    help='year the season began, for dates printed without one')
    ap.add_argument('--dry-run', action='store_true',
                    help='skip the browser; emit manual overrides only')
    ap.add_argument('--teams', nargs='*', default=None)
    args = ap.parse_args()

    teams = args.teams
    if not teams:
        elo_path = Path(args.data) / 'current_elo.json'
        teams = sorted(json.loads(elo_path.read_text()).keys())

    season = args.season_start_year or (
        datetime.now().year if datetime.now().month >= 8 else datetime.now().year - 1)

    print(f'Cup/European fixture occurrence -- {len(teams)} teams, season {season}/'
          f'{str(season + 1)[-2:]}')
    build(teams, args.data, season, args.dry_run)


if __name__ == '__main__':
    main()
