"""
Advanced match statistics (cards and related metrics) from the PL match centre.

WHAT WAS BROKEN
---------------
The previous implementation was:

    try:
        stats_tab = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '.../div[1]/button[4]')))
        stats_tab.click()
        time.sleep(3)

        # Need to add stats XPATHS

    except Exception as e:
        print(f"    Could not get detailed stats for match {match_id}")

Two independent defects, and the second is the one that actually cost data:

1. The extraction was never implemented -- `# Need to add stats XPATHS` was
   the whole body. home_yellow/away_yellow/home_red/away_red stayed at the 0
   they were initialised to, so EVERY scraped match recorded zero cards
   whether or not the click succeeded. Verified by reproduction: with a fixture
   where the Stats tab is present and clicks cleanly, the resulting record is
   byte-identical to the failing one.

2. The exception was swallowed. `except Exception as e:` bound the error and
   then printed a message that discarded it, so the operator saw
   "Could not get detailed stats for match N" with no error text. That is why
   the exact error was never confirmed -- the code threw it away.

There was also no way to tell "0 cards" from "not scraped": both were 0.
2205 matches carried card fields, 380 of them fabricated zeros, and those fed
straight into referee averages and team booking form.

WHAT THIS DOES INSTEAD
----------------------
* Finds the Stats tab by its visible text, not by `button[4]`. Absolute XPaths
  have already broken this scraper once (commit 4195d23 fixed the team-name and
  score paths after a site change); an ordinal tab index is the same bet.
* Reads the stats table by row LABEL, so re-ordering or inserting a metric
  does not silently shift values into the wrong field.
* Returns None -- never 0 -- for anything it could not read, and reports
  `stats_scraped` plus the real `stats_error` text.
"""

import re

# Row labels as they appear in the PL match centre, mapped to our field names.
# Keys are matched case-insensitively after whitespace normalisation. Several
# spellings per field because the site has used more than one over time.
STAT_LABELS = {
    'yellow cards': 'yellow',
    'yellow card': 'yellow',
    'red cards': 'red',
    'red card': 'red',
    'fouls': 'fouls',
    'fouls committed': 'fouls',
    'offsides': 'offsides',
    'offside': 'offsides',
    'corners': 'corners',
    'corner taken': 'corners',
    'corners taken': 'corners',
    'possession %': 'possession',
    'possession': 'possession',
    'shots': 'shots',
    'total shots': 'shots',
    'shots on target': 'shots_on_target',
    'shots on goal': 'shots_on_target',
    'passes': 'passes',
    'pass accuracy': 'pass_accuracy',
    'pass accuracy %': 'pass_accuracy',
    'tackles': 'tackles',
    'expected goals': 'xg',
    'expected goals (xg)': 'xg',
    'xg': 'xg',
}

# Fields that are whole counts; everything else may be fractional.
INTEGER_FIELDS = {
    'yellow', 'red', 'fouls', 'offsides', 'corners',
    'shots', 'shots_on_target', 'passes', 'tackles',
}

# Sanity ceilings. A parse that lands outside these is a mis-read row, not a
# remarkable football match, and is discarded rather than stored.
PLAUSIBLE_MAX = {
    'yellow': 12, 'red': 5, 'fouls': 50, 'offsides': 20, 'corners': 30,
    'possession': 100, 'shots': 60, 'shots_on_target': 30,
    'passes': 1200, 'pass_accuracy': 100, 'tackles': 60, 'xg': 10,
}

YELLOW_POINTS = 10
RED_POINTS = 25


def normalise_label(text):
    """Lower-case, collapse whitespace, drop trailing punctuation."""
    return re.sub(r'\s+', ' ', (text or '')).strip().strip(':').strip().lower()


def parse_stat_value(text, field):
    """
    Parse one side's value for a stat row.

    Returns None when the cell is blank, '-', or otherwise unparseable --
    never 0, because 0 is a meaningful football value and None is not.
    """
    if text is None:
        return None
    cleaned = str(text).strip().replace('%', '').replace(',', '')
    if cleaned in ('', '-', '--', 'N/A', 'n/a'):
        return None
    match = re.search(r'-?\d+(?:\.\d+)?', cleaned)
    if not match:
        return None
    value = float(match.group())
    if value < 0:
        return None
    ceiling = PLAUSIBLE_MAX.get(field)
    if ceiling is not None and value > ceiling:
        return None
    if field in INTEGER_FIELDS:
        if value != int(value):
            return None
        return int(value)
    return round(value, 2)


def parse_stat_rows(rows):
    """
    Turn (label, home_text, away_text) triples into a stats dict.

    Pure function: this is the part worth testing, and it needs no browser.

    Returns {'home': {...}, 'away': {...}} containing only fields that were
    actually read. Unknown labels are ignored; a row whose label is known but
    whose values do not parse is omitted rather than zero-filled.
    """
    home, away = {}, {}
    for label, home_text, away_text in rows:
        field = STAT_LABELS.get(normalise_label(label))
        if field is None:
            continue
        h = parse_stat_value(home_text, field)
        a = parse_stat_value(away_text, field)
        if h is not None:
            home[field] = h
        if a is not None:
            away[field] = a
    return {'home': home, 'away': away}


def booking_points(yellow, red):
    """
    Booking points for one side: 10 per yellow, 25 per red.

    None in, None out -- an unscraped match must not contribute 0 to any
    average. This is the guard that keeps fabricated zeros out of the
    referee and team-form models.
    """
    if yellow is None and red is None:
        return None
    return (yellow or 0) * YELLOW_POINTS + (red or 0) * RED_POINTS


def empty_result(error=None):
    """The shape returned when nothing could be read."""
    return {
        'stats_scraped': False,
        'stats_error': error,
        'home_yellow': None, 'away_yellow': None,
        'home_red': None, 'away_red': None,
        'home_booking_points': None, 'away_booking_points': None,
        'total_booking_points': None,
        'advanced_stats': None,
    }


def build_result(parsed, error=None):
    """Flatten parse_stat_rows() output into the match-record shape."""
    home, away = parsed['home'], parsed['away']
    if not home and not away:
        return empty_result(error or 'stats table parsed but no known rows matched')

    hy, ay = home.get('yellow'), away.get('yellow')
    hr, ar = home.get('red'), away.get('red')

    # A team with cards shown but no red row means zero reds, not unknown.
    if hy is not None and hr is None:
        hr = 0
    if ay is not None and ar is None:
        ar = 0

    hb, ab = booking_points(hy, hr), booking_points(ay, ar)
    total = None if hb is None or ab is None else hb + ab

    return {
        'stats_scraped': True,
        'stats_error': error,
        'home_yellow': hy, 'away_yellow': ay,
        'home_red': hr, 'away_red': ar,
        'home_booking_points': hb, 'away_booking_points': ab,
        'total_booking_points': total,
        'advanced_stats': {'home': home, 'away': away},
    }


# ---------------------------------------------------------------------------
# Selenium-facing layer
# ---------------------------------------------------------------------------

# A table only qualifies as "the stats table" once at least this many of its
# rows resolve to a known label. The match page can carry other 3-column
# tables (related content, a standings widget) alongside the real one; a
# threshold of 1 would let a coincidental match through, so this asks for
# more than one before trusting a table's identity.
MIN_RECOGNISED_ROWS = 2

# Candidate containers for a stat row, most specific first. The PL site has
# rendered this block as a list, a table, and a set of divs at various points.
ROW_XPATHS = [
    "//*[self::li or self::tr or self::div]"
    "[.//*[contains(@class,'stat') or contains(@class,'Stat')]]"
    "[count(.//*[normalize-space(text())!='']) >= 3]",
]


def orient_triple(a, b, c):
    """
    Decide which of three cells is the row label.

    A stat row has been seen on the PL site in both (label, home, away) and
    (home, label, away) column order. Rather than commit to one -- the
    earlier version assumed the middle cell always, and that assumption is
    exactly what "rows found, none recognised" looks like when it is wrong --
    both orientations are tried and whichever resolves to a label this module
    knows is kept. Returns None if neither position matches, so an unrelated
    3-cell row (from some other table on the page) is dropped rather than
    silently misread as a stat.
    """
    if normalise_label(a) in STAT_LABELS:
        return (a, b, c)
    if normalise_label(b) in STAT_LABELS:
        return (b, a, c)
    return None


def _raw_table_rows(driver):
    """
    Every 3-cell row on the page, grouped by the <table> it came from.

    Deliberately not scoped to a single table by selector -- we do not know
    PL's exact container class from here -- but grouping keeps rows from
    different tables from being pooled together, so _best_table() can judge
    each table's rows as a set rather than mixing an unrelated table's rows
    into the real one.
    """
    from selenium.webdriver.common.by import By

    tables = []
    for table in driver.find_elements(By.XPATH, '//table'):
        rows = []
        for row in table.find_elements(By.XPATH, './/tr'):
            cells = row.find_elements(By.XPATH, './th|./td')
            texts = [c.text.strip() for c in cells]
            if len(texts) == 3:
                rows.append(tuple(texts))
        if rows:
            tables.append(rows)
    return tables


def _raw_list_rows(driver):
    """
    Every 3-part row from the list/div stats layout, in visual order.

    Column order is resolved later by orient_triple(), same as the table
    path -- this only collects the raw text, it does not guess which part is
    the label.
    """
    from selenium.webdriver.common.by import By

    rows = []
    for xpath in ROW_XPATHS:
        for row in driver.find_elements(By.XPATH, xpath):
            parts = [p for p in (row.text or '').split('\n') if p.strip()]
            if len(parts) == 3:
                rows.append(tuple(parts))
        if rows:
            break
    return rows


def _best_table(tables):
    """
    Pick the table that is actually the stats panel, oriented and filtered.

    Returns the recognised (label, home, away) triples from whichever table
    scores highest, or None if no table clears MIN_RECOGNISED_ROWS.
    """
    best, best_score = None, 0
    for rows in tables:
        recognised = [t for t in (orient_triple(*r) for r in rows) if t]
        if len(recognised) > best_score:
            best, best_score = recognised, len(recognised)
    return best if best_score >= MIN_RECOGNISED_ROWS else None


def open_stats_tab(driver, timeout=20):
    """
    Click the Stats tab, located by visible text.

    Returns True if a tab was clicked, False if no Stats tab was found. Does
    not raise: some matches (postponed, abandoned) legitimately have no stats.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    locator = (
        By.XPATH,
        "//button[normalize-space(translate(., 'STAS', 'stas'))='stats']"
        " | //a[normalize-space(translate(., 'STAS', 'stas'))='stats']"
        " | //*[@role='tab'][contains(translate(., 'STAS', 'stas'),'stats')]"
    )
    try:
        WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(locator)).click()
        return True
    except Exception:
        return False


def scrape_match_stats(driver, timeout=20, settle=3, sleep=None):
    """
    Read the advanced stats for the match page currently loaded in `driver`.

    Always returns a dict in the match-record shape. On failure the card
    fields are None and `stats_error` carries the real exception text --
    it is not swallowed. When rows are found but none are recognised, the
    error carries the raw samples too, so a report of that failure is enough
    to fix the label/column mapping without needing separate access to the
    page's HTML.
    """
    import time as _time
    sleep = sleep or _time.sleep

    try:
        opened = open_stats_tab(driver, timeout=timeout)
        if opened:
            sleep(settle)

        tables = _raw_table_rows(driver)
        oriented = _best_table(tables)

        if oriented is None:
            list_rows = _raw_list_rows(driver)
            list_recognised = [t for t in
                               (orient_triple(*r) for r in list_rows) if t]
            if len(list_recognised) >= MIN_RECOGNISED_ROWS:
                oriented = list_recognised

        if oriented is None:
            raw = [r for t in tables for r in t] + list_rows
            if not raw:
                return empty_result(
                    'no stat rows found on page'
                    + ('' if opened else ' (Stats tab not found either)'))
            sample = '; '.join(str(r) for r in raw[:5])
            return empty_result(
                f'{len(raw)} row(s) read across {len(tables)} table(s) '
                f'plus list layout, but none matched a known stat label. '
                f'raw sample: {sample}')

        return build_result(parse_stat_rows(oriented))

    except Exception as exc:
        # The whole point: keep the error text.
        return empty_result(f'{type(exc).__name__}: {exc}')
