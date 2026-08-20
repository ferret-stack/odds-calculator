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

# A table -- or, for the list layout, the whole candidate set -- only
# qualifies as "the stats block" once at least this many of its rows resolve
# to a known label. The match page can carry other 3-column/3-part groupings
# (related content, a standings widget, the scoreline itself) alongside the
# real one; a threshold of 1 would let a coincidental match through, so this
# asks for more than one before trusting an identification.
MIN_RECOGNISED_ROWS = 2


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
    Every 3-part text group in the page, from list/div-based layouts.

    NOT scoped by CSS class. The previous version only looked at elements
    whose class contained the substring 'stat'/'Stat', as a guess that this
    marked the stats block -- and that guess collided with unrelated
    elements. A class like "MatchStatus" contains "Stat" as a literal
    substring, and on a page where the real stats content had not rendered
    (0 <table> candidates, in the report that found this), the scoreline's
    status container was the only element left matching, producing the raw
    sample ('3', '-', '0') -- the score, not a stat.

    Filtering is left entirely to recognition instead, the same way
    _best_table() already handles the <table> layout: collect every
    plausible 3-part group structurally, and let orient_triple() plus
    MIN_RECOGNISED_ROWS decide what is real. A lone scoreline or nav item
    does not resolve to a known label and is dropped; a genuine stats panel,
    with its 8-10 metrics, clears the threshold easily.
    """
    from selenium.webdriver.common.by import By

    rows = []
    for el in driver.find_elements(
            By.XPATH,
            "//*[self::li or self::div]"
            "[count(.//*[normalize-space(text())!='']) = 3]"):
        parts = [p for p in (el.text or '').split('\n') if p.strip()]
        if len(parts) == 3:
            rows.append(tuple(parts))
    return rows


def _wait_for_stats_content(driver, timeout, poll_interval=0.5, sleep=None,
                            probe=None):
    """
    Poll briefly for candidate rows to actually be in the DOM.

    A fixed sleep after the tab click is a guess about render timing, and a
    report where 0 tables and only a scoreline were found is consistent with
    the real panel not having rendered yet when the fixed sleep ran out. This
    returns as soon as either a table or a list-style candidate appears, or
    gives up silently at `timeout` -- a genuinely stats-less page (postponed
    match) is not turned into a hang, it just uses the full budget once.

    `probe` is injectable so the polling loop itself -- stop early once
    ready, otherwise use the full budget -- can be unit tested without
    building a fake Selenium element tree; production code always uses the
    default, which does the real DOM check.
    """
    import time as _time
    sleep = sleep or _time.sleep
    probe = probe or (
        lambda: bool(_raw_table_rows(driver)) or bool(_raw_list_rows(driver)))

    elapsed = 0.0
    while elapsed < timeout:
        if probe():
            return True
        sleep(poll_interval)
        elapsed += poll_interval
    return probe()


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

    Returns (opened, note). `opened` is True only if the click actually
    happened. `note` distinguishes WHY it did not, which matters for
    diagnosing a failure: the tab was never in the DOM at all (the locator is
    wrong, or the page genuinely has no stats -- e.g. a postponed match)
    versus the tab being present but never clickable, most often because
    something else is covering it. An undismissed cookie banner is exactly
    that: it points at accept_cookies() as the real fix, not at this
    function's locator.

    Never raises: a stats-less match must not stop the rest of the scrape.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        ElementClickInterceptedException, TimeoutException,
    )

    locator = (
        By.XPATH,
        "//button[normalize-space(translate(., 'STAS', 'stas'))='stats']"
        " | //a[normalize-space(translate(., 'STAS', 'stas'))='stats']"
        " | //*[@role='tab'][contains(translate(., 'STAS', 'stas'),'stats')]"
    )

    try:
        element = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable(locator))
    except TimeoutException:
        try:
            driver.find_element(*locator)
            return False, ('tab present but never became clickable '
                           '(hidden, or covered by something -- an '
                           'undismissed cookie banner is a common cause)')
        except Exception:
            return False, 'tab not found in the DOM'
    except Exception as exc:
        return False, f'{type(exc).__name__} while locating the tab'

    try:
        element.click()
        return True, None
    except ElementClickInterceptedException:
        # "Covered by another element" is only half a diagnosis, and the half
        # that does not identify the element is the half that cost several
        # rounds of guessing on the cookie banner. Ask the page which element
        # is actually on top at the point the click was aimed at, and name it.
        note = ('tab located but the click was intercepted -- it is '
                'covered by another element')
        try:
            from scrapers.browser import probe_click_target
            info = probe_click_target(driver, element)
        except Exception:
            info = None
        if info and info.get('blocker'):
            note += f'; the element on top at its centre is {info["blocker"]}'
        else:
            note += ', e.g. an undismissed cookie banner'
        return False, note
    except Exception as exc:
        return False, f'{type(exc).__name__} while clicking the tab'


def _diagnose_no_match(tab_status, tables, list_rows):
    """
    Build the failure message when nothing recognisable was found.

    Pure function over already-collected data -- kept separate from
    scrape_match_stats() so the exact text a live failure produces (which is
    the whole diagnostic interface: whoever hits this pastes it back) can be
    tested with plain Python lists and strings, without building a fake
    Selenium element tree to drive it.

    `tab_status` is a human-readable string, not a bool -- open_stats_tab()
    now distinguishes "not found" from "found but blocked" (an undismissed
    cookie banner is the common cause of the latter), and that distinction is
    exactly the information a failure report needs to carry.
    """
    raw = [r for t in tables for r in t] + list_rows
    if not raw:
        return f'no stat rows found on page ({tab_status})'
    sample = '; '.join(str(r) for r in raw[:5])
    return (
        f'{tab_status}; {len(raw)} row(s) read across {len(tables)} table(s) '
        f'plus list layout, but none matched a known stat label. '
        f'raw sample: {sample}')


def scrape_match_stats(driver, timeout=20, settle=5, sleep=None):
    """
    Read the advanced stats for the match page currently loaded in `driver`.

    Always returns a dict in the match-record shape. On failure the card
    fields are None and `stats_error` carries the real exception text --
    it is not swallowed. When rows are found but none are recognised, the
    error carries whether the tab was opened, the raw row counts, and up to 5
    raw samples, so a report of that failure is enough to fix from without
    needing separate access to the page's HTML.

    `settle` is a ceiling on how long to wait for stat content to actually
    appear after the tab click, not a fixed sleep -- a fixed sleep is a guess
    about render timing that a slower page or connection can simply outrun.
    See _wait_for_stats_content().

    Also re-checks for the cookie consent banner immediately before clicking
    the tab. Some CMPs re-render their banner on a client-side route or tab
    change even after an earlier dismissal on page load, and that would look
    exactly like "Stats tab not found" or "click intercepted" here despite
    accept_cookies() having already run once. cookie_banner_present() asks
    whether a banner is VISIBLE, not merely present in the markup -- an
    existence check would answer True forever, since every CMP dismisses its
    banner by hiding it and leaving the markup behind, and this re-check would
    then fire on every match after a perfectly successful dismissal.
    """
    import time as _time
    sleep = sleep or _time.sleep

    try:
        from scrapers.browser import accept_cookies, cookie_banner_present
        if cookie_banner_present(driver):
            accept_cookies(driver, timeout=2)

        opened, tab_note = open_stats_tab(driver, timeout=timeout)
        _wait_for_stats_content(driver, settle, sleep=sleep)

        tables = _raw_table_rows(driver)
        oriented = _best_table(tables)

        list_rows = []
        if oriented is None:
            list_rows = _raw_list_rows(driver)
            list_recognised = [t for t in
                               (orient_triple(*r) for r in list_rows) if t]
            if len(list_recognised) >= MIN_RECOGNISED_ROWS:
                oriented = list_recognised

        if oriented is None:
            tab_status = 'tab opened' if opened else (tab_note or 'Stats tab not found')
            return empty_result(_diagnose_no_match(tab_status, tables, list_rows))

        return build_result(parse_stat_rows(oriented))

    except Exception as exc:
        # The whole point: keep the error text.
        return empty_result(f'{type(exc).__name__}: {exc}')
