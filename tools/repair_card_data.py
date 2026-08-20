"""
Purge fabricated zero-card records left by the broken advanced-stats scrape.

Background
----------
scrape_matches() clicked the Stats tab and then never extracted anything --
the body of the block was the comment `# Need to add stats XPATHS`. The card
fields were initialised to 0 and written out unchanged, so every match the
scraper produced recorded 0 yellows, 0 reds and 0 booking points whether the
page loaded or not.

Those zeros are not data. They fed straight into:
  * referee averages   -- understated by 4 to 17 booking points
  * team booking form  -- 20 of 28 teams reading exactly 0 over their last 10
  * band avg_booking_points

Evidence the split is clean:
  * matches from the Excel import  (id < FIRST_SCRAPED_ID): 81 of 1825 are
    all-zero, i.e. 4.4% -- the rate you would expect for real cardless matches
  * matches from the scraper (id >= FIRST_SCRAPED_ID): 380 of 380 are all-zero

So every scraper-produced record is fabricated, and no imported record is.
This tool rewrites the former to null + stats_scraped=false, and stamps the
latter stats_scraped=true so provenance is explicit from here on.

Idempotent: re-running changes nothing. Run with --dry-run to preview.

    python3 tools/repair_card_data.py [--data DIR] [--dry-run]
"""

import argparse
import json
import shutil
from pathlib import Path

# First match_id produced by the Selenium scraper. Everything below this came
# from import_excel() and carries genuine card data.
FIRST_SCRAPED_ID = 2444838

CARD_FIELDS = ('home_yellow', 'away_yellow', 'home_red', 'away_red')


def is_fabricated(match):
    """A scraper-produced record whose card fields are the untouched zeros."""
    if match['match_id'] < FIRST_SCRAPED_ID:
        return False
    if any(match.get(f) is None for f in CARD_FIELDS):
        return False  # already repaired
    return all((match.get(f) or 0) == 0 for f in CARD_FIELDS)


def repair(matches):
    stats = {'nulled': 0, 'confirmed_real': 0, 'already_done': 0,
             'scraped_with_real_cards': 0}

    for m in matches:
        if m['match_id'] < FIRST_SCRAPED_ID:
            if m.get('stats_scraped') is not True:
                m['stats_scraped'] = True
                stats['confirmed_real'] += 1
            else:
                stats['already_done'] += 1
            continue

        if is_fabricated(m):
            for f in CARD_FIELDS:
                m[f] = None
            m['home_booking_points'] = None
            m['away_booking_points'] = None
            m['total_booking_points'] = None
            m['stats_scraped'] = False
            m.setdefault('advanced_stats', None)
            stats['nulled'] += 1
        elif all(m.get(f) is None for f in CARD_FIELDS):
            m['stats_scraped'] = False
            stats['already_done'] += 1
        else:
            # Genuinely scraped card data (post-fix runs).
            m['stats_scraped'] = True
            stats['scraped_with_real_cards'] += 1

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--data', default='data')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    path = Path(args.data) / 'matches_data.json'
    matches = json.loads(path.read_text())

    before = sum(1 for m in matches if m.get('total_booking_points') == 0)
    stats = repair(matches)
    after = sum(1 for m in matches if m.get('total_booking_points') == 0)

    print(f'matches                       : {len(matches)}')
    print(f'fabricated zeros nulled       : {stats["nulled"]}')
    print(f'imported records confirmed    : {stats["confirmed_real"]}')
    print(f'scraped with real card data   : {stats["scraped_with_real_cards"]}')
    print(f'already repaired (no change)  : {stats["already_done"]}')
    print(f'records reading 0 booking pts : {before} -> {after}')

    if args.dry_run:
        print('\n--dry-run: nothing written')
        return

    backup = path.with_suffix('.json.bak')
    shutil.copy(path, backup)
    path.write_text(json.dumps(matches, indent=2))
    print(f'\nwritten. backup at {backup}')


if __name__ == '__main__':
    main()
