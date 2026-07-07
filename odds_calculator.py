"""
Premier League Odds Calculator - Complete Pipeline

Flow:  facts in -> one deterministic derive -> JSON out.

- matches_data.json stores immutable match FACTS only (teams, date, goals,
  cards, referee, xg, possession). The scraper and the Excel importer write
  facts and nothing else.
- rebuild.py derives everything ELO (pre-match dual ratings, bands, winner
  labels, venue multipliers, current ratings, history) from one chronological
  replay on every run. No code path bakes a default rating into a stored match.
- This module then adds the presentation stats (referee/team/h2h/Poisson) and
  fetches bookmaker odds for upcoming fixtures.

Secrets: the-odds-api keys are read from the ODDS_API_KEYS (comma-separated)
or ODDS_API_KEY environment variable - never hardcoded.
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from scipy import stats

from elo_calculator import calculate_form_metrics
from rebuild import rebuild

# ============================================
# CONFIGURATION
# ============================================

# Team name mappings for standardization
TEAM_NAME_CHANGES = [
    ['Tottenham Hotspur', 'Spurs'],
    ['Tottenham', 'Spurs'],
    ['Manchester United', 'Man Utd'],
    ['Man United', 'Man Utd'],
    ['Manchester City', 'Man City'],
    ['West Ham United', 'West Ham'],
    ['Wolverhampton Wanderers', 'Wolves'],
    ['Leicester City', 'Leicester'],
    ['Brighton and Hove Albion', 'Brighton'],
    ['Newcastle United', 'Newcastle'],
    ['Nottingham Forest', "Nott'm Forest"],
    ['Forest', "Nott'm Forest"],
    ['Ipswich Town', 'Ipswich'],
    ['Leeds United', 'Leeds']
]


def get_api_keys():
    """the-odds-api keys from the environment. Never hardcode these."""
    raw = os.environ.get('ODDS_API_KEYS') or os.environ.get('ODDS_API_KEY') or ''
    return [k.strip() for k in raw.split(',') if k.strip()]


# ============================================
# MAIN CALCULATOR CLASS
# ============================================

class OddsCalculator:
    def __init__(self, data_dir='data'):
        """Initialize the calculator with data directory"""
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # facts (matches_data.json) and derived state (filled by rebuild_derived)
        self.matches_data = []
        self.matches_derived = []
        self.elo_bands = []
        self.current_elo = {}          # {team: driver elo}
        self.current_elo_full = {}     # {team: {elo, rank, long_*, rolling_*}}
        self.elo_history = {}
        self.venue_adjustment = {}

        self.load_existing_data()

    def load_existing_data(self):
        """Load existing facts if available"""
        matches_file = self.data_dir / 'matches_data.json'
        if matches_file.exists():
            with open(matches_file, 'r') as f:
                self.matches_data = json.load(f)
            print(f"Loaded {len(self.matches_data)} existing matches")

    def standardize_team_name(self, team_name):
        """Standardize team names for consistency"""
        if pd.isna(team_name):
            return None
        team_str = str(team_name)
        for old_name, new_name in TEAM_NAME_CHANGES:
            if old_name in team_str:
                return new_name
        return team_str

    # ============================================
    # DERIVE (the single source of every ELO number)
    # ============================================

    def rebuild_derived(self):
        """Replay the facts chronologically and refresh all derived state.
        Writes matches_derived.json, elo_bands.json, current_elo.json,
        elo_history.json and venue_adjustment.json."""
        derived, bands, current, venue = rebuild(self.data_dir)
        self.matches_derived = derived
        self.elo_bands = bands
        self.current_elo_full = current
        self.current_elo = {team: v['elo'] for team, v in current.items()}
        self.venue_adjustment = venue
        with open(self.data_dir / 'elo_history.json') as f:
            self.elo_history = json.load(f)

        print("\n  Current ELO Rankings:")
        for team, v in list(current.items())[:10]:
            print(f"    {v['rank']:2d}. {team:15s} {v['elo']}  "
                  f"(rolling {v['rolling_elo']})")

    # ============================================
    # IMPORT FROM EXCEL (one-time historical load; facts only)
    # ============================================

    def import_excel(self, filepath):
        """
        One-time import of historical data from Excel/CSV.
        Records match FACTS only - ratings are always derived by rebuild().
        """
        print(f"\nImporting historical data from {filepath}...")

        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)

        print(f"Found {len(df)} matches in file")

        df = df.rename(columns={
            'Home Yellow': 'home_yellow',
            'Away Yellow': 'away_yellow',
            'Home Red': 'home_red',
            'Away Red': 'away_red',
            'Referee': 'referee',
            'Home Possession': 'home_possession',
            'Away Posession': 'away_possession',  # also fixes source typo
            'Home XG': 'home_xg',
            'Away XG': 'away_xg'
        })

        def opt(row, col, cast):
            return cast(row[col]) if col in row and pd.notna(row[col]) else None

        imported_count = 0
        for _, row in df.iterrows():
            try:
                match_data = {
                    'match_id': int(row.get('ID', row.get('match_id', imported_count + 1))),
                    # source dates are d/m/Y - be explicit or a 05/10 kickoff
                    # silently becomes 10 May (the bug repair_facts.py cleaned up)
                    'date': pd.to_datetime(row['Date'], dayfirst=True).strftime('%Y-%m-%d'),
                    'home_team': self.standardize_team_name(row['Home_Team']),
                    'away_team': self.standardize_team_name(row['Away_Team']),
                    'home_goals': int(row['Home_Goals']) if pd.notna(row['Home_Goals']) else 0,
                    'away_goals': int(row['Away_Goals']) if pd.notna(row['Away_Goals']) else 0,
                    'home_yellow': opt(row, 'home_yellow', int) or 0,
                    'away_yellow': opt(row, 'away_yellow', int) or 0,
                    'home_red': opt(row, 'home_red', int) or 0,
                    'away_red': opt(row, 'away_red', int) or 0,
                    'referee': opt(row, 'referee', str),
                    'home_xg': opt(row, 'home_xg', float),
                    'away_xg': opt(row, 'away_xg', float),
                    'home_possession': opt(row, 'home_possession', float),
                    'away_possession': opt(row, 'away_possession', float),
                }
                self.matches_data.append(match_data)
                imported_count += 1
            except Exception as e:
                print(f"  Error importing row {_}: {e}")
                continue

        print(f"  ✓ Imported {imported_count} matches successfully")
        self.save_matches_data()
        return imported_count

    # ============================================
    # MATCH SCRAPING (facts only)
    # ============================================

    def scrape_matches(self, first_match_id, last_match_id):
        """Scrape new matches from the Premier League website.

        Known fragility: absolute XPaths against premierleague.com - a site
        redesign silently kills a matchweek. Replacing this with
        football-data.co.uk CSV ingestion is roadmap item B1.
        """
        # Selenium only loads when scraping is actually requested, so the
        # derive/backtest paths run in environments without a browser.
        from selenium import webdriver
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.firefox.service import Service as FirefoxService
        from selenium.webdriver.common.by import By
        from selenium.webdriver.firefox.options import Options
        from webdriver_manager.firefox import GeckoDriverManager

        firefox_options = Options()
        firefox_options.add_argument("--headless")
        service = FirefoxService(GeckoDriverManager().install())

        print(f"\nScraping matches {first_match_id} to {last_match_id}...")

        new_matches = []

        for match_id in range(first_match_id, last_match_id + 1):
            if any(m['match_id'] == match_id for m in self.matches_data):
                print(f"  Match {match_id} already exists, skipping...")
                continue

            url = f'https://www.premierleague.com/en/match/{match_id}'
            driver = webdriver.Firefox(service=service, options=firefox_options)

            try:
                driver.get(url)
                driver.maximize_window()
                time.sleep(5)

                # Accept cookies if present
                try:
                    WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'))
                    ).click()
                except Exception:
                    pass

                # Get teams
                home_team = driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[1]/span').text
                away_team = driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[3]/span').text

                home_team = self.standardize_team_name(home_team)
                away_team = self.standardize_team_name(away_team)

                # Get date
                date_elem = driver.find_element(By.XPATH, '/html/body/main//div[1]/div[2]/div[2]/div[1]/div/div/div/section/div[1]/div[2]/span[1]')
                date_text = date_elem.text.strip()
                try:
                    match_date = datetime.strptime(date_text, '%a %d %b %Y')
                except ValueError:
                    # Year missing from the page. Assume the season that is
                    # currently running: Aug-Dec matches carry the season's
                    # starting year, Jan-Jul the following year. (The old
                    # "current year" fallback stamped 30 Dec fixtures a year
                    # late when scraped after New Year.)
                    now = datetime.now()
                    partial = datetime.strptime(date_text, '%a %d %b')
                    season_start_year = now.year if now.month >= 8 else now.year - 1
                    year = season_start_year if partial.month >= 8 else season_start_year + 1
                    match_date = partial.replace(year=year)

                # Get scores
                home_goals = int(driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[2]/div/span[1]').text)
                away_goals = int(driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[2]/div/span[3]').text)

                # Get referee
                try:
                    referee = driver.find_element(By.XPATH, '/html/body/main//div[1]/div[2]/div[2]/div[1]/div/div/div/section/div[1]/div[2]/div/span').text
                except Exception:
                    referee = None

                # Get cards
                home_yellow = 0
                away_yellow = 0
                home_red = 0
                away_red = 0

                try:
                    # Click on stats tab
                    stats_tab = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[2]/div/div/div/div/div/div[1]/button[4]'))
                    )
                    stats_tab.click()
                    time.sleep(3)

                    # Need to add stats XPATHS

                except Exception:
                    print(f"    Could not get detailed stats for match {match_id}")

                # FACTS only - every ELO number is derived later by rebuild()
                match_data = {
                    'match_id': match_id,
                    'date': match_date.strftime('%Y-%m-%d'),
                    'home_team': home_team,
                    'away_team': away_team,
                    'home_goals': home_goals,
                    'away_goals': away_goals,
                    'home_yellow': home_yellow,
                    'away_yellow': away_yellow,
                    'home_red': home_red,
                    'away_red': away_red,
                    'referee': referee,
                    'home_xg': None,
                    'away_xg': None,
                    'home_possession': None,
                    'away_possession': None,
                }

                self.matches_data.append(match_data)
                new_matches.append(match_data)

                print(f"  ✓ Match {match_id}: {home_team} {home_goals}-{away_goals} {away_team}")

            except Exception as e:
                print(f"  ✗ Error scraping match {match_id}: {e}")

            finally:
                driver.quit()

        print(f"  ✓ Scraped {len(new_matches)} new matches")

        if new_matches:
            self.save_matches_data()

        return new_matches

    # ============================================
    # BOOKMAKER ODDS
    # ============================================

    def fetch_bookmaker_odds(self):
        """Fetch odds from the-odds-api.com"""
        print("\nFetching bookmaker odds...")

        keys = get_api_keys()
        if not keys:
            print("  ✗ No API key. Set ODDS_API_KEYS (comma-separated) or "
                  "ODDS_API_KEY in the environment / Actions secrets.")
            return []
        API_KEY = keys[0]
        base_url = 'https://api.the-odds-api.com/v4/sports/soccer_epl/odds'

        upcoming_fixtures = []

        try:
            # First get the list of games with H2H odds
            h2h_params = {
                'apiKey': API_KEY,
                'regions': 'uk',
                'markets': 'h2h',
                'oddsFormat': 'decimal'
            }

            h2h_response = requests.get(base_url, params=h2h_params)
            print(f"  API requests remaining: {h2h_response.headers.get('X-Requests-Remaining')}")

            if h2h_response.status_code != 200:
                print(f"  ✗ Error fetching H2H odds: {h2h_response.status_code}")
                return []

            games = h2h_response.json()[:10]  # First 10 games

            # Process each game
            for game in games:
                fixture = {
                    'home_team': self.standardize_team_name(game['home_team']),
                    'away_team': self.standardize_team_name(game['away_team']),
                    'date': datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00')).strftime('%Y-%m-%d'),
                    'time': datetime.fromisoformat(game['commence_time'].replace('Z', '+00:00')).strftime('%H:%M'),
                    'game_id': game['id']
                }

                # Extract H2H odds from initial response
                home_odds = []
                draw_odds = []
                away_odds = []

                for bookmaker in game.get('bookmakers', []):
                    for market in bookmaker['markets']:
                        if market['key'] == 'h2h':
                            for outcome in market['outcomes']:
                                if outcome['name'] == game['home_team']:
                                    home_odds.append(outcome['price'])
                                elif outcome['name'] == game['away_team']:
                                    away_odds.append(outcome['price'])
                                elif outcome['name'] == 'Draw':
                                    draw_odds.append(outcome['price'])

                # Now fetch ALL additional markets for this specific game
                event_url = f'https://api.the-odds-api.com/v4/sports/soccer_epl/events/{game["id"]}/odds'
                event_params = {
                    'apiKey': API_KEY,
                    'regions': 'uk',
                    'markets': 'totals,alternate_totals,btts',
                    'oddsFormat': 'decimal'
                }

                event_response = requests.get(event_url, params=event_params)
                print(f"  Fetching markets for {fixture['home_team']} vs {fixture['away_team']}")
                print(f"    API requests remaining: {event_response.headers.get('X-Requests-Remaining')}")

                # Initialize all odds dictionaries
                goals_odds = {
                    'over_05': [], 'under_05': [],
                    'over_15': [], 'under_15': [],
                    'over_25': [], 'under_25': [],
                    'over_35': [], 'under_35': [],
                    'over_45': [], 'under_45': []
                }
                btts_yes_odds = []
                btts_no_odds = []

                if event_response.status_code == 200:
                    event_data = event_response.json()

                    for bookmaker in event_data.get('bookmakers', []):
                        for market in bookmaker['markets']:
                            # Process totals and alternate_totals
                            if market['key'] in ['totals', 'alternate_totals']:
                                for outcome in market['outcomes']:
                                    point = float(outcome['point'])
                                    name = outcome['name'].lower()  # 'Over' or 'Under'
                                    price = outcome['price']

                                    # Map to our standard lines
                                    if point == 0.5:
                                        key = f"{name}_05"
                                    elif point == 1.5:
                                        key = f"{name}_15"
                                    elif point == 2.5:
                                        key = f"{name}_25"
                                    elif point == 3.5:
                                        key = f"{name}_35"
                                    elif point == 4.5:
                                        key = f"{name}_45"
                                    else:
                                        continue  # Skip non-standard lines

                                    if key in goals_odds:
                                        goals_odds[key].append(price)

                            # Process BTTS
                            elif market['key'] == 'btts':
                                for outcome in market['outcomes']:
                                    if outcome['name'] == 'Yes':
                                        btts_yes_odds.append(outcome['price'])
                                    elif outcome['name'] == 'No':
                                        btts_no_odds.append(outcome['price'])

                # Calculate averages, using None if no data available
                def avg(vals):
                    return round(np.mean(vals), 2) if vals else None

                bookmaker_odds = {
                    'home': avg(home_odds),
                    'draw': avg(draw_odds),
                    'away': avg(away_odds),
                    'btts_yes': avg(btts_yes_odds),
                    'btts_no': avg(btts_no_odds),
                }
                for key, vals in goals_odds.items():
                    bookmaker_odds[key] = avg(vals)

                fixture['bookmaker_odds'] = bookmaker_odds
                upcoming_fixtures.append(fixture)

                # Add a small delay to be respectful to the API
                time.sleep(0.5)

            print(f"  ✓ Fetched odds for {len(upcoming_fixtures)} fixtures")

        except Exception as e:
            print(f"  ✗ Error fetching odds: {e}")
            import traceback
            traceback.print_exc()

        return upcoming_fixtures

    # ============================================
    # CALCULATIONS
    # ============================================

    def get_band_probabilities(self, band_number):
        """Stronger/draw/weaker probabilities for a band (from the rebuilt
        tables; band data is stronger-vs-weaker, not home-vs-away)."""
        for band in self.elo_bands:
            if band['band'] == band_number:
                return {
                    'stronger_win': band['stronger_win_pct'],
                    'draw': band['draw_pct'],
                    'weaker_win': band['weaker_win_pct']
                }
        return {'stronger_win': 0.333, 'draw': 0.333, 'weaker_win': 0.334}

    def calculate_referee_stats(self):
        """Calculate statistics for each referee"""
        referee_data = {}

        for match in self.matches_derived:
            ref = match.get('referee')
            if ref and ref != 'None':
                if ref not in referee_data:
                    referee_data[ref] = {
                        'games': 0,
                        'total_booking_points': 0,
                        'total_yellows': 0,
                        'total_reds': 0
                    }

                referee_data[ref]['games'] += 1
                referee_data[ref]['total_booking_points'] += match.get('total_booking_points', 0)
                referee_data[ref]['total_yellows'] += match.get('home_yellow', 0) + match.get('away_yellow', 0)
                referee_data[ref]['total_reds'] += match.get('home_red', 0) + match.get('away_red', 0)

        # Calculate averages
        for ref, data in referee_data.items():
            if data['games'] > 0:
                data['avg_booking_points'] = round(data['total_booking_points'] / data['games'], 1)
                data['avg_yellows'] = round(data['total_yellows'] / data['games'], 1)
                data['avg_reds'] = round(data['total_reds'] / data['games'], 2)
                # Remove totals from final output
                del data['total_booking_points']
                del data['total_yellows']
                del data['total_reds']

        return referee_data

    def calculate_team_stats(self):
        """Calculate statistics for each team using elo_history.json for form."""
        team_data = {}

        for team in self.current_elo.keys():
            # Get last 10 matches for this team
            team_matches = []
            for match in reversed(self.matches_derived):
                if match['home_team'] == team or match['away_team'] == team:
                    team_matches.append(match)
                    if len(team_matches) >= 10:
                        break

            if team_matches:
                goals_for = []
                goals_against = []
                booking_points = []

                for match in team_matches:
                    if match['home_team'] == team:
                        goals_for.append(match['home_goals'])
                        goals_against.append(match['away_goals'])
                    else:
                        goals_for.append(match['away_goals'])
                        goals_against.append(match['home_goals'])

                    if match.get('total_booking_points') is not None:
                        booking_points.append(match['total_booking_points'])

                # Calculate form metrics from elo_history
                form = calculate_form_metrics(team, self.elo_history, num_matches=10)

                # Calculate season average booking points
                season_booking = [
                    m['total_booking_points']
                    for m in self.matches_derived
                    if (m['home_team'] == team or m['away_team'] == team)
                    and m.get('total_booking_points') is not None
                ]

                team_data[team] = {
                    'last_10_avg_goals_for': round(np.mean(goals_for), 1),
                    'last_10_avg_goals_against': round(np.mean(goals_against), 1),
                    'last_10_avg_booking_points': round(np.mean(booking_points), 1) if booking_points else 0,
                    'season_avg_booking_points': round(np.mean(season_booking), 1) if season_booking else 0,
                    'form': form
                }

        return team_data

    def calculate_h2h_records(self):
        """Calculate head-to-head records between all teams"""
        h2h_data = {}

        # Get all unique team pairs
        teams = list(self.current_elo.keys())

        for i, team1 in enumerate(teams):
            for team2 in teams[i+1:]:
                # Sort alphabetically to ensure consistency
                if team1 < team2:
                    key = f"{team1}_{team2}"
                    t1, t2 = team1, team2
                else:
                    key = f"{team2}_{team1}"
                    t1, t2 = team2, team1

                # Find all matches between these teams
                h2h_matches = [m for m in self.matches_data
                             if {m['home_team'], m['away_team']} == {team1, team2}]

                if h2h_matches:
                    t1_wins = 0
                    t2_wins = 0
                    draws = 0

                    for match in h2h_matches:
                        if match['home_goals'] > match['away_goals']:
                            if match['home_team'] == t1:
                                t1_wins += 1
                            else:
                                t2_wins += 1
                        elif match['home_goals'] < match['away_goals']:
                            if match['away_team'] == t1:
                                t1_wins += 1
                            else:
                                t2_wins += 1
                        else:
                            draws += 1

                    # Get last match
                    last_match = sorted(h2h_matches, key=lambda x: x['date'])[-1]
                    last_result = f"{last_match['home_team']} {last_match['home_goals']}-{last_match['away_goals']} {last_match['away_team']}"

                    h2h_data[key] = {
                        'total_games': len(h2h_matches),
                        f'{t1}_wins': t1_wins,
                        f'{t2}_wins': t2_wins,
                        'draws': draws,
                        'last_result': last_result,
                        'last_date': last_match['date']
                    }

        return h2h_data

    def calculate_poisson(self, home_team, away_team):
        """Calculate Poisson distribution for a match"""
        # Get team stats
        team_stats = self.calculate_team_stats()

        if home_team not in team_stats or away_team not in team_stats:
            return {'matrix': []}

        home_data = team_stats[home_team]
        away_data = team_stats[away_team]

        # League average goals
        all_goals = [m['home_goals'] + m['away_goals'] for m in self.matches_data]
        league_avg = np.mean(all_goals) / 2 if all_goals else 1.3

        # Calculate expected goals with home advantage
        home_attack = home_data['last_10_avg_goals_for'] / league_avg
        home_defense = home_data['last_10_avg_goals_against'] / league_avg
        away_attack = away_data['last_10_avg_goals_for'] / league_avg
        away_defense = away_data['last_10_avg_goals_against'] / league_avg

        home_expected = home_attack * away_defense * league_avg * 1.1
        away_expected = away_attack * home_defense * league_avg * 0.9

        # Generate Poisson matrix (0-5 goals each)
        matrix = []
        for i in range(6):
            row = []
            for j in range(6):
                prob = stats.poisson.pmf(i, home_expected) * stats.poisson.pmf(j, away_expected)
                row.append(round(prob, 4))
            matrix.append(row)

        return {'matrix': matrix}

    def get_expected_booking_points(self, home_team, away_team, elo_band):
        """Get expected booking points for a fixture"""
        # Get band average
        band_avg = 40  # Default
        for band in self.elo_bands:
            if band['band'] == elo_band:
                band_avg = band['avg_booking_points']
                break

        # Get team averages
        team_stats = self.calculate_team_stats()
        home_avg = team_stats.get(home_team, {}).get('last_10_avg_booking_points', 40)
        away_avg = team_stats.get(away_team, {}).get('last_10_avg_booking_points', 40)

        # Weight: 50% band, 25% each team
        expected = (band_avg * 0.5) + (home_avg * 0.25) + (away_avg * 0.25)

        return round(expected, 1)

    # ============================================
    # SAVE JSON FILES
    # ============================================

    def save_matches_data(self):
        """Save match facts to JSON"""
        self.matches_data.sort(key=lambda m: (m['date'], m['match_id']))
        with open(self.data_dir / 'matches_data.json', 'w') as f:
            json.dump(self.matches_data, f, indent=2)

    def generate_all_json_files(self):
        """Generate all JSON files for the website"""
        print("\nGenerating JSON files...")

        # 1. Facts
        self.save_matches_data()
        print("  ✓ matches_data.json")

        # 2. The derive step writes matches_derived.json, elo_bands.json,
        #    current_elo.json, elo_history.json and venue_adjustment.json
        self.rebuild_derived()
        print("  ✓ matches_derived.json, elo_bands.json, current_elo.json, "
              "elo_history.json, venue_adjustment.json (rebuilt)")

        # 3. Upcoming fixtures (only overwrite when the fetch produced data,
        #    so a missing API key doesn't wipe the last good file)
        fixtures = self.fetch_bookmaker_odds()
        if fixtures:
            with open(self.data_dir / 'upcoming_fixtures.json', 'w') as f:
                json.dump(fixtures, f, indent=2, default=str)
            print("  ✓ upcoming_fixtures.json")
        else:
            print("  - upcoming_fixtures.json left unchanged (no odds fetched)")

        # 4. Referee stats
        referee_stats = self.calculate_referee_stats()
        with open(self.data_dir / 'referee_stats.json', 'w') as f:
            json.dump(referee_stats, f, indent=2)
        print("  ✓ referee_stats.json")

        # 5. Team stats
        team_stats = self.calculate_team_stats()
        with open(self.data_dir / 'team_stats.json', 'w') as f:
            json.dump(team_stats, f, indent=2)
        print("  ✓ team_stats.json")

        # 6. H2H records
        h2h_records = self.calculate_h2h_records()
        with open(self.data_dir / 'h2h_records.json', 'w') as f:
            json.dump(h2h_records, f, indent=2)
        print("  ✓ h2h_records.json")

        print(f"\nAll JSON files saved to {self.data_dir}/")

        # Display summary statistics
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)
        print(f"Total matches: {len(self.matches_data)}")
        print(f"Teams tracked: {len(self.current_elo)}")
        print(f"Referees tracked: {len(referee_stats)}")
        print(f"Upcoming fixtures: {len(fixtures)}")

        return True

# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Ferret Stack odds pipeline')
    parser.add_argument('--skip-scrape', action='store_true',
                        help='rebuild + regenerate JSONs without scraping')
    args = parser.parse_args()

    calc = OddsCalculator()

    if not args.skip_scrape:
        # Auto-calculate match IDs based on last scraped match
        scraped_ids = [m['match_id'] for m in calc.matches_data
                       if m['match_id'] >= 1_000_000]
        if scraped_ids:
            first_match_id = max(scraped_ids) + 1
        else:
            first_match_id = 2561994  # Fallback for first run

        last_match_id = first_match_id + 9  # 10 matches per matchweek

        print(f"Auto-detected: Scraping matches {first_match_id} to {last_match_id}")
        calc.scrape_matches(first_match_id, last_match_id)

    calc.generate_all_json_files()

    print("\n✅ Complete! Push files to GitHub to update your website.")
