"""
Premier League Odds Calculator - Complete Pipeline
Handles data scraping, processing, and JSON generation for GitHub Pages
"""

import os
import numpy as np
import json
import requests
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from collections import defaultdict
from elo_calculator import (
    ELOCalculator,
    calculate_form_metrics,
    calculate_fair_odds,
    calculate_home_advantage_multipliers,
    determine_winner
)

# pandas is only needed for the one-off Excel/CSV historical import. Import it
# lazily so the ELO/stats pipeline can run without pandas installed.
try:
    import pandas as pd
except ImportError:
    pd = None

# Selenium is only needed for live scraping. Import it lazily so the pipeline
# (rebuild ELO, compute stats, fetch odds via the HTTP API) can run in
# environments without a browser/driver installed.
try:
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.firefox.options import Options
    from webdriver_manager.firefox import GeckoDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# ============================================
# CONFIGURATION
# ============================================

# API Keys for Odds — read from the ODDS_API_KEYS env var (comma-separated).
# NEVER hard-code keys: the previously committed keys are exposed in git
# history and should be rotated at the-odds-api.com.
API_KEYS = [k.strip() for k in os.environ.get('ODDS_API_KEYS', '').split(',') if k.strip()]

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

# Teams for ELO API (without spaces)
TEAMS_FOR_ELO_API = [
    'Arsenal', 'AstonVilla', 'Bournemouth', 'Brentford', 'Brighton',
    'Southampton', 'Chelsea', 'CrystalPalace', 'Everton', 'Forest',
    'Fulham', 'Liverpool', 'Leicester', 'ManCity', 'ManUnited',
    'Newcastle', 'Ipswich', 'Tottenham', 'WestHam', 'Wolves'
]

# ============================================
# STATS-PANEL (CARDS) XPATHS
# ============================================
# Absolute XPATHs into the Premier League match "Stats" tab. The only path
# confirmed against the live site is home_red (supplied from Matchweek 1 last
# season). The other three are derived from it by convention:
#   - the away value uses div[3] where home uses div[1] (home | separator | away)
#   - yellow cards sit one stat-row above red cards (div[7] vs div[8])
# These derived indices MUST be confirmed on the first live run — see the
# debug dump in scrape_matches (set CARD_XPATH_DEBUG=1) and adjust if the
# extracted numbers look wrong.
_STATS_BASE = '/html/body/main/div[1]/div[2]/div[2]/div[2]/section/div/div/div/div/div'
CARD_XPATHS = {
    # Confirmed:
    'home_red':    f'{_STATS_BASE}[8]/div[2]/div[1]/div[1]',
    # Derived (verify on first live run):
    'away_red':    f'{_STATS_BASE}[8]/div[2]/div[3]/div[1]',
    'home_yellow': f'{_STATS_BASE}[7]/div[2]/div[1]/div[1]',
    'away_yellow': f'{_STATS_BASE}[7]/div[2]/div[3]/div[1]',
}
# The parent of the stat rows, used for the optional debug dump.
CARD_STATS_CONTAINER_XPATH = '/html/body/main/div[1]/div[2]/div[2]/div[2]/section/div/div/div/div'

# ============================================
# MAIN CALCULATOR CLASS
# ============================================

class OddsCalculator:
    def __init__(self, data_dir='data'):
        """Initialize the calculator with data directory"""
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize data storage
        self.matches_data = []
        self.elo_history = defaultdict(list)
        self.current_elo = {}
        self.referee_stats = {}
        self.team_stats = {}
        self.h2h_records = {}
        
        # Load existing data if available
        self.load_existing_data()

        # Selenium is set up lazily on first scrape (see _ensure_driver_setup)
        # so the rest of the pipeline runs without a browser installed.
        self.firefox_options = None
        self.service = None

    def _ensure_driver_setup(self):
        """Prepare the Firefox driver/service on demand (scraping only)."""
        if not SELENIUM_AVAILABLE:
            raise RuntimeError(
                "Selenium/webdriver-manager are not installed; scraping is "
                "unavailable. Install requirements.txt to enable scraping."
            )
        if self.service is None:
            self.firefox_options = Options()
            self.firefox_options.add_argument("--headless")
            self.service = FirefoxService(GeckoDriverManager().install())
    
    def load_existing_data(self):
        """Load existing JSON data if available"""
        matches_file = self.data_dir / 'matches_data.json'
        if matches_file.exists():
            with open(matches_file, 'r') as f:
                self.matches_data = json.load(f)
            print(f"Loaded {len(self.matches_data)} existing matches")

        # Load current ELO so that scraping records real PRE-match ratings
        # instead of defaulting every new match to 1500 (the old Band 1 bug).
        current_elo_file = self.data_dir / 'current_elo.json'
        if current_elo_file.exists():
            with open(current_elo_file, 'r') as f:
                data = json.load(f)
            for team, value in data.items():
                self.current_elo[team] = value['elo'] if isinstance(value, dict) else value
            print(f"Loaded current ELO for {len(self.current_elo)} teams")
    
    def standardize_team_name(self, team_name):
        """Standardize team names for consistency"""
        if team_name is None or (pd is not None and pd.isna(team_name)):
            return None
        team_str = str(team_name)
        for old_name, new_name in TEAM_NAME_CHANGES:
            if old_name in team_str:
                return new_name
        return team_str
    
    # ============================================
    # IMPORT FROM EXCEL
    # ============================================
    
    
    # ============================================
    # ELO SCRAPING
    # ============================================
    
    def rebuild_elo(self):
        """
        Deterministically rebuild ALL ELO ratings and history from match
        results, every run.

        Why a full rebuild rather than an incremental update:
        - Each match record stores the PRE-match ELO of both teams (the rating
          that actually predicted the game). The previous incremental version
          stored POST-match ratings, mixing perspectives and biasing the ELO
          band tables that the whole model is built on.
        - It removes the fragile "is this match newer than the last history
          entry?" detection, which could silently skip matches (e.g. when a
          stray future-dated record poisoned the comparison) and leave them at
          the 1500 default.

        With only a couple of thousand matches this is fast and reproducible.
        """
        print("\nRebuilding ELO ratings from scratch...")

        calc = ELOCalculator(k_factor=20, home_advantage=100, use_mov=True)

        # Chronological order is essential for a correct rebuild.
        self.matches_data.sort(key=lambda m: m['date'])

        for match in self.matches_data:
            home_team = match['home_team']
            away_team = match['away_team']

            # PRE-match ratings: what each team was rated going INTO this game.
            home_elo = calc.get_current_elo(home_team)
            away_elo = calc.get_current_elo(away_team)
            elo_diff = abs(home_elo - away_elo)

            match['home_elo'] = round(home_elo)
            match['away_elo'] = round(away_elo)
            match['elo_diff'] = round(elo_diff)
            match['elo_band'] = min(int(elo_diff // 50) + 1, 10)
            match['winner'] = determine_winner(
                match['home_goals'], match['away_goals'], home_elo, away_elo
            )

            # Advance ratings/history using this result.
            calc.process_match(
                home_team=home_team,
                away_team=away_team,
                home_goals=match['home_goals'],
                away_goals=match['away_goals'],
                match_date=match['date']
            )

        # Store results
        self.current_elo = calc.current_elo
        self.elo_history = calc.elo_history

        print(f"  Processed {len(self.matches_data)} matches")
        print("\n  Current ELO Rankings:")
        for team, elo, rank in calc.get_rankings()[:10]:
            print(f"    {rank:2d}. {team:15s} {elo}")

        return calc
    
    # ============================================
    # MATCH SCRAPING
    # ============================================
    
    def scrape_matches(self, first_match_id, last_match_id):
        """Scrape new matches from Premier League website"""
        print(f"\nScraping matches {first_match_id} to {last_match_id}...")

        self._ensure_driver_setup()

        new_matches = []
        
        for match_id in range(first_match_id, last_match_id + 1):
            # Check if match already exists
            if any(m['match_id'] == match_id for m in self.matches_data):
                print(f"  Match {match_id} already exists, skipping...")
                continue
            
            url = f'https://www.premierleague.com/en/match/{match_id}'
            driver = webdriver.Firefox(service=self.service, options=self.firefox_options)
            
            try:
                driver.get(url)
                driver.maximize_window()
                time.sleep(5)
                
                # Accept cookies if present
                try:
                    WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="onetrust-accept-btn-handler"]'))
                    ).click()
                except:
                    pass

                # Get teams
                home_team = driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[1]/span').text
                away_team = driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[3]/span').text
                
                # Standardize team names
                home_team = self.standardize_team_name(home_team)
                away_team = self.standardize_team_name(away_team)
                
                # Get date
                date_elem = driver.find_element(By.XPATH, '/html/body/main//div[1]/div[2]/div[2]/div[1]/div/div/div/section/div[1]/div[2]/span[1]')
                date_text = date_elem.text.strip()
                try:
                    match_date = datetime.strptime(date_text, '%a %d %b %Y')
                except ValueError:
                    # No year on the page: infer the SEASON year rather than the
                    # calendar year. A PL season spans Aug (year N) to May
                    # (year N+1), so months Aug-Dec belong to the season's
                    # starting year and Jan-Jul to the following year. Using
                    # datetime.now().year blindly produced future-dated records
                    # (e.g. a December game stamped with next year's date).
                    partial = datetime.strptime(date_text, '%a %d %b')
                    today = datetime.now()
                    season_start_year = today.year if today.month >= 8 else today.year - 1
                    year = season_start_year if partial.month >= 8 else season_start_year + 1
                    match_date = partial.replace(year=year)
                
                # Get scores
                home_goals = int(driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[2]/div/span[1]').text)
                away_goals = int(driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[2]/div/span[3]').text)
                
                # Get referee
                try:
                    referee = driver.find_element(By.XPATH, '/html/body/main//div[1]/div[2]/div[2]/div[1]/div/div/div/section/div[1]/div[2]/div/span').text
                except:
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

                    # Optional: dump the stats container so the derived XPATH
                    # indices (away side, yellow-card row) can be confirmed on
                    # the live site. Enable with CARD_XPATH_DEBUG=1.
                    if os.environ.get('CARD_XPATH_DEBUG'):
                        try:
                            dump = driver.find_element(By.XPATH, CARD_STATS_CONTAINER_XPATH).text
                            print(f"    [stats dump match {match_id}]\n{dump}\n    [/stats dump]")
                        except Exception:
                            print(f"    [stats dump unavailable for match {match_id}]")

                    def _card_value(key):
                        """Read a single card count; 0 if the element is missing."""
                        try:
                            return int(driver.find_element(By.XPATH, CARD_XPATHS[key]).text.strip())
                        except Exception:
                            return 0

                    home_yellow = _card_value('home_yellow')
                    away_yellow = _card_value('away_yellow')
                    home_red = _card_value('home_red')
                    away_red = _card_value('away_red')

                except Exception as e:
                    print(f"    Could not get detailed stats for match {match_id}")
                
                # PRE-match ELO ratings (current_elo is loaded on init, so these
                # are real ratings, not the old 1500 default). rebuild_elo will
                # recompute these authoritatively afterwards regardless.
                home_elo = self.current_elo.get(home_team, 1500)
                away_elo = self.current_elo.get(away_team, 1500)
                elo_diff = abs(home_elo - away_elo)
                elo_band = min(int(elo_diff // 50) + 1, 10)

                # Determine winner relative to pre-match ELO (tie-safe)
                winner = determine_winner(home_goals, away_goals, home_elo, away_elo)
                
                # Calculate booking points
                home_booking = (home_yellow * 10) + (home_red * 25)
                away_booking = (away_yellow * 10) + (away_red * 25)
                total_booking = home_booking + away_booking
                
                # Create match data
                match_data = {
                    'match_id': match_id,
                    'date': match_date.strftime('%Y-%m-%d'),
                    'home_team': home_team,
                    'away_team': away_team,
                    'home_goals': home_goals,
                    'away_goals': away_goals,
                    'home_elo': round(home_elo),
                    'away_elo': round(away_elo),
                    'elo_diff': round(elo_diff),
                    'elo_band': elo_band,
                    'referee': referee,
                    'home_yellow': home_yellow,
                    'away_yellow': away_yellow,
                    'home_red': home_red,
                    'away_red': away_red,
                    'total_booking_points': total_booking,
                    'winner': winner,
                    'over_05': (home_goals + away_goals) > 0.5,
                    'over_15': (home_goals + away_goals) > 1.5,
                    'over_25': (home_goals + away_goals) > 2.5,
                    'over_35': (home_goals + away_goals) > 3.5,
                    'over_45': (home_goals + away_goals) > 4.5,
                    'btts': home_goals > 0 and away_goals > 0
                }
                
                self.matches_data.append(match_data)
                new_matches.append(match_data)
                
                print(f"  ✓ Match {match_id}: {home_team} {home_goals}-{away_goals} {away_team}")
                
            except Exception as e:
                print(f"  ✗ Error scraping match {match_id}: {e}")
            
            finally:
                driver.quit()
        
        print(f"  ✓ Scraped {len(new_matches)} new matches")
        
        # Save updated data
        if new_matches:
            self.save_matches_data()
        
        return new_matches
    
    # ============================================
    # BOOKMAKER ODDS
    # ============================================
    
    def fetch_bookmaker_odds(self):
        """Fetch odds from the-odds-api.com and attach model odds / value bets."""
        print("\nFetching bookmaker odds...")

        if not API_KEYS:
            print("  ✗ No API keys set. Provide ODDS_API_KEYS (comma-separated) "
                  "to fetch bookmaker odds. Skipping.")
            return []

        API_KEY = API_KEYS[0]
        base_url = 'https://api.the-odds-api.com/v4/sports/soccer_epl/odds'

        # ELO bands drive the model's fair odds; compute once for all fixtures.
        elo_bands = self.calculate_elo_bands()

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
                bookmaker_odds = {
                    'home': round(np.mean(home_odds), 2) if home_odds else None,
                    'draw': round(np.mean(draw_odds), 2) if draw_odds else None,
                    'away': round(np.mean(away_odds), 2) if away_odds else None,
                    'over_05': round(np.mean(goals_odds['over_05']), 2) if goals_odds['over_05'] else None,
                    'under_05': round(np.mean(goals_odds['under_05']), 2) if goals_odds['under_05'] else None,
                    'over_15': round(np.mean(goals_odds['over_15']), 2) if goals_odds['over_15'] else None,
                    'under_15': round(np.mean(goals_odds['under_15']), 2) if goals_odds['under_15'] else None,
                    'over_25': round(np.mean(goals_odds['over_25']), 2) if goals_odds['over_25'] else None,
                    'under_25': round(np.mean(goals_odds['under_25']), 2) if goals_odds['under_25'] else None,
                    'over_35': round(np.mean(goals_odds['over_35']), 2) if goals_odds['over_35'] else None,
                    'under_35': round(np.mean(goals_odds['under_35']), 2) if goals_odds['under_35'] else None,
                    'over_45': round(np.mean(goals_odds['over_45']), 2) if goals_odds['over_45'] else None,
                    'under_45': round(np.mean(goals_odds['under_45']), 2) if goals_odds['under_45'] else None,
                    'btts_yes': round(np.mean(btts_yes_odds), 2) if btts_yes_odds else None,
                    'btts_no': round(np.mean(btts_no_odds), 2) if btts_no_odds else None
                }
                
                fixture['bookmaker_odds'] = bookmaker_odds

                # Attach the model's fair odds + any value bets so the JSON
                # carries edges server-side (the website's +EV badges read
                # fixture.value_bets).
                self.add_model_odds(fixture, bookmaker_odds, elo_bands)

                upcoming_fixtures.append(fixture)

                # Add a small delay to be respectful to the API
                time.sleep(0.5)
            
            print(f"  ✓ Fetched odds for {len(upcoming_fixtures)} fixtures")
            
        except Exception as e:
            print(f"  ✗ Error fetching odds: {e}")
            import traceback
            traceback.print_exc()
        
        return upcoming_fixtures

    def add_model_odds(self, fixture, bookmaker_odds, elo_bands):
        """
        Compute the model's fair odds for a fixture and flag value bets.

        A value bet exists where the model's probability implies shorter fair
        odds than the bookmaker is offering, i.e. edge = model_prob * book_odds
        - 1 > 0. Edge is stored as a percentage to match the website's display
        and its +EV threshold slider.
        """
        home_elo = self.current_elo.get(fixture['home_team'])
        away_elo = self.current_elo.get(fixture['away_team'])

        # Can't model a team we've never rated.
        if home_elo is None or away_elo is None:
            fixture['model_odds'] = None
            fixture['value_bets'] = []
            return

        model = calculate_fair_odds(home_elo, away_elo, elo_bands)
        fixture['model_odds'] = {
            'home': model['home_win']['fair_odds'],
            'draw': model['draw']['fair_odds'],
            'away': model['away_win']['fair_odds'],
            'band': model['meta']['band'],
            'elo_diff': model['meta']['elo_diff']
        }

        value_bets = []
        market_map = {
            'home': ('home_win', 'Home Win'),
            'draw': ('draw', 'Draw'),
            'away': ('away_win', 'Away Win'),
        }
        for book_key, (model_key, label) in market_map.items():
            book_odds = bookmaker_odds.get(book_key)
            if not book_odds:
                continue
            prob = model[model_key]['probability']
            edge = round((prob * book_odds - 1) * 100, 1)
            if edge > 0:
                value_bets.append({
                    'market': label,
                    'model_prob': prob,
                    'model_odds': model[model_key]['fair_odds'],
                    'bookmaker_odds': book_odds,
                    'edge': edge
                })

        # Best value first.
        value_bets.sort(key=lambda b: b['edge'], reverse=True)
        fixture['value_bets'] = value_bets

    # ============================================
    # HISTORICAL IMPORT (one-off, manual)
    # ============================================
    def import_excel(self, filepath):

        if pd is None:
            raise RuntimeError("pandas is required for import_excel; install requirements.txt.")

        print(f"\nImporting historical data from {filepath}...")

        # Read file
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
        
        print(f"Found {len(df)} matches in file")
        
        # RENAME COLUMNS TO MATCH EXPECTED NAMES
        df = df.rename(columns={
            'Home_elo': 'Home_Elo',  # Fix capitalization
            'Away_elo': 'Away_Elo',  # Fix capitalization
            'dELO': 'd_elo',         # Match expected name
            'Home Yellow': 'home_yellow',
            'Away Yellow': 'away_yellow',
            'Home Red': 'home_red',
            'Away Red': 'away_red',
            'Referee': 'referee',
            'Home Possession': 'home_possession',
            'Away Posession': 'away_possession',  # Note: also fixes typo in "Posession"
            'Home XG': 'home_xg',
            'Away XG': 'away_xg'
        })
        
        imported_count = 0
        for _, row in df.iterrows():
            try:
                # Calculate total goals for boolean markets
                home_goals = int(row['Home_Goals']) if pd.notna(row['Home_Goals']) else 0
                away_goals = int(row['Away_Goals']) if pd.notna(row['Away_Goals']) else 0
                total_goals = home_goals + away_goals
                
                # Calculate booking points (10 for yellow, 25 for red)
                home_yellow = int(row['home_yellow']) if 'home_yellow' in row and pd.notna(row['home_yellow']) else 0
                away_yellow = int(row['away_yellow']) if 'away_yellow' in row and pd.notna(row['away_yellow']) else 0
                home_red = int(row['home_red']) if 'home_red' in row and pd.notna(row['home_red']) else 0
                away_red = int(row['away_red']) if 'away_red' in row and pd.notna(row['away_red']) else 0
                
                home_booking_points = (home_yellow * 10) + (home_red * 25)
                away_booking_points = (away_yellow * 10) + (away_red * 25)
                total_booking_points = home_booking_points + away_booking_points
                
                # Calculate ELO difference (absolute value)
                home_elo = float(row['Home_Elo']) if pd.notna(row['Home_Elo']) else None
                away_elo = float(row['Away_Elo']) if pd.notna(row['Away_Elo']) else None
                elo_diff = abs(home_elo - away_elo) if home_elo and away_elo else None
                
                # Determine ELO band (0-50 = Band 1, 51-100 = Band 2, etc.)
                elo_band = None
                if elo_diff is not None:
                    elo_band = min(int(elo_diff // 50) + 1, 10)  # Cap at band 10 for 450+
                
                # Determine winner
                if home_goals > away_goals:
                    if home_elo > away_elo:
                        winner = "stronger"
                    else:
                        winner = "weaker"
                elif away_goals > home_goals:
                    if away_elo > home_elo:
                        winner = "stronger"
                    else:
                        winner = "weaker"
                else:
                    winner = "draw"
                
                match_data = {
                    'match_id': int(row.get('ID', row.get('match_id', imported_count + 1))),
                    'date': pd.to_datetime(row['Date']).strftime('%Y-%m-%d'),
                    'home_team': self.standardize_team_name(row['Home_Team']),
                    'away_team': self.standardize_team_name(row['Away_Team']),
                    'home_goals': home_goals,
                    'away_goals': away_goals,
                    'home_elo': round(home_elo) if home_elo else None,
                    'away_elo': round(away_elo) if away_elo else None,
                    'elo_diff': round(elo_diff) if elo_diff else None,
                    'elo_band': elo_band,
                    'referee': row.get('referee', None) if pd.notna(row.get('referee')) else None,
                    'home_yellow': home_yellow,
                    'away_yellow': away_yellow,
                    'home_red': home_red,
                    'away_red': away_red,
                    'total_booking_points': total_booking_points,
                    'winner': winner,
                    
                    # Goal markets (boolean)
                    'over_05': total_goals > 0.5,
                    'over_15': total_goals > 1.5,
                    'over_25': total_goals > 2.5,
                    'over_35': total_goals > 3.5,
                    'over_45': total_goals > 4.5,
                    'btts': home_goals > 0 and away_goals > 0,
                    
                    # Optional stats (may not be in older data)
                    'home_xg': float(row['home_xg']) if 'home_xg' in row and pd.notna(row['home_xg']) else None,
                    'away_xg': float(row['away_xg']) if 'away_xg' in row and pd.notna(row['away_xg']) else None,
                    'home_possession': float(row['home_possession']) if 'home_possession' in row and pd.notna(row['home_possession']) else None,
                    'away_possession': float(row['away_possession']) if 'away_possession' in row and pd.notna(row['away_possession']) else None,
                }
                
                self.matches_data.append(match_data)
                imported_count += 1
                
                # Update ELO history
                if home_elo:
                    self.elo_history[match_data['home_team']].append({
                        'date': match_data['date'],
                        'elo': round(home_elo)
                    })
                if away_elo:
                    self.elo_history[match_data['away_team']].append({
                        'date': match_data['date'],
                        'elo': round(away_elo)
                    })
                    
            except Exception as e:
                print(f"  Error importing row {_}: {e}")
                continue
        
        print(f"  ✓ Imported {imported_count} matches successfully")
        
        # Save imported data
        self.save_matches_data()
        return imported_count
    
    # ============================================
    # CALCULATIONS
    # ============================================
    
    def calculate_elo_bands(self):
        """Calculate statistics for each ELO band"""
        bands_data = []
        
        for band in range(1, 11):  # Bands 1-10
            if band == 1:
                band_range = "0-50"
                min_diff = 0
                max_diff = 50
            elif band == 10:
                band_range = "450+"
                min_diff = 450
                max_diff = 10000
            else:
                min_val = (band - 1) * 50 + 1
                max_val = band * 50
                band_range = f"{min_val}-{max_val}"
                min_diff = min_val
                max_diff = max_val
            
            # Filter matches in this band
            band_matches = [m for m in self.matches_data 
                           if m.get('elo_band') == band]
            
            if band_matches:
                total = len(band_matches)
                
                # Calculate win/draw/loss percentages
                stronger_wins = sum(1 for m in band_matches if m.get('winner') == 'stronger')
                draws = sum(1 for m in band_matches if m.get('winner') == 'draw')
                weaker_wins = sum(1 for m in band_matches if m.get('winner') == 'weaker')
                
                # Calculate goal market percentages
                over_05 = sum(1 for m in band_matches if m.get('over_05', False))
                over_15 = sum(1 for m in band_matches if m.get('over_15', False))
                over_25 = sum(1 for m in band_matches if m.get('over_25', False))
                over_35 = sum(1 for m in band_matches if m.get('over_35', False))
                over_45 = sum(1 for m in band_matches if m.get('over_45', False))
                btts = sum(1 for m in band_matches if m.get('btts', False))
                
                # Calculate average booking points
                booking_points = [m['total_booking_points'] for m in band_matches 
                                 if m.get('total_booking_points') is not None]
                avg_booking = np.mean(booking_points) if booking_points else 0
                
                bands_data.append({
                    'band': band,
                    'range': band_range,
                    'total_games': total,
                    'stronger_win_pct': round(stronger_wins / total, 4),
                    'draw_pct': round(draws / total, 4),
                    'weaker_win_pct': round(weaker_wins / total, 4),
                    'avg_booking_points': round(avg_booking, 1),
                    'over_05_pct': round(over_05 / total, 4),
                    'over_15_pct': round(over_15 / total, 4),
                    'over_25_pct': round(over_25 / total, 4),
                    'over_35_pct': round(over_35 / total, 4),
                    'over_45_pct': round(over_45 / total, 4),
                    'btts_pct': round(btts / total, 4)
                })
            else:
                # No data for this band yet
                bands_data.append({
                    'band': band,
                    'range': band_range,
                    'total_games': 0,
                    'home_win_pct': 0.333,
                    'draw_pct': 0.333,
                    'away_win_pct': 0.334,
                    'avg_booking_points': 40,
                    'over_05_pct': 0.9,
                    'over_15_pct': 0.75,
                    'over_25_pct': 0.5,
                    'over_35_pct': 0.25,
                    'over_45_pct': 0.1,
                    'btts_pct': 0.5
                })
        
        return bands_data

    def calculate_referee_stats(self):
        """Calculate statistics for each referee"""
        referee_data = {}
        
        for match in self.matches_data:
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
        import numpy as np
        
        team_data = {}
        
        # Load elo_history for form calculations
        elo_history = dict(self.elo_history)
        
        for team in self.current_elo.keys():
            # Get last 10 matches for this team
            team_matches = []
            for match in reversed(self.matches_data):
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
                form = calculate_form_metrics(team, elo_history, num_matches=10)
                
                # Calculate season average booking points
                season_booking = [
                    m['total_booking_points'] 
                    for m in self.matches_data 
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

    # ============================================
    # SAVE JSON FILES
    # ============================================
    
    def save_matches_data(self):
        """Save matches data to JSON"""
        with open(self.data_dir / 'matches_data.json', 'w') as f:
            json.dump(self.matches_data, f, indent=2)
    
    def generate_all_json_files(self):
        """Generate all JSON files for the website"""
        print("\nGenerating JSON files...")
        
        # 1. Save matches data (already done in save_matches_data)
        self.save_matches_data()
        print("  ✓ matches_data.json")
        
        # 2. ELO bands
        bands = self.calculate_elo_bands()
        with open(self.data_dir / 'elo_bands.json', 'w') as f:
            json.dump(bands, f, indent=2)
        print("  ✓ elo_bands.json")
        
        # 3. ELO history - ensure it's a regular dict
        elo_history_dict = {
            team: sorted(history, key=lambda x: x['date'])
            for team, history in self.elo_history.items()
        }
        with open(self.data_dir / 'elo_history.json', 'w') as f:
            json.dump(elo_history_dict, f, indent=2)
        print("  ✓ elo_history.json")
        
        # 4. Current ELO - add rankings
        sorted_teams = sorted(self.current_elo.items(), key=lambda x: x[1], reverse=True)
        current_elo_with_rank = {}
        for rank, (team, elo) in enumerate(sorted_teams, 1):
            current_elo_with_rank[team] = {
                'elo': elo,
                'rank': rank
            }
        
        with open(self.data_dir / 'current_elo.json', 'w') as f:
            json.dump(current_elo_with_rank, f, indent=2)
        print("  ✓ current_elo.json")
        
        # 5. Upcoming fixtures
        fixtures = self.fetch_bookmaker_odds()
        with open(self.data_dir / 'upcoming_fixtures.json', 'w') as f:
            json.dump(fixtures, f, indent=2, default=str)
        print("  ✓ upcoming_fixtures.json")
        
        # 6. Referee stats
        referee_stats = self.calculate_referee_stats()
        with open(self.data_dir / 'referee_stats.json', 'w') as f:
            json.dump(referee_stats, f, indent=2)
        print("  ✓ referee_stats.json")
        
        # 7. Team stats
        team_stats = self.calculate_team_stats()
        with open(self.data_dir / 'team_stats.json', 'w') as f:
            json.dump(team_stats, f, indent=2)
        print("  ✓ team_stats.json")
        
        # 8. H2H records
        h2h_records = self.calculate_h2h_records()
        with open(self.data_dir / 'h2h_records.json', 'w') as f:
            json.dump(h2h_records, f, indent=2)
        print("  ✓ h2h_records.json")

        #9. Venue adjustment multipliers (NEW V6)
        venue_data = calculate_home_advantage_multipliers(self.matches_data)
        with open(self.data_dir / 'venue_adjustment.json', 'w') as f:
            json.dump(venue_data, f, indent=2)
        print("  ✓ venue_adjustment.json")
        
        print(f"\nAll JSON files saved to {self.data_dir}/")
        
        # Display summary statistics
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)
        print(f"Total matches: {len(self.matches_data)}")
        print(f"Teams tracked: {len(self.current_elo)}")
        print(f"Referees tracked: {len(referee_stats)}")
        print(f"Upcoming fixtures: {len(fixtures)}")
        
        # Show value bets if any
        value_bets_found = []
        for fixture in fixtures:
            if fixture.get('value_bets'):
                for bet in fixture['value_bets']:
                    if bet['edge'] > 5:  # 5% edge threshold
                        value_bets_found.append({
                            'match': f"{fixture['home_team']} vs {fixture['away_team']}",
                            'market': bet['market'],
                            'edge': bet['edge']
                        })
        
        if value_bets_found:
            print("\n📊 VALUE BETS FOUND (>5% edge):")
            for bet in value_bets_found:
                print(f"  {bet['match']}: {bet['market']} (+{bet['edge']}%)")
        
        return True

# ============================================
# MAIN EXECUTION
# ============================================

if __name__ == "__main__":
    calc = OddsCalculator()
    
    # 1. Auto-calculate match IDs based on last scraped match
    if calc.matches_data:
        last_scraped_id = max(m['match_id'] for m in calc.matches_data)
        first_match_id = last_scraped_id + 1
    else:
        first_match_id = 2561994  # Fallback for first run
    
    last_match_id = first_match_id + 9  # 10 matches per matchweek
    
    print(f"Auto-detected: Scraping matches {first_match_id} to {last_match_id}")
    calc.scrape_matches(first_match_id, last_match_id)
    
    # 2. Rebuild ELO ratings (pre-match ratings, deterministic, from scratch)
    calc.rebuild_elo()

    # 3. Generate all JSON files
    calc.generate_all_json_files()
    
    print("\n✅ Complete! Push files to GitHub to update your website.")