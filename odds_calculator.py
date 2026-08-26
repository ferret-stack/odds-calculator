"""
Premier League Odds Calculator - Complete Pipeline
Handles data scraping, processing, and JSON generation for GitHub Pages
"""

import pandas as pd
import numpy as np
import json
import requests
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from scipy import stats
from io import StringIO
from collections import defaultdict
from elo_calculator import (
    ELOCalculator,
    calculate_form_metrics,
    get_venue_adjusted_probabilities,
    calculate_fair_odds,
    calculate_home_advantage_multipliers,
    calculate_elo_bands,
    classify_winner,
    elo_band as elo_band_for_diff,
    league_baseline_elo,
)

# Selenium imports for scraping
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options

from scrapers.browser import make_driver, accept_cookies
from scrapers.match_stats import scrape_match_stats
from env_config import load_dotenv, odds_api_keys, request_with_key_failover

# ============================================
# CONFIGURATION
# ============================================

# API keys for the-odds-api.com now come from the environment, not this
# file -- see env_config.py. Copy .env.example to .env and fill in
# ODDS_API_KEY (and optionally ODDS_API_KEY_BACKUP). The three keys that
# used to be hardcoded here are public in this repo's git history; rotate
# them, they are not usable as a fallback going forward.
load_dotenv()

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
    ['Leeds United', 'Leeds'],

    # Promoted for 2026/27. These two map SHORT -> LONG, the opposite
    # direction to every rule above, because the seeded key is the long form:
    # tools/rebuild_elo.py writes 'Hull City' and 'Coventry City' into
    # current_elo.json verbatim (only 'Ipswich Town' is shortened there), and
    # pipeline.run_pipeline.price_fixture looks the rating up by exact key.
    #
    # Without these, a feed emitting 'Hull' or 'Coventry' gets None from
    # price_fixture and the fixture is dropped as "no ELO rating for one or
    # both teams" -- on a team that does have a rating. Matching on the bare
    # town name accepts every spelling the feed might use ('Hull',
    # 'Hull City', 'Hull City AFC') and canonicalises all of them to the one
    # key the ratings are stored under, so this does not depend on guessing
    # which form the feed emits. No other club name contains either token,
    # so neither rule can capture a different side.
    ['Hull', 'Hull City'],
    ['Coventry', 'Coventry City'],
]

# Teams for ELO API (without spaces)
TEAMS_FOR_ELO_API = [
    'Arsenal', 'AstonVilla', 'Bournemouth', 'Brentford', 'Brighton',
    'Southampton', 'Chelsea', 'CrystalPalace', 'Everton', 'Forest',
    'Fulham', 'Liverpool', 'Leicester', 'ManCity', 'ManUnited',
    'Newcastle', 'Ipswich', 'Tottenham', 'WestHam', 'Wolves'
]

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

        # Browser setup is deliberately NOT done here. It used to call
        # GeckoDriverManager().install() -- a network round-trip -- on every
        # construction, so the validation tools, the tests and the nightly
        # pipeline all needed a reachable driver CDN just to read JSON.
        # scrapers.browser.make_driver() resolves lazily, on first scrape.


    def load_existing_data(self):
        """
        Load existing JSON data if available.

        This previously loaded matches_data.json ONLY, leaving self.current_elo
        as an empty dict. scrape_matches() runs before update_elo_ratings() in
        the main pipeline and reads self.current_elo, so every freshly scraped
        match was stamped with the fallback rating on both sides -- not just
        genuinely unknown teams, but every team, every run. Loading the ratings
        here is the fix at source.
        """
        matches_file = self.data_dir / 'matches_data.json'
        if matches_file.exists():
            with open(matches_file, 'r') as f:
                self.matches_data = json.load(f)
            print(f"Loaded {len(self.matches_data)} existing matches")

        current_elo_file = self.data_dir / 'current_elo.json'
        if current_elo_file.exists():
            with open(current_elo_file, 'r') as f:
                data = json.load(f)
            # Accepts both {team: elo} and {team: {'elo': x, 'rank': y}}
            self.current_elo = {
                team: (value['elo'] if isinstance(value, dict) else value)
                for team, value in data.items()
            }
            print(f"Loaded ELO ratings for {len(self.current_elo)} teams")

        elo_history_file = self.data_dir / 'elo_history.json'
        if elo_history_file.exists():
            with open(elo_history_file, 'r') as f:
                for team, history in json.load(f).items():
                    self.elo_history[team] = history
            print(f"Loaded ELO history for {len(self.elo_history)} teams")

    def fallback_elo(self, team):
        """
        Rating to use for a team with no rating on record.

        Never a bare literal: 1500 is 284 points below league average on the
        displayed scale, and using it was what pushed phantom fixtures into
        Band 1. This tracks the live ratings instead, and says so out loud when
        it fires, because a fallback firing is a data problem worth seeing.
        """
        baseline = league_baseline_elo(self.current_elo)
        print(f"  ! No rating on record for '{team}' -- falling back to league "
              f"baseline {baseline}. Check team-name standardisation.")
        return baseline
    
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
    # IMPORT FROM EXCEL
    # ============================================
    
    
    # ============================================
    # ELO SCRAPING
    # ============================================
    
    def update_elo_ratings(self):
        """
        Update ELO ratings after processing new matches.
        Uses local calculation instead of ClubELO API.
        """
        print("\nUpdating ELO ratings...")
        
        # Initialize calculator
        calc = ELOCalculator(k_factor=20, home_advantage=100, use_mov=True)
        
        # Load current ELO as baseline
        current_elo_file = self.data_dir / 'current_elo.json'
        elo_history_file = self.data_dir / 'elo_history.json'
        
        if current_elo_file.exists():
            calc.load_current_elo(current_elo_file)
        
        if elo_history_file.exists():
            calc.load_elo_history(elo_history_file)
        
        # Find matches that need ELO processing.
        #
        # This used to select matches whose date was later than the team's last
        # elo_history entry. That test is not idempotent and it self-destructs:
        # a single match carrying a bad future date writes that date into
        # elo_history, after which every real match compares as "already
        # processed" and the update silently becomes a no-op forever. That is
        # exactly what had happened -- all 2205 matches were being skipped, so
        # the records stamped with the fallback rating were never repaired.
        #
        # An explicit per-match flag replaces it. It cannot be poisoned by one
        # bad date, and a match is processed exactly once.
        #
        # Ordered by match_id, not date: IDs are issued per matchweek in
        # fixture order, so ID order is true match order, and it does not
        # depend on the dates being right. The chain is order-dependent, so
        # replaying a match out of sequence applies its update against the
        # wrong prior ratings.
        new_matches = [m for m in self.matches_data if not m.get('elo_processed')]
        new_matches = sorted(new_matches, key=lambda x: x['match_id'])

        # A record can be unprocessed *and* scoreless -- e.g. a fixture that
        # was appended before full-time (or by some other path that never
        # got the goals fields written) sitting in matches_data.json with no
        # elo_processed flag. That is a data problem, not a reason to crash
        # the whole update and leave every other pending match unprocessed
        # again. Split it out, report it loudly (never a bare KeyError), and
        # replay only the matches that actually have a result.
        unscoreable = [m for m in new_matches
                       if 'home_goals' not in m or 'away_goals' not in m]
        if unscoreable:
            print(f"  ! Skipping {len(unscoreable)} unprocessed match(es) "
                  f"with no recorded score (not yet played, or an "
                  f"incomplete record):")
            for m in unscoreable:
                print(f"      match {m.get('match_id', '?')} "
                      f"({m.get('date', '?')}): "
                      f"{m.get('home_team', '?')} vs {m.get('away_team', '?')}")
            new_matches = [m for m in new_matches if m not in unscoreable]

        if new_matches:
            print(f"  Processing {len(new_matches)} new matches...")

            for match in new_matches:
                result = calc.process_match(
                    home_team=match['home_team'],
                    away_team=match['away_team'],
                    home_goals=match['home_goals'],
                    away_goals=match['away_goals'],
                    match_date=match['date']
                )

                # Stamp the match with its PRE-match ratings. Using post-match
                # ratings (as this did) makes band membership a function of the
                # result: a favourite's win widens the gap and pushes the match
                # up a band, an upset narrows it and pulls the match down into
                # Band 1. That selection effect loads Band 1 with upsets and is
                # a second, independent driver of the Band 1 miscalibration.
                match['home_elo'] = result['pre_home']
                match['away_elo'] = result['pre_away']
                match['elo_diff'] = abs(result['pre_home'] - result['pre_away'])
                match['elo_band'] = elo_band_for_diff(match['elo_diff'])

                # Re-derive the label from the ratings just stamped. Previously
                # the ELOs were rewritten here but `winner` was left as-is, so
                # records ended up contradicting their own ratings.
                match['winner'] = classify_winner(
                    match['home_goals'], match['away_goals'],
                    match['home_elo'], match['away_elo']
                )
                match['elo_processed'] = True

                print(f"    {match['date']}: {match['home_team']} "
                      f"({result['home_change']:+.1f}→{result['post_home']}) vs "
                      f"{match['away_team']} "
                      f"({result['away_change']:+.1f}→{result['post_away']})")
        else:
            print("  No new matches to process")

        if calc.seeded_teams:
            print(f"  ! {len(calc.seeded_teams)} team(s) started from a default "
                  f"rather than a known rating: {calc.seeded_teams}")

        # Store results
        self.current_elo = calc.current_elo
        self.elo_history = calc.elo_history
        
        # Print rankings
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
        
        new_matches = []
        
        for match_id in range(first_match_id, last_match_id + 1):
            # Check if match already exists
            if any(m['match_id'] == match_id for m in self.matches_data):
                print(f"  Match {match_id} already exists, skipping...")
                continue
            
            url = f'https://www.premierleague.com/en/match/{match_id}'
            driver = make_driver()

            try:
                driver.get(url)
                time.sleep(5)

                accept_cookies(driver)

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
                    current_year = datetime.now().year
                    date_with_year = f"{date_text} {current_year}"
                    match_date = datetime.strptime(date_with_year, '%a %d %b %Y')
                
                # Get scores
                home_goals = int(driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[2]/div/span[1]').text)
                away_goals = int(driver.find_element(By.XPATH, '/html/body/main/div[1]/div[2]/div[2]/div[1]/div/div/header/div/div[2]/div/span[3]').text)
                
                # Get referee
                try:
                    referee = driver.find_element(By.XPATH, '/html/body/main//div[1]/div[2]/div[2]/div[1]/div/div/div/section/div[1]/div[2]/div/span').text
                except:
                    referee = None
                
                # Get cards and related advanced stats.
                #
                # This block previously clicked the stats tab and then did
                # nothing -- the extraction was never written -- so every
                # scraped match stored 0 cards regardless of outcome, and the
                # exception (when the tab lookup failed) was printed without
                # its text. Both are fixed in scrapers/match_stats.py.
                stats = scrape_match_stats(driver)

                home_yellow = stats['home_yellow']
                away_yellow = stats['away_yellow']
                home_red = stats['home_red']
                away_red = stats['away_red']

                if not stats['stats_scraped']:
                    # Report the real reason rather than a generic message.
                    print(f"    ⚠ No advanced stats for match {match_id}: "
                          f"{stats['stats_error']}")

                # Get current ELO ratings. These are the pre-match ratings --
                # what was knowable before kickoff -- which is what the band
                # assignment must be based on.
                if home_team in self.current_elo:
                    home_elo = self.current_elo[home_team]
                else:
                    home_elo = self.fallback_elo(home_team)
                if away_team in self.current_elo:
                    away_elo = self.current_elo[away_team]
                else:
                    away_elo = self.fallback_elo(away_team)

                elo_diff = abs(home_elo - away_elo)
                band = elo_band_for_diff(elo_diff)
                winner = classify_winner(home_goals, away_goals, home_elo, away_elo)

                # Calculate booking points. None (not 0) when stats were not
                # read -- 0 is a real football value and must stay
                # distinguishable from "not scraped", or unscraped matches
                # drag every referee and team booking average toward zero.
                home_booking = stats['home_booking_points']
                away_booking = stats['away_booking_points']
                total_booking = stats['total_booking_points']

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
                    'elo_band': band,
                    'referee': referee,
                    'home_yellow': home_yellow,
                    'away_yellow': away_yellow,
                    'home_red': home_red,
                    'away_red': away_red,
                    'home_booking_points': home_booking,
                    'away_booking_points': away_booking,
                    'total_booking_points': total_booking,
                    'stats_scraped': stats['stats_scraped'],
                    'advanced_stats': stats['advanced_stats'],
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
        """Fetch odds from the-odds-api.com"""
        print("\nFetching bookmaker odds...")

        keys = odds_api_keys()
        if not keys:
            print("  ✗ No API key configured -- set ODDS_API_KEY in .env "
                  "(copy .env.example) or in the real environment")
            return []

        base_url = 'https://api.the-odds-api.com/v4/sports/soccer_epl/odds'

        upcoming_fixtures = []
        key_index = 0  # sticky across this run -- see request_with_key_failover()

        try:
            # First get the list of games with H2H odds
            h2h_params = {
                'regions': 'uk',
                'markets': 'h2h',
                'oddsFormat': 'decimal'
            }

            h2h_response, key_index = request_with_key_failover(
                requests.get, base_url, h2h_params, keys, start_at=key_index)
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
                    'regions': 'uk',
                    'markets': 'totals,alternate_totals,btts',
                    'oddsFormat': 'decimal'
                }

                event_response, key_index = request_with_key_failover(
                    requests.get, event_url, event_params, keys, start_at=key_index)
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
    # DEBUGGING
    # ============================================    
    def import_excel(self, filepath):

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
                band = None
                if elo_diff is not None:
                    band = elo_band_for_diff(elo_diff)  # Capped at band 10 for 450+

                winner = classify_winner(home_goals, away_goals, home_elo, away_elo)

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
                    'elo_band': band,
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
        """
        Calculate statistics for each ELO band.

        Delegates to elo_calculator.calculate_elo_bands so the band maths can
        be run (and validated) without pulling in the scraping stack.
        """
        return calculate_elo_bands(self.matches_data)

    def get_band_probabilities(self, band_number):
        """
        Get probabilities for a specific band, in stronger/draw/weaker terms.

        This previously read 'home_win_pct'/'away_win_pct', keys that only ever
        existed on the empty-band fallback -- so it raised KeyError for every
        band that actually had data. The bands are keyed by relative strength,
        not by venue; mapping strength to home/away is the venue adjustment's
        job, in get_venue_adjusted_probabilities().
        """
        bands = self.calculate_elo_bands()
        for band in bands:
            if band['band'] == band_number:
                return {
                    'stronger_win': band['stronger_win_pct'],
                    'draw': band['draw_pct'],
                    'weaker_win': band['weaker_win_pct']
                }
        # Default if band not found
        return {'stronger_win': 0.333, 'draw': 0.333, 'weaker_win': 0.334}

    def calculate_referee_stats(self):
        """
        Calculate statistics for each referee.

        Averages are taken over matches with CARD DATA, not over all matches
        the referee officiated. Those are different denominators whenever a
        scrape failed, and the old code used the wrong one: it counted every
        match as a game and added `.get('total_booking_points', 0)`, so the
        380 matches the broken stats scrape stored as zeros pulled every
        referee average down -- by 4 to 17 booking points depending on how
        much of their record fell in the affected season.

        `games` remains the true appearance count; `games_with_card_data` is
        the denominator actually used, so a thin sample is visible rather
        than disguised as a low average.
        """
        referee_data = {}

        for match in self.matches_data:
            ref = match.get('referee')
            if not ref or ref == 'None':
                continue

            data = referee_data.setdefault(ref, {
                'games': 0,
                'games_with_card_data': 0,
                'total_booking_points': 0,
                'total_yellows': 0,
                'total_reds': 0,
            })
            data['games'] += 1

            bp = match.get('total_booking_points')
            hy, ay = match.get('home_yellow'), match.get('away_yellow')
            if bp is None or hy is None or ay is None:
                continue

            data['games_with_card_data'] += 1
            data['total_booking_points'] += bp
            data['total_yellows'] += hy + ay
            data['total_reds'] += (match.get('home_red') or 0) + (match.get('away_red') or 0)

        for data in referee_data.values():
            n = data['games_with_card_data']
            if n:
                data['avg_booking_points'] = round(data['total_booking_points'] / n, 1)
                data['avg_yellows'] = round(data['total_yellows'] / n, 1)
                data['avg_reds'] = round(data['total_reds'] / n, 2)
            else:
                data['avg_booking_points'] = None
                data['avg_yellows'] = None
                data['avg_reds'] = None
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
            # Get last 10 *scored* matches for this team.
            #
            # matches_data can hold a record for this team with no result yet
            # -- a fixture appended before kickoff, or a scrape artifact like
            # the "Season"/"Start" record seen in a live data file -- and
            # nothing about matching on team name guarantees home_goals/
            # away_goals are present. That is the same class of bug fixed in
            # update_elo_ratings() (see the 2026-08-26 dev log entry, KeyError:
            # 'home_goals'): skip scoreless records here too rather than
            # indexing into a result that doesn't exist.
            team_matches = []
            for match in reversed(self.matches_data):
                if 'home_goals' not in match or 'away_goals' not in match:
                    continue
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
                    # None, not 0, when no match in the window carries card
                    # data -- a team with no data must fall back to the band
                    # average downstream, not claim a zero-booking record.
                    'last_10_avg_booking_points': round(np.mean(booking_points), 1) if booking_points else None,
                    'last_10_matches_with_card_data': len(booking_points),
                    'season_avg_booking_points': round(np.mean(season_booking), 1) if season_booking else None,
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
        
        # League average goals.
        #
        # Same scoreless-record hazard as calculate_team_stats() above: not
        # every record in matches_data is guaranteed to carry home_goals/
        # away_goals (an unplayed fixture, a scrape artifact), so this must
        # skip those rather than KeyError on the whole matrix.
        all_goals = [m['home_goals'] + m['away_goals'] for m in self.matches_data
                     if 'home_goals' in m and 'away_goals' in m]
        league_avg = np.mean(all_goals) / 2 if all_goals else 1.3
        
        # Calculate expected goals with home advantage
        home_attack = home_data['last_10_avg_goals_for'] / league_avg
        home_defense = home_data['last_10_avg_goals_against'] / league_avg
        away_attack = away_data['last_10_avg_goals_for'] / league_avg
        away_defense = away_data['last_10_avg_goals_against'] / league_avg
        
        home_expected = home_attack * away_defense * league_avg * 1.1
        away_expected = away_attack * home_defense * league_avg * 0.9
        
        # Generate Poisson matrix (0-15 goals each).
        #
        # This grid ran 0-5, which truncated the joint distribution without
        # renormalizing it: every scoreline with 6+ goals on either side was
        # simply absent, so the matrix summed to less than 1.0. The deficit
        # scaled with expected goals -- worst observed was 7.5% of the mass
        # missing on a high-xG fixture -- which distorts scoreline
        # probabilities relative to each other, not just in aggregate.
        # The grid captures the tail rather than rescaling a truncated one,
        # so widening it never disturbs the probabilities already present.
        #
        # Two separate error sources have to stay under the 0.001 tolerance,
        # and which one binds depends on the grid size:
        #   - Truncation, which shrinks as the grid widens. At 0-10 it alone
        #     exceeded 0.001 once expected goals passed ~3.5; by 0-15 it is
        #     under 0.0006 even at 6 goals a side.
        #   - Rounding, which *grows* with the grid, since every cell carries
        #     up to half an ulp and there are now 256 of them. At 4dp that
        #     noise alone breached 0.001 in the high-xG range, which would
        #     have made the wider grid's headroom illusory -- hence 6dp.
        # Widening the grid further therefore means revisiting the rounding
        # too, not just the range.
        matrix = []
        for i in range(16):
            row = []
            for j in range(16):
                prob = stats.poisson.pmf(i, home_expected) * stats.poisson.pmf(j, away_expected)
                row.append(round(prob, 6))
            matrix.append(row)
        
        return {'matrix': matrix}
    
    def get_expected_booking_points(self, home_team, away_team, elo_band):
        """Get expected booking points for a fixture"""
        # Get band average
        bands = self.calculate_elo_bands()
        band_avg = 40  # Default
        for band in bands:
            if band['band'] == elo_band:
                band_avg = band['avg_booking_points']
                break
        
        # Get team averages. A team with no card data falls back to the band
        # average rather than to a literal 40 or an implied 0 -- .get(k, 40)
        # did not catch this, because the key existed and held 0.
        team_stats = self.calculate_team_stats()
        home_avg = team_stats.get(home_team, {}).get('last_10_avg_booking_points')
        away_avg = team_stats.get(away_team, {}).get('last_10_avg_booking_points')
        if home_avg is None:
            home_avg = band_avg
        if away_avg is None:
            away_avg = band_avg

        # Weight: 50% band, 25% each team
        expected = (band_avg * 0.5) + (home_avg * 0.25) + (away_avg * 0.25)

        return round(expected, 1)
    
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
    
    # 2. Update ELO ratings
    calc.update_elo_ratings()
    
    # 3. Generate all JSON files
    calc.generate_all_json_files()
    
    print("\n✅ Complete! Push files to GitHub to update your website.")