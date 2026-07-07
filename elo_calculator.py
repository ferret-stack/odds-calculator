"""
V1
ELO Calculator Module for Premier League Betting Model
Calculates ELO ratings independently with margin of victory adjustment.
No external API dependencies.

V2: Added home and away advantages to dynamically update

Author: Ferret Stack
Version: 1.1
"""

import json
import math
from collections import defaultdict
from typing import Dict, List, Tuple
from pathlib import Path


# ============================================
# VENUE ADJUSTMENT
# ============================================
# Multipliers are MEASURED from the data by rebuild.calculate_venue_adjustment
# and stored in venue_adjustment.json - nothing here hardcodes a venue effect.
# Multiplier semantics: P(outcome | venue of the stronger team) / P(outcome).


def adjust_probability_for_venue(base_prob, is_stronger_team_home, market,
                                 venue_multipliers):
    """
    Adjust a band base probability for venue.

    Args:
        base_prob: Raw probability from elo_bands.json
        is_stronger_team_home: True if the ELO-stronger team is at home
        market: 'stronger_win', 'weaker_win', or 'draw'
        venue_multipliers: dict from venue_adjustment.json

    Returns:
        Adjusted probability (not yet normalized)
    """
    v = venue_multipliers
    if market == 'stronger_win':
        mult = v['home_multiplier'] if is_stronger_team_home else v['away_multiplier']
    elif market == 'weaker_win':
        # the weaker team is home exactly when the stronger team is away
        mult = v['weaker_away_multiplier'] if is_stronger_team_home else v['weaker_home_multiplier']
    elif market == 'draw':
        mult = v['draw_home_multiplier'] if is_stronger_team_home else v['draw_away_multiplier']
    else:
        mult = 1.0
    return base_prob * mult


def get_venue_adjusted_probabilities(home_elo, away_elo, elo_bands,
                                     venue_multipliers):
    """
    Match probabilities with venue adjustment applied.

    Args:
        home_elo: ELO rating of home team
        away_elo: ELO rating of away team
        elo_bands: List of band dictionaries from elo_bands.json
        venue_multipliers: dict from venue_adjustment.json

    Returns:
        Dict with 'home_win', 'draw', 'away_win' probabilities (sum to 1.0)
    """
    elo_diff = abs(home_elo - away_elo)
    band_num = min(int(elo_diff // 50) + 1, 10)

    band_data = next((b for b in elo_bands if b['band'] == band_num), None)
    if not band_data:
        return {'home_win': 0.333, 'draw': 0.333, 'away_win': 0.334}

    # exact-equality rule: home side is the de-facto stronger team
    is_stronger_home = home_elo >= away_elo

    stronger_adj = adjust_probability_for_venue(
        band_data['stronger_win_pct'], is_stronger_home, 'stronger_win',
        venue_multipliers)
    weaker_adj = adjust_probability_for_venue(
        band_data['weaker_win_pct'], is_stronger_home, 'weaker_win',
        venue_multipliers)
    draw_adj = adjust_probability_for_venue(
        band_data['draw_pct'], is_stronger_home, 'draw', venue_multipliers)

    total = stronger_adj + weaker_adj + draw_adj
    stronger_adj /= total
    weaker_adj /= total
    draw_adj /= total

    if is_stronger_home:
        home_win, away_win = stronger_adj, weaker_adj
    else:
        home_win, away_win = weaker_adj, stronger_adj
    return {'home_win': round(home_win, 4), 'draw': round(draw_adj, 4),
            'away_win': round(away_win, 4)}


class ELOCalculator:
    """
    Calculates and maintains ELO ratings for football teams.
    
    Features:
    - Standard ELO calculation with configurable K-factor
    - Home advantage adjustment
    - Margin of Victory (MOV) multiplier (FiveThirtyEight-style)
    """
    
    def __init__(
        self,
        k_factor: int = 20,
        home_advantage: int = 100,
        use_mov: bool = True,
        default_elo: int = 1500,
        mov_formula: str = 'v6blog'
    ):
        """
        Initialize the ELO calculator.

        Args:
            k_factor: Base K-factor for ELO changes (default 20, same as ClubELO)
            home_advantage: ELO points added to home team's effective rating (default 100)
            use_mov: Whether to apply margin of victory multiplier (default True)
            default_elo: Starting ELO for new teams (default 1500)
            mov_formula: 'fte' (FiveThirtyEight-style, what the code has always
                run) or 'v6blog' (the 0.7 + ln(gd+1)*0.5 variant with upset bonus
                that the V6 blog post documented). Both are kept so the
                calibration harness can compare them; the shipped default is the
                calibration winner.
        """
        self.k_factor = k_factor
        self.home_advantage = home_advantage
        self.use_mov = use_mov
        self.default_elo = default_elo
        if mov_formula not in ('fte', 'v6blog'):
            raise ValueError(f"unknown mov_formula: {mov_formula}")
        self.mov_formula = mov_formula
        
        # Storage
        self.current_elo: Dict[str, int] = {}
        self.elo_history: Dict[str, List[Dict]] = defaultdict(list)
    
    def expected_score(self, team_elo: float, opponent_elo: float, is_home: bool = False) -> float:
        """
        Calculate expected score (probability of winning) using ELO formula.
        
        Args:
            team_elo: The team's current ELO rating
            opponent_elo: The opponent's current ELO rating  
            is_home: Whether the team is playing at home
            
        Returns:
            Expected score between 0 and 1
        """
        effective_elo = team_elo + (self.home_advantage if is_home else 0)
        opponent_effective = opponent_elo + (self.home_advantage if not is_home else 0)
        
        return 1 / (1 + 10 ** ((opponent_effective - effective_elo) / 400))
    
    def margin_of_victory_multiplier(self, goal_diff: int, elo_diff: float) -> float:
        """
        Calculate the Margin of Victory multiplier (FiveThirtyEight-style).
        
        This adjusts K-factor based on:
        - How many goals the match was won by (logarithmic scale)
        - The ELO difference (beating stronger teams by large margins is harder)
        
        Args:
            goal_diff: Absolute goal difference (always positive)
            elo_diff: Winner's ELO minus loser's ELO (can be negative if upset)
            
        Returns:
            Multiplier to apply to K-factor (typically 1.0 to ~1.7)
        """
        if goal_diff == 0:
            return 1.0

        if self.mov_formula == 'v6blog':
            base = math.log(abs(goal_diff) + 1)
            # Upset bonus: elo_diff is winner-minus-loser, negative on an upset
            if elo_diff < -50:
                base *= 1.0 + abs(elo_diff) / 500
            return 0.7 + base * 0.5

        # 'fte' (FiveThirtyEight-style): logarithmic goal scaling, dampened
        # when the winner was already rated far above the loser
        goal_factor = math.log(abs(goal_diff) + 1)
        dampening = 2.2 / ((elo_diff * 0.001) + 2.2)

        return goal_factor * dampening
    
    def calculate_elo_change(
        self,
        team_elo: float,
        opponent_elo: float,
        team_goals: int,
        opponent_goals: int,
        is_home: bool
    ) -> float:
        """
        Calculate ELO change for a single team after a match.
        
        Args:
            team_elo: Team's ELO before the match
            opponent_elo: Opponent's ELO before the match
            team_goals: Goals scored by the team
            opponent_goals: Goals scored by the opponent
            is_home: Whether the team was at home
            
        Returns:
            ELO change (positive for improvement, negative for decline)
        """
        # Determine actual score (1 = win, 0.5 = draw, 0 = loss)
        if team_goals > opponent_goals:
            actual_score = 1.0
        elif team_goals < opponent_goals:
            actual_score = 0.0
        else:
            actual_score = 0.5
        
        # Calculate expected score
        expected = self.expected_score(team_elo, opponent_elo, is_home)
        
        # Base ELO change
        k = self.k_factor
        
        # Apply MOV multiplier if enabled and not a draw
        if self.use_mov and team_goals != opponent_goals:
            goal_diff = abs(team_goals - opponent_goals)
            
            # ELO diff from winner's perspective
            if team_goals > opponent_goals:
                elo_diff = team_elo - opponent_elo
            else:
                elo_diff = opponent_elo - team_elo
            
            mov_mult = self.margin_of_victory_multiplier(goal_diff, elo_diff)
            k = k * mov_mult
        
        return k * (actual_score - expected)
    
    def process_match(
        self,
        home_team: str,
        away_team: str,
        home_goals: int,
        away_goals: int,
        match_date: str,
        update_history: bool = True
    ) -> Tuple[int, int, float, float]:
        """
        Process a single match and update ELO ratings.
        
        Args:
            home_team: Name of home team
            away_team: Name of away team
            home_goals: Goals scored by home team
            away_goals: Goals scored by away team
            match_date: Date string (YYYY-MM-DD format)
            update_history: Whether to record this in elo_history
            
        Returns:
            Tuple of (new_home_elo, new_away_elo, home_change, away_change)
        """
        # Get current ELO (or default for new teams)
        home_elo = self.current_elo.get(home_team, self.default_elo)
        away_elo = self.current_elo.get(away_team, self.default_elo)
        
        # Calculate ELO changes
        home_change = self.calculate_elo_change(
            home_elo, away_elo, home_goals, away_goals, is_home=True
        )
        away_change = self.calculate_elo_change(
            away_elo, home_elo, away_goals, home_goals, is_home=False
        )
        
        # Update current ELO
        new_home_elo = round(home_elo + home_change)
        new_away_elo = round(away_elo + away_change)
        
        self.current_elo[home_team] = new_home_elo
        self.current_elo[away_team] = new_away_elo
        
        # Record in history if requested
        if update_history:
            if home_team not in self.elo_history:
                self.elo_history[home_team] = []
            if away_team not in self.elo_history:
                self.elo_history[away_team] = []
            
            self.elo_history[home_team].append({
                'date': match_date,
                'elo': new_home_elo
            })
            self.elo_history[away_team].append({
                'date': match_date,
                'elo': new_away_elo
            })
        
        return (new_home_elo, new_away_elo, round(home_change, 1), round(away_change, 1))
    
    def load_current_elo(self, filepath: Path) -> None:
        """
        Load current ELO ratings from current_elo.json.
        
        Args:
            filepath: Path to current_elo.json
        """
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Handle both formats: {team: elo} and {team: {elo: x, rank: y}}
        for team, value in data.items():
            if isinstance(value, dict):
                self.current_elo[team] = value['elo']
            else:
                self.current_elo[team] = value
        
        print(f"  ✓ Loaded {len(self.current_elo)} teams from {filepath.name}")
    
    def load_elo_history(self, filepath: Path) -> None:
        """
        Load ELO history from elo_history.json.
        
        Args:
            filepath: Path to elo_history.json
        """
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        for team, history in data.items():
            self.elo_history[team] = history
        
        print(f"  ✓ Loaded history for {len(self.elo_history)} teams from {filepath.name}")
    
    def get_current_elo(self, team: str) -> int:
        """Get current ELO for a team."""
        return self.current_elo.get(team, self.default_elo)
    
    def get_elo_diff(self, team1: str, team2: str) -> int:
        """Get absolute ELO difference between two teams."""
        return abs(self.get_current_elo(team1) - self.get_current_elo(team2))
    
    def get_elo_band(self, team1: str, team2: str) -> int:
        """Get ELO band (1-10) for a fixture."""
        diff = self.get_elo_diff(team1, team2)
        return min(int(diff // 50) + 1, 10)
    
    def get_rankings(self) -> List[Tuple[str, int, int]]:
        """
        Get teams ranked by ELO.
        
        Returns:
            List of (team_name, elo, rank) tuples
        """
        sorted_teams = sorted(
            self.current_elo.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        return [(team, elo, rank + 1) for rank, (team, elo) in enumerate(sorted_teams)]
    
    def export_current_elo(self) -> Dict[str, Dict]:
        """Export current ELO in the format expected by the betting system."""
        rankings = self.get_rankings()
        return {
            team: {'elo': elo, 'rank': rank}
            for team, elo, rank in rankings
        }
    
    def export_elo_history(self) -> Dict[str, List[Dict]]:
        """Export ELO history in the format expected by the betting system."""
        return {
            team: sorted(history, key=lambda x: x['date'])
            for team, history in self.elo_history.items()
        }


def calculate_form_metrics(
    team: str,
    elo_history: Dict[str, List[Dict]],
    num_matches: int = 10
) -> Dict:
    """
    Calculate form metrics for a team based on ELO history.
    
    Args:
        team: Team name
        elo_history: Dictionary of ELO history from elo_history.json
        num_matches: Number of recent matches to analyze
        
    Returns:
        Dictionary with form metrics
    """
    if team not in elo_history or len(elo_history[team]) < 2:
        return {
            'elo_change_last_5': 0,
            'elo_change_last_10': 0,
            'trend': 'stable',
            'form_rating': 5.0
        }
    
    # Sort by date (most recent last)
    sorted_history = sorted(elo_history[team], key=lambda x: x['date'])
    
    # Calculate changes between consecutive entries
    recent_changes = []
    for i in range(len(sorted_history) - 1, max(len(sorted_history) - num_matches - 1, 0), -1):
        if i > 0:
            change = sorted_history[i]['elo'] - sorted_history[i-1]['elo']
            recent_changes.append(change)
    
    # Calculate 5-game and 10-game changes
    elo_change_5 = sum(recent_changes[:5]) if len(recent_changes) >= 5 else sum(recent_changes)
    elo_change_10 = sum(recent_changes[:10]) if len(recent_changes) >= 10 else sum(recent_changes)
    
    # Determine trend based on 5-game change
    if elo_change_5 > 15:
        trend = "improving"
    elif elo_change_5 < -15:
        trend = "declining"
    else:
        trend = "stable"
    
    # Enhanced form rating calculation
    # Base component (60% weight): ELO change over last 5 games
    base_form = 5 + (elo_change_5 / 10)
    
    # Momentum component (40% weight): Compare 5-game vs 10-game trend
    if elo_change_10 != 0:
        momentum = (elo_change_5 - (elo_change_10 / 2)) / 10
    else:
        momentum = 0
    
    # Combined form rating (clamped to 0-10)
    form_rating = min(10, max(0, (base_form * 0.6) + ((5 + momentum) * 0.4)))
    
    return {
        'elo_change_last_5': round(elo_change_5),
        'elo_change_last_10': round(elo_change_10),
        'trend': trend,
        'form_rating': round(form_rating, 1)
    }


def calculate_fair_odds(
    home_team_elo: int,
    away_team_elo: int,
    elo_bands: List[Dict],
    venue_multipliers: Dict
) -> Dict[str, Dict[str, float]]:
    """
    Fair decimal odds for a fixture, venue-adjusted. The single canonical
    implementation (the module used to carry two competing definitions).

    Args:
        home_team_elo: ELO rating of home team
        away_team_elo: ELO rating of away team
        elo_bands: List of band data from elo_bands.json
        venue_multipliers: dict from venue_adjustment.json

    Returns:
        Dictionary with adjusted probabilities and fair odds for each market
    """
    probs = get_venue_adjusted_probabilities(
        home_team_elo, away_team_elo, elo_bands, venue_multipliers)

    elo_diff = abs(home_team_elo - away_team_elo)
    result = {
        market: {
            'probability': probs[key],
            'fair_odds': round(1 / probs[key], 2)
        }
        for market, key in (('home_win', 'home_win'), ('draw', 'draw'),
                            ('away_win', 'away_win'))
    }
    result['meta'] = {
        'elo_diff': elo_diff,
        'band': min(int(elo_diff // 50) + 1, 10),
        'stronger_team': 'home' if home_team_elo >= away_team_elo else 'away',
        'home_elo': home_team_elo,
        'away_elo': away_team_elo
    }
    return result