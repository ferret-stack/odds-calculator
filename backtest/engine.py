"""
Walk-forward simulation of the production ELO band model against closing odds.

WHAT IS REUSED, AND WHY IT IS REUSED THAT WAY
---------------------------------------------
Everything that prices or sizes a bet is imported from production, not
reimplemented:

    elo_calculator.ELOCalculator      -- rating, MOV multiplier, update
    elo_calculator.calculate_fair_odds -- band -> venue-adjusted 1x2 probs
    elo_calculator.calculate_elo_bands -- the band table itself
    elo_calculator.elo_band / classify_winner / SEED_ELO
    tools.rebuild_elo.season_of / bottom_four_average -- promoted-team seeding
    pipeline.staking.size_bet / apply_sanity_checks / apply_stake_cap

`tools/rebuild_elo.py:rebuild()` itself is NOT reused, and that is a
deliberate fork, recorded in backtest/NOTES.md. It is a single whole-dataset
pass: it orders by `match_id` (absent from these CSVs), repairs scraped date
corruption that this source does not have, and -- the part that actually
blocks reuse -- it stamps every match's band from a chain it has already run
to completion, which is exactly the lookahead a walk-forward must not have.
Its two seeding helpers are season-scoped and are reused unchanged.

THE ONE MODELLING DECISION THIS FILE MAKES
------------------------------------------
Production prices from `data/elo_bands.json`, a band table built by
`calculate_elo_bands` over the WHOLE match history. Replaying that table
against the matches it was built from is not a backtest -- every fixture's
price would carry knowledge of its own result. So the default here
(`bands='walkforward'`) rebuilds the table at each new match date from
matches completed strictly before it, and the first season is a warm-up in
which the chain and the table build but nothing is staked.

`bands='frozen'` is offered for comparison and uses production's shipped
`data/elo_bands.json` read-only. Its numbers are contaminated by design; it
exists so the size of the leak is visible rather than assumed.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from elo_calculator import (  # noqa: E402
    SEED_ELO,
    ELOCalculator,
    calculate_elo_bands,
    classify_winner,
    elo_band,
    get_venue_adjusted_probabilities,
)
from pipeline.staking import (  # noqa: E402
    IMPLAUSIBLE_EDGE,
    MAX_STAKE_FRACTION,
    MIN_EV,
    STANDARD,
    apply_sanity_checks,
    apply_stake_cap,
)
from pipeline.staking import size_bet  # noqa: E402
from tools.rebuild_elo import bottom_four_average, season_of  # noqa: E402

# Model selection -> the calculate_fair_odds key it is priced from. Same
# mapping as run_pipeline.MARKETS, minus the market label the ledger uses.
MARKETS = {'home': 'home_win', 'draw': 'draw', 'away': 'away_win'}

# The result letter that settles each selection.
SETTLES = {'home': 'H', 'draw': 'D', 'away': 'A'}

# Stakes are computed against a fixed notional bankroll so that a bucket's
# yield is not an artefact of where in the run its bets happened to fall.
# The compounded curve is tracked separately, from the same stake fractions.
NOTIONAL_BANKROLL = 1000.0

# The literal that caused the Band 1 miscalibration. Nothing in current
# production can produce it -- ELOCalculator defaults to SEED_ELO (1784) and
# the sentinel checks are gone -- so this is an assertion, not a workaround:
# if it ever appears in a pre-match rating the fixture is flagged and the run
# continues, per the brief.
BAND1_BUG_RATING = 1500


class Fixture(dict):
    """One row of the fixture log. A dict so the CSV writer stays trivial."""


def _price(home_elo, away_elo, bands):
    """
    Production pricing for one fixture: {'home_win', 'draw', 'away_win'}.

    Uses `get_venue_adjusted_probabilities` rather than the
    `calculate_fair_odds` that `run_pipeline.price_fixture` calls. The two
    derive the SAME probabilities from the same band row, through the same
    `adjust_probability_for_venue` and the same normalisation, rounded the
    same way -- verified identical wherever both run.

    They differ only in that `calculate_fair_odds` additionally inverts each
    probability to a fair price, and does so unguarded. Any band row with a
    zero rate therefore raises ZeroDivisionError instead of returning a
    price. That is not hypothetical and not confined to this harness: in the
    SHIPPED `data/elo_bands.json`, band 9 has weaker_win_pct 0.0 (n=6) and
    band 10 has draw_pct and weaker_win_pct 0.0 (n=4), so every fixture with
    an ELO gap of 401 or more takes down a live pipeline run. See
    backtest/NOTES.md. Not fixed here -- the brief is read-only against
    production -- but the backtest cannot call the crashing path, so it
    calls the equivalent one that does not invert.
    """
    return get_venue_adjusted_probabilities(home_elo, away_elo, bands)


def _devig(prices):
    """
    Market probabilities with the overround removed proportionally.

    The raw 1/odds implied probabilities sum to ~1.05 on these files, so
    comparing a model probability to 1/odds compares it against a number that
    is inflated by the book's margin. Both are reported: `market_prob` is the
    raw implied figure the EV formula actually uses, `market_fair` is the
    de-vigged one, which is the honest reference for calibration.

    Proportional (multiplicative) de-vigging is the simplest defensible
    choice. It is known to under-correct favourite-longshot bias at long
    prices, which matters for exactly the away-underdog cut below -- so
    `market_fair` is used for calibration commentary only, never for EV or
    for settling a bet.
    """
    total = sum(1.0 / p for p in prices.values() if p)
    if not total:
        return {}
    return {sel: (1.0 / p) / total for sel, p in prices.items() if p}


def _seed_missing_teams(calc, match, completed, first_season, log):
    """
    Give both sides a rating before the fixture is priced.

    Same rule as `tools/rebuild_elo.py:rebuild()`: opening season at
    SEED_ELO, any later first appearance at the bottom-four average of the
    season just gone, via the shared `bottom_four_average` helper.

    A team RETURNING after relegation keeps the rating it carried out of the
    league, because that is what the production replay does -- rebuild() only
    seeds teams absent from `current_elo`, and reseeds returning sides only
    for the upcoming season in main(). Carried forward here deliberately so
    the backtest measures production's chain; every occurrence is logged.
    """
    season = match['season']
    for team in (match['home_team'], match['away_team']):
        if team in calc.current_elo:
            continue
        if season == first_season:
            calc.seed_team(team, SEED_ELO)
            log.append({'team': team, 'season': season, 'elo': SEED_ELO,
                        'note': 'opening-season baseline'})
            continue
        previous = f'{int(season[:4]) - 1}-{season[:4][2:]}'
        average, bottom = bottom_four_average(completed, previous,
                                              calc.current_elo)
        if average is None:
            average, note = SEED_ELO, 'no prior season completed; baseline'
        else:
            note = f'bottom-4 of {previous}: {", ".join(bottom)}'
        calc.seed_team(team, average)
        log.append({'team': team, 'season': season, 'elo': average,
                    'note': note})


def _band_row(bands, number):
    for row in bands:
        if row['band'] == number:
            return row
    return {}


def band_is_degenerate(band_row):
    """
    True when a band row cannot be priced from.

    `calculate_elo_bands` builds a band's three 1x2 rates from the matches
    whose `winner` is not 'even'; when a band holds ONLY evenly-rated matches
    it falls back to `rated = band_matches` and every predicate then misses,
    so the row comes out 0.0/0.0/0.0. `calculate_fair_odds` normalises by the
    sum of those three and divides by zero.

    That is a live production defect, reachable by any caller that rebuilds
    bands from a small sample -- which a walk-forward does at every early
    date, because every team seeds at SEED_ELO and so every opening fixture
    is evenly rated and lands in Band 1. It cannot fire against the shipped
    `data/elo_bands.json`, which is why it has not been seen.

    The brief forbids editing production, so this harness detects the row and
    declines to price the fixture rather than repairing `calculate_elo_bands`.
    See backtest/NOTES.md -- it is reported, not silently patched.
    """
    if not band_row:
        return True
    total = (band_row.get('stronger_win_pct', 0.0)
             + band_row.get('draw_pct', 0.0)
             + band_row.get('weaker_win_pct', 0.0))
    return total <= 0


def run(matches, bands_mode='walkforward', warmup_seasons=1,
        frozen_bands=None, min_ev=MIN_EV, books=('avg', 'b365'),
        max_stake_fraction=MAX_STAKE_FRACTION):
    """
    Replay every match in date order, pricing before updating.

    Returns a dict with:
        fixtures  -- one row per (match, book, selection), bet or not
        clamps    -- per-bet cap firings
        seeding   -- every deliberate seed taken during the walk
        anomalies -- Band-1-bug sightings and empty-band prices
        meta      -- run configuration, echoed for the report header
    """
    if bands_mode not in ('walkforward', 'frozen'):
        raise ValueError(f'unknown bands mode {bands_mode!r}')
    if bands_mode == 'frozen' and not frozen_bands:
        raise ValueError('bands_mode="frozen" needs frozen_bands')

    calc = ELOCalculator(k_factor=20, home_advantage=100, use_mov=True,
                         default_elo=SEED_ELO)

    seasons = sorted({m['season'] for m in matches})
    first_season = seasons[0]
    warmup = set(seasons[:warmup_seasons]) if bands_mode == 'walkforward' else set()

    completed = []          # stamped matches, the walk-forward band evidence
    fixtures, clamps, seeding, anomalies = [], [], [], []
    bands, bands_date = (frozen_bands, None) if bands_mode == 'frozen' else ([], None)

    for match in matches:
        # --- band table as of BEFORE this date -----------------------------
        if bands_mode == 'walkforward' and match['date'] != bands_date:
            bands = calculate_elo_bands(completed)
            bands_date = match['date']

        _seed_missing_teams(calc, match, completed, first_season, seeding)

        home_elo = calc.current_elo[match['home_team']]
        away_elo = calc.current_elo[match['away_team']]
        diff = abs(home_elo - away_elo)
        band = elo_band(diff)
        band_row = _band_row(bands, band)
        band_games = band_row.get('total_games', 0)

        # Brief: log the Band 1 default-1500 bug and continue, do not fix.
        # Recorded as an anomaly rather than asserted away, so its absence is
        # evidence rather than an assumption.
        bugged = [t for t, e in ((match['home_team'], home_elo),
                                 (match['away_team'], away_elo))
                  if e == BAND1_BUG_RATING]
        if bugged:
            anomalies.append({
                'kind': 'band1_default_1500', 'date': match['date'],
                'fixture': f"{match['home_team']} v {match['away_team']}",
                'detail': f"pre-match rating of exactly {BAND1_BUG_RATING} "
                          f"for {', '.join(bugged)}; band {band}"})
        # An empty band prices at calculate_elo_bands' 0.333/0.333/0.334
        # fallback, which is not a model opinion. Flagged, not suppressed --
        # production would stake it, so hiding it here would flatter the run.
        if band_games == 0:
            anomalies.append({
                'kind': 'empty_band', 'date': match['date'],
                'fixture': f"{match['home_team']} v {match['away_team']}",
                'detail': f'band {band} had no completed matches; priced from '
                          f'the 0.333/0.333/0.334 fallback'})

        degenerate = band_is_degenerate(band_row)
        if degenerate:
            anomalies.append({
                'kind': 'degenerate_band', 'date': match['date'],
                'fixture': f"{match['home_team']} v {match['away_team']}",
                'detail': f'band {band} rates sum to zero (every match in it '
                          f'is evenly rated); calculate_fair_odds would divide '
                          f'by zero, so this fixture was not priced'})

        model = (_price(home_elo, away_elo, bands)
                 if bands and not degenerate else None)
        betting = match['season'] not in warmup and model is not None

        stronger = 'home' if home_elo > away_elo else (
            'away' if away_elo > home_elo else 'even')

        if betting:
            for book in books:
                prices = {s: p for s, p in match['odds'][book].items() if p}
                fair = _devig(prices)
                name = f"{match['home_team']} v {match['away_team']}"

                decisions = []
                for selection, model_key in MARKETS.items():
                    price = prices.get(selection)
                    if not price:
                        continue
                    decisions.append(size_bet(
                        fixture=name, market='1x2', selection=selection,
                        probability=model[model_key],
                        odds=price, bankroll=NOTIONAL_BANKROLL,
                        confidence=STANDARD, min_ev=min_ev,
                        match_date=match['date']))

                # Production's own checks, in production's order: block
                # two +EV selections in one market, then clamp per-bet
                # exposure. The implausible-edge downgrade already happened
                # inside size_bet.
                decisions, _findings = apply_sanity_checks(decisions)
                decisions, events = apply_stake_cap(
                    decisions, NOTIONAL_BANKROLL,
                    max_fraction=max_stake_fraction, logger=lambda _msg: None)
                for event in events:
                    clamps.append({'date': match['date'], 'book': book,
                                   'fixture': name,
                                   'selection': event.selection,
                                   'calculated': event.calculated_stake,
                                   'capped': event.capped_stake})

                for decision in decisions:
                    won = match['ftr'] == SETTLES[decision.selection]
                    profit = (decision.stake * (decision.bookmaker_odds - 1)
                              if won else -decision.stake)
                    fixtures.append(Fixture(
                        date=match['date'], season=match['season'], book=book,
                        home_team=match['home_team'],
                        away_team=match['away_team'],
                        home_elo=home_elo, away_elo=away_elo,
                        elo_diff=diff, band=band, band_games=band_games,
                        stronger_side=stronger,
                        selection=decision.selection,
                        model_prob=round(decision.model_probability, 6),
                        odds=decision.bookmaker_odds,
                        market_prob=round(decision.implied_probability, 6),
                        market_fair=round(fair.get(decision.selection, 0.0), 6),
                        edge_pct=round(decision.expected_value * 100, 4),
                        full_kelly=round(decision.full_kelly, 6),
                        confidence=decision.confidence,
                        multiplier=decision.multiplier,
                        stake_fraction=round(decision.stake_fraction, 8),
                        stake=round(decision.stake, 2),
                        bet=decision.bet,
                        reason=decision.reason,
                        flags='|'.join(decision.flags),
                        ftr=match['ftr'],
                        home_goals=match['home_goals'],
                        away_goals=match['away_goals'],
                        won=won,
                        profit=round(profit, 2) if decision.bet else 0.0,
                        away_underdog=(decision.selection == 'away'
                                       and stronger == 'home'),
                        implausible=decision.expected_value >= IMPLAUSIBLE_EDGE,
                    ))

        # --- only now does the result enter the model ----------------------
        result = calc.process_match(
            home_team=match['home_team'], away_team=match['away_team'],
            home_goals=match['home_goals'], away_goals=match['away_goals'],
            match_date=match['date'])

        # The ratings priced from must be the ratings the update started from.
        # A mismatch means something mutated state between pricing and update.
        if (result['pre_home'], result['pre_away']) != (home_elo, away_elo):
            raise RuntimeError(
                f"pre-match ratings moved between pricing and update for "
                f"{match['home_team']} v {match['away_team']} on {match['date']}")

        stamped = dict(match)
        stamped.update({
            'home_elo': home_elo, 'away_elo': away_elo, 'elo_diff': diff,
            'elo_band': band,
            'winner': classify_winner(match['home_goals'], match['away_goals'],
                                      home_elo, away_elo)})
        completed.append(stamped)

    return {
        'fixtures': fixtures,
        'clamps': clamps,
        'seeding': seeding,
        'anomalies': anomalies,
        'final_elo': dict(calc.current_elo),
        'meta': {
            'bands_mode': bands_mode,
            'warmup_seasons': sorted(warmup),
            'seasons': seasons,
            'matches': len(matches),
            'priced_matches': len([m for m in matches
                                   if m['season'] not in warmup]),
            'min_ev': min_ev,
            'notional_bankroll': NOTIONAL_BANKROLL,
            'max_stake_fraction': max_stake_fraction,
            'books': list(books),
        },
    }
