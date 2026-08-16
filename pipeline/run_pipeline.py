"""
End-to-end local pipeline. Run it by hand; it never runs itself.

    python3 -m pipeline.run_pipeline [--dry-run] [--bankroll N] [--data DIR]

Stages
------
  1. Load the validated ELO/Poisson model output (Day 1)
  2. Price every upcoming fixture and find +EV selections (>= +5% EV)
  3. Size them: Quarter-Kelly ceiling standard, Eighth-Kelly hedge/low-conf
  4. Run the sanity checks (correlation, two-+EV-in-one-market)
  5. Attach qualitative context (styles, news, formations, congestion)
  6. Write the run report and update the bankroll ledger

Design notes
------------
This is a script the operator triggers, not a service. It has no scheduler, no
daemon and no retry loop, and it exits non-zero on failure so a shell can see
it. Nothing local is hardcoded though: paths come from --data, bankroll state
lives in the ledger, and no absolute path to anyone's home directory appears
anywhere -- so dropping it on a VM later needs no code change.

--dry-run prices and reports without writing bets to the ledger. That is the
mode to run first; the ledger is the one piece of state a mistaken run is
awkward to unwind.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from elo_calculator import calculate_fair_odds
from pipeline import qualitative as qual
from pipeline.bankroll import Ledger
from pipeline.staking import (
    HEDGE, LOW_CONFIDENCE, STANDARD, apply_sanity_checks, size_bet,
)

# Model selection -> the bookmaker_odds key it is priced against.
MARKETS = {
    'home': ('1x2', 'home_win'),
    'draw': ('1x2', 'draw'),
    'away': ('1x2', 'away_win'),
}


def load_json(path, default=None):
    path = Path(path)
    if not path.exists():
        if default is None:
            raise FileNotFoundError(path)
        return default
    return json.loads(path.read_text())


# --- stage 1: model -------------------------------------------------------

def price_fixture(fixture, current_elo, elo_bands):
    """
    Model probabilities for one fixture from the Day 1 ELO output.

    Returns None when either side has no rating -- an unrated team must not be
    priced off a fallback and then staked against.
    """
    home, away = fixture['home_team'], fixture['away_team']
    home_elo = current_elo.get(home)
    away_elo = current_elo.get(away)
    if home_elo is None or away_elo is None:
        return None

    home_elo = home_elo['elo'] if isinstance(home_elo, dict) else home_elo
    away_elo = away_elo['elo'] if isinstance(away_elo, dict) else away_elo

    return calculate_fair_odds(home_elo, away_elo, elo_bands)


# --- stage 2/3: edges and staking ----------------------------------------

def classify_confidence(fixture, model, selection, context, low_confidence_bands):
    """
    Decide which Kelly multiplier a selection earns.

    STANDARD -> Quarter-Kelly. HEDGE / LOW_CONFIDENCE -> Eighth-Kelly.

    A play is downgraded to lower-confidence when the band it sits in is one
    the operator has marked thin, or when the fixture is flagged as a hedge in
    the run config. Qualitative context does NOT downgrade automatically --
    congestion and team news inform the human, they do not size the bet.
    """
    if selection in fixture.get('hedge_selections', []):
        return HEDGE
    if model['meta']['band'] in low_confidence_bands:
        return LOW_CONFIDENCE
    return STANDARD


def find_edges(fixtures, current_elo, elo_bands, bankroll, config):
    """Price every fixture, size every selection that clears the EV floor."""
    decisions, skipped = [], []
    low_bands = set(config.get('low_confidence_bands', []))

    for fixture in fixtures:
        name = f"{fixture['home_team']} v {fixture['away_team']}"
        model = price_fixture(fixture, current_elo, elo_bands)
        if model is None:
            skipped.append((name, 'no ELO rating for one or both teams'))
            continue

        odds = fixture.get('bookmaker_odds') or {}
        for selection, (market, model_key) in MARKETS.items():
            price = odds.get(selection)
            if not price:
                continue
            probability = model[model_key]['probability']
            confidence = classify_confidence(
                fixture, model, selection, None, low_bands)

            decisions.append(size_bet(
                fixture=name, market=market, selection=selection,
                probability=probability, odds=price, bankroll=bankroll,
                confidence=confidence))

    return decisions, skipped


# --- stage 5: qualitative -------------------------------------------------

def attach_context(fixtures, matches, data_dir):
    """Qualitative context per fixture. Never merged into the pricing."""
    qualitative = qual.load_qualitative(data_dir)
    cup_fixtures = load_json(Path(data_dir) / 'cup_fixtures.json',
                             default={'teams': {}})

    return {
        f"{f['home_team']} v {f['away_team']}":
            qual.fixture_context(f, matches, qualitative, cup_fixtures)
        for f in fixtures
    }


# --- reporting ------------------------------------------------------------

def build_report(decisions, findings, contexts, skipped, ledger, dry_run):
    placed = [d for d in decisions if d.bet]
    return {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'dry_run': dry_run,
        'staking_rule': {
            'standard': 'Quarter-Kelly (0.25) -- ceiling',
            'hedge_or_low_confidence': 'Eighth-Kelly (0.125)',
            'minimum_edge': '+5% EV',
            'note': 'not Half-Kelly; older repo references are superseded',
        },
        'congestion_signal': qual.CONGESTION_CAVEAT,
        'bankroll': ledger.summary(),
        'selections': [
            {
                'fixture': d.fixture, 'market': d.market,
                'selection': d.selection,
                'model_probability': round(d.model_probability, 4),
                'bookmaker_odds': d.bookmaker_odds,
                'implied_probability': round(d.implied_probability, 4),
                'edge_pct': round(d.expected_value * 100, 2),
                'full_kelly': round(d.full_kelly, 4),
                'confidence': d.confidence,
                'kelly_multiplier': d.multiplier,
                'stake_fraction': round(d.stake_fraction, 5),
                'stake': d.stake,
                'bet': d.bet,
                'reason': d.reason,
                'flags': d.flags,
            }
            for d in decisions
        ],
        'sanity_findings': [
            {'kind': f.kind, 'fixture': f.fixture, 'detail': f.detail,
             'selections': f.selections, 'blocking': f.blocks}
            for f in findings
        ],
        'skipped_fixtures': [{'fixture': f, 'reason': r} for f, r in skipped],
        'qualitative_context': contexts,
        'totals': {
            'priced': len(decisions),
            'bets_placed': len(placed),
            'total_stake': round(sum(d.stake for d in placed), 2),
        },
    }


def print_report(report, decisions, findings, contexts, ledger):
    print('\n' + '=' * 72)
    print('BANKROLL')
    print('=' * 72)
    print(ledger.format_summary())

    print('\n' + '=' * 72)
    print('SELECTIONS')
    print('=' * 72)
    if not decisions:
        print('  no fixtures priced')
    for d in decisions:
        marker = '✓' if d.bet else '·'
        print(f'  {marker} {d.fixture:<28} {d.explain()}')

    if findings:
        print('\n' + '=' * 72)
        print('SANITY CHECKS')
        print('=' * 72)
        for f in findings:
            marker = '✗ BLOCKING' if f.blocks else '• noted'
            print(f'  {marker}  [{f.kind}] {f.fixture}')
            print(f'      {f.detail}')

    print('\n' + '=' * 72)
    print('QUALITATIVE CONTEXT')
    print(f'({qual.CONGESTION_CAVEAT})')
    print('=' * 72)
    for name, ctx in contexts.items():
        print(f'  {name}')
        print(f'    home: {qual.summarise(ctx, "home")}')
        print(f'    away: {qual.summarise(ctx, "away")}')

    t = report['totals']
    print('\n' + '=' * 72)
    print(f"  priced {t['priced']} selection(s), "
          f"{t['bets_placed']} bet(s), total stake {t['total_stake']:.2f}")
    print('=' * 72)


# --- entry point ----------------------------------------------------------

def run(data_dir='data', bankroll=None, dry_run=False, config=None):
    data_dir = Path(data_dir)
    config = config or load_json(data_dir / 'pipeline_config.json', default={})

    matches = load_json(data_dir / 'matches_data.json')
    current_elo = load_json(data_dir / 'current_elo.json')
    elo_bands = load_json(data_dir / 'elo_bands.json')
    fixtures = load_json(data_dir / 'upcoming_fixtures.json', default=[])

    ledger = Ledger(path=data_dir / 'bankroll.json',
                    starting_bankroll=bankroll if bankroll is not None
                    else config.get('starting_bankroll', 1000.0))
    if bankroll is not None:
        ledger.starting_bankroll = bankroll

    staking_bankroll = ledger.staking_bankroll
    print(f'Pricing {len(fixtures)} fixture(s) against a staking bankroll of '
          f'{staking_bankroll:.2f}' + ('   [DRY RUN]' if dry_run else ''))

    decisions, skipped = find_edges(
        fixtures, current_elo, elo_bands, staking_bankroll, config)

    explanations = {
        frozenset(pair.split('+')): reason
        for pair, reason in config.get('correlation_explanations', {}).items()
    }
    decisions, findings = apply_sanity_checks(decisions, explanations)

    contexts = attach_context(fixtures, matches, data_dir)
    report = build_report(decisions, findings, contexts, skipped, ledger, dry_run)

    print_report(report, decisions, findings, contexts, ledger)

    if not dry_run:
        for d in decisions:
            if d.bet:
                ledger.place(d)
        ledger.save()
        print(f'\n  {len([d for d in decisions if d.bet])} bet(s) written to '
              f'{ledger.path}')
    else:
        print('\n  --dry-run: ledger untouched')

    report_path = data_dir / 'pipeline_report.json'
    report_path.write_text(json.dumps(report, indent=2))
    print(f'  report written to {report_path}')

    return report


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--data', default='data')
    ap.add_argument('--bankroll', type=float, default=None,
                    help='override the ledger starting bankroll')
    ap.add_argument('--dry-run', action='store_true',
                    help='price and report without writing bets to the ledger')
    args = ap.parse_args()

    try:
        run(data_dir=args.data, bankroll=args.bankroll, dry_run=args.dry_run)
    except FileNotFoundError as exc:
        print(f'ERROR: required data file missing: {exc}', file=sys.stderr)
        return 1
    except Exception as exc:
        print(f'ERROR: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
