"""
Super 6 correct-score picks.

For every fixture in the upcoming-fixtures file, generate the Poisson score
matrix, pick a scoreline from it, and attach a one-line rationale from a local
Ollama model.

This is the correct-score jackpot feature and is deliberately separate from the
+EV betting pipeline: it reads no bankroll, sizes no stake, and writes nothing
the ledger consumes. A Super 6 entry is a fixed-cost jackpot punt, not a priced
bet, so none of the staking machinery applies to it.

The pick is the argmax of the score matrix, with one adjustment. Poisson grids
routinely put 0-0, 1-0 and 1-1 within a few tenths of a percentage point of
each other, which makes a bare argmax close to arbitrary between them. So any
scoreline within TIE_TOLERANCE of the argmax is treated as tied with it, and
among those the lower-scoring outcome wins -- low scores are the safer jackpot
pick when the model cannot separate them. Ties are broken by, in order:

  1. fewest total goals
  2. highest probability
  3. lowest home goals, then lowest away goals (so the pick is deterministic)

The matrix itself comes from OddsCalculator.calculate_poisson(), which is
validated separately by tools/validate_poisson.py and is used here as-is.

The Ollama call is a placeholder for a narrative layer that has not been
designed yet. It is one request with no retries and no configurable template:
if it fails for any reason the pick still ships, with a null narrative and the
reason recorded. Do not build this out here -- it is due to be replaced.

Run:  python3 tools/super6_picks.py [--data DIR] [--limit N]
                                    [--tolerance T] [--model TAG]
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import requests

# Importable and runnable from anywhere: odds_calculator.py lives at the repo
# root, one level up from tools/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Half a percentage point. Wide enough to catch the genuine near-ties that make
# an argmax arbitrary, narrow enough that a scoreline the model actually
# prefers is not displaced by a materially less likely one.
TIE_TOLERANCE = 0.005

OLLAMA_ENDPOINT = 'http://localhost:11434/api/generate'
OLLAMA_MODEL = 'llama3.2:3b'
OLLAMA_TIMEOUT = 60


def load_fixtures(data_dir, limit=None):
    """Real fixtures from the stored upcoming-fixtures file."""
    path = Path(data_dir) / 'upcoming_fixtures.json'
    with open(path) as f:
        fixtures = json.load(f)
    return fixtures[:limit] if limit else fixtures


def pick_scoreline(matrix, tolerance=TIE_TOLERANCE):
    """
    Choose a scoreline from a score matrix.

    Returns (chosen, argmax, tied) where each entry is an (i, j, probability)
    tuple and `tied` is every candidate within `tolerance` of the argmax,
    including the argmax itself. Returning the argmax and the candidate set
    alongside the pick is what lets the caller record whether the tie-break
    actually fired, rather than leaving that to be inferred later.
    """
    cells = [(i, j, matrix[i][j])
             for i in range(len(matrix))
             for j in range(len(matrix[i]))]

    argmax = max(cells, key=lambda c: c[2])
    tied = [c for c in cells if argmax[2] - c[2] <= tolerance]
    chosen = min(tied, key=lambda c: (c[0] + c[1], -c[2], c[0], c[1]))

    return chosen, argmax, tied


def generate_narrative(fixture_label, home_goals, away_goals, probability,
                       model=OLLAMA_MODEL):
    """
    One-line rationale for a pick, from a local Ollama model.

    Raises on any failure -- the caller decides what a failure means. See the
    module docstring: this is a placeholder, keep it to one request.
    """
    prompt = (
        f'{fixture_label}. The model predicts {home_goals}-{away_goals}, '
        f'a {probability * 100:.1f}% chance. '
        'Give a single short sentence explaining this scoreline. '
        'No preamble, no bullet points, one sentence only.'
    )
    response = requests.post(
        OLLAMA_ENDPOINT,
        json={'model': model, 'prompt': prompt, 'stream': False},
        timeout=OLLAMA_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()['response'].strip()


def build_pick(fixture, chosen, argmax, tied, narrative, narrative_error):
    """Assemble the output record for one fixture."""
    home_goals, away_goals, probability = chosen

    # Everything tied with the argmax except the pick itself -- the alternatives
    # the tie-break passed over, which is the part worth being able to audit.
    others = [c for c in tied if (c[0], c[1]) != (home_goals, away_goals)]

    return {
        'fixture': f"{fixture['home_team']} vs {fixture['away_team']}",
        'home_team': fixture['home_team'],
        'away_team': fixture['away_team'],
        'date': fixture.get('date'),
        'time': fixture.get('time'),
        'game_id': fixture.get('game_id'),
        'scoreline': f'{home_goals}-{away_goals}',
        'home_goals': home_goals,
        'away_goals': away_goals,
        'probability': round(probability, 6),
        'argmax_scoreline': f'{argmax[0]}-{argmax[1]}',
        'tie_break_applied': (argmax[0], argmax[1]) != (home_goals, away_goals),
        'tied_candidates': [
            {'scoreline': f'{i}-{j}', 'probability': round(p, 6)}
            for i, j, p in sorted(others, key=lambda c: -c[2])
        ],
        'narrative': narrative,
        'narrative_error': narrative_error,
    }


def build_report(picks, skipped, data_dir, tolerance, model):
    generated = sum(1 for p in picks if p['narrative'])
    return {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source': str(Path(data_dir) / 'upcoming_fixtures.json'),
        # The rule is recorded with the output because the picks are not
        # reproducible from the matrix alone -- a reader needs the tolerance and
        # the ordering to see why a non-argmax scoreline was chosen.
        'tie_break_rule': {
            'tolerance': tolerance,
            'note': ('Scorelines within the tolerance of the argmax count as '
                     'tied. Among those: fewest total goals, then highest '
                     'probability, then lowest home and away goals.'),
        },
        'narrative_model': {
            'provider': 'ollama',
            'model': model,
            'endpoint': OLLAMA_ENDPOINT,
            'note': ('Placeholder integration -- one call, no retries. A failed '
                     'call yields a null narrative, never a failed run.'),
        },
        'picks': picks,
        'skipped_fixtures': skipped,
        'totals': {
            'fixtures': len(picks),
            'skipped': len(skipped),
            'narratives_generated': generated,
            'narratives_failed': len(picks) - generated,
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--data', default='data', help='data directory')
    parser.add_argument('--limit', type=int, default=None,
                        help='process only the first N fixtures (testing)')
    parser.add_argument('--tolerance', type=float, default=TIE_TOLERANCE,
                        help='probability window treated as a tie with the argmax')
    parser.add_argument('--model', default=OLLAMA_MODEL,
                        help='local Ollama model tag')
    args = parser.parse_args()

    from odds_calculator import OddsCalculator

    fixtures = load_fixtures(args.data, args.limit)
    calc = OddsCalculator(data_dir=args.data)

    print(f'\nSuper 6 correct-score picks -- {len(fixtures)} fixtures '
          f'from {args.data}/upcoming_fixtures.json')
    print(f'Tie tolerance: {args.tolerance} ({args.tolerance * 100:.1f} percentage points)')
    print(f'Narrative model: {args.model} via {OLLAMA_ENDPOINT}\n')

    picks = []
    skipped = []

    for fixture in fixtures:
        home_team = fixture['home_team']
        away_team = fixture['away_team']
        label = f'{home_team} vs {away_team}'

        matrix = calc.calculate_poisson(home_team, away_team).get('matrix', [])
        if not matrix:
            # calculate_poisson() returns an empty matrix for a team it has no
            # stats for. Skip that fixture rather than losing the whole slate.
            print(f'[SKIP] {label} -- no score matrix (team missing from stats)')
            skipped.append({'fixture': label,
                            'reason': 'no score matrix -- team missing from team stats'})
            continue

        chosen, argmax, tied = pick_scoreline(matrix, tolerance=args.tolerance)
        home_goals, away_goals, probability = chosen

        narrative = None
        narrative_error = None
        try:
            narrative = generate_narrative(label, home_goals, away_goals,
                                           probability, model=args.model)
        except Exception as exc:
            narrative_error = f'{type(exc).__name__}: {exc}'[:200]
            print(f'[WARN] {label} -- narrative failed, shipping pick without one: '
                  f'{narrative_error}', file=sys.stderr)

        picks.append(build_pick(fixture, chosen, argmax, tied,
                                narrative, narrative_error))

        marker = ' (tie-break)' if (argmax[0], argmax[1]) != (home_goals, away_goals) else ''
        print(f'[PICK] {label}')
        print(f'       {home_goals}-{away_goals}  {probability * 100:.2f}%{marker}'
              f'   argmax={argmax[0]}-{argmax[1]} {argmax[2] * 100:.2f}%'
              f'   tied={len(tied)}')

    report = build_report(picks, skipped, args.data, args.tolerance, args.model)
    output_path = Path(args.data) / 'super6_output.json'
    output_path.write_text(json.dumps(report, indent=2))

    totals = report['totals']
    print('\n' + '-' * 68)
    print(f"{totals['fixtures']} picks, {totals['skipped']} skipped, "
          f"{totals['narratives_generated']} narratives generated, "
          f"{totals['narratives_failed']} without one")
    print(f'Written to {output_path}')


if __name__ == '__main__':
    raise SystemExit(main())
