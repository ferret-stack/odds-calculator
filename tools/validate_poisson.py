"""
Validation harness for the Poisson score matrix.

Checks the output of OddsCalculator.calculate_poisson() for its own
mathematical correctness, independent of any market that consumes it:

  1. Total probability mass sums to ~1.0 (within a tolerance, default 0.001)
  2. No scoreline probability is negative
  3. No scoreline probability exceeds 1.0

This is a regression check, not a fix: it reports on the matrix as
generated and never modifies it.

Run:  python3 tools/validate_poisson.py [--data DIR] [--tolerance T] [--limit N]
"""

import argparse
import json
import sys
from pathlib import Path

# Importable and runnable from anywhere: odds_calculator.py lives at the repo
# root, one level up from tools/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def validate_score_matrix(matrix, tolerance=0.001):
    """
    Validate a single score matrix.

    Returns a dict with 'passed' plus the measured numbers, so a caller can
    report the actual values rather than only a boolean.
    """
    if not matrix:
        return {
            'passed': False,
            'failures': ['matrix is empty'],
            'total': 0.0,
            'min': None,
            'max': None,
            'cells': 0,
        }

    flat = [(i, j, p) for i, row in enumerate(matrix) for j, p in enumerate(row)]
    total = sum(p for _, _, p in flat)
    lo = min(flat, key=lambda c: c[2])
    hi = max(flat, key=lambda c: c[2])

    failures = []

    # 1. Total mass
    if abs(total - 1.0) > tolerance:
        failures.append(
            f'total probability {total:.6f} differs from 1.0 by '
            f'{abs(total - 1.0):.6f} (tolerance {tolerance})'
        )

    # 2. Negative probabilities
    negatives = [(i, j, p) for i, j, p in flat if p < 0]
    if negatives:
        failures.append(
            f'{len(negatives)} negative probabilit{"y" if len(negatives) == 1 else "ies"}, '
            f'lowest {negatives[0][2]:.6f} at {negatives[0][0]}-{negatives[0][1]}'
        )

    # 3. Probabilities above 1.0
    over_one = [(i, j, p) for i, j, p in flat if p > 1.0]
    if over_one:
        failures.append(
            f'{len(over_one)} probabilit{"y" if len(over_one) == 1 else "ies"} above 1.0, '
            f'highest {over_one[0][2]:.6f} at {over_one[0][0]}-{over_one[0][1]}'
        )

    return {
        'passed': not failures,
        'failures': failures,
        'total': total,
        'min': lo,
        'max': hi,
        'cells': len(flat),
    }


def validate_fixture(calc, home_team, away_team, tolerance=0.001):
    """Generate the matrix for one fixture and validate it."""
    matrix = calc.calculate_poisson(home_team, away_team).get('matrix', [])
    result = validate_score_matrix(matrix, tolerance=tolerance)
    result['home_team'] = home_team
    result['away_team'] = away_team
    result['matrix'] = matrix
    return result


def load_fixtures(data_dir, limit=None):
    """Real fixtures from the stored upcoming-fixtures file."""
    path = Path(data_dir) / 'upcoming_fixtures.json'
    with open(path) as f:
        fixtures = json.load(f)
    pairs = [(f['home_team'], f['away_team']) for f in fixtures]
    return pairs[:limit] if limit else pairs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', default='data', help='data directory')
    parser.add_argument('--tolerance', type=float, default=0.001,
                        help='allowed deviation of the total from 1.0')
    parser.add_argument('--limit', type=int, default=None,
                        help='validate only the first N fixtures')
    args = parser.parse_args()

    from odds_calculator import OddsCalculator

    calc = OddsCalculator(data_dir=args.data)
    pairs = load_fixtures(args.data, args.limit)

    print(f'\nPoisson score-matrix validation -- {len(pairs)} real fixtures '
          f'from {args.data}/upcoming_fixtures.json')
    print(f'Tolerance on total probability: +/-{args.tolerance}\n')

    results = []
    for home, away in pairs:
        r = validate_fixture(calc, home, away, tolerance=args.tolerance)
        results.append(r)
        status = 'PASS' if r['passed'] else 'FAIL'
        print(f'[{status}] {home} vs {away}')
        print(f'         sum={r["total"]:.6f}  '
              f'deviation={abs(r["total"] - 1.0):.6f}  cells={r["cells"]}')
        if r['min'] and r['max']:
            print(f'         min={r["min"][2]:.6f} at {r["min"][0]}-{r["min"][1]}   '
                  f'max={r["max"][2]:.6f} at {r["max"][0]}-{r["max"][1]}')
        for msg in r['failures']:
            print(f'         -> {msg}')
        print()

    passed = sum(1 for r in results if r['passed'])
    failed = len(results) - passed
    print('-' * 68)
    print(f'{passed} passed, {failed} failed, {len(results)} total')

    if failed:
        worst = max(results, key=lambda r: abs(r['total'] - 1.0))
        print(f'Largest deviation: {worst["home_team"]} vs {worst["away_team"]} '
              f'-- sum={worst["total"]:.6f} '
              f'({abs(worst["total"] - 1.0) * 100:.2f}% of mass unaccounted for)')

    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
