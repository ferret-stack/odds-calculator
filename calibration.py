"""
Calibration / backtest harness: prove a pipeline change is better, don't guess.

Every configuration is evaluated walk-forward: for each match after the
warm-up, the prediction uses only information available before kickoff —
pre-match ratings, and band/venue tables accumulated over strictly earlier
matches (Laplace-smoothed). Metrics: multiclass Brier score and log-loss over
home/draw/away, plus a reliability table.

What gets compared:

- OLD pipeline (emulated from the archived data/legacy/matches_data_v6.json,
  bugs and all: post-match band stamping, the equal-ELO winner mislabelling,
  baked-in 1500 defaults, corrupted dates, hardcoded venue constants)
- NEW pipeline over the repaired facts, crossed over:
    * ELO variant:  long | rolling          (which should drive the bands?)
    * MOV formula:  fte | v6blog            (code vs blog-post formula)
    * venue:        none | global | per-band
- Null baselines: uniform 1/3, and running home/draw/away base rates
  (a "home-advantage only" model any real model must beat).

Run:  python calibration.py            (full grid, prints a report table)
Results also land in data/reference/calibration_results.json.
"""

import json
import math
from collections import defaultdict
from pathlib import Path

from rebuild import (load_facts, replay_long, rolling_pre_ratings, band_of,
                     derive_winner)

EVAL_START = '2022-08-01'     # two seasons of warm-up before scoring begins
ALPHA = 1.0                   # Laplace smoothing for all walk-forward tables
OLD_VENUE = {'stronger_home': 1.11, 'stronger_away': 0.89,
             'draw_home': 0.95, 'draw_away': 1.05}

OUTCOMES = ('home', 'draw', 'away')


def outcome_of(m):
    if m['home_goals'] > m['away_goals']:
        return 'home'
    if m['home_goals'] < m['away_goals']:
        return 'away'
    return 'draw'


def normalise(p):
    s = sum(p.values())
    return {k: v / s for k, v in p.items()}


class BandTable:
    """Walk-forward band WDL table with Laplace smoothing; optionally split by
    venue of the stronger team."""

    def __init__(self, per_venue=False):
        self.per_venue = per_venue
        self.counts = defaultdict(lambda: defaultdict(float))

    def _key(self, band, stronger_home):
        return (band, stronger_home) if self.per_venue else band

    def probs(self, band, stronger_home):
        c = self.counts[self._key(band, stronger_home)]
        tot = sum(c.values()) + 3 * ALPHA
        return {w: (c[w] + ALPHA) / tot for w in ('stronger', 'draw', 'weaker')}

    def update(self, band, stronger_home, winner):
        self.counts[self._key(band, stronger_home)][winner] += 1


class VenueTable:
    """Walk-forward global venue multipliers: P(outcome | venue) / P(outcome)."""

    def __init__(self):
        self.by_venue = {True: defaultdict(float), False: defaultdict(float)}

    def multiplier(self, stronger_home, w):
        cond = self.by_venue[stronger_home]
        both = {k: self.by_venue[True][k] + self.by_venue[False][k]
                for k in ('stronger', 'draw', 'weaker')}
        n_cond = sum(cond.values()) + 3 * ALPHA
        n_all = sum(both.values()) + 3 * ALPHA
        p_cond = (cond[w] + ALPHA) / n_cond
        p_all = (both[w] + ALPHA) / n_all
        return p_cond / p_all

    def update(self, stronger_home, winner):
        self.by_venue[stronger_home][winner] += 1


def to_home_away(p_swd, stronger_home):
    """Map stronger/draw/weaker probabilities onto home/draw/away."""
    if stronger_home:
        return {'home': p_swd['stronger'], 'draw': p_swd['draw'],
                'away': p_swd['weaker']}
    return {'home': p_swd['weaker'], 'draw': p_swd['draw'],
            'away': p_swd['stronger']}


def eval_new(facts, elo='long', mov='fte', venue='global'):
    """Walk-forward predictions for one NEW-pipeline configuration."""
    if elo == 'long':
        ratings = [(r['pre_home'], r['pre_away'])
                   for r in replay_long(facts, mov)[0]]
    else:
        ratings = [(r['pre_home'], r['pre_away'])
                   for r in rolling_pre_ratings(facts, mov_formula=mov)]

    table = BandTable(per_venue=(venue == 'per-band'))
    vtable = VenueTable()
    preds = {}
    for m, (eh, ea) in zip(facts, ratings):
        stronger_home = eh >= ea
        band = band_of(abs(eh - ea))
        if m['date'] >= EVAL_START:
            p = table.probs(band, stronger_home)
            if venue == 'global':
                p = normalise({w: p[w] * vtable.multiplier(stronger_home, w)
                               for w in p})
            preds[m['match_id']] = to_home_away(p, stronger_home)
        w = derive_winner(m['home_goals'], m['away_goals'], eh, ea)
        table.update(band, stronger_home, w)
        if eh != ea:                      # ties excluded from venue measurement
            vtable.update(stronger_home, w)
    return preds


def eval_old(legacy_path='data/legacy/matches_data_v6.json'):
    """Walk-forward emulation of the shipped V6 pipeline, from its own archived
    data: its stored (post-match) band stamps and winner labels feed the table,
    its own rating trajectory feeds the lookup, its hardcoded venue constants
    adjust the result."""
    with open(legacy_path) as f:
        legacy = json.load(f)
    legacy.sort(key=lambda m: (m['date'], m['match_id']))

    counts = defaultdict(lambda: defaultdict(float))
    last_elo = {}
    preds = {}
    for m in legacy:
        eh = last_elo.get(m['home_team'], 1500)
        ea = last_elo.get(m['away_team'], 1500)
        stronger_home = eh >= ea
        band = band_of(abs(eh - ea))
        if m['date'] >= EVAL_START:
            c = counts[band]
            tot = sum(c.values()) + 3 * ALPHA
            p = {w: (c[w] + ALPHA) / tot for w in ('stronger', 'draw', 'weaker')}
            adj = {
                'stronger': p['stronger'] * (OLD_VENUE['stronger_home']
                                             if stronger_home else OLD_VENUE['stronger_away']),
                'weaker': p['weaker'] * (OLD_VENUE['stronger_away']
                                         if stronger_home else OLD_VENUE['stronger_home']),
                'draw': p['draw'] * (OLD_VENUE['draw_home']
                                     if stronger_home else OLD_VENUE['draw_away']),
            }
            preds[m['match_id']] = to_home_away(normalise(adj), stronger_home)
        # the old pipeline files each match under its STORED (corrupt) stamps
        if m.get('elo_band') and m.get('winner'):
            counts[m['elo_band']][m['winner']] += 1
        if m.get('home_elo'):
            last_elo[m['home_team']] = m['home_elo']
        if m.get('away_elo'):
            last_elo[m['away_team']] = m['away_elo']
    return preds


def eval_baseline(facts, kind):
    """uniform: 1/3 each. base-rates: running home/draw/away frequencies."""
    counts = defaultdict(float)
    preds = {}
    for m in facts:
        if m['date'] >= EVAL_START:
            if kind == 'uniform':
                preds[m['match_id']] = {w: 1 / 3 for w in OUTCOMES}
            else:
                tot = sum(counts.values()) + 3 * ALPHA
                preds[m['match_id']] = {w: (counts[w] + ALPHA) / tot
                                        for w in OUTCOMES}
        counts[outcome_of(m)] += 1
    return preds


def per_match_scores(preds, facts, eval_ids):
    """(log-loss, brier) per match, keyed by match_id, on the shared eval set."""
    out = {}
    for m in facts:
        mid = m['match_id']
        if mid not in preds or mid not in eval_ids:
            continue
        p = preds[mid]
        y = outcome_of(m)
        out[mid] = (-math.log(max(p[y], 1e-12)),
                    sum((p[w] - (1.0 if w == y else 0.0)) ** 2 for w in OUTCOMES))
    return out


def score(preds, facts, eval_ids):
    s = per_match_scores(preds, facts, eval_ids)
    n = len(s)
    return {'log_loss': sum(v[0] for v in s.values()) / n,
            'brier': sum(v[1] for v in s.values()) / n, 'n': n}


def paired_bootstrap(preds_a, preds_b, facts, eval_ids, iters=10_000, seed=42):
    """P(model A's mean log-loss < model B's) under paired resampling."""
    import random
    sa = per_match_scores(preds_a, facts, eval_ids)
    sb = per_match_scores(preds_b, facts, eval_ids)
    ids = sorted(set(sa) & set(sb))
    diffs = [sa[i][0] - sb[i][0] for i in ids]
    rng = random.Random(seed)
    n = len(diffs)
    wins = sum(
        sum(diffs[rng.randrange(n)] for _ in range(n)) < 0
        for _ in range(iters))
    return wins / iters


def reliability(preds, facts, buckets=10):
    """Predicted home-win probability vs realised home-win rate, by decile."""
    rows = defaultdict(lambda: [0, 0.0, 0])
    for m in facts:
        if m['match_id'] not in preds:
            continue
        p = preds[m['match_id']]['home']
        b = min(int(p * buckets), buckets - 1)
        rows[b][0] += 1
        rows[b][1] += p
        rows[b][2] += 1 if outcome_of(m) == 'home' else 0
    return [{'bucket': f"{b / buckets:.1f}-{(b + 1) / buckets:.1f}",
             'n': n, 'mean_predicted': round(s / n, 3),
             'actual_home_rate': round(h / n, 3)}
            for b, (n, s, h) in sorted(rows.items())]


def run_all(data_dir='data'):
    facts = load_facts(Path(data_dir) / 'matches_data.json')
    eval_ids = {m['match_id'] for m in facts if m['date'] >= EVAL_START}
    print(f"eval window: {EVAL_START} onward, {len(eval_ids)} matches "
          f"(warm-up: everything before)\n")

    results = {}

    def run(name, preds):
        # every config is scored on the same eval matches
        results[name] = score(preds, facts, eval_ids)
        r = results[name]
        print(f"{name:38s} log-loss {r['log_loss']:.4f}   "
              f"Brier {r['brier']:.4f}   n={r['n']}")
        return preds

    run('baseline: uniform', eval_baseline(facts, 'uniform'))
    run('baseline: home/draw/away base rates', eval_baseline(facts, 'rates'))
    old_preds = run('OLD pipeline (shipped V6, emulated)', eval_old())
    print()

    chosen_preds = {}
    for elo in ('long', 'rolling'):
        for mov in ('fte', 'v6blog'):
            for venue in ('none', 'global', 'per-band'):
                name = f"NEW elo={elo} mov={mov} venue={venue}"
                chosen_preds[(elo, mov, venue)] = run(
                    name, eval_new(facts, elo, mov, venue))

    best = min((k for k in results if k.startswith('NEW')),
               key=lambda k: results[k]['log_loss'])
    print(f"\nbest NEW config by log-loss: {best}")

    key = tuple(kv.split('=')[1] for kv in best.split()[1:])
    p_beats_old = paired_bootstrap(chosen_preds[key], old_preds, facts, eval_ids)
    print(f"paired bootstrap P(best NEW beats OLD on log-loss): {p_beats_old:.3f}")
    rel = reliability(chosen_preds[key], facts)
    print("\nreliability (home win), best config:")
    for row in rel:
        print(f"  p in {row['bucket']}: n={row['n']:4d}  "
              f"predicted {row['mean_predicted']:.3f}  "
              f"actual {row['actual_home_rate']:.3f}")

    out = Path(data_dir) / 'reference' / 'calibration_results.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, 'w') as f:
        json.dump({'eval_start': EVAL_START, 'results': results,
                   'best_new_config': best,
                   'p_best_new_beats_old': p_beats_old,
                   'reliability_best': rel}, f, indent=2)
    print(f"\nsaved: {out}")
    return results


if __name__ == '__main__':
    run_all()
