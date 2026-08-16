"""
Staking: Kelly sizing and the pre-bet sanity checks.

THE STAKING RULE (the whole rule, stated once)
----------------------------------------------
    Quarter-Kelly (0.25 x full Kelly) is a CEILING for standard +EV plays.
    Eighth-Kelly (0.125 x full Kelly) for hedge positions and lower-confidence
    plays.
    Act only on edges of +5% EV or better.

This is not Half-Kelly. Older documents in this repository and its history
reference Half-Kelly; they are superseded and must not be followed. There is
deliberately no 0.5 multiplier anywhere in this module, and a test asserts its
absence.

"Ceiling" is meant literally: the fraction is an upper bound, never a target to
be adjusted upward. Nothing in this module ever raises a stake above the
multiplier for its confidence class.

MATHS
-----
For decimal odds d and model probability p, with net odds b = d - 1:

    EV per unit staked  =  p*d - 1
    full Kelly f*       =  (p*b - (1-p)) / b  =  (p*d - 1) / (d - 1)

so f* is exactly EV / b. Both quantities share a numerator, which is why the
+5% EV floor and a positive Kelly fraction are the same test -- but they are
kept separate below because the floor is a policy choice and Kelly is not.

    stake fraction      =  multiplier x f*      multiplier in {0.25, 0.125}
    stake              =  bankroll x stake fraction
"""

from dataclasses import dataclass, field
from typing import Optional

# --- the rule, as constants ----------------------------------------------

QUARTER_KELLY = 0.25    # standard +EV plays -- ceiling
EIGHTH_KELLY = 0.125    # hedges and lower-confidence plays

MIN_EV = 0.05           # +5% EV; below this we do not act

STANDARD = 'standard'
HEDGE = 'hedge'
LOW_CONFIDENCE = 'low_confidence'

# Confidence class -> Kelly multiplier. The only mapping there is.
MULTIPLIER = {
    STANDARD: QUARTER_KELLY,
    HEDGE: EIGHTH_KELLY,
    LOW_CONFIDENCE: EIGHTH_KELLY,
}


# --- market taxonomy ------------------------------------------------------
# Which selections belong to the same market. Two +EV selections inside one
# family is a model-recheck signal, never a pair of bets.
MARKET_FAMILY = {
    'home': '1x2', 'draw': '1x2', 'away': '1x2',
    'over_05': 'goals_05', 'under_05': 'goals_05',
    'over_15': 'goals_15', 'under_15': 'goals_15',
    'over_25': 'goals_25', 'under_25': 'goals_25',
    'over_35': 'goals_35', 'under_35': 'goals_35',
    'over_45': 'goals_45', 'under_45': 'goals_45',
    'btts_yes': 'btts', 'btts_no': 'btts',
}


def market_family(selection):
    return MARKET_FAMILY.get(selection, selection)


# --- core maths -----------------------------------------------------------

def expected_value(probability, odds):
    """EV per unit staked. +0.05 means a 5% edge."""
    _validate(probability, odds)
    return probability * odds - 1


def full_kelly_fraction(probability, odds):
    """
    Full Kelly f* = (p*d - 1) / (d - 1).

    Returns 0.0 for a non-positive edge -- Kelly's answer to a bad bet is to
    stake nothing, not to stake negatively.
    """
    _validate(probability, odds)
    net_odds = odds - 1
    if net_odds <= 0:
        return 0.0
    fraction = (probability * odds - 1) / net_odds
    return max(0.0, fraction)


def _validate(probability, odds):
    if not 0 <= probability <= 1:
        raise ValueError(f'probability must be in [0, 1], got {probability}')
    if odds <= 1:
        raise ValueError(f'decimal odds must exceed 1.0, got {odds}')


# --- staking decision -----------------------------------------------------

@dataclass
class StakeDecision:
    """The full arithmetic for one selection, kept auditable end to end."""
    fixture: str
    market: str
    selection: str
    model_probability: float
    bookmaker_odds: float
    expected_value: float
    full_kelly: float
    confidence: str
    multiplier: float
    stake_fraction: float
    stake: float
    bet: bool
    reason: str
    flags: list = field(default_factory=list)

    @property
    def implied_probability(self):
        return 1 / self.bookmaker_odds

    def explain(self):
        """One-line derivation, so a stake can always be checked by hand."""
        if not self.bet:
            return f'{self.selection}: NO BET -- {self.reason}'
        return (
            f'{self.selection} @ {self.bookmaker_odds:.2f} | '
            f'p={self.model_probability:.4f} '
            f'EV={self.expected_value:+.4f} ({self.expected_value * 100:+.2f}%) | '
            f'full Kelly={self.full_kelly:.4f} x {self.multiplier} '
            f'({self.confidence}) = {self.stake_fraction:.4f} | '
            f'stake {self.stake:.2f}'
        )


def size_bet(fixture, market, selection, probability, odds, bankroll,
             confidence=STANDARD, min_ev=MIN_EV):
    """
    Apply the staking rule to a single selection.

    Always returns a StakeDecision -- rejections carry their reason and the
    arithmetic that produced it, so a skipped bet is as auditable as a placed
    one.
    """
    if confidence not in MULTIPLIER:
        raise ValueError(
            f'unknown confidence class {confidence!r}; '
            f'expected one of {sorted(MULTIPLIER)}')

    ev = expected_value(probability, odds)
    kelly = full_kelly_fraction(probability, odds)
    multiplier = MULTIPLIER[confidence]

    def decision(bet, reason, stake_fraction=0.0):
        return StakeDecision(
            fixture=fixture, market=market, selection=selection,
            model_probability=probability, bookmaker_odds=odds,
            expected_value=ev, full_kelly=kelly, confidence=confidence,
            multiplier=multiplier, stake_fraction=stake_fraction,
            stake=round(bankroll * stake_fraction, 2),
            bet=bet, reason=reason)

    if bankroll <= 0:
        return decision(False, 'bankroll exhausted')
    if ev < min_ev:
        return decision(
            False,
            f'edge {ev * 100:+.2f}% below the {min_ev * 100:.0f}% floor')
    if kelly <= 0:
        return decision(False, 'full Kelly non-positive')

    return decision(True, f'{confidence} play at {multiplier} Kelly',
                    stake_fraction=multiplier * kelly)


# --- sanity checks --------------------------------------------------------

# Cross-market pairs that move together within a single fixture. Values are
# the direction of association; the number is documentation, not a weight --
# nothing here scales a stake.
KNOWN_CORRELATIONS = {
    frozenset({'btts', 'goals_25'}): 'both driven by total goals',
    frozenset({'btts', 'goals_15'}): 'both driven by total goals',
    frozenset({'btts', 'goals_35'}): 'both driven by total goals',
    frozenset({'goals_15', 'goals_25'}): 'nested goal lines',
    frozenset({'goals_25', 'goals_35'}): 'nested goal lines',
    frozenset({'goals_05', 'goals_15'}): 'nested goal lines',
    frozenset({'goals_35', 'goals_45'}): 'nested goal lines',
    frozenset({'1x2', 'goals_25'}): 'favourite winning tends to raise total goals',
    frozenset({'1x2', 'btts'}): 'a one-sided win suppresses BTTS',
}


@dataclass
class SanityFinding:
    kind: str
    fixture: str
    detail: str
    selections: list
    blocks: bool


def check_same_market_conflicts(decisions):
    """
    Two +EV selections in the same market within one fixture.

    That is a model-recheck signal, not two bets: the market's outcomes are
    mutually exclusive and their probabilities are normalised to sum to one,
    so the model cannot genuinely favour both against the same overround. Both
    selections are blocked.
    """
    findings = []
    grouped = {}
    for d in decisions:
        if not d.bet:
            continue
        grouped.setdefault((d.fixture, market_family(d.selection)), []).append(d)

    for (fixture, family), group in grouped.items():
        if len(group) > 1:
            names = [d.selection for d in group]
            findings.append(SanityFinding(
                kind='same_market_multiple_ev',
                fixture=fixture,
                detail=(f'{len(group)} +EV selections in market {family} '
                        f'({", ".join(names)}). Mutually exclusive outcomes '
                        f'cannot both be value against one overround -- '
                        f're-check the model, do not bet both.'),
                selections=names,
                blocks=True))
    return findings


def check_correlated_bets(decisions, explanations=None):
    """
    Multiple selections on the same fixture.

    Two bets on one match share the same 90 minutes, so they are correlated by
    default and the pair must be explained before it is allowed. `explanations`
    maps frozenset({selection_a, selection_b}) -> the reason for taking both.
    An unexplained pair blocks; an explained one is recorded and allowed.
    """
    explanations = explanations or {}
    findings = []

    by_fixture = {}
    for d in decisions:
        if d.bet:
            by_fixture.setdefault(d.fixture, []).append(d)

    for fixture, group in by_fixture.items():
        if len(group) < 2:
            continue
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                if market_family(a.selection) == market_family(b.selection):
                    continue  # handled by check_same_market_conflicts
                pair = frozenset({a.selection, b.selection})
                families = frozenset({market_family(a.selection),
                                      market_family(b.selection)})
                known = KNOWN_CORRELATIONS.get(families)
                explanation = explanations.get(pair)

                if explanation:
                    findings.append(SanityFinding(
                        kind='correlation_explained',
                        fixture=fixture,
                        detail=f'{a.selection} + {b.selection}: {explanation}',
                        selections=[a.selection, b.selection],
                        blocks=False))
                else:
                    reason = known or 'same fixture, same 90 minutes'
                    findings.append(SanityFinding(
                        kind='correlated_bets_unexplained',
                        fixture=fixture,
                        detail=(f'{a.selection} + {b.selection} are correlated '
                                f'({reason}) and no explanation was supplied. '
                                f'Blocked -- add one to pipeline config to take '
                                f'both knowingly.'),
                        selections=[a.selection, b.selection],
                        blocks=True))
    return findings


# An edge above this is far more often a stale price or a model artefact than
# genuine value. Advisory only -- see check_implausible_edge.
IMPLAUSIBLE_EDGE = 0.20


def check_implausible_edge(decisions, threshold=IMPLAUSIBLE_EDGE):
    """
    Flag edges large enough to be suspicious. NON-BLOCKING by design.

    The band model prices by ELO band, so every fixture in a band shares one
    set of probabilities -- Man Utd away at Brighton is priced identically to
    Leeds away at West Ham if both sit in Band 1. Against a sharp market, a
    +30% edge from that model usually means the price is stale or the band is
    too coarse for the fixture, not that the market is wrong by a third.

    This does not block, because the operator's staking rules do not call for
    it. It exists so a number that large cannot pass unremarked.
    """
    findings = []
    for d in decisions:
        if d.bet and d.expected_value >= threshold:
            findings.append(SanityFinding(
                kind='implausible_edge',
                fixture=d.fixture,
                detail=(f'{d.selection} shows {d.expected_value * 100:+.1f}% edge, '
                        f'above the {threshold * 100:.0f}% advisory threshold. '
                        f'Check the price is current and that the band is not '
                        f'too coarse for this fixture before staking.'),
                selections=[d.selection],
                blocks=False))
    return findings


def apply_sanity_checks(decisions, explanations=None):
    """
    Run every check and switch off any decision a blocking finding touches.

    Returns (decisions, findings). Decisions are mutated in place: `bet` goes
    False, the stake goes to 0 and the flag is recorded, so a blocked bet
    stays visible in the output rather than vanishing from it.
    """
    findings = (check_same_market_conflicts(decisions)
                + check_correlated_bets(decisions, explanations)
                + check_implausible_edge(decisions))

    blocked = {}
    for finding in findings:
        if not finding.blocks:
            continue
        for name in finding.selections:
            blocked.setdefault((finding.fixture, name), []).append(finding)

    for d in decisions:
        hits = blocked.get((d.fixture, d.selection))
        if not hits:
            continue
        d.bet = False
        d.stake = 0.0
        d.stake_fraction = 0.0
        d.reason = f'blocked by sanity check: {hits[0].kind}'
        d.flags.extend(f.kind for f in hits)

    return decisions, findings
