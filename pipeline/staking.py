"""
Staking: Kelly sizing and the pre-bet sanity checks.

THE STAKING RULE (the whole rule, stated once)
----------------------------------------------
    Quarter-Kelly (0.25 x full Kelly) is a CEILING for standard +EV plays.
    Eighth-Kelly (0.125 x full Kelly) for hedge positions, lower-confidence
    plays, and edges above the implausible-edge threshold (+20% EV).
    Act only on edges of +5% EV or better.
    No single stake exceeds MAX_STAKE_FRACTION of the staking bankroll.
    A week's total stake above MAX_WEEKLY_STAKE_FRACTION is FLAGGED, never
    silently rescaled -- that call belongs to the operator.

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
    stake              =  min(stake, bankroll x MAX_STAKE_FRACTION)

The per-bet cap is applied last, after Kelly and after the sanity checks, so
the uncapped arithmetic stays visible beside the number actually staked.
"""

from dataclasses import dataclass, field
from typing import Optional

# --- the rule, as constants ----------------------------------------------

QUARTER_KELLY = 0.25    # standard +EV plays -- ceiling
EIGHTH_KELLY = 0.125    # hedges, lower-confidence plays, and large edges

MIN_EV = 0.05           # +5% EV; below this we do not act

# An edge above this is far more often a stale price or a model artefact than
# genuine value, so it is sized at Eighth-Kelly rather than Quarter. Defined
# here, above size_bet, because it is part of the sizing rule -- not merely a
# reporting threshold. See LARGE_EDGE and check_implausible_edge.
IMPLAUSIBLE_EDGE = 0.20

# Exposure caps, as fractions of the staking bankroll. These are defaults:
# the pipeline reads the live values from pipeline_config.json ("staking_
# limits"), and staking_limits() below resolves the two. They are NOT
# hardcoded at the call site.
MAX_STAKE_FRACTION = 0.03         # no single bet above 3% of bankroll
MAX_WEEKLY_STAKE_FRACTION = 0.12  # no week's total above 12% of bankroll

STANDARD = 'standard'
HEDGE = 'hedge'
LOW_CONFIDENCE = 'low_confidence'
# A standard play whose edge cleared IMPLAUSIBLE_EDGE. Kept as its own class
# rather than folded into LOW_CONFIDENCE so the ledger records WHY the stake
# was halved -- a thin band and a suspicious edge are different problems.
LARGE_EDGE = 'large_edge'

# Confidence class -> Kelly multiplier. The only mapping there is.
MULTIPLIER = {
    STANDARD: QUARTER_KELLY,
    HEDGE: EIGHTH_KELLY,
    LOW_CONFIDENCE: EIGHTH_KELLY,
    LARGE_EDGE: EIGHTH_KELLY,
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
    # Kickoff date of the match. Carried so the ledger can tell two meetings
    # of the same fixture apart -- without it, this season's bet and last
    # season's look identical.
    match_date: str = ''
    # What Kelly asked for before the per-bet cap. Equal to stake when no cap
    # fired, so the pair is always safe to compare and a clamp is never
    # invisible: the report shows both numbers side by side.
    uncapped_stake: float = 0.0
    uncapped_stake_fraction: float = 0.0
    capped: bool = False
    # Sample size and interval behind the band probability. INSTRUMENTATION
    # ONLY -- nothing in this module reads it, and nothing in it sizes a bet.
    band_evidence: dict = field(default_factory=dict)

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
            + (f' [CAPPED from {self.uncapped_stake:.2f}]' if self.capped
               else '')
        )


def size_bet(fixture, market, selection, probability, odds, bankroll,
             confidence=STANDARD, min_ev=MIN_EV, match_date='',
             implausible_edge=IMPLAUSIBLE_EDGE, band_evidence=None):
    """
    Apply the staking rule to a single selection.

    Always returns a StakeDecision -- rejections carry their reason and the
    arithmetic that produced it, so a skipped bet is as auditable as a placed
    one.

    An edge at or above `implausible_edge` downgrades a STANDARD play to
    LARGE_EDGE, which is Eighth-Kelly. The downgrade happens here, where the
    stake is computed, rather than in the sanity checks -- a check that runs
    after the arithmetic can only comment on it, which is exactly the defect
    this replaces. It only ever moves DOWN: a play already at Eighth-Kelly is
    left alone, and nothing here raises a multiplier.
    """
    if confidence not in MULTIPLIER:
        raise ValueError(
            f'unknown confidence class {confidence!r}; '
            f'expected one of {sorted(MULTIPLIER)}')

    ev = expected_value(probability, odds)
    kelly = full_kelly_fraction(probability, odds)

    flags = []
    if ev >= implausible_edge:
        flags.append('implausible_edge')
        if MULTIPLIER[confidence] > EIGHTH_KELLY:
            confidence = LARGE_EDGE

    multiplier = MULTIPLIER[confidence]

    def decision(bet, reason, stake_fraction=0.0):
        stake = round(bankroll * stake_fraction, 2)
        return StakeDecision(
            fixture=fixture, market=market, selection=selection,
            model_probability=probability, bookmaker_odds=odds,
            expected_value=ev, full_kelly=kelly, confidence=confidence,
            multiplier=multiplier, stake_fraction=stake_fraction,
            stake=stake,
            uncapped_stake=stake, uncapped_stake_fraction=stake_fraction,
            bet=bet, reason=reason, match_date=match_date,
            flags=list(flags), band_evidence=dict(band_evidence or {}))

    if bankroll <= 0:
        return decision(False, 'bankroll exhausted')
    if ev < min_ev:
        return decision(
            False,
            f'edge {ev * 100:+.2f}% below the {min_ev * 100:.0f}% floor')
    if kelly <= 0:
        return decision(False, 'full Kelly non-positive')

    reason = f'{confidence} play at {multiplier} Kelly'
    if confidence == LARGE_EDGE:
        reason += (f' (edge {ev * 100:+.1f}% at or above the '
                   f'{implausible_edge * 100:.0f}% implausible-edge '
                   f'threshold; sized down from Quarter)')
    return decision(True, reason, stake_fraction=multiplier * kelly)


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


def check_implausible_edge(decisions, threshold=IMPLAUSIBLE_EDGE):
    """
    Report edges large enough to be suspicious. NON-BLOCKING by design.

    The band model prices by ELO band, so every fixture in a band shares one
    set of probabilities -- Man Utd away at Brighton is priced identically to
    Leeds away at West Ham if both sit in Band 1. Against a sharp market, a
    +30% edge from that model usually means the price is stale or the band is
    too coarse for the fixture, not that the market is wrong by a third.

    The SIZING consequence is not applied here. `size_bet` has already halved
    the stake to Eighth-Kelly by the time this runs -- this function exists so
    the reason appears in the run report, and so a number that large cannot
    pass unremarked. It does not block: a large edge is sized down, not
    refused.

    Historical note: this check was once advisory only, and changed nothing
    about the stake. That is the defect fixed here; the check reports, and
    `size_bet` sizes.
    """
    findings = []
    for d in decisions:
        if d.bet and d.expected_value >= threshold:
            sized = ('sized at Eighth-Kelly' if d.multiplier == EIGHTH_KELLY
                     else f'sized at {d.multiplier} Kelly')
            findings.append(SanityFinding(
                kind='implausible_edge',
                fixture=d.fixture,
                detail=(f'{d.selection} shows {d.expected_value * 100:+.1f}% edge, '
                        f'at or above the {threshold * 100:.0f}% implausible-edge '
                        f'threshold, so it is {sized} rather than Quarter. '
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
        d.uncapped_stake = 0.0
        d.uncapped_stake_fraction = 0.0
        d.capped = False
        d.reason = f'blocked by sanity check: {hits[0].kind}'
        d.flags.extend(f.kind for f in hits)

    return decisions, findings


# --- exposure caps --------------------------------------------------------
#
# Kelly sizes a bet against the bankroll but knows nothing about the rest of
# the book, and nothing about how wrong the model might be. The caps are the
# operator's answer to both. They are applied AFTER Kelly and after the sanity
# checks, and they only ever reduce a stake.
#
# The two caps behave differently ON PURPOSE:
#
#   per-bet   -- clamped automatically. One oversized bet is a local problem
#                with an obvious fix, and the clamp is logged every time.
#   weekly    -- FLAGGED, never rescaled. Deciding which bet to drop, or
#                whether to take the week at all, is a portfolio judgement.
#                Silently shrinking six stakes to fit a number would hide that
#                decision rather than surface it.


@dataclass
class ClampEvent:
    """One firing of the per-bet cap, kept for the log and the report."""
    fixture: str
    market: str
    selection: str
    calculated_stake: float
    capped_stake: float
    cap_amount: float
    cap_fraction: float
    bankroll: float

    @property
    def reduction(self):
        return round(self.calculated_stake - self.capped_stake, 2)

    def describe(self):
        return (
            f'{self.fixture} / {self.selection}: Kelly asked for '
            f'{self.calculated_stake:.2f} '
            f'({self.calculated_stake / self.bankroll * 100:.2f}% of '
            f'{self.bankroll:.2f}); capped to {self.capped_stake:.2f} at the '
            f'{self.cap_fraction * 100:.1f}% per-bet limit '
            f'(-{self.reduction:.2f})')


def staking_limits(config=None):
    """
    Resolve the exposure caps from run config, falling back to the defaults.

    Config lives in pipeline_config.json under "staking_limits". It is read
    here rather than at the call site so there is one place the values come
    from, and so the numbers are never written into the sizing code.

    bankroll.json is deliberately NOT the home for these: Ledger.save()
    rewrites that file from a fixed set of keys, so a limit added there would
    be silently discarded on the next live run.
    """
    limits = (config or {}).get('staking_limits') or {}
    per_bet = float(limits.get('max_stake_fraction', MAX_STAKE_FRACTION))
    weekly = float(limits.get('max_weekly_stake_fraction',
                              MAX_WEEKLY_STAKE_FRACTION))

    for name, value in (('max_stake_fraction', per_bet),
                        ('max_weekly_stake_fraction', weekly)):
        if not 0 < value <= 1:
            raise ValueError(
                f'{name} must be a fraction in (0, 1], got {value}')
    if per_bet > weekly:
        raise ValueError(
            f'max_stake_fraction ({per_bet}) exceeds '
            f'max_weekly_stake_fraction ({weekly}); a single bet could never '
            f'be placed without breaching the weekly cap')
    return per_bet, weekly


def apply_stake_cap(decisions, bankroll, max_fraction=MAX_STAKE_FRACTION,
                    logger=None):
    """
    Clamp every live stake to `max_fraction` of the bankroll.

    Mutates decisions in place and returns (decisions, clamp_events). The
    pre-cap figure survives on `uncapped_stake` / `uncapped_stake_fraction`,
    so the report can show what Kelly wanted beside what was actually staked
    -- a clamp is never invisible.

    `stake_fraction` is recomputed from the capped stake so that
    stake == bankroll x stake_fraction continues to hold; `multiplier` and
    `full_kelly` are left untouched, because the Kelly arithmetic did not
    change, only the amount permitted through it.
    """
    events = []
    if bankroll <= 0:
        return decisions, events

    cap_amount = round(bankroll * max_fraction, 2)

    for d in decisions:
        if not d.bet:
            continue
        # Idempotent: a decision already clamped keeps the ORIGINAL pre-cap
        # figure, so a second pass cannot quietly record the capped stake as
        # what Kelly asked for, or append the cap note to `reason` twice.
        if not d.capped:
            d.uncapped_stake = d.stake
            d.uncapped_stake_fraction = d.stake_fraction
        if d.stake <= cap_amount:
            continue

        event = ClampEvent(
            fixture=d.fixture, market=d.market, selection=d.selection,
            calculated_stake=d.stake, capped_stake=cap_amount,
            cap_amount=cap_amount, cap_fraction=max_fraction,
            bankroll=bankroll)
        events.append(event)

        d.stake = cap_amount
        d.stake_fraction = cap_amount / bankroll
        d.capped = True
        if 'per_bet_cap' not in d.flags:
            d.flags.append('per_bet_cap')
        d.reason += (f'; capped at {max_fraction * 100:.1f}% of bankroll '
                     f'({event.calculated_stake:.2f} -> {cap_amount:.2f})')

        (logger or print)(f'  [CAP] {event.describe()}')

    return decisions, events


def check_weekly_exposure(decisions, bankroll,
                          max_fraction=MAX_WEEKLY_STAKE_FRACTION):
    """
    Total the week's live stakes against the weekly cap.

    Returns a dict describing the week's exposure whether or not the cap is
    breached, and a SanityFinding when it is. The finding does NOT block and
    nothing is rescaled: the total is put in front of the operator so the
    decision about which bet to drop stays theirs.

    Call this AFTER apply_stake_cap -- the weekly total is a total of the
    stakes actually going on, not of what Kelly first asked for.
    """
    live = [d for d in decisions if d.bet]
    total = round(sum(d.stake for d in live), 2)
    limit = round(bankroll * max_fraction, 2)
    fraction = (total / bankroll) if bankroll > 0 else 0.0
    breached = total > limit

    exposure = {
        'total_stake': total,
        'bankroll': round(bankroll, 2),
        'exposure_fraction': round(fraction, 5),
        'exposure_pct': round(fraction * 100, 2),
        'weekly_cap_fraction': max_fraction,
        'weekly_cap_amount': limit,
        'over_cap_by': round(total - limit, 2) if breached else 0.0,
        'bets': len(live),
        'breached': breached,
        # Stated in the report so nobody has to infer it from the absence of
        # a change: the pipeline did not touch these stakes.
        'action_taken': ('none -- flagged for operator decision, stakes NOT '
                         'rescaled' if breached else 'none required'),
    }

    finding = None
    if breached:
        finding = SanityFinding(
            kind='weekly_exposure_exceeded',
            fixture='(portfolio)',
            detail=(f"{len(live)} bet(s) total {total:.2f}, which is "
                    f"{fraction * 100:.2f}% of the {bankroll:.2f} staking "
                    f"bankroll and exceeds the {max_fraction * 100:.0f}% "
                    f"weekly cap ({limit:.2f}) by {total - limit:.2f}. "
                    f"Stakes have NOT been rescaled -- drop or resize bets "
                    f"before placing."),
            selections=[d.selection for d in live],
            blocks=False)

    return exposure, finding
