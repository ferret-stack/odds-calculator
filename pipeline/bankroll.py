"""
Bankroll and PnL tracking.

An append-only ledger in data/bankroll.json. Every placed bet is written when
it is struck and settled later by result, so the bankroll each run stakes
against is derived from settled history rather than typed in by hand.

Kelly is a fraction OF CURRENT BANKROLL, so this file is not bookkeeping
sitting beside the staking rule -- it is an input to it. Staking against a
stale balance silently changes the effective fraction.

Open (unsettled) bets are excluded from the staking balance. Their stake is
already committed and cannot be staked twice.
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

PENDING = 'pending'
WON = 'won'
LOST = 'lost'
VOID = 'void'

SETTLED = {WON, LOST, VOID}


@dataclass
class Bet:
    bet_id: str
    placed_at: str
    fixture: str
    market: str
    selection: str
    odds: float
    stake: float
    model_probability: float
    expected_value: float
    confidence: str
    kelly_multiplier: float
    status: str = PENDING
    settled_at: Optional[str] = None
    returned: float = 0.0
    notes: str = ''

    @property
    def profit(self):
        """Profit relative to stake. Pending bets have not resolved: 0.0."""
        if self.status == PENDING:
            return 0.0
        return round(self.returned - self.stake, 2)


class Ledger:
    def __init__(self, path='data/bankroll.json', starting_bankroll=1000.0):
        self.path = Path(path)
        self.starting_bankroll = starting_bankroll
        self.bets = []
        self._load()

    # --- persistence ------------------------------------------------------

    def _load(self):
        if not self.path.exists():
            return
        data = json.loads(self.path.read_text())
        self.starting_bankroll = data.get('starting_bankroll',
                                          self.starting_bankroll)
        self.bets = [Bet(**b) for b in data.get('bets', [])]

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({
            'starting_bankroll': self.starting_bankroll,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'summary': self.summary(),
            'bets': [asdict(b) for b in self.bets],
        }, indent=2))

    # --- balances ---------------------------------------------------------

    @property
    def realised_pnl(self):
        return round(sum(b.profit for b in self.bets if b.status in SETTLED), 2)

    @property
    def committed(self):
        """Stake tied up in bets that have not settled."""
        return round(sum(b.stake for b in self.bets if b.status == PENDING), 2)

    @property
    def bankroll(self):
        """Settled balance: what the ledger says we are worth."""
        return round(self.starting_bankroll + self.realised_pnl, 2)

    @property
    def staking_bankroll(self):
        """
        The balance Kelly sizes against.

        Excludes stake already committed to open bets -- that money is spent
        and staking a fraction of it again would over-bet the book.
        """
        return round(self.bankroll - self.committed, 2)

    # --- mutation ---------------------------------------------------------

    def place(self, decision, bet_id=None, placed_at=None):
        """Record a StakeDecision as a struck bet."""
        if not decision.bet:
            raise ValueError('refusing to place a decision marked bet=False')

        bet = Bet(
            bet_id=bet_id or f'{len(self.bets) + 1:05d}',
            placed_at=placed_at or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            fixture=decision.fixture,
            market=decision.market,
            selection=decision.selection,
            odds=decision.bookmaker_odds,
            stake=decision.stake,
            model_probability=decision.model_probability,
            expected_value=round(decision.expected_value, 4),
            confidence=decision.confidence,
            kelly_multiplier=decision.multiplier,
        )
        self.bets.append(bet)
        return bet

    def settle(self, bet_id, status, returned=None, notes=''):
        """
        Settle a bet.

        `returned` is the gross return including stake. It defaults to
        stake x odds for a win, 0 for a loss, and stake for a void -- so a
        void is PnL-neutral rather than a loss.
        """
        if status not in SETTLED:
            raise ValueError(f'status must be one of {sorted(SETTLED)}')

        bet = next((b for b in self.bets if b.bet_id == bet_id), None)
        if bet is None:
            raise KeyError(f'no bet with id {bet_id}')
        if bet.status != PENDING:
            raise ValueError(f'bet {bet_id} already settled as {bet.status}')

        if returned is None:
            returned = {
                WON: round(bet.stake * bet.odds, 2),
                LOST: 0.0,
                VOID: bet.stake,
            }[status]

        bet.status = status
        bet.returned = round(returned, 2)
        bet.settled_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        bet.notes = notes
        return bet

    # --- reporting --------------------------------------------------------

    def summary(self):
        settled = [b for b in self.bets if b.status in SETTLED]
        counted = [b for b in settled if b.status != VOID]
        staked = sum(b.stake for b in counted)
        won = [b for b in counted if b.status == WON]
        roi = round(self.realised_pnl / staked, 4) if staked else None

        return {
            'starting_bankroll': round(self.starting_bankroll, 2),
            'bankroll': self.bankroll,
            'staking_bankroll': self.staking_bankroll,
            'realised_pnl': self.realised_pnl,
            'committed_to_open_bets': self.committed,
            'bets_total': len(self.bets),
            'bets_open': sum(1 for b in self.bets if b.status == PENDING),
            'bets_settled': len(settled),
            'bets_void': sum(1 for b in settled if b.status == VOID),
            'wins': len(won),
            'losses': len(counted) - len(won),
            'strike_rate': round(len(won) / len(counted), 4) if counted else None,
            'total_staked': round(staked, 2),
            # ROI is profit per unit staked -- the measure that survives
            # varying stake sizes, which Kelly guarantees we will have.
            # Carried twice: 'roi' is the fraction the pipeline reports
            # against, 'roi_pct' the same number as a percentage, which is
            # what anything display-facing wants.
            'roi': roi,
            'roi_pct': round(roi * 100, 2) if roi is not None else None,
            'growth': (round(self.bankroll / self.starting_bankroll - 1, 4)
                       if self.starting_bankroll else None),
        }

    def format_summary(self):
        s = self.summary()
        lines = [
            f"  starting bankroll   : {s['starting_bankroll']:.2f}",
            f"  current bankroll    : {s['bankroll']:.2f}",
            f"  open bets           : {s['bets_open']} "
            f"({s['committed_to_open_bets']:.2f} committed)",
            f"  staking bankroll    : {s['staking_bankroll']:.2f}",
            f"  realised PnL        : {s['realised_pnl']:+.2f}",
            f"  settled             : {s['bets_settled']} "
            f"({s['wins']}W / {s['losses']}L / {s['bets_void']}V)",
        ]
        if s['strike_rate'] is not None:
            lines.append(f"  strike rate         : {s['strike_rate']:.1%}")
        if s['roi'] is not None:
            lines.append(f"  ROI                 : {s['roi']:+.2%} "
                         f"on {s['total_staked']:.2f} staked")
        if s['growth'] is not None:
            lines.append(f"  bankroll growth     : {s['growth']:+.2%}")
        return '\n'.join(lines)
