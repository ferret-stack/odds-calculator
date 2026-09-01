"""
Standalone historical backtest harness for the ELO band model.

READ-ONLY against production. This package imports from `elo_calculator`,
`tools/rebuild_elo.py` and `pipeline/staking.py`; it writes nothing outside
`backtest/out/` and never touches `data/`, `bankroll.json` or the pipeline.
"""
