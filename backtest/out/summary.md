# ELO band model vs. market closing odds — historical backtest

- **Bands**: `walkforward`  (rebuilt at each match date from matches completed strictly before it)
- **Seasons**: 2020-21, 2021-22, 2022-23, 2023-24, 2024-25, 2025-26 (2,280 matches)
- **Warm-up (no bets)**: 2020-21 — 1,900 matches priced
- **Staking**: production `pipeline.staking` — Quarter-Kelly at the +5% EV floor, Eighth-Kelly at or above +20%, 3% per-bet cap, notional bankroll 1,000
- **Anomalies logged**: degenerate_band x8, empty_band x13

`yield_pct` = profit / staked at Kelly sizing. `flat_roi_pct` = profit / bets at a flat 1-unit stake. `market_fair` = de-vigged market probability.

## AvgH/AvgD/AvgA — market-average closing odds (primary)

Selections considered: 5,700 · bets placed: 1,298 · blocked by same-market sanity check: 288 · sized down to Eighth-Kelly for a >=20% edge: 548 · per-bet cap fired: 296

Compounded bankroll: 1,000 -> 67.46 · max drawdown 94.10%

### Overall

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| all bets | 1,298 | 22,972.80 | -2,350.70 | -10.23 | -11.68 | 31.74 | 0.42 | 0.34 | 23.29 | 3.98 |


### By selection (home / draw / away)

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| home | 372 | 7,888.17 | -917.53 | -11.63 | -11.90 | 39.25 | 0.50 | 0.42 | 19.91 | 3.02 |
| draw | 105 | 856.62 | -278.17 | -32.47 | -23.81 | 15.24 | 0.23 | 0.18 | 22.68 | 5.48 |
| away | 821 | 14,228.01 | -1,155.00 | -8.12 | -10.03 | 30.45 | 0.41 | 0.32 | 24.90 | 4.23 |


### By ELO band

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| band 1 | 328 | 4,938.12 | -331.95 | -6.72 | -10.68 | 26.83 | 0.37 | 0.29 | 20.93 | 3.41 |
| band 2 | 291 | 5,033.26 | -67.30 | -1.34 | 2.30 | 33.33 | 0.40 | 0.32 | 20.36 | 3.36 |
| band 3 | 249 | 5,335.88 | -607.73 | -11.39 | -25.40 | 34.54 | 0.48 | 0.38 | 22.13 | 3.40 |
| band 4 | 157 | 2,780.37 | -352.87 | -12.69 | -16.46 | 30.57 | 0.40 | 0.32 | 29.06 | 4.94 |
| band 5 | 135 | 2,408.00 | -570.06 | -23.67 | -20.96 | 31.11 | 0.45 | 0.37 | 19.07 | 4.20 |
| band 6 | 76 | 1,210.42 | -285.20 | -23.56 | -1.07 | 28.95 | 0.42 | 0.33 | 31.80 | 7.07 |
| band 7 | 29 | 441.68 | -141.82 | -32.11 | 0.76 | 20.69 | 0.38 | 0.29 | 46.90 | 6.55 |
| band 8 | 25 | 609.22 | 18.68 | 3.07 | -16.28 | 68.00 | 0.76 | 0.60 | 36.49 | 5.79 |
| band 9 | 6 | 155.85 | -19.35 | -12.42 | -24.17 | 66.67 | 0.87 | 0.73 | 14.22 | 1.93 |
| band 10 | 2 | 60.00 | 6.90 | 11.50 | 11.50 | 100.00 | 1.00 | 0.86 | 11.50 | 1.12 |


### By edge size

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| 5-10% | 304 | 4,219.29 | -712.76 | -16.89 | -21.56 | 32.57 | 0.43 | 0.38 | 7.40 | 3.20 |
| 10-20% | 446 | 9,212.94 | -335.96 | -3.65 | -7.23 | 37.44 | 0.45 | 0.37 | 14.80 | 3.26 |
| 20%+ | 548 | 9,540.57 | -1,301.98 | -13.65 | -9.82 | 26.64 | 0.39 | 0.28 | 39.02 | 5.00 |


### Away-underdog x implausible-edge (the Theme 2 cut)

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| away-underdog, edge <20% | 152 | 1,190.55 | -311.23 | -26.14 | -38.01 | 13.82 | 0.23 | 0.20 | 12.09 | 5.33 |
| away-underdog, edge >=20% | 160 | 1,300.32 | 27.17 | 2.09 | 7.76 | 16.25 | 0.20 | 0.13 | 44.90 | 8.73 |
| other selection, edge <20% | 598 | 12,241.68 | -737.49 | -6.02 | -6.69 | 40.97 | 0.49 | 0.42 | 11.72 | 2.70 |
| other selection, edge >=20% | 388 | 8,240.25 | -1,329.15 | -16.13 | -17.07 | 30.93 | 0.48 | 0.34 | 36.60 | 3.46 |


### Away underdog AND edge >=20%, isolated

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| away underdog with edge >=20% | 160 | 1,300.32 | 27.17 | 2.09 | 7.76 | 16.25 | 0.20 | 0.13 | 44.90 | 8.73 |


### By season

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| 2021-22 | 285 | 5,413.45 | -955.00 | -17.64 | -24.83 | 31.93 | 0.45 | 0.36 | 28.57 | 4.02 |
| 2022-23 | 290 | 4,999.99 | -303.35 | -6.07 | -7.80 | 31.38 | 0.41 | 0.34 | 21.21 | 4.01 |
| 2023-24 | 252 | 4,490.87 | -752.73 | -16.76 | -18.99 | 28.57 | 0.41 | 0.33 | 24.33 | 4.34 |
| 2024-25 | 250 | 4,232.47 | 62.39 | 1.47 | 7.17 | 35.60 | 0.42 | 0.34 | 20.74 | 3.81 |
| 2025-26 | 221 | 3,836.02 | -402.01 | -10.48 | -12.79 | 31.22 | 0.41 | 0.32 | 20.92 | 3.68 |


### Calibration: model probability vs. what happened

| model_prob_range | bets | mean_model_prob | actual_win_rate | mean_market_fair |
|---|---|---|---|---|
| 0.00-0.20 | 196 | 0.15 | 0.07 | 0.12 |
| 0.20-0.30 | 282 | 0.26 | 0.22 | 0.21 |
| 0.30-0.40 | 169 | 0.36 | 0.24 | 0.29 |
| 0.40-0.50 | 202 | 0.45 | 0.36 | 0.36 |
| 0.50-0.60 | 201 | 0.54 | 0.42 | 0.44 |
| 0.60-1.01 | 248 | 0.73 | 0.56 | 0.60 |


## B365H/B365D/B365A — single bookmaker (secondary)

Selections considered: 5,700 · bets placed: 1,254 · blocked by same-market sanity check: 248 · sized down to Eighth-Kelly for a >=20% edge: 531 · per-bet cap fired: 270

Compounded bankroll: 1,000 -> 82.74 · max drawdown 93.24%

### Overall

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| all bets | 1,254 | 21,982.74 | -2,149.88 | -9.78 | -11.13 | 31.50 | 0.42 | 0.33 | 23.10 | 3.98 |


### By selection (home / draw / away)

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| home | 359 | 7,445.52 | -1,065.78 | -14.31 | -14.76 | 38.72 | 0.50 | 0.41 | 19.32 | 3.03 |
| draw | 106 | 852.26 | -210.80 | -24.73 | -20.93 | 15.09 | 0.23 | 0.18 | 23.54 | 5.54 |
| away | 789 | 13,684.96 | -873.30 | -6.38 | -8.16 | 30.42 | 0.40 | 0.32 | 24.77 | 4.20 |


### By ELO band

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| band 1 | 320 | 4,751.91 | -288.45 | -6.07 | -8.83 | 27.50 | 0.37 | 0.29 | 20.73 | 3.40 |
| band 2 | 285 | 4,898.24 | 207.82 | 4.24 | 6.56 | 33.33 | 0.39 | 0.31 | 21.39 | 3.48 |
| band 3 | 239 | 5,061.09 | -741.54 | -14.65 | -32.18 | 32.22 | 0.47 | 0.37 | 22.87 | 3.56 |
| band 4 | 145 | 2,631.62 | -382.70 | -14.54 | -13.63 | 31.72 | 0.41 | 0.32 | 27.48 | 4.68 |
| band 5 | 131 | 2,349.50 | -622.98 | -26.52 | -21.86 | 30.53 | 0.45 | 0.37 | 19.02 | 4.17 |
| band 6 | 73 | 1,094.00 | -275.29 | -25.16 | -4.56 | 28.77 | 0.41 | 0.33 | 28.02 | 6.83 |
| band 7 | 30 | 410.33 | -55.99 | -13.65 | 10.43 | 20.00 | 0.34 | 0.25 | 45.71 | 6.61 |
| band 8 | 24 | 576.05 | 16.75 | 2.91 | -17.67 | 66.67 | 0.75 | 0.59 | 32.74 | 5.39 |
| band 9 | 5 | 150.00 | -13.80 | -9.20 | -9.20 | 80.00 | 1.00 | 0.83 | 14.80 | 1.15 |
| band 10 | 2 | 60.00 | 6.30 | 10.50 | 10.50 | 100.00 | 1.00 | 0.85 | 10.50 | 1.10 |


### By edge size

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| 5-10% | 302 | 4,258.19 | -839.06 | -19.70 | -15.80 | 33.11 | 0.43 | 0.38 | 7.48 | 3.17 |
| 10-20% | 420 | 8,742.44 | -67.84 | -0.78 | -3.58 | 38.57 | 0.45 | 0.37 | 14.84 | 3.24 |
| 20%+ | 532 | 8,982.11 | -1,242.98 | -13.84 | -14.44 | 25.00 | 0.38 | 0.27 | 38.50 | 5.02 |


### Away-underdog x implausible-edge (the Theme 2 cut)

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| away-underdog, edge <20% | 140 | 1,106.07 | -81.79 | -7.39 | -13.23 | 17.86 | 0.23 | 0.20 | 11.99 | 5.27 |
| away-underdog, edge >=20% | 167 | 1,343.96 | 56.53 | 4.21 | -4.60 | 14.97 | 0.20 | 0.14 | 41.32 | 8.21 |
| other selection, edge <20% | 583 | 11,924.56 | -855.11 | -7.17 | -7.76 | 40.65 | 0.49 | 0.42 | 11.72 | 2.71 |
| other selection, edge >=20% | 364 | 7,608.15 | -1,269.51 | -16.69 | -18.71 | 29.67 | 0.47 | 0.33 | 37.25 | 3.57 |


### Away underdog AND edge >=20%, isolated

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| away underdog with edge >=20% | 167 | 1,343.96 | 56.53 | 4.21 | -4.60 | 14.97 | 0.20 | 0.14 | 41.32 | 8.21 |


### By season

| bucket | bets | staked | profit | yield_pct | flat_roi_pct | strike_rate_pct | mean_model_prob | mean_market_fair | mean_edge_pct | mean_odds |
|---|---|---|---|---|---|---|---|---|---|---|
| 2021-22 | 276 | 5,188.79 | -985.34 | -18.99 | -26.95 | 31.52 | 0.45 | 0.35 | 27.45 | 3.97 |
| 2022-23 | 276 | 4,712.85 | -430.48 | -9.13 | -10.08 | 30.43 | 0.41 | 0.33 | 20.98 | 4.01 |
| 2023-24 | 253 | 4,404.05 | -575.00 | -13.06 | -18.55 | 28.06 | 0.40 | 0.32 | 24.85 | 4.43 |
| 2024-25 | 230 | 3,936.33 | -16.89 | -0.43 | 7.60 | 35.22 | 0.42 | 0.33 | 21.24 | 3.79 |
| 2025-26 | 219 | 3,740.72 | -142.17 | -3.80 | -3.61 | 32.88 | 0.40 | 0.32 | 20.23 | 3.62 |


### Calibration: model probability vs. what happened

| model_prob_range | bets | mean_model_prob | actual_win_rate | mean_market_fair |
|---|---|---|---|---|
| 0.00-0.20 | 188 | 0.16 | 0.06 | 0.12 |
| 0.20-0.30 | 286 | 0.26 | 0.22 | 0.20 |
| 0.30-0.40 | 167 | 0.36 | 0.25 | 0.29 |
| 0.40-0.50 | 193 | 0.45 | 0.36 | 0.36 |
| 0.50-0.60 | 183 | 0.54 | 0.42 | 0.43 |
| 0.60-1.01 | 237 | 0.73 | 0.55 | 0.60 |

