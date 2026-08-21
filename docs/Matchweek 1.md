# Matchweek 1

## layout: post title: Matchweek 1 - A Fresh Bankroll and the First Override date: 2026-08-21 ferret: boolean author: Ferret Stack

## 🧠 This week's best bets

> **TL;DR:** New season, new bankroll. £1,000, reset from scratch, and this is the first live run of the rebuilt pipeline — the automated system that prices fixtures, sizes stakes at Quarter-Kelly, and writes to the ledger itself. The model found seven +EV selections. The operator pulled two of them before either reached a bookmaker. What's actually on the book: **five bets, all match-result, £114.37 staked (11.44% of bankroll)**, stake-weighted expected value **+27.85%**.
>
> 💰 **Total Portfolio EV:** +27.85% (stake-weighted) 📊 **Bankroll Stake:** £114.37 of £1,000 (11.44%) ⚡ **Theme:** Band 1 was broken last season — this is the week we start betting on the fix, and the week we first overruled it.

**See for yourself:** [Odds Calculator](https://ferret-stack.github.io/boolean/odds-calculator/)

Consider **following me on X** for live-play updates and betting thoughts

- Click [here](https://x.com/intent/post?text=@Ferret_Stack%20I%20still%20call%20it%20Twitter) if you still call it "Twitter"
- Click [here](https://x.com/intent/post?text=@Ferret_Stack%20I%20call%20it%20X) if you call it "X"

---

## 📊 Last season, briefly

We're not going to pretend we know exactly where last season ended. The matchweek posts on this blog run through MW34, and the record from there to the final whistle of 2025/26 isn't one we're carrying forward cleanly, so rather than quote a number we can't stand behind, we're drawing a line under it. The point of the £100 challenge was never the final balance — it was building and stress-testing the pipeline in public, warts and all. That pipeline is now good enough to run itself, which is the actual headline this week.

So: fresh season, fresh ledger, fresh bankroll. **£1,000**, opened 2026-08-21. The old ledger is archived, not deleted, and we'll keep pointing back at it when it's useful. From here on the numbers are new.

---

## 📈 The bigger picture

New table, new base. One row so far:

| Week | Starting | Bets | Wins | Staked | P/L | Ending |
| --- | --- | --- | --- | --- | --- | --- |
| MW1 | £1,000.00 | 5 (7 priced, 2 overridden pre-placement) | — | £114.37 | — | — |

We'll build this out week by week from here, same as before. The only wrinkle worth flagging now: MW1 shows 5 bets on the book, not the 7 the model actually priced — see below for why.

---

## 🎯 This week's theme: The fix, and the first override

Band 1 — fixtures where the two teams are within 50 ELO points of each other — was the model's worst-performing band last season. Before this month's audit, it had the **weaker** team winning 43.07% of the time in that band and the stronger team just 29.59%. That's not noise, that's inverted. After the fix, the same band reads 41.15% stronger / 33.89% weaker — the right way round.

Matchweek 1 hands us about as much exposure to that fix as a slate can: **five of this week's ten fixtures are Band 1**. This is the first week we're genuinely betting on the repaired band structure.

It's also the first week we've overruled it. Two of the seven fixtures the model cleared for betting were pulled by hand before either reached a bookmaker — both Band 1, both away-side picks on the stronger team, both already flagged by the Poisson cross-check as disagreeing with the band model. We'll get into the mechanics of both stories below, because they're the same story from two different angles.

One honest caveat before the picks: this is also the model's **blindest** week of the year. Every "last 10 games" figure it's using is drawn from last season — there's no current-season signal anywhere in the numbers yet. The ELO band structure is doing almost all of the work this week; the Poisson layer is running entirely on May's data. We'll flag that again where it matters.

---

## 🔥 PREMIUM PICK: Fulham Win vs Chelsea

**The Bet:** Fulham to win @ **3.66** (27.3% implied) **Model Probability:** 37.5% (fair odds: 2.67) **Expected Value:** **+37.18%** **Stake:** Quarter-Kelly, £34.94 (3.49% of bankroll) **Confidence:** Standard

This is the biggest edge on the card, and it comes from a genuinely fat overround — 107.18%, the week's most expensive market. The book has Chelsea at 1.88, a 53% favourite, on what's actually only a 32-point ELO gap (Fulham 1790, 12th; Chelsea 1822, 9th — Band 1). The model reads that gap as close to a coin-flip with a slight lean to the visitors. That's a 17-point disagreement between the book and the model, and it's the largest one on the slate.

Form backs the model up more than we expected. Fulham are 5.4/10, stable — nothing special, and the lowest scoring rate on the whole card at 0.7 goals for per game. But Chelsea are 3.6/10 and **declining**, with a ΔELO of −25 over their last five and −53 over their last ten — the worst 10-game ELO slide of any side in these ten fixtures. Head-to-head over the last ten meetings favours Chelsea 6-3-1, but the most recent one is a 2-1 Fulham win, at Craven Cottage, last January.

The Poisson cross-check agrees emphatically here — more emphatically than the band model itself (+73.4% EV on the Poisson view). It also thinks this is a low-scoring, tight game: modal score 1-0 to Fulham at 22.16%, with 0-0 close behind at 21.79%. Both are the two lowest-scoring outcomes on the entire card. Take that agreement with a grain of salt given the caveat above about May's data, but two independent models landing on the same conclusion, for different-sounding reasons, is worth noting.

---

## 🔥 PREMIUM PICK: Brentford Win vs Spurs

**The Bet:** Brentford to win @ **2.42** (41.3% implied) **Model Probability:** 54.0% (fair odds: 1.85) **Expected Value:** **+30.78%** **Stake:** Quarter-Kelly, £54.18 (5.42% of bankroll) **Confidence:** Standard

The biggest single stake of the week, and the fixture where the model and the book disagree most sharply on outright direction. Brentford are 69 ELO points better than Spurs (1819 vs 1750, Band 2), and the model gives them a 54% chance of winning outright. The book has the two sides almost level — 41.3% Brentford, 36.4% Spurs — pricing Spurs at 2.75 to win away at a team meaningfully better than them.

We'll say the quiet part: form actually runs against this bet. Brentford are 5.3/10, stable, with a negative ten-game ELO trend (−10). Spurs are 6.9/10 and **improving**, up 17 over their last five games. If you're betting on form alone, this looks like the wrong side. We're not betting on form alone — we're betting that a 69-point ELO gap is a real, structural edge that a five-game form wobble doesn't erase, and the book is pricing this fixture off reputation and recency rather than the underlying gap.

Head-to-head over the last ten meetings leans Spurs (5-4-1), and the most recent meeting was a 0-0 draw. The Poisson cross-check **softens but doesn't contradict** the pick — it drops the edge to +11.3% EV rather than +30.8%, with Brentford 1.178 xG against Spurs' 0.751, and a modal score of 1-0 (17.12%). Softening, not reversing, is the tell here: two different models, pointed the same direction, disagreeing only on magnitude.

---

## 💎 VALUE PICK: Newcastle Win vs Liverpool

**The Bet:** Newcastle to win @ **3.82** (26.2% implied) **Model Probability:** 29.5% (fair odds: 3.39) **Expected Value:** **+12.58%** **Stake:** Quarter-Kelly, £11.15 (1.11% of bankroll) **Confidence:** Standard

A modest, clean edge. Newcastle (1826, 7th) are 69 points behind Liverpool (1895, 3rd) — Band 2 — and the model agrees Liverpool are favourites. It just doesn't think they're 53% favourites at St James' Park, which is what the book is pricing. A nice sanity-check detail: the draw price in this market is almost exactly what the model would also give it — the book gets things right more often than not, and this is a case of it being slightly off on the win market specifically.

Form doesn't do Newcastle any favours on its own — they're 5.6/10, stable — but Liverpool are **declining**, 3.5/10, with ΔELO of −22 over five games and −36 over ten. Head-to-head is brutal against this bet, worth saying plainly: Liverpool have won 9 of the last 12 meetings, Newcastle none, and the most recent was a 4-1 Liverpool win in January.

The Poisson cross-check agrees strongly here — Newcastle 1.591 xG vs Liverpool's 1.301, home win at 44.18% and +68.8% EV on this price, modal score 1-1 (11.48%). Where the band model is cautious, Poisson is enthusiastic; we're pricing this conservatively at the band model's number, not the Poisson one.

---

## 💰 SOLID PICK: Man City / Bournemouth Draw

**The Bet:** Draw @ **4.79** (20.9% implied) **Model Probability:** 22.2% (fair odds: 4.50) **Expected Value:** **+6.34%** **Stake:** Quarter-Kelly, £4.18 (0.42% of bankroll) **Confidence:** Standard

The smallest stake of the week, and the edge reflects it. This isn't a case of the model thinking Man City (1987, 2nd) don't win — they're still the clear favourites against Bournemouth (1830, 6th) on a 157-point gap, Band 4. It's that the book has City at 68.5% to win outright, and the model thinks that's a touch high, with the draw slightly underpriced at 4.79 against a fair price of 4.50.

City's form rating is a surprisingly unremarkable 3.9/10, stable, for the league's second-ranked side (ΔELO −11 over five games). Bournemouth are the opposite story: 6.0/10 and improving, up 17 over five games and 35 over ten, with the best defensive ten-game record in the slate at 0.8 goals against per game. Head-to-head favours City heavily (7-1-0 over eight meetings), but the one draw is the most recent result, last May.

The Poisson cross-check agrees strongly — City 1.101 xG vs Bournemouth 0.876, draw at 30.79% and +47.5% EV, modal score 1-0 (15.25%). One caveat worth stating: a 1.10 xG for a City side that finished last season scoring far more freely is Poisson reading Bournemouth's stinginess and City's late-season output, not a forecast of how City will actually play in August. Treat it as a directional nudge, not a prediction.

---

## 💰 SOLID PICK: Nott'm Forest Win vs Leeds

**The Bet:** Forest to win @ **2.30** (43.5% implied) **Model Probability:** 45.7% (fair odds: 2.19) **Expected Value:** **+5.16%** **Stake:** Quarter-Kelly, £9.92 (0.99% of bankroll) **Confidence:** Standard

The thinnest edge of the week — this one barely clears our +5% floor. Forest (1798, 11th) are 46 points better than Leeds (1752, 15th), Band 1, but unlike every other Band 1 fixture this week, Forest are the stronger side **at home**, which flips the probability triplet in their favour. The book, at 43.5% implied, isn't far off the model's 45.7% — this is close to the "the book is nearly right" bet of the week.

Where this one earns its place isn't the model edge, it's the underlying picture. Forest are 6.7/10 and **improving** — the best form rating on the entire slate — with a ΔELO of +28 over five games and +57 over ten, the biggest ten-game gain of anyone on the card, and the best attacking record in the slate at 2.2 goals for per game. Leeds are steady at 5.3/10. Head-to-head is even at 2-2 over four meetings with no draws, though the last one was a 3-1 Leeds win.

The Poisson cross-check agrees strongly: Forest 1.514 xG vs Leeds 0.751, home win at 55.48% and +27.6% EV, modal score 1-0 (15.72%). The nice line for this one: it's the bet with the thinnest edge from the band model and the strongest underlying fundamentals — the band model can't see that Forest are the form team on the slate, but Poisson can.

---

## 🎲 The Poisson cross-check

Every pick above gets priced by the **ELO-band + venue-adjustment model** — band base rates drawn from roughly 2,200 historical matches, adjusted for home advantage (×1.11 home / ×0.89 away, with the draw nudged toward whichever side is stronger), then normalised. That's the model that finds the edges and sizes the stakes. It is not the model that runs the goal-by-goal Poisson grid, and we want to be upfront that the two are different tools doing different jobs.

**How the Poisson layer works, briefly:** for each side, we take its last 10 Premier League matches and build attack and defence indices against a league baseline of 1.4384 goals per team per game (drawn from 2,215 matches since September 2020). Multiply those out with a home/away adjustment and you get expected goals for the fixture, which generates a 16×16 independent-Poisson score grid — wide enough now to capture effectively all of the probability mass on high-scoring games (worst deviation from 1.0 across this week's ten fixtures: 0.000004, after a fix that widened the grid from 6×6 earlier this month).

**Poisson doesn't price our bets — it checks them.** `calculate_poisson()` has no callers in the actual staking pipeline; it exists to cross-check the band model and to generate Super 6 predictions. Two caveats worth carrying:

1. At Matchweek 1, every "last 10" figure is last season's. There's no current-season signal in any of these Poisson numbers yet.
2. Hull City and Coventry City have no Premier League match history at all, so they get no Poisson index — they're priced by the band model on a seeded ELO of 1685 (the average of last season's bottom four), and Poisson simply can't see them.

Here's the scoreboard on the five bets actually on the book:

| Bet | Odds | Band model p | Band EV | Poisson p | Poisson EV | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| Forest home v Leeds | 2.30 | 45.72% | +5.16% | 55.48% | **+27.60%** | ✅ Reinforces |
| Brentford home v Spurs | 2.42 | 54.04% | +30.78% | 45.98% | **+11.27%** | ⚠️ Softens |
| Draw, City v Bournemouth | 4.79 | 22.20% | +6.34% | 30.79% | **+47.48%** | ✅ Reinforces |
| Newcastle home v Liverpool | 3.82 | 29.47% | +12.58% | 44.18% | **+68.77%** | ✅ Reinforces |
| Fulham home v Chelsea | 3.66 | 37.48% | +37.18% | 47.39% | **+73.44%** | ✅ Reinforces |

Four reinforce, one softens, none contradict. Every bet still on the ledger has Poisson agreeing on direction, even where it disagrees on size.

Now the two that aren't on the ledger — and this is the interesting part:

| Bet (overridden, not placed) | Odds | Band model p | Band EV | Poisson p | Poisson EV | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| Palace away @ Everton | 3.31 | 36.31% | +20.19% | 26.63% | **−11.85%** | ❌ Conflict |
| Villa away @ Brighton | 3.09 | 36.31% | +12.20% | 22.90% | **−29.23%** | ❌ Conflict |

Worth saying plainly: the operator pulled these two fixtures from the book on a risk-management basis (see below), before this cross-check table was even built — and it turns out they're exactly the two fixtures where the band model and the Poisson layer disagree outright. That's not foresight, it's a good after-the-fact sanity check on the call, and we're calling it that rather than pretending we saw it coming. The pattern in both: they're Band 1 fixtures where the away side is the stronger team but the home side has the better recent scoring record — something the band model, pricing the band rather than the individual teams, can't see by construction.

We also ran the Poisson grid against goals and BTTS markets this week, purely as a "here's what the model can see but isn't allowed to act on" exercise — the pipeline currently only prices match-result markets. The notional edges there are enormous (Fulham/Chelsea Under 2.5 at +82.2%, for instance), and enormous notional edges from a model running entirely on last May's goal data are a warning sign, not a windfall. We're not betting them, and neither should you read them as forecasts.

---

## 🔻 The override: Everton/Palace and Brighton/Villa

Two of the model's seven selections were pulled from the book before either was placed with a bookmaker. This is a plan edit, not a settled result — nothing here is a loss, a void, or a bet that was later cashed out. They simply never happened.

**Crystal Palace away win @ Everton** — 3.31, +20.19% EV, would-be stake £21.85. Everton and Palace are two ELO points apart, the closest fixture of the week (Band 1), and the book gives the home side a 16-point probability edge the model doesn't think is justified. Both sides are in matched decline on form. Head-to-head runs against the pick (Everton 7-4-1 over the last twelve), and the Poisson cross-check disagrees outright — see table above.

**Aston Villa away win @ Brighton** — 3.09, +12.20% EV, would-be stake £14.59. Brighton (1823) are actually the weaker side by ELO here relative to Villa (1859), Band 1, and again the book has the home side over-favoured. Head-to-head is the one signal that genuinely supports this pick — Villa have won 8 of the last 11 meetings — but the Poisson gap is the largest of the whole slate (−29.2% EV on the Poisson view, the single biggest band-vs-Poisson disagreement on the card).

**Why these two, and not others:** both are Band 1, both are away-side picks on the nominally stronger team, and both are higher-volatility picks than the rest of the portfolio. The operator's stated reasoning for pulling them was exposure and variance management, not a reading of the Poisson table — the model's raw seven-bet output would have staked 15.08% of bankroll, the highest of this project so far. Pulling these two brought it back to **11.44%**, inside the range last season's posts actually ran at (4.0%–11.8%).

The ledger reflects this directly: `data/bankroll.json` now holds five bets, numbered `00002`, `00003`, `00005`, `00006`, `00007`. Bet IDs `00001` and `00004` — the two overridden picks — were deleted outright rather than marked void, since neither was ever placed with a bookmaker.

It's worth sitting with this for a second rather than filing it as a footnote. This is the first week we've overridden the model before placement, rather than just skipping a fixture the model itself rejected. It's a judgement call layered on top of the pricing, not a change to the pricing rule — and it happens to line up with what the model's own cross-check found independently. Both things are true and worth saying: the override was a risk call, not a forecast, and it turned out to land on the two fixtures the cross-check liked least.

---

## ❌ Bets we're skipping

Beyond the two overridden picks, three more fixtures produced no bet at all — for three different reasons.

### Hull City v Man Utd — blocked by the model itself

Hull (seeded ELO 1685, Band 5, 203-point gap to Man Utd's 1888) actually cleared the floor on **two** outcomes at once — Hull home at +30.33% EV and the draw at +23.60% EV. The pipeline refused both. Two positive-EV selections in the same 1X2 market are mutually exclusive outcomes priced off one overround; if the model genuinely favours both, that's a signal to distrust the pricing, not a pair of bets to place. The rule exists specifically for cases like this, and it fired correctly: both were blocked, stakes zeroed, and left visible in the report rather than quietly dropped.

The underlying cause is worth naming: the book has Man Utd at 1.36 (73.5% implied) away at a promoted side, while the band model's Band 5 base rate says 60.1% — a 13-point gap that spills value into both other outcomes. Hull also have zero Premier League history in our dataset; they're priced entirely off a seeded rating. Refusing both bets here is the system correctly saying no to itself — which is more or less the point of running it this way.

### Ipswich v Sunderland — below the floor

Ipswich (seeded 1685) home win comes out at +1.95% EV against Sunderland (1728) — real, positive, and below our +5% minimum, so no bet. The floor exists precisely so a 2% edge from a model that admits its own error bars doesn't turn into a stake.

### Arsenal v Coventry City — the market got it right

Arsenal (2013, 1st) at 328 ELO points clear of Coventry (seeded 1685) is the widest gap on the card, and it's also the tightest-priced market of the week (104.96% overround) on the thinnest historical band we have (50 games). Every outcome comes back negative EV. That's not the model failing to find something — that's the book's sharpest price landing on its most predictable fixture, and the model correctly finding nothing to argue with.

---

## 🔍 How the model works — the coarseness worth knowing about

One thing worth being upfront about: the match-result model prices fixtures by **band and venue**, not fixture by fixture. Look at three of this week's original seven selections:

| Fixture | Band | Model home | Model draw | Model away |
| --- | --- | --- | --- | --- |
| Everton v Crystal Palace | 1 | 37.48% | 26.21% | 36.31% |
| Brighton v Aston Villa | 1 | 37.48% | 26.21% | 36.31% |
| Fulham v Chelsea | 1 | 37.48% | 26.21% | 36.31% |

Identical. All three are Band 1 fixtures with the stronger side playing away, so all three get the exact same probability triplet — every difference in expected value between them comes purely from the bookmaker's price, not from anything fixture-specific the model knows. (Forest v Leeds is also Band 1, but with the stronger side at home, so it gets a different triplet: 45.72 / 23.94 / 30.34.) Two of these three identical-probability picks are the pair we pulled before placement — only Fulham stayed on the book, and it's the one of the three where the Poisson cross-check agreed most strongly.

This is a known limitation, not a secret one — it's the reason we run the Poisson cross-check at all, and it's why any edge of 20% or more triggers a non-blocking advisory flag, on the logic that against a genuinely sharp market, an edge that large is more often a stale price or a too-coarse model than real value. It fired five times this week: Fulham (+37.2%), Brentford (+30.8%), both Hull outcomes (+30.3% / +23.6%), and Palace away (+20.2%). Two of those five are on the live book (Fulham, Brentford — both backed by the Poisson cross-check); Hull's pair was blocked on its own separate grounds; Palace's was one of the two we pulled. Every fixture that tripped the advisory this week ended up blocked, overridden, or is one of our two strongest live picks — which is roughly what we'd want from a flag like this.

**On staking, for the record:** +5% EV is the minimum to bet at all. Quarter-Kelly (0.25 of full Kelly) is the ceiling for standard plays; Eighth-Kelly (0.125) is reserved for hedges and low-sample bands. All five bets this week are standard-confidence Quarter-Kelly.

---

## 📋 Portfolio summary

| Bet | Odds | EV% | Sizing | % of Bankroll | Type |
| --- | --- | --- | --- | --- | --- |
| 🔥 Fulham Win vs Chelsea | 3.66 | +37.2% | Quarter-Kelly | 3.5% | PREMIUM |
| 🔥 Brentford Win vs Spurs | 2.42 | +30.8% | Quarter-Kelly | 5.4% | PREMIUM |
| 💎 Newcastle Win vs Liverpool | 3.82 | +12.6% | Quarter-Kelly | 1.1% | VALUE |
| 💰 Man City / Bournemouth Draw | 4.79 | +6.3% | Quarter-Kelly | 0.4% | SOLID |
| 💰 Nott'm Forest Win vs Leeds | 2.30 | +5.2% | Quarter-Kelly | 1.0% | SOLID |
| **TOTAL** | — | **+27.9% (wtd)** | — | **11.4%** | — |

**Priced but not placed** — the two overridden picks, for reference against the "seven bets" the model actually found: Crystal Palace win @ Everton (3.31, +20.2% EV, would-be 2.2% of bankroll); Aston Villa win @ Brighton (3.09, +12.2% EV, would-be 1.5% of bankroll).

**Bankroll:** £1,000.00 **Committed:** £114.37 (11.44%) **Remaining:** £885.63

If the model's right across the board: expected profit **+£31.85**, expected winners **1.89 of 5**, and a **91.4%** chance of landing at least one.

One nuance worth a sentence, for honesty's sake: all five stakes were sized off the full £1,000 in a single pass, since nothing was committed yet when the pipeline ran. Priced sequentially against a shrinking bankroll, they'd come out marginally smaller.

---

## Final thoughts

New season, new number, same discipline. The headline isn't really any single bet this week — it's that the pipeline ran itself for the first time, found seven edges, and a human being still looked at two of them and said "not those, not like this." That's the system working as intended in a way we haven't been able to show before: not just finding value, but having a second, independent check (Poisson) and a human risk filter both push in the same direction on the same two fixtures, for different reasons, without coordinating.

Band 1 was broken last season. This week we're leaning on the repaired version harder than any week before it — five of ten fixtures are Band 1 — and the two we pulled are a reminder that "the model says so" and "we should bet it" aren't the same sentence, even when the model's freshly fixed.

Eleven percent of a fresh £1,000 is out on the board. We'll know more next week.

---

_All probabilities calculated using 5+ seasons of Premier League data (2,000+ matches). ELO ratings updated weekly. Model performance tracked publicly at [ferret-stack.github.io](https://ferret-stack.github.io)._
