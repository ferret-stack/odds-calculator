# Matchweek 2: Away Days

Hey, it's the Ferret Stack. Last week we made a couple of quid (RoI of 1.7%). This week has the real potential to get us off to a rocket start, or knock us down a peg; I'm super excited

While we hope volatility will be in our favour, we've an expected return of 2.5% for Matchweek 2

This weekend I'm heading to a hotel with Mrs. FS. It's not far from us, but I'm counting it as an away day nonetheless, in theme with this week's portfolio; six picks back the away team, with half of these being the underdog

It's highlighted something interesting in the system. For those interested in how this is being deployed and built, I dive into some of these considerations at the end of the post

But if you wanna know what the choices are.. Pack your bag, grab a *tinnie from the offie*, and let's go on an away day journey...

## This Week's Bets

**The Numbers**
> **Six** total bets; all away
> **Four** times our `implausible_edge` error mechanism in the model got tripped
> **Three** bets on the underdog
> **Two** bets with modest EV, but solid football reasoning behind

| Fixture                   | Bet                             | Odds | Edge   | Stake  |
| ------------------------- | ------------------------------- | ---- | ------ | ------ |
| Liverpool v Nott'm Forest | Forest (away, underdog)         | 6.19 | +7.6%  | £3.74  |
| Bournemouth v Everton     | Everton (away, underdog)        | 3.46 | +6.1%  | £6.32  |
| Tottenham v Newcastle     | Newcastle (away, favourite)     | 2.99 | +33.0% | £21.10 |
| Leeds v Brentford         | Brentford (away, close to even) | 2.70 | +20.8% | £30.09 |
| Sunderland v Fulham       | Fulham (away, favourite)        | 2.91 | +29.5% | £19.60 |
| Chelsea v Brighton        | Brighton (away, underdog)       | 3.72 | +34.5% | £16.12 |

Only three of these are genuine underdog bets - Forest, Everton, and Brighton. Newcastle and Fulham are actually the stronger side on paper despite playing away; Brentford are close to a coin-flip

What all six have in common isn't underdog status, it's an away pick - and for four of them, an edge large enough to trip the sanity check below.

---

## Pick of the Week: Forest at Anfield

**Nott'm Forest away @ 6.19** - £3.74

This is the one to lead with. Two completely different ways of pricing this fixture landed on almost the same number for Forest, and the bookmaker's own price isn't far off either. 

It's also the fixture where the football and the maths tell the same story from different directions. 

Liverpool are in more flux than a club of their stature usually is: Andoni Iraola replaces Arne Slot, Salah has left on a free along with Konaté and Robertson, and five players are unavailable and with Jones doubtful. Iraola's method is a very high line and a press that starts closer to the opposition goal than Slot's did - quick vertical passing after regains rather than patient recycling. The cost is obvious and well documented: more bodies committed forward means more space behind when possession goes

Forest under Glasner are the profile built to punish exactly that

A compact block that cedes territory on purpose, wing-backs carrying the width, and rapid vertical transitions as the whole point

## Big Away Edges, For Different Reasons

Newcastle, Brentford, Fulham, and Brighton all cleared edges big enough to raise an eyebrow (20–35%)

### Tottenham v Newcastle 

Newcastle are the stronger team on paper, but they’re a team particularly rocked by changes. Let’s cut into what this means

The model does account for form, but the signal is pretty useless at season start - especially for teams like this. So the signal that Newcastle is +EV isn’t as sexy as it might first appear

Thankfully, Tottenham are also going through their own pains. Narrowly surviving relegation, they opened the season in a way that could only be described as “oh so Spurs” and lost 3-0 at home 

Patterns exist…

Newcastle sold Gordon, Tonali and Guimarães for a combined £236m and lost Eddie Howe on top

Spurs begin with ten absentees including long-term knee cases, Romero sold, and a brand-new manager in De Zerbi who has said he's reintroducing his system's complexity gradually rather than switching it all on

We’ll see… 

### Leeds v Brentford

Brentford remain a set-piece side by construction - Andrews was promoted from set-piece coach - so expect a physical, low-control game with a chunk of the shot volume arriving from restarts

That's a note about how the game will look, not who wins it

### Bournemouth v Fulham

Fulham are a genuine mixed picture

Arbeloa's default is an aggressive 4-3-3 with adventurous full-backs - which is exactly what Le Bris's compact mid-block and fast vertical breaks are designed to punish

Against that, Sunderland made one permanent signing all summer against ten-plus departures, and Le Bris's structure depends on personnel executing rehearsed roles he's just lost a chunk of. 

### Chelsea v Brighton

Brighton is the one where the story genuinely supports the pick, but not in a necessarily obvious way

Chelsea have the heaviest confirmed spend in the league under a brand-new manager who went through pre-season alternating between a back four and a back three and hasn't locked either in, with Fofana suspended and Colwill a coin-flip

Alonso's principle is provocative build-up - bait the press, play through it. Brighton, unchanged under Hürzeler into his third season, press with high intensity and thrive on precisely the volatility that unfamiliar positional patterns create.

A back line that hasn't decided what shape it is, being asked to bait a press it hasn't practised baiting, is the shape of an early-season accident.

The Seagulls are flying high after smashing Aston Villa last week. It's early days, but they're going to be feeling mighty good coming into this game, so have the confidence to put their money where their mouth is

Directionally the picture supports Brighton. It does not support them by 34.5%

That’s a large edge. We see a lot of these, and so they’re worth looking into

Why?

Because they are going to make or break the system

> Any pick (especially away and/or underdog) with an edge this large gets staked at roughly a third of the normal size until we've figured out how to navigate them


#### ..FFS Just tell us the bets

**Newcastle away @ 2.99** - £21.10
**Brentford away @ 2.70** - £30.09
**Fulham away @ 2.91** - £19.60
**Brighton away @ 3.72** - £16.12

## Everton at Bournemouth

**Everton away @ 3.46** - £6.32

A cleaner, smaller edge. Moyes is one of only four Premier League managers with two-plus years in post going into a season where nine of twenty clubs changed dugout, and Everton carry the lightest injury list in the league. In a division where half the teams are still learning their manager's name, continuity is a genuine and under-priced asset in the opening weeks.

Bournemouth are rebuilding on both fronts. Marco Rose replaces Iraola with an explicit continuity brief in style terms - Red Bull-school pressing, evolve rather than rebuild - but a different base shape, and the back four now contains three players signed this summer against ten pre-season absentees. A high-pressing, high-line home side against a disciplined low block means Bournemouth will likely dominate the ball and the danger to them arrives almost entirely in transition

Bournemouth have now sold their best player every summer for several windows running

This is the fixture where the football most cleanly agrees with the model - and the edge is modest enough that the agreement doesn't feel like coincidence

## Not Betting This Week

- **Coventry v Hull** - lack of data (both seeded at same rating as newly promoted). Model strongly favours Hull. I’ll be honest, I’m focusing my attention elsewhere on this one

- **Man Utd v Ipswich** and **Villa v Arsenal** - both fixtures threw up two conflicting value bets. I’ve not decided how to handle these yet

---

## Something We Noticed

The two fixtures with the strongest football story (LIV**FOR** | BOU**EVE**) carry the smallest edges

The one with the least data? 

That has the highest edge

[See for yourself](https://ferret-stack.github.io/odds-calculator/)

---

Three underdogs, three big-edge away favourites, one system flag doing its job, and a pick at Anfield we're happy to lead with. That's the week.