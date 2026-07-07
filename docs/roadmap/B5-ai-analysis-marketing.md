# B5 — AI analysis & marketing workflow

> **Status: SPEC — execution-ready.** Written by Fable (Brief Part B), to be
> implemented by Opus. Soft dependency on B3 (value-bet + ledger JSONs feed
> the strongest content); previews/reviews work off Part A data alone.

## Objective

An analysis + marketing engine: a prompt library that turns the model's JSON
output plus the owner's qualitative notes into publish-ready drafts in the
house voice, a content calendar around the season rhythm, and a distribution
plan for the anti-tipster brand. The differentiator is the combination the
market can't fake: **honest quantitative edge (CLV-tracked) + genuine
qualitative reads (formations, manager styles) + radical transparency.**

## House voice (distilled — verify against `_posts/` before writing prompts)

Studied from `2026-01-12-Matchweek-22.md` (preview) and
`2026-04-20-Matchweek-33-review.md` (review), among others:

- First person, self-deprecating, poker-literate. Losing weeks are reported
  with the same energy as winning ones — that IS the brand.
- Catchphrase, used at emotional low points: *"the model isn't always right,
  but it's never wrong."* Trust-the-process framing; EV language everywhere
  (edge %, fractional Kelly — "sixteenth kelly" at short odds).
- Preview format: `## 🧠 This Week's Best Bets` → blockquote TL;DR (headline
  bet + Total Portfolio EV + Bankroll Stake + one-para summary) → link to the
  odds calculator → X follow CTA (the "Twitter vs X" joke pair) → last week's
  review table (emoji confidence tiers 🔥💎💰, columns Bet/Odds/Stake/Result/
  Score/P&L) → per-fixture sections.
- Review format: `##` narrative headline → bad news/good news opener →
  per-bet `###` sections with a one-line lesson each → closer linking to next
  matchweek. Fan disclosure when relevant ("Yes, I'm a Chelsea fan. Yes, I'm
  backing Brentford.").
- Bankroll is small and real (~£100) and quoted in pounds and %.

## Files touched

| File | Change |
|---|---|
| `prompts/` (in this repo) | NEW — prompt library, one file per content type |
| `prompts/_context.md` | NEW — shared context-assembly instructions |
| `prompts/voice.md` | NEW — the voice guide above, expanded with 3 verbatim excerpts |
| `docs/CONTENT_CALENDAR.md` | NEW |
| `docs/MARKETING_PLAN.md` | NEW |
| `qualitative/` (in this repo) | NEW — owner's formation/manager-style notes, one file per team (owner writes; prompts consume) |

## Prompt library (each template = role + context manifest + task + format + checks)

| Template | Cadence | Data context (paste keys, not whole files) |
|---|---|---|
| `matchweek-preview.md` | Tue in season | this week's `upcoming_fixtures.json` fixtures incl. `value_bets` (B3); per fixture: both teams from `current_elo.json` (all six rating fields), `team_stats.json` form blocks, band row from `elo_bands.json`, `h2h_records.json` entry, referee from `referee_stats.json`; last week's settled ledger entries; `qualitative/{team}.md` for teams involved |
| `matchweek-review.md` | Mon in season | settled ledger entries for the week + `pnl_summary.json` totals + the preview post being reviewed (paste it) + final scores from facts |
| `value-bet-writeup.md` | when a 🔥 bet ships | one `value_bets` entry + its full model trail: band row, venue multipliers applied, form, qualitative notes both teams |
| `methodology-post.md` | off-season / after big changes | `docs/PART_A_REPORT.md`-style material; calibration tables from `data/reference/calibration_results.json` |
| `season-sim-showpiece.md` | late Aug + monthly | `season_simulation.json` (B4) + market futures for contrast |
| `class-vs-form.md` | monthly | teams where `long_rank` and `rolling_rank` diverge ≥ 3 from `current_elo.json`; their recent results |

Template skeleton (enforce in every file):

```markdown
# Role
You write for Ferret Stack. Follow prompts/voice.md exactly.

# Context
{manifest: the exact JSON fragments and qualitative files to paste}

# Task
{content type, target length, required sections in house format}

# Hard rules
- Every number must come from the pasted context. If a number you want is
  missing, write [NEEDS DATA: what] instead of inventing it.
- Never soften a losing week. Never claim an edge the value_bets JSON
  doesn't show. Quote edge/stake/odds exactly.
- UK gambling-content hygiene: 18+, no "guaranteed", bankroll-percentage
  framing, gamble-aware footer.

# Output
Jekyll front matter (layout: post, title, date, ferret: boolean, author:
Ferret Stack) + markdown body.
```

## Step-by-step

1. Write `prompts/voice.md`: the distillation above + three verbatim
   excerpts (one preview TL;DR, one review section, one catchphrase moment)
   lifted from `_posts/`.
2. Write `prompts/_context.md`: for each JSON, the exact `jq`-style paths a
   human (or a script) pulls before prompting — keeps context small and
   makes "every number traceable" mechanical.
3. Write the six templates.
4. `qualitative/` scaffold: `_template.md` with headings (Formation & shape /
   Manager tendencies / Set pieces / Squad state / How they beat you) — the
   owner fills these in; prompts degrade gracefully when a team's file is
   missing (write from data only, flag the gap).
5. Dry-run acceptance: regenerate **Matchweek 22's preview** from its
   historical inputs and diff against the real
   `_posts/2026-01-12-Matchweek-22.md` — structure must match section-for-
   section; voice judged by the owner; every number traced (step 2 makes
   this checkable).
6. Write `docs/CONTENT_CALENDAR.md`:
   - **Off-season (now→Aug):** methodology post ("We rebuilt the
     foundations" — Part A material: the Band-1 confession + calibration
     proof, the strongest anti-tipster content we have); B1 announcement
     when leagues expand; season-sim showpiece last week of August.
   - **In season, weekly:** Tue preview, Mon review; monthly class-vs-form
     and sim-refresh posts; ad-hoc 🔥 value writeups.
7. Write `docs/MARKETING_PLAN.md`:
   - X as primary channel (account exists): preview-thread template (one
     tweet per 🔥 bet, quote edge + stake), results screenshot Mondays —
     **post the losses with the same template**; pin the CLV/track-record
     page once B3 ships.
   - Secondary: r/soccerbetting and football-forum threads where
     self-promotion rules allow — lead with methodology transparency, not
     picks. Newsletter (Buttondown/Substack) repackaging the weekly pair.
   - Positioning line to reuse: tipsters sell certainty; we publish our
     losing weeks, our staking maths, and whether we beat the closing line.
   - Compliance guardrails: 18+/GambleAware footer on every betting post,
     no affiliate links until the owner decides, never DM "tips".

## Verification method

- Step 5 dry-run: side-by-side committed to
  `docs/roadmap/b5-dryrun-mw22.md` with a trace table (claim → JSON path).
  Zero untraceable numbers is the acceptance bar; voice sign-off by owner.
- A second dry-run on a LOSING week (MW33) to prove the honesty constraint
  survives generation — the draft must not spin the loss.
- Calendar and marketing docs reviewed by owner; X thread template test-fired
  on one historical week (not posted).
