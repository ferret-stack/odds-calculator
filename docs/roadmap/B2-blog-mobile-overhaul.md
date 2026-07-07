# B2 — Blog / mobile presentation overhaul

> **Status: SPEC — execution-ready.** Written by Fable (Brief Part B), to be
> implemented by Opus in the `ferret-stack.github.io` repo.
> Before writing any chart code, load the `dataviz` skill; before styling,
> load `artifact-design` — both calibrate the visual system.

## Objective

Make the site render properly on phones — that is where betting content is
read. Today: `odds-calculator.html` (908 lines, markup + inline JS) sits on
`assets/css/style.css` (2,923 lines, **5** `@media` queries total); the
viewport meta exists but the Poisson matrix, the odds comparison rows and the
band table all overflow a 360-px screen. Deliver a mobile-first responsive
redesign of the odds page + post layout, and **surface the dual ELO**
("class vs form") the pipeline now publishes.

## Files touched

| File | Change |
|---|---|
| `odds-calculator.html` | restructure markup into components; move JS to `assets/js/odds-calculator.js` |
| `assets/js/odds-calculator.js` | NEW — extracted, unchanged logic unless noted |
| `assets/css/odds-calculator.css` | NEW — page-scoped styles, mobile-first |
| `assets/css/style.css` | global fixes only: fluid type scale, container widths, table-overflow utility class; no page styles added |
| `_layouts/default.html`, `_layouts/post.html` | wrap content in a `.container` with fluid padding; nav collapse under 768px |
| `_includes/head.html` | link the new CSS/JS assets |
| `_posts/*` | no content edits; post tables inherit the overflow utility |

## Design constraints

- **Mobile-first**: base styles are the 360-px layout; `@media (min-width:
  768px)` and `(min-width: 1024px)` add columns. Nothing may cause horizontal
  body scroll at any width ≥ 320px.
- Breakpoints: 360 (baseline), 768 (two-column), 1024 (full desktop).
- Fluid type: `clamp(0.95rem, 0.9rem + 0.4vw, 1.125rem)` body; headings scale
  similarly. Touch targets ≥ 44×44px (team selectors, tab buttons).
- Keep the existing dark theme tokens — extract the current colours into CSS
  custom properties at the top of `style.css` (`--bg`, `--surface`,
  `--accent`, `--text`, `--text-dim`) and reference them in new CSS, so the
  redesign cannot drift off-theme.
- Wide content pattern: every table gets wrapped in
  `<div class="table-scroll">` (`overflow-x: auto; -webkit-overflow-scrolling:
  touch`) — the page body never scrolls sideways, the table does.

## Page structure (mobile order)

1. **Fixture picker** — two selects stacked, swap button between.
2. **ELO header card** — per team: crest-less name, then the dual rating:
   - `Class 1757 (#1)` — long ELO (`long_elo`/`long_rank`)
   - `Form 1682 (#1)` — rolling ELO (`rolling_elo`/`rolling_rank`)
   - a divergence chip when |long_rank − rolling_rank| ≥ 3:
     `▲ form above class` / `▼ form below class` — this is the
     class-vs-form storyline made visible.
3. **Probabilities + fair odds** — three outcome cards in one row (they fit
   at 360px: ~110px each), book odds + edge underneath each when available.
4. **Goal markets** — 2-col grid of stat chips (O0.5 … O4.5, BTTS).
5. **Poisson matrix** — the one legitimate `table-scroll` case; sticky first
   column (`position: sticky; left: 0`) so home goals stay visible while
   scrolling. Cells get a sequential background scale (see `dataviz`
   guidance; single-hue ramp on the theme accent, text contrast ≥ 4.5:1).
6. **ELO history chart** — full-width, height clamped `clamp(180px, 40vw,
   320px)`; draw BOTH lines per team when the pipeline ships rolling history
   (see data contract note below).
7. **H2H / referee** — stacked cards.

## Data contracts consumed (all already published, one optional extension)

- `current_elo.json`:
  `{team: {elo, rank, long_elo, long_rank, rolling_elo, rolling_rank}}` —
  `elo`/`rank` remain the driver values (long); use the explicit fields for
  the dual display. `rolling_elo` may be `null` for teams outside the 2-year
  window (relegated) — render `Form —` and no divergence chip.
- `elo_bands.json`: `{band, range, total_games, stronger_win_pct, draw_pct,
  weaker_win_pct, over_XX_pct, btts_pct, avg_booking_points}`.
- `venue_adjustment.json`: six multipliers (`home_multiplier`,
  `away_multiplier`, `draw_home_multiplier`, `draw_away_multiplier`,
  `weaker_home_multiplier`, `weaker_away_multiplier`) — the JS already reads
  them (keep the fallbacks).
- `matches_data.json` is **facts-only** — the page only uses goals from it.
  Anything ELO-ish must come from `current_elo.json` / `matches_derived.json`.
- OPTIONAL pipeline extension (small `rebuild.py` change, coordinate with the
  odds-calculator repo): emit `elo_history.json` points as
  `{date, elo, rolling}` (additive key; `elo` stays long/post-match so the
  existing chart is unaffected). Gate the second chart line on the key's
  presence: `point.rolling ?? null`.

## Step-by-step

1. Extract inline JS → `assets/js/odds-calculator.js`; extract page CSS from
   `style.css` (everything selecting `.odds-calculator-page` descendants) →
   `assets/css/odds-calculator.css`. No behaviour change; commit separately
   so the diff is reviewable.
2. Add theme custom properties + `.table-scroll` + `.container` utilities to
   `style.css`; convert `_layouts/*` to use them.
3. Rebuild the page markup into the component order above, mobile-first CSS.
4. Add the dual-ELO card + divergence chip (pure JS over `current_elo.json`).
5. Poisson matrix: sticky first column + sequential cell shading + scroll
   affordance (fade-out edge gradient).
6. Wrap every table in every `_posts` rendering path via `post.html` content
   styles (`.post-content table` gets the scroll wrapper via CSS
   `display:block; overflow-x:auto` — no per-post edits).
7. Nav: collapse to a hamburger under 768px (details/summary pattern — no JS
   dependency).
8. Cross-page sweep: `index.html`, `poker/`, `boolean/`, `creative/` pages
   inherit the container + type scale; fix any page that still overflows.

## Verification method

- Playwright (or manual devtools) renders at **360 / 768 / 1200 px**:
  screenshot each; assert
  `document.documentElement.scrollWidth <= window.innerWidth` on every page
  (the no-horizontal-scroll invariant).
- The Poisson matrix scrolls inside its container at 360px; first column
  stays pinned.
- Dual ELO shows for a current fixture (e.g. Arsenal v Man City: Class #1
  vs #2, Form #1 vs #2) and degrades to `Form —` for a relegated team
  (pick one from `current_elo.json` with `rolling_elo: null`).
- All nine JSON fetches still resolve (no renamed paths).
- Lighthouse mobile: Performance ≥ 85, Accessibility ≥ 95 on the odds page.
- Tap every interactive control on a 360-px viewport — nothing under 44px.
