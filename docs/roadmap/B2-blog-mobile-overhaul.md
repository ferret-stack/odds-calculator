# B2 — Blog / mobile presentation overhaul

> **Status: SPEC STUB — to be written by Fable (Brief Part B), implemented later by Opus.**

## Objective
Make `ferret-stack.github.io` render well on mobile. The odds page
(`odds-calculator.html`, 902 lines) sits over `assets/css/style.css` (2923 lines) with
essentially no page-level responsiveness; the Poisson table overflows on small screens.
Deliver a mobile-first, responsive redesign; surface the dual ELO (class vs form); keep it
theme-consistent.

## Fable to fill in
- Files touched (`odds-calculator.html`, `assets/css/style.css`, `_layouts/`, `_includes/`)
- Concrete step-by-step (responsive grid, table→scroll/card patterns, breakpoints, viewport)
- Data contracts (dual-ELO fields consumed from `assets/data/*.json`)
- Verification method (render at 360px / 768px / 1200px; no horizontal body scroll)
