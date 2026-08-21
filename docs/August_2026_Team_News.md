# Team News — All 20 Clubs, 2026/27 Season
*Compiled 16 Aug 2026. Season kicks off Friday 21 Aug 2026. Replaces January_Team_News.md, which is now 7 months stale.*

**Confirmed roster change:** West Ham, Wolves, Burnley relegated (2025-26). Hull City, Coventry City, Ipswich Town promoted. This matches the project's existing assumption in `System_Source_of_Truth.md` — confirmed against real-world results, not just carried over from that doc.

**Headline spinach for the pipeline:** nine of twenty clubs have a new permanent manager since the January file was written — Man City, Man Utd, Liverpool, Chelsea, Newcastle, Nottingham Forest, Crystal Palace, Fulham, Bournemouth. Any manager-style tags feeding the model (e.g. `Manager_Styles_2025.md` / `Managerial_Styles_Feb-26_Update.md`) are stale for these nine and need fresh profiles before the model leans on tactical-identity priors for them. This is a bigger issue than any single injury update below.

**Season-state caveat:** we're in pre-season week, not mid-season. "Current injuries" below are pre-season fitness pictures, not in-season absence lists — expect volatility once competitive minutes start Aug 21. No suspension list has meaningful depth yet (yellow-card accumulation resets each season); the only actives are pre-season disciplinary carryovers, noted per club.

---

## Manager changes since January (10 total, 1 mid-season already reflected in Jan file)

| Club | Jan-26 manager | Now (Aug-26) |
|---|---|---|
| Manchester City | Pep Guardiola | **Enzo Maresca** (in from Chelsea, 3-yr deal) |
| Manchester United | Ruben Amorim | **Michael Carrick** (made permanent 13 Jan 2026) |
| Liverpool | Arne Slot | **Andoni Iraola** (in from Bournemouth) |
| Chelsea | Enzo Maresca (left 1 Jan 26) → Rosenior → McFarlane (interim) | **Xabi Alonso** |
| Newcastle United | Eddie Howe | **Matthias Jaissle** |
| Nottingham Forest | Vítor Pereira | **Oliver Glasner** (in from Crystal Palace) |
| Crystal Palace | Oliver Glasner | **Pierre Sage** |
| Fulham | Marco Silva | **Alvaro Arbeloa** |
| Bournemouth | Andoni Iraola | **Marco Rose** |
| Everton | David Moyes | David Moyes (unchanged) |
| Arsenal | Mikel Arteta | Mikel Arteta (unchanged) |
| Aston Villa | Unai Emery | Unai Emery (unchanged) |
| Brighton | Fabian Hürzeler | Fabian Hürzeler (unchanged) |
| Brentford | Keith Andrews | Keith Andrews (unchanged) |
| Leeds United | Daniel Farke | Daniel Farke (unchanged) |
| Sunderland | Régis Le Bris | Régis Le Bris (unchanged) |
| Tottenham Hotspur | (not in Jan file) | **Roberto De Zerbi** |
| Hull City | n/a — promoted | **Sergej Jakirovic** |
| Coventry City | n/a — promoted | **Frank Lampard** |
| Ipswich Town | Kieran McKenna | **Gary O'Neil** (McKenna left after promotion) |

Arteta is now the league's longest-serving manager (Dec 2019). Only Arteta, Howe (departed), Emery, and Farke had 2+ years in post entering this summer — that tells you how much continuity data got wiped this off-season.

---

## Injuries & Suspensions by Club

*Source: Squawka tracker (compiled from Premier Injuries / Fantasy Football Scout / club channels), as of 15 Aug 2026. "Doubt" = uncertain for opener, with rough confidence where given.*

### Arsenal — Mikel Arteta
**Out:** Jurrien Timber (groin/hip — had minor surgery this summer, hoping for some pre-season involvement); William Saliba (lower back — exacerbated at the World Cup, Arteta has said Arsenal "have lost an incredible player" for an extended period, could be out into 2027).
No suspensions. This is a significant blow — Arsenal's first-choice back four is missing both first-choice centre-backs/full-back options right as the season starts.

### Aston Villa — Unai Emery
**Out:** Amadou Onana (knee), Leon Bailey (unspecified), Johan Manzambi (knee).
**Doubt:** Alejandro Garnacho (head/face injury, ~50% chance) — cause not specified in sources found; flagging rather than guessing.

### Bournemouth — Marco Rose
**Out:** Eli Kroupi (ankle/foot, exp. return 7 Nov), Ryan Christie (suspended, returns GW2), Veljko Milosavljevic (knee), Julian Araujo (thigh), Amine Adli (unspecified).
**Doubt:** David Brooks, Tyler Adams, Julio Soler, Alvaro Rodriguez (all 25-50% chance).
**Suspension:** Ryan Christie serving a 1-match ban, returns GW2.
Bournemouth carry one of the heavier pre-season injury lists (10 absentees) under their new manager.

### Brentford — Keith Andrews
**Out:** Antoni Milambo (knee), Sepp van den Berg (unspecified).

### Brighton — Fabian Hürzeler
**Out:** Stefanos Tzimas (knee, exp. return 12 Sep), Evan Ferguson (ankle/foot), Carlos Baleba (ankle/foot).
**Doubt:** Kaoru Mitoma (thigh, 25%), Yankuba Minteh (calf, 50%), Matthew O'Riley (illness, 50%).

### Chelsea — Xabi Alonso
**Out:** Emmanuel Emegha (thigh, exp. return 30 Aug), Wesley Fofana (suspended, returns GW3), Jordan Henderson (wrist/hand).
**Doubt:** Geovany Quenda, Levi Colwill (both 50%), Aaron Anselmino, Mamadou Sarr (both 25%).
**Suspension:** Wesley Fofana, 2-match ban, returns GW3.
Note: Mykhailo Mudryk's FA suspension (failed drug test, missed all of 2025-26) is referenced in older tracker text but doesn't appear on the current active list — needs a direct check with Chelsea before treating him as available or unavailable; don't assume resolved.

### Coventry City — Frank Lampard *(new PL entrant)*
**Out:** Luke Woolfenden (unspecified).
**Doubt:** Jack Rudoni (shoulder, 25%, targeting 21 Aug — the opening weekend).
Lightest injury list of the promoted three. First top-flight season since 2000-01; Lampard was EFL Championship Manager of the Season after winning the title.

### Crystal Palace — Pierre Sage
**Out:** Adam Wharton (ankle/foot).
**Doubt:** Dwight McNeil (50%), Cheick Doucouré (25%).
New manager after Glasner left for Forest — Sage previously worked in Lyon's academy setup, no prior head-coach role at senior level. Worth flagging as a genuine unknown for tactical modelling, not just a name swap.

### Everton — David Moyes
**Out:** James Garner (groin/hip).
**Doubt:** Timothy Iroegbunam (25%).
Lightest injury list in the league alongside Man City.

### Fulham — Alvaro Arbeloa
**Out:** Tom Cairney (knee, exp. return 17 Oct), Joachim Andersen (suspended, returns GW2).
**Suspension:** Joachim Andersen, 1-match ban, returns GW2.
New manager — Arbeloa's only prior senior head-coach role was Real Madrid's reserve side; first job at this level.

### Hull City — Sergej Jakirovic *(new PL entrant)*
**Out (7):** Eliot Matazo (knee, exp. return 20 Feb 2027 — long-term), Jack Butland (arm/elbow, exp. 21 Nov), Darko Gyabi (thigh, exp. 10 Oct), Oscar Zambrano (thigh, exp. 12 Sep), Charlie Hughes (groin/hip, exp. 5 Sep), Hidemasa Morita (calf/shin, exp. 5 Sep), Cody Drameh (thigh, exp. 29 Aug).
**Doubt (6):** Matty Jacob, John Egan, Jens Hjerto-Dahl, Liam Miller (all 50%), Enis Destan, Paddy McNair (both 25%).
**By far the heaviest injury list in the Premier League** — 13 absentees, more than double most other clubs, right as a newly promoted side needs a settled spine. Their first five fixtures are Man Utd, Villa, Chelsea, Newcastle — a brutal opener made worse by this. Flag this combination (thin squad depth + tough opener + heaviest injury list) as a real signal, not noise, if it's feeding into early-season model confidence for Hull matches.

### Ipswich Town — Gary O'Neil *(new PL entrant)*
**Doubt:** Azor Matusiwa, Jack Taylor (both 25%, both targeting 22 Aug).
Lightest injury picture of the promoted three alongside Coventry. O'Neil replaces McKenna, who left shortly after securing promotion — McKenna's exit means Ipswich also lose their promotion-winning manager, unlike Hull and Coventry who kept theirs.

### Leeds United — Daniel Farke
**Out:** Gabriel Gudmundsson (thigh).
**Doubt:** Lucas Perri (wrist/hand, 25%), Ilia Gruev (knee, 25%).

### Liverpool — Andoni Iraola
**Out:** Conor Bradley (knee, exp. return 1 Jan 2027 — long-term), Hugo Ekitike (calf/shin, exp. 12 Oct), Joe Gomez (unspecified, exp. 4 Sep), Jayden Danns (thigh), Giovanni Leoni (knee).
**Doubt:** Alexis Mac Allister, Curtis Jones (both 50%).
**Back in training:** Jeremy Jacquet (knee, 75% chance available).
New manager (Iraola in from Bournemouth) plus five outs — a lot of change for the model to absorb on a club that's usually a defensive/tactical constant.

### Manchester City — Enzo Maresca
**Out:** Rodri (lower back — had back surgery, timeline unclear beyond "building fitness").
**Doubt:** Savio (illness, 50%).
Not on the Squawka list but worth flagging from separate reporting: **Erling Haaland** had not yet returned to training as of 9-10 Aug after his World Cup 2026 campaign with Norway — treat his opening-weeks availability as uncertain even though he's not formally "out." **Jack Grealish** is back from a foot injury but his future at the club (Real Madrid interest) is unresolved, which affects squad-rotation assumptions more than fitness. Big squad turnover under Maresca: in — Elliot Anderson (£116m marquee signing from Forest); out — James Trafford, Manuel Akanji, Nathan Aké, John Stones, Bernardo Silva.

### Manchester United — Michael Carrick
**Out:** Manuel Ugarte (knee), Karl Darlow (unspecified).
**Doubt:** Matthijs de Ligt (lower back, exp. 6 Sep, 25%), Benjamin Šeško (calf/shin, exp. 22 Aug, 50%), Kobbie Mainoo (lower back, 25%), Lisandro Martínez (thigh, 25%), Mason Mount (ankle/foot, 50%), Tom Heaton (25%).
Second-longest pre-season absentee list in the league (8) — a lot of fitness uncertainty for a first full pre-season under permanent boss Carrick.

### Newcastle United — Matthias Jaissle
**Doubt:** Valentino Livramento (calf/shin, exp. 23 Aug, 25%), Fabian Schär (groin/hip, 50%), Lewis Miley (calf/shin, 50%).
No confirmed "outs" — lightest genuine absence list, though new manager Jaissle (in from Howe) is a bigger unknown than any individual injury here.

### Nottingham Forest — Oliver Glasner
**Doubt:** Nicolò Savona (knee, exp. 22 Aug, 25%), Chris Wood (50%).
**Back in training:** Callum Hudson-Odoi (thigh, exp. 22 Aug, 75% chance).
Fifth different Forest head coach in 12 months under Marinakis (Nuno → Postecoglou → Dyche → Pereira → now Glasner). That instability is itself a data point worth weighting more than any individual player fitness line.

### Sunderland — Régis Le Bris
**Doubt:** Omar Alderete (exp. 22 Aug, 50%), Nordi Mukiele (exp. 22 Aug, 50%).
Only manager among the top half of the table with zero managerial change and a light fitness picture — the most "as you were" club on this list.
Flag: the tracker's FAQ text separately mentions a Dan Ballard suspension "returning 25 May" — that's leftover text from last season, not a current suspension. Don't carry it into this season's data.

### Tottenham Hotspur — Roberto De Zerbi
**Out (6):** Xavi Simons (knee, exp. 20 Feb 2027 — long-term), Wilson Odobert (knee, exp. 28 Nov), Dejan Kulusevski (knee), Cristian Romero (knee), James Maddison (ankle/foot), Pedro Neto listed as "Mateus Espanha Fernandes" in source, unspecified.
**Doubt:** Mohammed Kudus, Micky van de Ven, Destiny Udogie (all 25%), Dominic Solanke (ankle/foot, 50%).
Tied with Hull for second-heaviest list (10) and under a brand-new manager (De Zerbi wasn't even in the January file — Spurs changed manager since then too, worth double-checking against Spurs' own channels since this wasn't covered in the broader manager-change search above).

---

## Explicitly not covered here (state, don't infer)
- No in-season form/results data exists yet — season hasn't started.
- Transfer business is incomplete in these sources for most clubs; only Man City's business was confirmed in enough depth to include.
- AFCON absences: not applicable to this update — tournament isn't in the current window. Re-check in December when it becomes relevant again, per the seasonal pattern in the older team-news files.
- Community Shield result (Arsenal v Man City, 16 Aug) — was upcoming as of the sources pulled, not confirmed complete.
