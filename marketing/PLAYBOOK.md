# PropBetIQ daily Instagram content — playbook

Every morning, make **one** ready-to-post Instagram piece for @propbetiq plus its caption, and deliver both to Chris. Chris reviews and posts it himself. Never post anywhere, never commit to the repo, never message anyone else.

## Brand rules (non-negotiable)

- **Data, not picks.** PropBetIQ teaches bettors to use hit rates instead of "locks", podcasters or gut feel. Never say "lock", "hammer", "free pick" or tell people what to bet.
- **Every number must come from the data.** Hit rates come from the PropBetIQ site data in the repo; shot locations from ESPN play-by-play via sportsdataverse. If a number can't be verified, cut it.
- **Prop lines must be real.** Find the actual line from a published source (e.g. SportsBettingDime, Covers, Action Network, a sportsbook's news page). Never use BettingPros hit rates. If you can't find a line for a player, pick a different player. Keep the source URL for the delivery note (not the caption).
- **Recaps use pre-game data only.** When the piece is about a game that already happened, every hit rate must come from games *before* that game (the tools enforce this — keep it that way).
- **No player photos, team logos or wordmarks.** The templates draw a generic silhouette badge in team colors with the jersey number. Don't add images.
- **Look and tone:** dark navy/purple, orange accent, clean and data-centric. Captions are short, confident and plain. No emojis. Always end with the free-trial CTA, "Not betting advice. Please bet responsibly." and 8–10 hashtags.

## Weekly calendar (Pacific time)

| Day | Piece | Sport |
|---|---|---|
| Mon | Shot-chart reel — last night's standout scorer | NBA |
| Tue | Data card — NFL recap of a Sunday/MNF prop the data called (pre-game rates) | NFL |
| Wed | Shot-chart reel | NBA |
| Thu | Data card — tonight's NBA prop to watch (preview, with the halftime "live-bet split") | NBA |
| Fri | Shot-chart reel | NBA |
| Sat | Data card — NBA recap or preview, whichever has the stronger story | NBA |
| Sun | Data card — NFL slate preview: one player prop with a strong split | NFL |

Fallbacks: no NBA games last night → make a preview card for tonight. No NBA games at all (All-Star break, offseason) → NFL card. NFL offseason → NBA card instead of the NFL slot. If no story is strong enough, say so and deliver the best available card rather than forcing a weak one.

## What makes a good story

Look for a player whose **overall** hit rate is ordinary (roughly 45–65%) but whose **split** is decisive — halftime pace (`h1_*`), first quarter (`q1_*`), home/away, vs this opponent, last 10 — and, for recaps, where that split called last night's result. Prefer well-known players. The SGA example in `examples/` is the model: 63% overall → 79% when 15+ at the half → he had 16 at the half and scored 32.

## Setup (each run starts fresh)

```bash
git clone --depth 1 https://github.com/backironanalytics/DDS.git
cd DDS/marketing/tools
python3 -c "import playwright" && which ffmpeg     # both should already exist
```

If Playwright can't find Chromium, set `PW_CHROMIUM` to the installed Chromium binary (e.g. `/opt/pw-browsers/chromium`) — don't run `playwright install`.

Check data freshness: `NBA/propbetiq_nba-*.json` → `meta.generated_at` should be from the last ~30 hours. If it's older, note it in the delivery message (the nightly GitHub Action may have failed).

## NBA workflow

```bash
R=../..   # repo root
python3 nba_data.py --repo $R last-night                         # last night's box scores + recent averages
python3 nba_data.py --repo $R player "Player Name" --line 26.5   # splits + halftime threshold table
```

Pick the player and line (web search for the real line), then choose `--hi`/`--lo` halftime thresholds from the table: each side needs 8+ games and a clear gap (e.g. 79% vs 17%).

**Reel (Mon/Wed/Fri):**
```bash
python3 nba_data.py --repo $R reel-spec "Player Name" --line 29.5 --date YYYY-MM-DD \
   --sample <season year> --hi 15 --lo 12 --label "LAST NIGHT vs BOS" --tag "LAST NIGHT" --out /tmp/spec.json
python3 render.py still /tmp/spec.json /tmp/check13.png --t 13      # look at it
python3 render.py still /tmp/spec.json /tmp/check30.png --t 30.5
python3 render.py reel  /tmp/spec.json /tmp/out/<player>_<date>_reel.mp4
```
- `--sample`: the season the hit rates come from. Use last season (e.g. 2026 for 2025-26) until this season has 10+ games before the featured date; the tool refuses a sample under 10 games.
- `--stat fg3m` makes a made-threes version (line = threes line).
- Both `VERIFY` lines must be clean (0 mismatches; featured total matches the site box score once it's there). Look at both check stills before rendering.

**Data card (Thu/Sat):**
```bash
python3 nba_data.py --repo $R card-spec "Player Name" --line 25.5 --stat pts --opp PHI --loc home --hi 12 \
   --kicker "NBA · TONIGHT" --matchup "vs PHI · 7:30 ET" --headline "<short data-backed headline>" \
   [--before YYYY-MM-DD]  --out /tmp/card.json
```
Then edit `/tmp/card.json`: write the `headline` (≤ 2 lines), an `insight` (1–2 short lines explaining the split), set `highlight` to the row that carries the story, and for recaps add `result` (e.g. `{"value":32,"text":"32 PTS · OVER"}`) and pass `--before <game date>`. Render:
```bash
python3 render.py card /tmp/card.json /tmp/out/<player>_<date>_card.png
```

## NFL workflow (Tue/Sun)

The NFL site data is `NFL/propbetiq_nfl-*.json`: `players[id] = {display_name, current_team, position, passing|rushing|receiving: [game rows]}`; each row has `wk, season, yards, loc ('home'/'away'), wx (weather bucket), opp ('top_/mid_/bottom_pass_def' or '_run_def'), q1_yds, h1_yds` plus `cmp/att/td/int` (passing), `carries` (rushing), `rec/tgt/q1_rec/h1_rec` (receiving). A game "hits" when the stat is **≥** the line rounded up (yards > line).

Build the card spec by hand in Python from those rows using the same JSON shape as an NBA card (`rows` = label/hits/n; first row = baseline, the highlighted row = the split that matters: location, opponent tier, weather, Q1/H1 pace, last 4). Use `NBA.find`-style team colors: pick the team's real primary/secondary colors (no logos). For Tuesday recaps, only use games before that week.

## Delivery

1. Send the rendered file with SendUserFile (reel `.mp4` 1080×1080, or card `.png` 1080×1350).
2. Present the caption with the writing-draft card (short: hook line, 2–3 stat lines, the payoff, CTA, responsible-gambling line, hashtags).
3. In one short message: the story in a sentence, the prop line and where it came from (link), and any caveat (stale data, small sample).
