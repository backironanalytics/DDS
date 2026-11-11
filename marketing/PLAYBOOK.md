# PropBetIQ weekly Instagram content: playbook (v2)

Four posts a week for @propbetiq. Goal: engagement and follower growth by showing the depth of our data with a data-science angle. Each post must be unique, easy for the average fan to follow, and about a well-known player.

Chris reviews and publishes every post himself. Never post or publish anything, never commit or push to the repo, never message anyone.

## The weekly schedule (Pacific time)

| Day | Format | Output | Example |
|---|---|---|---|
| Mon | **Player Scouting Report**: one player worth a look, hit rates across several props, Kirk Goldsberry-style visuals | Infographic PNG 1080x1350 | `examples/strawman_mon_scouting_report.png` (Wembanyama) |
| Wed | **Bold claim, "hear me out"**: provocative take, three data receipts, one honest counterpoint, vote CTA | Reel MP4 1080x1920, voiceover, ~15-20s | `examples/kd_reel_example.mp4`, `examples/strawman_wed_bold_claim_storyboard.png` (Giannis) |
| Fri | **One number**: a single striking data point. Does not need a betting angle | Reel MP4 1080x1920, voiceover, ~15s | `examples/strawman_fri_one_number_storyboard.png` (Luka pace) |
| Sat | **Weekend Watchlist**: 4 props on a streak heading into the weekend | Infographic PNG 1080x1350 | `examples/strawman_sat_*.png` |

All four formats are NBA during the NBA season. Players in the Saturday watchlist must have a game that weekend (Sat or Sun).

## Brand and content rules (non-negotiable)

- **Data, not picks.** Never say "lock", "hammer", "free pick" or tell people what to bet. Phrase as "worth a look", "the data says", "keep an eye on". End graphics with "not betting advice".
- **Every number must be verified.** Game results come from the site data (`NBA/propbetiq_nba-*.json`). Shot locations come from ESPN play-by-play via `tools/nba_data.py`. If a number can't be verified, cut it.
- **Lines must be real and readable.**
  - Only show lines a sportsbook actually posted. Never show an averaged line (e.g. "23.4") or a range as a bet (e.g. "OVER 19.5-23.5").
  - Hit rates grade each game against **that game's own closing line**. Say so on the graphic ("each game vs that night's line").
  - When one number is needed for the prop label, use the **latest line** and label it "latest line 19.5". A range may appear only as context in a sentence ("his line has stayed between 19.5 and 23.5").
- **Never mention Covers** (or any data vendor) anywhere on screen, in captions or in voiceovers. Behind the scenes, closing lines come from Covers player pages (see Data below).
- **No player photos, avatars, silhouettes, jersey-number badges, team logos or wordmarks.** Plain player name and team text only. (Some older templates draw a `badge()`: never call it.)
- **No emojis.** Dark navy/purple background, orange #F7A325 accent, fonts Inter / JetBrains Mono / Fraunces (in `fonts/`). Green = over, red = under, yellow = the line.
- **Well-known players only** (regular starters / names casual fans know). **No player repeats within 28 days**: check `posted_log.csv` before choosing. Also avoid using the same player in two formats in the same week.
- Small samples are fine (we use the last 10 games), but say "last 10" and never imply certainty.

## Data

- Site data: `NBA/propbetiq_nba-*.json` (meta.generated_at should be < ~30h old; note it in the delivery if stale). `players[nba_id].games[]` has date, opp, loc, season (2027 = 2026-27), pts, reb, ast, fg3m, stl, blk, h1_*, q1_*.
- Shots: `tools/nba_data.py` (`NBA(repo).find(name)` then `.pbp(2027, espn_id)`) gives every shot with `coordinate_x_raw`, `coordinate_y_raw` (rim at x=25, y=0; feet), `scoring_play`, `score_value`. League zone averages: compute from the same pbp file (rim <4ft, paint <14ft, mid, 3 = corner |x-25|>=21.9 & y<=8.8 or distance >=23.7).
- Closing lines: fetch the player's Covers page with WebFetch, `https://www.covers.com/sport/basketball/nba/players/<id>/<slug>` (find the URL by web search "covers <player name> props"). Each prop section has a 10-game log: date, opponent, result (o/u), closing line (odds). Ask WebFetch for the full table for each prop you need (points, rebounds, assists, 3-pointers made, steals and blocks, points and rebounds...).
  - **Cross-check every result against the site data** (points/rebounds/assists/threes for that date). If a row doesn't match, drop that player.
  - Keep only rows from the current season.
- Facts outside our data (e.g. all-time records for Friday): use a reputable stats source, double-check, and footnote edge cases (e.g. qualified seasons).

## Format details

### Monday: Player Scouting Report (infographic)
- Pick a well-known player with a clear, explainable split across props (e.g. points over 8/10 but rebounds under 6/10). Headline is a short contrast ("Bet the bucket. Not the boards.").
- Layout (see `templates/weekly_infographics.html`, `monday()`): left panel = signature visual; right panel = hit rate vs the line for ~5 props (one dot per game, latest line under each label); bottom = three big-number insight cards.
- **Rotate the left-panel visual** to fit the player: hex shot chart (scorers; size = volume, color = FG% vs league in that zone), assist/creation breakdown, home vs away split, last-10 trend line. Don't use the same left visual two Mondays in a row.

### Wednesday: Bold claim, "hear me out" (reel)
- Arc: HOOK claim (0-3s) -> "Hear me out." -> 3 receipts (each one stat, one visual metaphor) -> honest COUNTERPOINT -> CTA vote ("Overpriced or due?", "Washed or not washed?").
- Each reel needs a **visual metaphor** (KD: the rim is his line and shots fall short; Giannis: a limbo bar he keeps ducking under). Paint a picture; don't just put stats on screen.
- Animate in a copy of `templates/reel_animated_example_kd.html` (functions `S[1..7](t)`, `window.draw(i,t)`); `templates/reel_storyboard_frames.html` has the Giannis and Luka frames as static starting points.

### Friday: One number (reel)
- One striking, easy number with context (historic pace, rarity, "needs X per game the rest of the way"). Arc: number -> why it's insane -> context chart -> the math -> "can he?" -> CTA question.

### Saturday: Weekend Watchlist (infographic)
- 4 props on a streak (7/10 or better, over or under) for well-known players who play this weekend. Mix overs and unders; mix stat types; different teams where possible.
- Card: name, team, "PROP SIDE · latest line X", X/10 hit, the chart, one-sentence "why" (plain English).
- **Rotate the chart by ISO week number** (`date +%V` mod 4): 0 = bars (`sat`), 1 = margin lollipops (`lolli`), 2 = scoreboard tiles (`tiles`), 3 = rolling-ball line (`line`).

## Voiceover (Wed and Fri reels)
- ElevenLabs connector: `creative_generate_speech`, model `eleven_v4`, voice **Olumide Gbenro** `0m1WGWVzxS7KbWobXtnw` (the account's voice; always use it), **generations_count = 1** (one take).
- Write the script to match the storyboard line by line; use `[short pause]` / `[beat]` tags between beats. Poll `creative_get_flow_run_status`, download the `content_url` mp3 with curl.
- Speed up with `atempo=1.08` if long. Find line starts with ffmpeg `silencedetect` and set each scene's duration so it changes when the narrator starts that line (`tools/render_reel.py`).
- No music: Chris adds a trending sound in Instagram at publish time.

## Captions
- First line = a hook with a number not on screen (only ~125 characters show before "more").
- 2-3 short stat lines that add something the visual doesn't say, a plain-English takeaway, a comment question, "Follow @propbetiq for ..." line, 4-6 hashtags. No emojis. Never mention Covers.

## Build and check (every run)
```bash
git clone --depth 1 https://github.com/backironanalytics/DDS.git && cd DDS/marketing
python3 -c "import playwright" && which ffmpeg     # preinstalled; set PW_CHROMIUM if needed, never run playwright install
python3 tools/render_infographic.py <mon|sat|lolli|tiles|line> data.json out.png
python3 tools/render_reel.py stills my_reel.html "3.8,1.2,5.3,4.4,4.6,3.8,5.6"
python3 tools/render_reel.py video  my_reel.html "<durations>" vo.mp3 out.mp4 1.08
```
Look at the rendered PNG / reel stills before delivering: no overlapping text, nothing cut off, every number matches your verified data.

## Delivery (to Chris, in chat)
1. SendUserFile: the finished PNG or MP4, named `<yyyy-mm-dd>_<format>_<player>.<ext>`.
2. The caption as a writing-draft card.
3. One short message: the story in one sentence, the data caveats (sample size, stale data), and the next steps:
   "Drag the file into `marketing/media/` on GitHub and add this row to `marketing/posted_log.csv`: `<row>`. Then reply **uploaded** with the time you want it to go out (default 5:00pm PT)."
4. When Chris replies "uploaded": create the Metricool post with `createScheduledPost` (brand/blogId `8041027`, timezone America/Los_Angeles), media = `https://raw.githubusercontent.com/backironanalytics/DDS/main/marketing/media/<file>`, Instagram type REEL (video) or POST (image), the caption, `autoPublish: false` (Metricool notifies Chris's phone at post time and he publishes from Instagram, adding music), `draft: false`. Send him the `plannerUrl`. Never set autoPublish true unless Chris asks for that post.
