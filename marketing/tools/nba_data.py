"""PropBetIQ marketing data helper (NBA).

Sources (all reachable from a cloud session):
  - PropBetIQ site data:   <repo>/NBA/propbetiq_nba-*.json   (game logs + Q1/H1/Q3 splits, NBA player IDs)
  - ESPN -> NBA id map:    <repo>/data/nba_id_map.csv
  - Shot locations / pbp:  sportsdataverse ESPN play-by-play CSV on GitHub releases
  - Jersey numbers/teams:  sportsdataverse ESPN player box CSV on GitHub releases

Usage (run from anywhere; --repo points at a clone of backironanalytics/DDS):
  python nba_data.py --repo DDS last-night                      # yesterday's best performances vs typical lines
  python nba_data.py --repo DDS player "Shai Gilgeous-Alexander" --line 29.5 --stat pts
  python nba_data.py --repo DDS reel-spec "Shai Gilgeous-Alexander" --line 29.5 --date 2026-10-20 \
        --sample 2026 --hi 15 --lo 12 --label "OPENING NIGHT @ SAS" --out spec.json
  python nba_data.py --repo DDS card-spec "Jalen Brunson" --line 26.5 --stat pts --opp PHI --loc home --out card.json
Every number printed comes straight from the data; check the VERIFY lines before using a spec.
"""
import argparse, csv, glob, json, os, re, statistics, sys, unicodedata, urllib.request, datetime as dt

CACHE = os.environ.get("PBIQ_CACHE", "/tmp/pbiq_cache")
SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download"
ESPN2NBA = {"GS": "GSW", "NY": "NYK", "SA": "SAS", "NO": "NOP", "UTAH": "UTA", "WSH": "WAS"}
# primary, light, secondary, dark  (team colors only; never logos)
COLORS = {
 "ATL":("#E03A3E","#FF6B6F","#C1D32F","#26282A"),"BOS":("#007A33","#2FA35E","#BA9653","#00381A"),
 "BKN":("#1A1A1A","#4A4A4A","#FFFFFF","#000000"),"CHA":("#1D1160","#3A2A8C","#00788C","#0B0630"),
 "CHI":("#CE1141","#E84A6F","#111111","#5A0A1E"),"CLE":("#860038","#B0335F","#FDBB30","#41001B"),
 "DAL":("#00538C","#2A7BBF","#B8C4CA","#002B5E"),"DEN":("#0E2240","#2A4A7A","#FEC524","#060F1F"),
 "DET":("#C8102E","#E54A60","#1D42BA","#5C0715"),"GSW":("#1D428A","#3F6BC2","#FFC72C","#0B1F44"),
 "HOU":("#CE1141","#E84A6F","#C4CED4","#5A0A1E"),"IND":("#002D62","#1F4E8C","#FDBB30","#00152E"),
 "LAC":("#C8102E","#E54A60","#1D428A","#5C0715"),"LAL":("#552583","#7A4BAA","#FDB927","#2A1242"),
 "MEM":("#5D76A9","#8098C6","#F5B112","#12173F"),"MIA":("#98002E","#C23358","#F9A01B","#41000F"),
 "MIL":("#00471B","#1F6B38","#EEE1C6","#00230D"),"MIN":("#0C2340","#2A4A72","#78BE20","#050F1D"),
 "NOP":("#0C2340","#2A4A72","#C8102E","#050F1D"),"NYK":("#006BB6","#2F8FD6","#F58426","#00365C"),
 "OKC":("#007AC1","#2A9BE0","#EF3B24","#002D62"),"ORL":("#0077C0","#2F98DC","#C4CED4","#003A5E"),
 "PHI":("#006BB6","#2F8FD6","#ED174C","#00365C"),"PHX":("#1D1160","#3A2A8C","#E56020","#0B0630"),
 "POR":("#E03A3E","#FF6B6F","#111111","#5C1416"),"SAC":("#5A2D81","#7E55A6","#63727A","#2A1240"),
 "SAS":("#8A949A","#C4CED4","#111111","#3E4448"),"TOR":("#CE1141","#E84A6F","#111111","#5A0A1E"),
 "UTA":("#002B5C","#1F4E8C","#F9A01B","#00152E"),"WAS":("#002B5C","#1F4E8C","#E31837","#00152E"),
}
STAT_LABEL = {"pts": ("POINTS", "PTS"), "fg3m": ("THREES", "3PM"), "reb": ("REBOUNDS", "REB"), "ast": ("ASSISTS", "AST")}

def key(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[^a-z ]", "", s); s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", s)
    return " ".join(s.split())

def fetch(url, name):
    os.makedirs(CACHE, exist_ok=True); p = os.path.join(CACHE, name)
    if not os.path.exists(p) or os.path.getmtime(p) < (dt.datetime.now().timestamp() - 6 * 3600):
        print(f"  downloading {name} ...", file=sys.stderr); urllib.request.urlretrieve(url, p)
    return p

def season_label(y): return f"{y-1}-{str(y)[-2:]}"

class NBA:
    def __init__(self, repo):
        f = sorted(glob.glob(os.path.join(repo, "NBA", "propbetiq_nba-*.json")))
        if not f: sys.exit("No NBA/propbetiq_nba-*.json in repo")
        self.site = json.load(open(f[-1])); self.players = self.site["players"]
        self.e2n, self.n2e = {}, {}
        mp = os.path.join(repo, "data", "nba_id_map.csv")
        if os.path.exists(mp):
            for r in csv.DictReader(open(mp, encoding="utf-8")):
                self.e2n[r["espn_id"]] = r["nba_id"]; self.n2e[r["nba_id"]] = r["espn_id"]
        self._box, self._pbp = {}, {}

    def box(self, y):
        if y not in self._box:
            p = fetch(f"{SDV}/espn_nba_player_boxscores/player_box_{y}.csv", f"player_box_{y}.csv")
            self._box[y] = list(csv.DictReader(open(p, encoding="utf-8")))
        return self._box[y]

    def pbp(self, y, espn_id):
        k = (y, espn_id)
        if k not in self._pbp:
            p = fetch(f"{SDV}/espn_nba_pbp/play_by_play_{y}.csv", f"pbp_{y}.csv")
            rows = []
            with open(p, encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    if r["athlete_id_1"] == espn_id and r["shooting_play"] == "true": rows.append(r)
            self._pbp[k] = rows
        return self._pbp[k]

    def find(self, name):
        k = key(name)
        hits = [(pid, p) for pid, p in self.players.items() if key(p["display_name"]) == k] or \
               [(pid, p) for pid, p in self.players.items() if k in key(p["display_name"])]
        if not hits: sys.exit(f"Player not found on site: {name}")
        pid, p = hits[0]; espn = self.n2e.get(pid, pid)
        number = None
        for y in (self.cur(), self.cur() - 1):
            try:
                rows = [r for r in self.box(y) if r["athlete_id"] == espn and r.get("athlete_jersey")]
                if rows: number = rows[-1]["athlete_jersey"]; break
            except Exception: pass
        team = p.get("current_team")
        return {"nba_id": pid, "espn_id": espn, "name": p["display_name"], "team": team, "number": number,
                "colors": dict(zip(("primary", "light", "secondary", "dark"), COLORS.get(team, COLORS["OKC"])))}

    def cur(self): return int(self.site["meta"]["current_season_year"])

    def games(self, pid, season=None, stype="regular"):
        g = [x for x in self.players[pid]["games"] if (season is None or x["season"] == season) and (stype is None or x["season_type"] == stype)]
        return sorted(g, key=lambda x: x["date"])

def made_shots_by_date(rows):
    out = {}
    for r in rows:
        if "Free Throw" in r["type_text"] or r["scoring_play"] != "true": continue
        x, y = float(r["coordinate_x_raw"]), float(r["coordinate_y_raw"])
        if x < -1000: continue
        out.setdefault(r["game_date"], []).append([round(x, 1), round(y, 1), int(r["points_attempted"] or 2)])
    return out

def pbp_points_by_date(rows):
    out = {}
    for r in rows:
        if r["scoring_play"] == "true": out[r["game_date"]] = out.get(r["game_date"], 0) + int(r["score_value"] or 0)
    return out

def featured_shots(rows, date):
    sh = []
    for r in sorted([r for r in rows if r["game_date"] == date], key=lambda r: int(r["game_play_number"])):
        x, y = float(r["coordinate_x_raw"]), float(r["coordinate_y_raw"]); ft = "Free Throw" in r["type_text"]
        sh.append([int(r["period_number"]), r["clock_display_value"], 1 if r["scoring_play"] == "true" else 0,
                   int(r["score_value"] or 0), int(r["points_attempted"] or (1 if ft else 2)),
                   None if (ft or x < -1000) else round(x, 1), None if (ft or y < -1000) else round(y, 1)])
    return sh

def rate(gs, line, f="pts"):
    h = sum(1 for g in gs if g[f] > line); return h, len(gs)

def cmd_player(nba, a):
    P = nba.find(a.name); f = a.stat; h1f = "h1_" + f if f in ("pts", "ast", "reb") else None
    print(json.dumps({k: P[k] for k in ("name", "team", "number", "nba_id", "espn_id")}))
    for season in (nba.cur() - 1, nba.cur()):
        gs = nba.games(P["nba_id"], season)
        if not gs: continue
        h, n = rate(gs, a.line, f); print(f"\n{season_label(season)} regular season: over {a.line} in {h}/{n} ({round(100*h/n)}%)  avg {statistics.mean(g[f] for g in gs):.1f}")
        for lbl, sub in [("home", [g for g in gs if g["loc"] == "home"]), ("away", [g for g in gs if g["loc"] == "away"]), ("last 10", gs[-10:])]:
            if sub: hh, nn = rate(sub, a.line, f); print(f"  {lbl:8s} {hh}/{nn} ({round(100*hh/nn)}%)")
        if h1f:
            vals = sorted(set(g[h1f] for g in gs))
            print(f"  halftime ({h1f}) thresholds -> over {a.line}:")
            for t in range(int(min(vals)), int(max(vals)) + 1):
                hi = [g for g in gs if g[h1f] >= t]; lo = [g for g in gs if g[h1f] < t]
                if len(hi) >= 6 and len(lo) >= 6:
                    print(f"    {t:>3}+ : {rate(hi,a.line,f)[0]}/{len(hi)} ({round(100*rate(hi,a.line,f)[0]/len(hi))}%)   under {t}: {rate(lo,a.line,f)[0]}/{len(lo)} ({round(100*rate(lo,a.line,f)[0]/len(lo))}%)")

def cmd_last_night(nba, a):
    day = a.date or (dt.date.today() - dt.timedelta(days=1)).isoformat()
    rows = []
    for pid, p in nba.players.items():
        for g in p["games"]:
            if g["date"] == day:
                prev = [x for x in p["games"] if x["date"] < day and x["season_type"] == "regular"]
                avg = statistics.mean(x["pts"] for x in prev[-40:]) if len(prev) >= 10 else None
                rows.append((g["pts"], p["display_name"], p["current_team"], g["opp"], g["loc"], g["h1_pts"], g["reb"], g["ast"], g["fg3m"], round(avg, 1) if avg else None))
    rows.sort(reverse=True)
    print(f"Games on {day}: {len(set((r[2], r[3]) for r in rows))//1 if rows else 0} team-games, {len(rows)} player rows")
    print("pts | player | team vs opp | loc | half pts | reb | ast | 3pm | recent avg pts")
    for r in rows[:25]: print(" | ".join(str(x) for x in r))

def cmd_reel_spec(nba, a):
    P = nba.find(a.name); f = a.stat; assert f in ("pts", "fg3m"), "shot-chart reels support pts or fg3m"
    sample = a.sample or nba.cur(); gs = [g for g in nba.games(P["nba_id"], sample) if g["date"] < a.date]  # pre-game only
    if len(gs) < 10: sys.exit(f"Only {len(gs)} pre-game games in {season_label(sample)} - use --sample {sample-1} (last season) until this season has 10+ games")
    rows_s = nba.pbp(sample, P["espn_id"]); made = made_shots_by_date(rows_s); ppts = pbp_points_by_date(rows_s)
    feat_year = nba.cur(); rows_f = rows_s if feat_year == sample else nba.pbp(feat_year, P["espn_id"])
    sh = featured_shots(rows_f, a.date)
    if not sh: sys.exit(f"No play-by-play shots for {P['name']} on {a.date} (data may not be published yet)")
    games = []
    mism = 0
    for g in gs:
        m = made.get(g["date"], [])
        if f == "fg3m": m = [s for s in m if s[2] == 3]
        if f == "pts" and ppts.get(g["date"]) != g["pts"]: mism += 1
        games.append({"d": g["date"], "opp": g["opp"], "v": g[f], "h1": g.get("h1_" + f, 0) if f == "pts" else 0, "m": m})
    fin = sum(s[3] for s in sh) if f == "pts" else sum(1 for s in sh if s[2] and s[4] == 3)
    site_feat = [g for g in nba.players[P["nba_id"]]["games"] if g["date"] == a.date]
    print(f"VERIFY: {len(games)} sample games; pbp-vs-site point mismatches: {mism} (should be 0)")
    print(f"VERIFY: featured game total from pbp = {fin}; site box = {site_feat[0][f] if site_feat else 'not on site yet'}")
    spec = {"player": {"name": P["name"], "number": P["number"] or "", "colors": P["colors"]},
            "stat": {"label": STAT_LABEL[f][0], "short": STAT_LABEL[f][1], "line": a.line,
                     "madeLabel": "MADE FIELD GOALS" if f == "pts" else "MADE THREES"},
            "season": {"label": f"{season_label(sample)} REGULAR SEASON", "short": season_label(sample)},
            "games": games, "tell": {"hi": a.hi, "lo": a.lo},
            "featured": {"label": a.label, "tag": a.tag, "shots": sh if f == "pts" else [s for s in sh if s[4] == 3]}}
    json.dump(spec, open(a.out, "w")); print(f"wrote {a.out}")

def cmd_card_spec(nba, a):
    P = nba.find(a.name); f = a.stat; L = a.line
    cur, prev = nba.cur(), nba.cur() - 1
    cut = a.before or "9999-12-31"   # for recaps pass --before <game date> so rows are pre-game only
    cg = [g for g in nba.games(P["nba_id"], cur) if g["date"] < cut]
    base_season = cur if len(cg) >= 10 else prev
    gs = [g for g in nba.games(P["nba_id"], base_season) if g["date"] < cut]; allg = [g for g in nba.games(P["nba_id"], None, None) if g["date"] < cut]
    def row(label, sub, note=""):
        h, n = rate(sub, L, f); return {"label": label, "hits": h, "n": n, "note": note}
    rows = [row(f"ALL GAMES {season_label(base_season)}", gs)]
    if a.loc: rows.append(row(f"{a.loc.upper()} GAMES", [g for g in gs if g["loc"] == a.loc]))
    rows.append(row("LAST 10 (ALL SEASONS)", [g for g in allg if g["season_type"] == "regular"][-10:]))
    if a.opp:
        vs = [g for g in allg if g["opp"] == a.opp]
        if vs: rows.append(row(f"vs {a.opp} (LAST 2 SEASONS)", vs))
    h1f = "h1_" + f
    if a.hi and h1f in gs[0]:
        rows.append(row(f"{a.hi}+ {STAT_LABEL.get(f,(f.upper(),))[0]} AT HALF", [g for g in gs if g[h1f] >= a.hi], "LIVE-BET SPLIT"))
    team = P["team"]
    spec = {"kicker": a.kicker or "NBA", "player": {"name": P["name"], "team": team, "number": P["number"] or "", "colors": P["colors"]},
            "prop": {"label": STAT_LABEL.get(f, (f.upper(),))[0], "line": L, "matchup": a.matchup or ""},
            "headline": a.headline or "", "rows": rows, "highlight": len(rows) - 1, "insight": [], "result": None,
            "source": f"Data: PropBetIQ · games through {max(g['date'] for g in allg)}"}
    json.dump(spec, open(a.out, "w")); print(json.dumps(rows, indent=1)); print(f"wrote {a.out} (fill in headline/insight/result before rendering)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--repo", required=True)
    sp = ap.add_subparsers(dest="cmd", required=True)
    p1 = sp.add_parser("player"); p1.add_argument("name"); p1.add_argument("--line", type=float, required=True); p1.add_argument("--stat", default="pts")
    p2 = sp.add_parser("last-night"); p2.add_argument("--date")
    p3 = sp.add_parser("reel-spec"); p3.add_argument("name"); p3.add_argument("--line", type=float, required=True); p3.add_argument("--stat", default="pts")
    p3.add_argument("--date", required=True); p3.add_argument("--sample", type=int); p3.add_argument("--hi", type=int, required=True); p3.add_argument("--lo", type=int, required=True)
    p3.add_argument("--label", required=True); p3.add_argument("--tag", default="LAST NIGHT"); p3.add_argument("--out", default="spec.json")
    p4 = sp.add_parser("card-spec"); p4.add_argument("name"); p4.add_argument("--line", type=float, required=True); p4.add_argument("--stat", default="pts")
    p4.add_argument("--opp"); p4.add_argument("--loc", choices=["home", "away"]); p4.add_argument("--hi", type=int); p4.add_argument("--matchup"); p4.add_argument("--kicker"); p4.add_argument("--headline")
    p4.add_argument("--before"); p4.add_argument("--out", default="card.json")
    a = ap.parse_args(); nba = NBA(a.repo)
    {"player": cmd_player, "last-night": cmd_last_night, "reel-spec": cmd_reel_spec, "card-spec": cmd_card_spec}[a.cmd](nba, a)
