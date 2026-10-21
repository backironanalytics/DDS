# ══════════════════════════════════════════════════════════════════════════════
# NBA DATA SOURCES — PropBetIQ
# Replaces the stats.nba.com calls (nbastatR::game_logs, nbastatR::nba_teams,
# hoopR::nba_commonteamroster). stats.nba.com — and cdn.nba.com — refuse
# requests that don't come from a browser on nba.com, so they can't be used
# from GitHub Actions.
#
#   Box scores : hoopR / sportsdataverse ESPN player box scores (hosted on
#                GitHub, refreshed daily), plus a per-game ESPN top-up for any
#                finished game that isn't in the daily file yet
#   Rosters    : ESPN site API
#   Player IDs : data/nba_id_map.csv maps ESPN athlete IDs -> official NBA IDs,
#                so JSON keys and headshots stay exactly as before. Players not
#                in the map yet (e.g. new rookies) keep their ESPN ID.
#
# Data frames use the same column names nbastatR produced (idPlayer, namePlayer,
# dateGame, slugTeam, slugOpponent, locationGame, typeSeason, yearSeason,
# slugSeason, pts, ast, treb, ...), so the rest of Compiled_daily_run.R works
# unchanged. Checked against the live 2025-26 data: 28,444 of 28,462 player-games
# line up with identical stats.
# ══════════════════════════════════════════════════════════════════════════════

`%||%` <- function(a, b) if (is.null(a)) b else a

NBA_ID_MAP_FILE <- Sys.getenv("PROPBETIQ_NBA_ID_MAP", "data/nba_id_map.csv")

ESPN_TO_NBA_ABBR <- c(GS = "GSW", NY = "NYK", SA = "SAS", NO = "NOP", UTAH = "UTA", WSH = "WAS")

.nba_abbr <- function(x) {
  x <- as.character(x)
  ifelse(x %in% names(ESPN_TO_NBA_ABBR), unname(ESPN_TO_NBA_ABBR[x]), x)
}

.nba_season_slug <- function(season_year) {
  paste0(season_year - 1, "-", sprintf("%02d", season_year %% 100))
}

.col <- function(df, name, default = NA) {
  if (name %in% names(df)) df[[name]] else rep(default, nrow(df))
}

.as_num <- function(x) suppressWarnings(as.numeric(gsub("\\+", "", as.character(x))))

.as_flag <- function(x) {
  x <- tolower(as.character(x))
  !is.na(x) & x %in% c("true", "t", "1", "yes")
}

# ── Teams (replaces nbastatR::nba_teams) ───────────────────────────────────────
# cityTeam matches the opponent names on ESPN schedule pages;
# nameTeam matches ESPN play-by-play full team names.
nba_teams_table <- function() {
  tibble::tribble(
    ~idTeam,     ~slugTeam, ~cityTeam,       ~nameTeam,
    1610612737,  "ATL",     "Atlanta",       "Atlanta Hawks",
    1610612738,  "BOS",     "Boston",        "Boston Celtics",
    1610612751,  "BKN",     "Brooklyn",      "Brooklyn Nets",
    1610612766,  "CHA",     "Charlotte",     "Charlotte Hornets",
    1610612741,  "CHI",     "Chicago",       "Chicago Bulls",
    1610612739,  "CLE",     "Cleveland",     "Cleveland Cavaliers",
    1610612742,  "DAL",     "Dallas",        "Dallas Mavericks",
    1610612743,  "DEN",     "Denver",        "Denver Nuggets",
    1610612765,  "DET",     "Detroit",       "Detroit Pistons",
    1610612744,  "GSW",     "Golden State",  "Golden State Warriors",
    1610612745,  "HOU",     "Houston",       "Houston Rockets",
    1610612754,  "IND",     "Indiana",       "Indiana Pacers",
    1610612746,  "LAC",     "LA",            "LA Clippers",
    1610612747,  "LAL",     "Los Angeles",   "Los Angeles Lakers",
    1610612763,  "MEM",     "Memphis",       "Memphis Grizzlies",
    1610612748,  "MIA",     "Miami",         "Miami Heat",
    1610612749,  "MIL",     "Milwaukee",     "Milwaukee Bucks",
    1610612750,  "MIN",     "Minnesota",     "Minnesota Timberwolves",
    1610612740,  "NOP",     "New Orleans",   "New Orleans Pelicans",
    1610612752,  "NYK",     "New York",      "New York Knicks",
    1610612760,  "OKC",     "Oklahoma City", "Oklahoma City Thunder",
    1610612753,  "ORL",     "Orlando",       "Orlando Magic",
    1610612755,  "PHI",     "Philadelphia",  "Philadelphia 76ers",
    1610612756,  "PHX",     "Phoenix",       "Phoenix Suns",
    1610612757,  "POR",     "Portland",      "Portland Trail Blazers",
    1610612758,  "SAC",     "Sacramento",    "Sacramento Kings",
    1610612759,  "SAS",     "San Antonio",   "San Antonio Spurs",
    1610612761,  "TOR",     "Toronto",       "Toronto Raptors",
    1610612762,  "UTA",     "Utah",          "Utah Jazz",
    1610612764,  "WAS",     "Washington",    "Washington Wizards"
  ) |>
    dplyr::mutate(
      idDivision       = 1,
      urlThumbnailTeam = paste0("https://cdn.nba.com/logos/nba/", idTeam, "/primary/L/logo.svg")
    )
}

# ── ESPN -> NBA player ID map ──────────────────────────────────────────────────
.nba_id_map <- function() {
  if (!file.exists(NBA_ID_MAP_FILE)) {
    message("  ⚠ ", NBA_ID_MAP_FILE, " not found — players will keep ESPN IDs")
    return(tibble::tibble(espn_id = character(), nba_id = numeric(), nba_name = character()))
  }
  utils::read.csv(NBA_ID_MAP_FILE, colClasses = c("character", "numeric", "character"),
                  fileEncoding = "UTF-8") |>
    tibble::as_tibble() |>
    dplyr::distinct(espn_id, .keep_all = TRUE)
}

# ── Raw ESPN player box scores for one season ──────────────────────────────────
.espn_player_box <- function(season_year) {
  box <- tryCatch(
    suppressWarnings(hoopR::load_nba_player_box(seasons = season_year)),
    error = function(e) { message("  Daily box score file for ", season_year, " not available: ", conditionMessage(e)); NULL }
  )
  if (!is.null(box)) box <- tibble::as_tibble(box)

  box
}

# Finished games this season that aren't in hoopR's daily file yet (e.g. last
# night, or the whole season before hoopR publishes it) — fetched straight from
# ESPN's box score API and cached in data/box so each game is downloaded once.
.espn_topup_box <- function(box, season_year) {
  sched <- espn_league_schedule(seasontype = 2)
  po    <- espn_league_schedule(seasontype = 3)
  sched <- dplyr::bind_rows(sched, po)
  if (!nrow(sched)) return(box)
  sched <- sched[sched$season_year == season_year & sched$completed, ]
  want  <- unique(sched$event_id)
  have  <- if (is.null(box)) character() else unique(as.character(box$game_id))
  missing <- setdiff(want, have)
  if (!length(missing)) return(box)

  cache_dir <- file.path(Sys.getenv("PROPBETIQ_BOX_DIR", "data/box"), "espn")
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  message("  ", length(missing), " finished ", .nba_season_slug(season_year),
          " game(s) not in hoopR's file yet — using ESPN box scores")
  stype_of <- stats::setNames(sched$season_type, sched$event_id)

  extra <- lapply(missing, function(gid) {
    f <- file.path(cache_dir, paste0(gid, ".rds"))
    if (file.exists(f)) return(readRDS(f))
    Sys.sleep(0.3)
    rows <- .espn_game_box(gid, season_year, stype_of[[gid]])
    if (!is.null(rows) && nrow(rows)) saveRDS(rows, f)
    rows
  })
  extra <- dplyr::bind_rows(extra)
  if (!nrow(extra)) return(box)
  if (is.null(box)) return(extra)
  dplyr::bind_rows(dplyr::mutate(box, dplyr::across(dplyr::everything(), as.character)), extra)
}

# One game's player box score from ESPN's summary API, in hoopR's column names
.espn_game_box <- function(event_id, season_year, season_type) {
  js <- .espn_get_json(paste0(
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=", event_id))
  if (is.null(js$boxscore$players)) {
    message("    ESPN box ", event_id, " unavailable: ", attr(js, "error") %||% "no players")
    return(NULL)
  }
  comp  <- js$header$competitions[[1]]
  when  <- as.POSIXct(comp$date, format = "%Y-%m-%dT%H:%MZ", tz = "UTC")
  gdate <- format(when, "%Y-%m-%d", tz = "America/New_York")
  ha    <- stats::setNames(
    vapply(comp$competitors, function(t) t$homeAway, ""),
    vapply(comp$competitors, function(t) t$team$abbreviation, "")
  )
  abbrs <- names(ha)
  split_made <- function(x, i) {
    v <- strsplit(ifelse(is.na(x), "", x), "-", fixed = TRUE)
    vapply(v, function(z) if (length(z) >= i) z[i] else NA_character_, "")
  }
  dplyr::bind_rows(lapply(js$boxscore$players, function(tp) {
    team <- tp$team$abbreviation
    opp  <- setdiff(abbrs, team)[1]
    st   <- tp$statistics[[1]]
    keys <- unlist(st$keys)
    dplyr::bind_rows(lapply(st$athletes, function(a) {
      v <- unlist(a$stats)
      get <- function(k) { i <- match(k, keys); if (is.na(i) || i > length(v)) NA_character_ else v[[i]] }
      fg  <- get("fieldGoalsMade-fieldGoalsAttempted")
      fg3 <- get("threePointFieldGoalsMade-threePointFieldGoalsAttempted")
      ft  <- get("freeThrowsMade-freeThrowsAttempted")
      tibble::tibble(
        game_id = as.character(event_id), season = as.character(season_year),
        season_type = as.character(season_type), game_date = gdate,
        athlete_id = as.character(a$athlete$id),
        athlete_display_name = a$athlete$displayName %||% NA_character_,
        team_abbreviation = team, opponent_team_abbreviation = opp,
        home_away = ha[[team]] %||% NA_character_,
        did_not_play = as.character(isTRUE(a$didNotPlay) || length(v) == 0),
        minutes = get("minutes"), points = get("points"), assists = get("assists"),
        rebounds = get("rebounds"), offensive_rebounds = get("offensiveRebounds"),
        defensive_rebounds = get("defensiveRebounds"), steals = get("steals"),
        blocks = get("blocks"), turnovers = get("turnovers"), fouls = get("fouls"),
        field_goals_made = split_made(fg, 1), field_goals_attempted = split_made(fg, 2),
        three_point_field_goals_made = split_made(fg3, 1),
        three_point_field_goals_attempted = split_made(fg3, 2),
        free_throws_made = split_made(ft, 1), free_throws_attempted = split_made(ft, 2),
        plus_minus = get("plusMinus"),
        athlete_headshot_href = a$athlete$headshot$href %||% NA_character_
      )
    }))
  }))
}

# ── Game logs (replaces nbastatR::game_logs(result_types = "player")) ─────────
# season_year = ending year (2025-26 -> 2026), same as nbastatR.
nba_game_logs <- function(season_year, season_complete = FALSE) {
  slug <- .nba_season_slug(season_year)
  box  <- .espn_player_box(season_year)
  if (!season_complete) box <- .espn_topup_box(box, season_year)
  if (is.null(box) || nrow(box) == 0) {
    message("  No ", slug, " box scores yet")
    return(NULL)
  }

  idmap <- .nba_id_map()

  out <- tibble::tibble(
    espn_id      = as.character(box$athlete_id),
    espn_name    = as.character(box$athlete_display_name),
    idGame       = .as_num(box$game_id),
    dateGame     = as.Date(substr(as.character(box$game_date), 1, 10)),
    season_code  = as.character(.col(box, "season_type")),
    slugTeam     = .nba_abbr(box$team_abbreviation),
    slugOpponent = .nba_abbr(.col(box, "opponent_team_abbreviation")),
    locationGame = ifelse(tolower(as.character(.col(box, "home_away"))) == "home", "H", "A"),
    dnp          = .as_flag(.col(box, "did_not_play", FALSE)),
    minutes      = .as_num(.col(box, "minutes")),
    pts          = .as_num(.col(box, "points")),
    ast          = .as_num(.col(box, "assists")),
    treb         = .as_num(.col(box, "rebounds")),
    oreb         = .as_num(.col(box, "offensive_rebounds")),
    dreb         = .as_num(.col(box, "defensive_rebounds")),
    stl          = .as_num(.col(box, "steals")),
    blk          = .as_num(.col(box, "blocks")),
    tov          = .as_num(.col(box, "turnovers")),
    pf           = .as_num(.col(box, "fouls")),
    fgm          = .as_num(.col(box, "field_goals_made")),
    fga          = .as_num(.col(box, "field_goals_attempted")),
    fg3m         = .as_num(.col(box, "three_point_field_goals_made")),
    fg3a         = .as_num(.col(box, "three_point_field_goals_attempted")),
    ftm          = .as_num(.col(box, "free_throws_made")),
    fta          = .as_num(.col(box, "free_throws_attempted")),
    plusminus    = .as_num(.col(box, "plus_minus")),
    espn_headshot = as.character(.col(box, "athlete_headshot_href"))
  ) |>
    # Regular season + playoffs only (no play-in / All-Star), players who played
    dplyr::filter(season_code %in% c("2", "3"),
                  slugTeam %in% nba_teams_table()$slugTeam,
                  !dnp, !is.na(pts)) |>
    dplyr::distinct(espn_id, idGame, .keep_all = TRUE) |>
    dplyr::left_join(idmap, by = "espn_id") |>
    dplyr::mutate(
      idPlayer   = dplyr::coalesce(nba_id, .as_num(espn_id)),
      namePlayer = dplyr::coalesce(nba_name, espn_name),
      typeSeason = ifelse(season_code == "3", "Playoffs", "Regular Season"),
      yearSeason = as.numeric(season_year),
      slugSeason = slug,
      urlPlayerHeadshot = ifelse(
        !is.na(nba_id),
        paste0("https://cdn.nba.com/headshots/nba/latest/1040x760/", nba_id, ".png"),
        espn_headshot
      )
    ) |>
    dplyr::left_join(nba_teams_table() |> dplyr::select(slugTeam, idTeam), by = "slugTeam") |>
    dplyr::select(idGame, dateGame, yearSeason, slugSeason, typeSeason, idTeam, slugTeam,
                  slugOpponent, locationGame, idPlayer, namePlayer, espn_id, minutes,
                  pts, ast, treb, oreb, dreb, stl, blk, tov, pf, fgm, fga, fg3m, fg3a,
                  ftm, fta, plusminus, urlPlayerHeadshot) |>
    dplyr::arrange(dateGame, idGame)

  # The NBA Cup championship game doesn't count in official stats (nbastatR left it
  # out). It's the only game on its date, in mid-December — drop it the same way.
  cup_final <- out |>
    dplyr::distinct(idGame, dateGame, typeSeason) |>
    dplyr::add_count(dateGame, name = "games_that_day") |>
    dplyr::filter(typeSeason == "Regular Season", games_that_day == 1,
                  format(dateGame, "%m") == "12",
                  as.integer(format(dateGame, "%d")) >= 13,
                  as.integer(format(dateGame, "%d")) <= 20) |>
    dplyr::pull(idGame)
  if (length(cup_final)) {
    message("  Excluding NBA Cup final (doesn't count in official stats): game ", paste(cup_final, collapse = ", "))
    out <- dplyr::filter(out, !idGame %in% cup_final)
  }

  n_unmapped <- dplyr::n_distinct(out$espn_id[is.na(match(out$espn_id, idmap$espn_id))])
  message("  ", slug, ": ", dplyr::n_distinct(out$idGame), " games, ", nrow(out),
          " player rows (", n_unmapped, " players not yet in the NBA ID map)")
  out
}

# Team-level game list (only dateGame / typeSeason are used downstream)
nba_team_games <- function(playerdata) {
  if (is.null(playerdata)) return(NULL)
  playerdata |>
    dplyr::distinct(idGame, dateGame, typeSeason, yearSeason, slugSeason,
                    slugTeam, slugOpponent, locationGame)
}

# ── ESPN JSON request (browser-like headers, retries, real error messages) ──
.espn_get_json <- function(url, tries = 3) {
  err <- "unknown error"
  for (i in seq_len(tries)) {
    res <- tryCatch(
      httr::GET(url, httr::timeout(30),
                httr::user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
                httr::accept_json()),
      error = function(e) { err <<- conditionMessage(e); NULL }
    )
    if (!is.null(res)) {
      if (httr::status_code(res) == 200) {
        out <- tryCatch(
          jsonlite::fromJSON(httr::content(res, as = "text", encoding = "UTF-8"), simplifyVector = FALSE),
          error = function(e) { err <<- paste("bad JSON:", conditionMessage(e)); NULL }
        )
        if (!is.null(out)) return(out)
      } else {
        err <- paste("HTTP", httr::status_code(res))
      }
    }
    Sys.sleep(2 * i)
  }
  structure(list(), error = err)
}

# ── Rosters (replaces hoopR::nba_commonteamroster) ────────────────────────────
nba_rosters <- function(playerdata) {
  teams_tbl <- nba_teams_table()
  idmap     <- .nba_id_map()

  raw <- dplyr::bind_rows(lapply(1:30, function(tid) {
    url <- paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/", tid, "/roster")
    js  <- .espn_get_json(url)
    if (is.null(js$athletes)) {
      message("  ESPN roster failed for ESPN team ", tid, ": ", attr(js, "error") %||% "no athletes in response")
      return(NULL)
    }
    Sys.sleep(0.3)
    abbr <- .nba_abbr(if (is.null(js$team$abbreviation)) NA_character_ else js$team$abbreviation)
    dplyr::bind_rows(lapply(js$athletes, function(a) {
      tibble::tibble(
        espn_id           = as.character(a$id),
        ESPN_PLAYER       = if (is.null(a$displayName)) NA_character_ else a$displayName,
        POSITION          = if (is.null(a$position$abbreviation)) NA_character_ else a$position$abbreviation,
        team_abbreviation = abbr
      )
    }))
  }))
  if (!nrow(raw)) {
    # Fallback: each player's team from his most recent game, so the run can continue
    message("  ⚠ ESPN rosters unavailable — using each player's most recent team instead")
    return(
      playerdata |>
        dplyr::arrange(dplyr::desc(dateGame)) |>
        dplyr::distinct(idPlayer, .keep_all = TRUE) |>
        dplyr::left_join(teams_tbl |> dplyr::select(slugTeam, tid = idTeam), by = "slugTeam") |>
        dplyr::transmute(PLAYER = namePlayer, PLAYER_ID = idPlayer, POSITION = NA_character_,
                         team_abbreviation = slugTeam, team_id = as.character(tid))
    )
  }

  # Use the same ID and name spelling the game logs use
  name_by_id <- playerdata |>
    dplyr::arrange(dplyr::desc(dateGame)) |>
    dplyr::distinct(espn_id, .keep_all = TRUE) |>
    dplyr::select(espn_id, log_id = idPlayer, log_name = namePlayer)

  out <- raw |>
    dplyr::left_join(idmap, by = "espn_id") |>
    dplyr::left_join(name_by_id, by = "espn_id") |>
    dplyr::mutate(
      PLAYER_ID = dplyr::coalesce(log_id, nba_id, .as_num(espn_id)),
      PLAYER    = dplyr::coalesce(log_name, nba_name, ESPN_PLAYER)
    ) |>
    dplyr::left_join(teams_tbl |> dplyr::select(slugTeam, idTeam),
                     by = c("team_abbreviation" = "slugTeam")) |>
    dplyr::transmute(PLAYER, PLAYER_ID, POSITION, team_abbreviation,
                     team_id = as.character(idTeam))

  message("  Rosters: ", nrow(out), " players on ", dplyr::n_distinct(out$team_abbreviation), " teams")
  out
}


# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULE — ESPN API (replaces scraping espn.com team schedule pages, which
# ESPN blocks from cloud servers)
# ══════════════════════════════════════════════════════════════════════════════

ESPN_TEAM_IDS <- c(ATL = 1, BOS = 2, NOP = 3, CHI = 4, CLE = 5, DAL = 6, DEN = 7, DET = 8,
                   GSW = 9, HOU = 10, IND = 11, LAC = 12, LAL = 13, MIA = 14, MIL = 15,
                   MIN = 16, BKN = 17, NYK = 18, ORL = 19, PHI = 20, PHX = 21, POR = 22,
                   SAC = 23, SAS = 24, OKC = 25, UTA = 26, WAS = 27, TOR = 28, MEM = 29,
                   CHA = 30)

.espn_sched_cache <- new.env()

# Every team's schedule for the current season, one row per team per game
espn_league_schedule <- function(seasontype = 2) {
  key <- paste0("st", seasontype)
  if (!is.null(.espn_sched_cache[[key]])) return(.espn_sched_cache[[key]])
  out <- dplyr::bind_rows(lapply(names(ESPN_TEAM_IDS), function(tri) {
    js <- .espn_get_json(paste0("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/",
                                ESPN_TEAM_IDS[[tri]], "/schedule?seasontype=", seasontype))
    if (is.null(js$events)) {
      message("  ESPN schedule failed for ", tri, ": ", attr(js, "error") %||% "no events")
      return(NULL)
    }
    Sys.sleep(0.2)
    yr <- js$requestedSeason$year %||% js$season$year %||% NA
    dplyr::bind_rows(lapply(js$events, function(e) {
      cmp  <- e$competitions[[1]]
      me   <- Filter(function(t) .nba_abbr(t$team$abbreviation) == tri, cmp$competitors)
      them <- Filter(function(t) .nba_abbr(t$team$abbreviation) != tri, cmp$competitors)
      if (!length(me) || !length(them)) return(NULL)
      me <- me[[1]]; them <- them[[1]]
      st <- cmp$status$type
      when <- as.POSIXct(e$date, format = "%Y-%m-%dT%H:%MZ", tz = "UTC")
      tv <- paste(vapply(cmp$broadcasts %||% list(),
                         function(b) b$media$shortName %||% "", ""), collapse = "/")
      score <- function(t) t$score$displayValue %||% ""
      tibble::tibble(
        event_id    = as.character(e$id),
        season_year = as.numeric(yr),
        season_type = as.character(seasontype),
        Team        = tri,
        opp         = .nba_abbr(them$team$abbreviation),
        location    = ifelse(me$homeAway == "home", "Home", "Away"),
        game_date   = as.Date(format(when, "%Y-%m-%d", tz = "America/New_York")),
        status      = st$name %||% "",
        completed   = isTRUE(st$completed),
        Time = dplyr::case_when(
          isTRUE(st$completed) ~ paste0(ifelse(isTRUE(me$winner), "W ", "L "), score(me), "-", score(them)),
          grepl("POSTPONED|CANCELED", st$name %||% "") ~ "Postponed",
          TRUE ~ trimws(format(when, "%I:%M %p", tz = "America/New_York"))
        ),
        TV = tv
      )
    }))
  }))
  .espn_sched_cache[[key]] <- out
  out
}

# Drop-in replacement for get_team_schedule() + process_schedule():
# returns one team's processed schedule with the same columns as before.
espn_team_schedule <- function(x, seasontype) {
  tri <- toupper(x)
  tri <- ifelse(tri == "UTAH", "UTA", ifelse(tri == "NO", "NOP", tri))
  sched <- espn_league_schedule(seasontype)
  if (!nrow(sched)) return(NULL)
  teams <- nba_teams_table()
  sched |>
    dplyr::filter(Team == tri) |>
    dplyr::arrange(game_date) |>
    dplyr::left_join(teams |> dplyr::select(slugTeam, idTeam, nameTeam, urlThumbnailTeam, cityTeam),
                     by = c("opp" = "slugTeam")) |>
    dplyr::mutate(slugTeam = opp, Opponent = cityTeam, game_number = dplyr::row_number()) |>
    dplyr::select(game_date, location, Opponent, slugTeam, idTeam, nameTeam,
                  urlThumbnailTeam, Team, game_number, TV, Time)
}
