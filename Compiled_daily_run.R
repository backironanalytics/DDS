pkgs <- c(
  "tidyverse", "nbastatR", "dplyr", "flexdashboard", "parallel",
  "rvest", "stringr", "caret", "ggplot2", "ggpubr", "knitr",
  "broom", "grid", "gridExtra", "formattable", "markdown",
  "magick", "highcharter", "extrafont", "cowplot", "fmsb",
  "shiny", "fontawesome", "bslib", "plotly", "ggbreak",
  "hoopR", "crosstalk", "htmltools","httr","jsonlite","progressr",
  "logger","glue","lubridate","arrow","future","furrr","stringi"
)

invisible(lapply(pkgs, library, character.only = TRUE))

# ── Email notification — script started ───────────────────────────────────────
script_start_time <- Sys.time()

tryCatch({
  start_email <- blastula::compose_email(
    body = blastula::md(glue::glue("
## NBA Props Matrix — Run Started

| | |
|---|---|
| **Date** | {format(Sys.Date(), '%A, %B %d %Y')} |
| **Time** | {format(script_start_time, '%I:%M %p')} |
| **Status** | Script is running... |

You will receive another email when the run completes or if it fails.
    "))
  )

  blastula::smtp_send(
    start_email,
    to          = "ccraig2435@gmail.com",
    from        = "ccraig2435@gmail.com",
    subject     = glue::glue("⏳ NBA Props Matrix Started — {format(Sys.Date(), '%b %d %Y')}"),
    credentials = blastula::creds_file("gmail_creds")
  )
  message("Start notification email sent.")
}, error = function(e) {
  message("Start email failed to send: ", conditionMessage(e))
})


`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR    <- "data/pbp"           # where parquet files live
LOG_DIR     <- "logs"
SEASON      <- 2026                 # hoopR uses ending year (2024-25 = 2025)
SLEEP_SEC   <- 1.5                  # polite delay per worker (keeps API happy)
MAX_RETRIES <- 5                    # retry attempts per game on API failure
N_WORKERS   <- 3                    # parallel workers — don't exceed 6 or ESPN
# will rate-limit you. 4 is the safe default.

# ── Setup ─────────────────────────────────────────────────────────────────────
dir.create(DATA_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(LOG_DIR,  recursive = TRUE, showWarnings = FALSE)

log_appender(appender_file(file.path(LOG_DIR, glue("pbp_{Sys.Date()}.log"))))
log_threshold(INFO)
log_info("=== NBA PBP Pipeline START | Season {SEASON} | Workers {N_WORKERS} ===")


# ── Helper: safe API call with retry ──────────────────────────────────────────
# NOTE: logger calls are intentionally removed here — logger is not safe to use
# inside furrr workers (each worker is a separate R process). Instead we return
# a status object and log results back on the main process after collection.

safe_pbp <- function(game_id, retries = MAX_RETRIES, sleep = SLEEP_SEC) {
  attempt <- 0
  last_error <- NULL
  
  while (attempt < retries) {
    attempt <- attempt + 1
    tryCatch({
      
      fetch_page <- function(page) {
        url  <- paste0(
          "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/",
          game_id, "/competitions/", game_id,
          "/plays?limit=400&page=", page
        )
        resp <- httr::GET(
          url,
          httr::add_headers(`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
          httr::timeout(30)
        )
        if (httr::status_code(resp) != 200) stop("HTTP ", httr::status_code(resp))
        jsonlite::fromJSON(
          httr::content(resp, as = "text", encoding = "UTF-8"),
          flatten = TRUE
        )
      }
      
      # Page 1
      parsed      <- fetch_page(1)
      total_plays <- parsed$count %||% 0
      
      if (is.null(parsed$items) || total_plays == 0) stop("No plays returned")
      
      plays       <- parsed$items
      total_pages <- ceiling(total_plays / 400)
      
      # Remaining pages if needed
      if (total_pages > 1) {
        for (pg in 2:total_pages) {
          Sys.sleep(sleep)
          pg_parsed <- fetch_page(pg)
          if (!is.null(pg_parsed$items)) {
            plays <- dplyr::bind_rows(plays, pg_parsed$items)
          }
        }
      }
      
      # ── Rename raw API columns to match hoopR conventions ─────────────────
      plays <- plays |>
        dplyr::rename_with(~ dplyr::case_when(
          .x == "sequenceNumber"      ~ "sequence_number",
          .x == "awayScore"           ~ "away_score",
          .x == "homeScore"           ~ "home_score",
          .x == "scoringPlay"         ~ "scoring_play",
          .x == "scoreValue"          ~ "score_value",
          .x == "shootingPlay"        ~ "shooting_play",
          .x == "pointsAttempted"     ~ "points_attempted",
          .x == "type.id"             ~ "type_id",
          .x == "type.text"           ~ "type_text",
          .x == "period.number"       ~ "period_number",
          .x == "period.displayValue" ~ "period_display_value",
          .x == "clock.value"         ~ "clock_value",
          .x == "clock.displayValue"  ~ "clock_display_value",
          .x == "coordinate.x"        ~ "coordinate_x",
          .x == "coordinate.y"        ~ "coordinate_y",
          .x == "shortText"           ~ "short_text",
          .x == "alternativeText"     ~ "alternative_text",
          .x == "shortAlternativeText"~ "short_alternative_text",
          TRUE                        ~ .x
        )) |>
        # Drop columns not needed
        dplyr::select(-dplyr::any_of(c(
          "$ref", "team.$ref", "probability.$ref",
          "participants", "modified", "valid", "priority"
        )))
      
      Sys.sleep(sleep)
      return(list(status = "ok", data = plays, error = NULL))
      
    }, error = function(e) {
      last_error <<- conditionMessage(e)
      Sys.sleep(sleep * attempt * 2)
    })
  }
  
  list(status = "failed", data = NULL, error = last_error)
}


# ── Step 1: Pull full season schedule ─────────────────────────────────────────

get_season_game_ids <- function(season = SEASON, max_retries = 3) {
  log_info("Fetching schedule for season {season}...")
  
  schedule <- NULL
  attempt  <- 0
  
  while (is.null(schedule) && attempt < max_retries) {
    attempt  <- attempt + 1
    schedule <- tryCatch({
      s <- hoopR::load_nba_schedule(seasons = season)
      if (!"status_type_completed" %in% names(s)) {
        log_warn("Schedule response missing status_type_completed — attempt {attempt}")
        Sys.sleep(10 * attempt)
        return(NULL)
      }
      s
    }, error = function(e) {
      log_warn("Schedule fetch failed attempt {attempt}: {conditionMessage(e)}")
      Sys.sleep(10 * attempt)
      NULL
    }, warning = function(w) {
      log_warn("Schedule fetch warning attempt {attempt}: {conditionMessage(w)}")
      Sys.sleep(10 * attempt)
      NULL
    })
  }
  
  if (is.null(schedule)) {
    log_warn("Schedule unavailable after {max_retries} attempts — using cached game IDs")
    return(data.frame(
      game_id             = integer(),
      game_date           = as.Date(character()),
      home_abbreviation   = character(),
      away_abbreviation   = character(),
      home_id             = character(),
      away_id             = character(),
      home_display_name   = character(),
      away_display_name   = character(),
      home_logo           = character(),
      away_logo           = character(),
      home_color          = character(),
      away_color          = character(),
      season_type         = integer()
    ))
  }
  
  completed <- schedule |>
    dplyr::filter(status_type_completed == TRUE) |>
    dplyr::select(
      game_id,
      game_date,
      season_type,
      home_abbreviation,
      away_abbreviation,
      home_id,
      away_id,
      home_display_name,
      away_display_name,
      home_logo,
      away_logo,
      home_color,
      away_color
    )
  
  log_info("Found {nrow(completed)} completed games in schedule")
  completed
}


# ── Step 2: Determine which games need (re)fetching ───────────────────────────
games_needing_update <- function(schedule_df) {
  existing_files <- list.files(DATA_DIR, pattern = "\\.parquet$", full.names = FALSE)
  existing_ids   <- sub("\\.parquet$", "", existing_files)
  
  # Always re-fetch games from the last 2 days (in case PBP was incomplete)
  recent_cutoff  <- Sys.Date() - 2
  
  to_fetch <- schedule_df |>
    filter(
      !(game_id %in% existing_ids) |                    # never fetched
        as.Date(game_date) >= recent_cutoff             # recent — refresh
    )
  
  log_info(
    "{nrow(to_fetch)} games to fetch | {nrow(schedule_df) - nrow(to_fetch)} already cached"
  )
  to_fetch
}


# ── Step 3: Fetch & store PBP for a batch of games (parallel) ────────────────
fetch_and_store_pbp <- function(games_df) {
  if (nrow(games_df) == 0) {
    log_info("No games to fetch — all up to date.")
    return(invisible(NULL))
  }
  
  log_info("Starting parallel fetch for {nrow(games_df)} games | {N_WORKERS} workers...")
  
  options(future.stdout = FALSE)
  plan(multisession, workers = N_WORKERS)
  on.exit(plan(sequential), add = TRUE)
  
  meta_lookup <- games_df |>
    select(    game_id,
               game_date,
               season_type,
               home_abbreviation,
               away_abbreviation,
               home_id,
               away_id,
               home_display_name,
               away_display_name,
               home_logo,
               away_logo,
               home_color,
               away_color)
  
  with_progress({
    p <- progressor(steps = nrow(games_df))
    raw_results <- furrr::future_map(
      games_df$game_id,
      function(gid) {
        p(message = glue::glue("game {gid}"))
        result <- safe_pbp(gid, retries = MAX_RETRIES, sleep = SLEEP_SEC)
        result$game_id <- gid
        result
      },
      .options = furrr_options(
        globals    = c("safe_pbp", "MAX_RETRIES", "SLEEP_SEC"),
        packages   = c("hoopR", "glue"),
        stdout     = FALSE,
        conditions = character(0)
      )
    )
  })
  
  # ── Collect parallel results ───────────────────────────────────────────────
  n_success  <- 0L
  n_failed   <- 0L
  failed_ids <- character()
  
  for (res in raw_results) {
    gid <- res$game_id
    if (res$status == "failed" || is.null(res$data) || nrow(res$data) == 0) {
      failed_ids <- c(failed_ids, as.character(gid))
      next
    }
    write_pbp(res$data, gid, meta_lookup)
    n_success <- n_success + 1L
  }
  
  # ── Sequential retry pass for any games that failed ───────────────────────
  # Runs one at a time with a longer sleep — avoids rate limiting
  if (length(failed_ids) > 0) {
    log_info("Retrying {length(failed_ids)} failed games sequentially...")
    still_failed <- character()
    
    for (gid in failed_ids) {
      Sys.sleep(3)   # longer pause before each retry
      res <- safe_pbp(gid, retries = 5, sleep = 2)
      
      if (res$status == "failed" || is.null(res$data) || nrow(res$data) == 0) {
        log_warn("Retry failed for game_id={gid}: {res$error %||% 'empty response'}")
        still_failed <- c(still_failed, as.character(gid))
        n_failed     <- n_failed + 1L
      } else {
        write_pbp(res$data, gid, meta_lookup)
        n_success <- n_success + 1L
        log_info("Retry succeeded for game_id={gid}")
      }
    }
    
    failed_ids <- still_failed
  }
  
  log_info("Fetch complete | success={n_success} | failed={n_failed}")
  if (length(failed_ids) > 0) {
    log_warn("Still failed after retry: {paste(failed_ids, collapse=', ')}")
  }
  
  invisible(list(success = n_success, failed = n_failed, failed_ids = failed_ids))
}


# ── Helper: write one game's PBP to parquet ───────────────────────────────────
# Extracted so both the parallel pass and retry pass can call the same logic
write_pbp <- function(pbp_data, gid, meta_lookup) {
  meta <- meta_lookup |> dplyr::filter(game_id == gid)
  pbp  <- pbp_data |>
    dplyr::mutate(
      game_id             = gid,
      game_date           = meta$game_date[[1]],
      home_team_abbrev    = meta$home_abbreviation[[1]],
      away_team_abbrev    = meta$away_abbreviation[[1]],
      home_team_id        = meta$home_id[[1]],
      away_team_id        = meta$away_id[[1]],
      home_team_full_name = meta$home_display_name[[1]],
      away_team_full_name = meta$away_display_name[[1]],
      home_team_logo      = meta$home_logo[[1]],
      away_team_logo      = meta$away_logo[[1]],
      home_team_color     = meta$home_color[[1]],
      away_team_color     = meta$away_color[[1]],
      season_type         = meta$season_type[[1]],
      fetched_at          = Sys.time(),
      scorer = dplyr::if_else(
        grepl("\\bmakes\\b", text, ignore.case = TRUE),
        trimws(sub("\\s+makes\\b.*$", "", text, ignore.case = TRUE, perl = TRUE)),
        NA_character_
      ),
      assister = dplyr::if_else(
        grepl("\\bassists\\)", text, ignore.case = TRUE),
        trimws(gsub(".*\\((.+?)\\s+assists\\).*", "\\1", text,
                    ignore.case = TRUE, perl = TRUE)),
        NA_character_
      ),
      rebounder = dplyr::if_else(
        grepl("\\brebound\\b", text, ignore.case = TRUE),
        trimws(sub("\\s+(offensive|defensive)?\\s*rebound.*$", "", text,
                   ignore.case = TRUE, perl = TRUE)),
        NA_character_
      )
    )
  out_path <- file.path(DATA_DIR, glue::glue("{gid}.parquet"))
  arrow::write_parquet(pbp, out_path)
}

# ── Step 4: Load entire season PBP into one data frame ────────────────────────
load_all_pbp <- function() {
  files <- list.files(DATA_DIR, pattern = "\\.parquet$", full.names = TRUE)
  if (length(files) == 0) stop("No parquet files found in ", DATA_DIR)
  
  log_info("Reading {length(files)} parquet files...")
  arrow::open_dataset(DATA_DIR) |> collect()
}


# ── Main pipeline function ─────────────────────────────────────────────────────
run_pipeline <- function() {
  schedule  <- get_season_game_ids(SEASON)
  to_fetch  <- games_needing_update(schedule)
  fetch_and_store_pbp(to_fetch)
  log_info("=== Pipeline DONE ===")
}


# ── Run ────────────────────────────────────────────────────────────────────────
run_pipeline()




select <- dplyr::select
filter <- dplyr::filter   # caret also conflicts with filter


##Date used for schedule

scheduleDate <- "2025-10-01"

#Season Identifier

current_season <- "2025-26"
last_season <- "2024-25"
season_current <- 2026
season_previous <- 2025

color_set <- viridis::magma(5)

# ── Roster Grab ───────────────────────────────────────────────────────────────

# Replace hoopR::nba_teams() which no longer exists in hoopR 3.0.0
# with hardcoded team IDs — these never change
nba_teams_active <- tibble::tribble(
  ~team_id,      ~team_abbreviation,
  "1610612737",  "ATL",
  "1610612738",  "BOS",
  "1610612739",  "CLE",
  "1610612740",  "NOP",
  "1610612741",  "CHI",
  "1610612742",  "DAL",
  "1610612743",  "DEN",
  "1610612744",  "GSW",
  "1610612745",  "HOU",
  "1610612746",  "LAC",
  "1610612747",  "LAL",
  "1610612748",  "MIA",
  "1610612749",  "MIL",
  "1610612750",  "MIN",
  "1610612751",  "BKN",
  "1610612752",  "NYK",
  "1610612753",  "ORL",
  "1610612754",  "IND",
  "1610612755",  "PHI",
  "1610612756",  "PHX",
  "1610612757",  "POR",
  "1610612758",  "SAC",
  "1610612759",  "SAS",
  "1610612760",  "OKC",
  "1610612761",  "TOR",
  "1610612762",  "UTA",
  "1610612763",  "MEM",
  "1610612764",  "WAS",
  "1610612765",  "DET",
  "1610612766",  "CHA"
)

# nba_commonteamroster() works fine on DigitalOcean — not IP blocked
all_rosters_raw <- purrr::map(nba_teams_active$team_id, function(tid) {
  message("Fetching roster for team_id: ", tid)
  Sys.sleep(1)
  tryCatch({
    result <- hoopR::nba_commonteamroster(team_id = tid)
    result$CommonTeamRoster |>
      dplyr::mutate(team_id = tid) |>
      dplyr::left_join(nba_teams_active, by = "team_id")
  }, error = function(e) {
    message("Failed TEAM_ID=", tid, ": ", conditionMessage(e))
    NULL
  })
}) |>
  dplyr::bind_rows()

library(nbastatR)

Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 2)

gamedata <- nbastatR::game_logs(seasons = season_previous, result_types = "team", season_types = c("Regular Season","Playoffs"))
gamedata_current <- tryCatch({
  nbastatR::game_logs(
    seasons      = season_current,
    result_types = "team",
    season_types = c("Regular Season", "Playoffs")
  )
}, error = function(e) {
  message("Playoffs not available yet — falling back to Regular Season only")
  nbastatR::game_logs(
    seasons      = season_current,
    result_types = "team",
    season_types = c("Regular Season")
  )
})

gamedata <- bind_rows(gamedata,gamedata_current)

game_type_lookup <- gamedata_current |>
  distinct(dateGame, typeSeason) |>
  rename(game_date = dateGame, game_type = typeSeason)

playerdata <- tryCatch({
  nbastatR::game_logs(
    seasons      = season_previous:season_current,
    result_types = "player",
    season_types = c("Regular Season", "Playoffs")
  )
}, error = function(e) {
  message("Playoffs not available yet — falling back to Regular Season only")
  nbastatR::game_logs(
    seasons      = season_previous:season_current,
    result_types = "player",
    season_types = c("Regular Season")
  )
})


gameids<- playerdata %>% dplyr::group_by(idGame) %>% dplyr::summarize(n = n()) %>% pull(idGame)

## PBP

DATA_DIR <- "data/pbp"

# ── Load all PBP (lazy — only reads what you filter) ──────────────────────────
pbp_ds <- arrow::open_dataset(DATA_DIR)   # Arrow Dataset — no RAM spike

play_play <- pbp_ds %>% collect()

play_play <- play_play |>
  left_join(game_type_lookup, by = "game_date") |>
  mutate(
    game_type = case_when(
      game_type == "Regular Season" ~ "Regular Season",
      game_type == "Playoffs"       ~ "Playoffs",
      is.na(game_type)              ~ "Play-In",   # dates not in gamedata = play-in
      TRUE                          ~ game_type
    )
  )


#Schedule

teams <- nbastatR::nba_teams(league = "NBA")

teams <- teams %>% dplyr::filter(idDivision != 0) %>% 
  dplyr::select(cityTeam, slugTeam, idTeam, nameTeam, urlThumbnailTeam) %>% rename(Opponent = cityTeam) %>% 
  dplyr::mutate(urlThumbnailTeam = ifelse(slugTeam == "GSW", "https://cdn.nba.com/logos/nba/1610612744/primary/L/logo.svg",urlThumbnailTeam))


slugteams <- teams %>% dplyr::select(slugTeam)

slugteams_list <- slugteams %>% dplyr::mutate(slugTeam = tolower(slugTeam)) %>% 
  dplyr::mutate(slugTeam = ifelse(slugTeam == "uta","utah",
                           ifelse(slugTeam == "nop","no",slugTeam))) %>% pull(slugTeam)

get_team_schedule <- function(x, seasontype) {
  url <- paste0("https://www.espn.com/nba/team/schedule/_/name/", x, "/seasontype/", seasontype)
  tryCatch({
    h   <- read_html(url)
    tab <- h |> html_nodes("table")
    if (length(tab) == 0) return(NULL)
    tab <- tab[[1]] |> html_table()
    
    ncols <- ncol(tab)
    
    # Regular season has 8 cols, playoffs has 6 cols
    if (ncols == 8) {
      tab <- tab |>
        setNames(c("game_date_raw", "Opponenet", "Time", "TV", "Tickets", "Tickets_dup", "Unused1", "Unused2"))
    } else if (ncols == 6) {
      tab <- tab |>
        setNames(c("game_date_raw", "Opponenet", "Time", "TV", "Tickets", "Tickets_dup"))
    } else {
      message("Unexpected column count (", ncols, ") for ", x, " — skipping")
      return(NULL)
    }
    
    # Remove header rows — any row where game_date_raw is "DATE" or a round name
    tab <- tab |>
      filter(
        !game_date_raw %in% c("DATE", ""),
        !str_detect(game_date_raw, "Round|Conference|Finals|Semifinal"),
        !is.na(game_date_raw)
      )
    
    if (nrow(tab) == 0) return(NULL)
    
    tab <- tab |>
      mutate(
        # Playoffs uses "vs" with no space, regular season uses "vs " with space
        # Normalize both to detect home/away
        location      = ifelse(str_detect(Opponenet, "^@"), "Away", "Home"),
        Opponent      = str_remove(Opponenet, "^vs|^@") |> str_trim(),
        game_date_raw = str_extract(game_date_raw, "\\b[^,]+$") |> str_trim()
      )
    
    tab <- tab |>
      left_join(teams, by = "Opponent") |>
      mutate(
        Team        = toupper(x),
        Team        = ifelse(Team == "UTAH", "UTA", ifelse(Team == "NO", "NOP", Team)),
        game_number = 1:n()
      )
    
    tab
    
  }, error = function(e) {
    message("Failed schedule for ", x, ": ", conditionMessage(e))
    NULL
  })
}

process_schedule <- function(schedule_raw) {
  bind_rows(schedule_raw) |>
    filter(!is.na(game_date_raw)) |>
    mutate(
      game_date = ifelse(
        substr(game_date_raw, 1, 3) %in% c("Oct", "Nov", "Dec"),
        paste(game_date_raw, season_previous),
        paste(game_date_raw, season_current)
      ),
      game_date = as.Date(game_date, "%b%d%Y")
    ) |>
    select(game_date, location, Opponent, slugTeam, idTeam, nameTeam,
           urlThumbnailTeam, Team, game_number, TV, Time)
}

# Regular season
schedule_raw <- lapply(slugteams_list, get_team_schedule, seasontype = 2)
schedule     <- process_schedule(schedule_raw)

# Check for upcoming regular season games
upcoming_regular <- schedule |>
  filter(
    !is.na(game_date),
    game_date >= Sys.Date(),
    !str_detect(Time, "^[WL] "),
    !str_detect(Time, "Postponed"),
    !str_detect(Time, "-"),
    !str_detect(TV,   "Postponed"),
    !str_detect(TV,   "-")
  )

regular_season_active <- nrow(upcoming_regular) > 0
message("Regular season active: ", regular_season_active)

# Switch to playoffs if regular season is done
if (!regular_season_active) {
  message("Regular season complete — switching to playoff schedule...")
  schedule_raw <- lapply(slugteams_list, get_team_schedule, seasontype = 3)
  schedule     <- process_schedule(schedule_raw)
  message("Playoff schedule loaded — ", nrow(schedule), " rows")
} else {
  message("Regular season schedule loaded — ", nrow(upcoming_regular), " upcoming games")
}

# Find these lines and update Date -> game_date
next_team_batch_date <- schedule |>
  arrange(game_date) |>
  filter(
    !str_detect(Time, "Postponed"),
    !str_detect(Time, "-"),
    !str_detect(TV, "-"),
    !str_detect(TV, "Postponed"),
    !is.na(game_date)
  ) |>
  head(1) |>
  pull(game_date)

next_game_date_teams <- schedule |>
  filter(game_date == next_team_batch_date) |>
  pull(slugTeam)

next_game_date_teams_full <- schedule |>
  filter(game_date == next_team_batch_date) |>
  pull(nameTeam)

matchup <- schedule |>
  filter(game_date == next_team_batch_date) |>
  mutate(matchup = ifelse(location == "Away", paste("vs.", Team), paste("@", Team)))

next_team_batch <- all_rosters_raw %>% filter(team_abbreviation %in% next_game_date_teams) %>% select(PLAYER,team_abbreviation)

team_opponent_map <- matchup |>
  dplyr::select(Team, slugTeam, nameTeam) |>
  dplyr::distinct() |>
  # Team = player's team, slugTeam = opponent abbrev, nameTeam = opponent full name
  dplyr::rename(
    player_team     = Team,
    opponent_abbrev = slugTeam,
    opponent_name   = nameTeam
  )

# Join roster to get opponent for each player
player_opponent_lookup <- all_rosters_raw |>
  dplyr::filter(team_abbreviation %in% next_game_date_teams) |>
  dplyr::inner_join(
    team_opponent_map,
    by = c("team_abbreviation" = "player_team")
  ) |>
  dplyr::rename(player = PLAYER) |>
  dplyr::mutate(
    type_label = paste0("vs ", opponent_abbrev, " (Reg Season)")
  )

# Also get opponent full name for the Type label
opponent_name_lookup <- teams |>
  dplyr::select(slugTeam, nameTeam) |>
  dplyr::rename(
    opponent_abbrev = slugTeam,
    opponent_name   = nameTeam
  )

player_opponent_lookup <- player_opponent_lookup |>
  left_join(opponent_name_lookup, by = "opponent_abbrev") |>
  mutate(
    # Short label e.g. "vs OKC (Reg Season)"
    type_label = paste0("vs ", opponent_abbrev, " (Reg Season)")
  )

cat("Player opponent lookup:\n")
print(player_opponent_lookup)


stat_names <- c("fg3a","ftm","pts_reb_ast","fgm","pts_reb","ast_reb","pts_ast","stl_blk","fg3m","stl","blk","tov","pts","ast","treb")

hit_rate_seq <- seq(0.5, 60.5, 1)

# ── Pre-compute the enriched player data ONCE ─────────────────────────
# Build all stat combos and filter to the relevant players here on the main
# process. Workers receive a clean slice — no redundant joins inside workers.

df_enriched <- playerdata %>% filter(typeSeason == "Regular Season", 
                                     slugSeason == current_season) %>% 
  mutate(pts_reb_ast = pts+treb+ast, pts_reb = pts+treb,ast_reb = ast+treb, pts_ast = pts+ast,stl_blk = stl+blk)

df_enriched_home <- df_enriched %>% filter(locationGame == "H")
df_enriched_away <- df_enriched %>% filter(locationGame == "A")

# ── Enriched datasets — Playoffs ──────────────────────────────────────────────
df_enriched_playoffs <- playerdata %>%
  filter(typeSeason == "Playoffs", slugSeason == current_season) %>%
  mutate(
    pts_reb_ast = pts + treb + ast,
    pts_reb     = pts + treb,
    ast_reb     = ast + treb,
    pts_ast     = pts + ast,
    stl_blk     = stl + blk
  )

df_enriched_playoffs_home <- df_enriched_playoffs %>% filter(locationGame == "H")
df_enriched_playoffs_away <- df_enriched_playoffs %>% filter(locationGame == "A")

df_enriched_all <- playerdata %>%
  filter(slugSeason == current_season) %>%
  mutate(
    pts_reb_ast = pts + treb + ast,
    pts_reb     = pts + treb,
    ast_reb     = ast + treb,
    pts_ast     = pts + ast,
    stl_blk     = stl + blk
  )



# ── Shared worker logic ────────────────────────────────────────────────────────
# Single internal function — all worker functions call this
.compute_hit_rates <- function(player_id, pbp_data) {
  library(dplyr)
  
  slug_team <- all_rosters_raw %>%
    filter(PLAYER == player_id) %>%
    rename(slugTeam = team_abbreviation) %>%
    pull(slugTeam) %>%
    head(1)
  
  opp_info <- player_opponent_lookup %>%
    filter(player == player_id) %>%
    head(1)
  
  if (nrow(opp_info) == 0) return(NULL)
  
  opp_abbrev  <- opp_info$opponent_abbrev
  type_label  <- opp_info$type_label
  
  # Filter playerdata for regular season games against this opponent
  player_df <- df_enriched |>
    filter(
      namePlayer == player_id,
      slugOpponent == opp_abbrev
    )
  
  name_val <- player_id
  
  # If no games against this opponent return 0s
  if (nrow(player_df) == 0) {
    return(
      bind_rows(lapply(stat_names, function(stat) {
        bind_rows(lapply(hit_rate_seq, function(line) {
          tibble(
            namePlayer = name_val,
            idPlayer   = player_id,
            slugTeam   = slug_team,
            metric     = stat,
            OU         = line,
            hit_rate   = 0,
            n_games    = 0L
          )
        }))
      }))
    )
  }
  
  player_df <- pbp_data %>% filter(namePlayer == player_id)
  
  if (nrow(player_df) == 0) return(NULL)
  
  name_val <- player_df$namePlayer[[1]]
  
  results <- lapply(stat_names, function(stat) {
    amounts <- player_df %>% pull(.data[[stat]])
    if (length(amounts) == 0 || all(is.na(amounts))) return(NULL)
    
    bind_rows(lapply(hit_rate_seq, function(line) {
      tibble(
        namePlayer = name_val,
        idPlayer   = player_id,
        slugTeam   = slug_team,
        metric     = stat,
        OU         = line,
        hit_rate   = mean(amounts > line, na.rm = TRUE),
        n_games    = nrow(player_df)
      )
    }))
  })
  
  bind_rows(results)
}

# ── Named worker functions — one per split ────────────────────────────────────
compute_player_hit_rates              <- function(p) .compute_hit_rates(p, df_enriched)
compute_player_hit_rates_home         <- function(p) .compute_hit_rates(p, df_enriched_home)
compute_player_hit_rates_away         <- function(p) .compute_hit_rates(p, df_enriched_away)
compute_player_hit_rates_playoffs     <- function(p) .compute_hit_rates(p, df_enriched_playoffs)
compute_player_hit_rates_playoff_home <- function(p) .compute_hit_rates(p, df_enriched_playoffs_home)
compute_player_hit_rates_playoff_away <- function(p) .compute_hit_rates(p, df_enriched_playoffs_away)
compute_player_hit_rates_five         <- function(p) {
  .compute_hit_rates(
    p,
    df_enriched_all %>%
      filter(namePlayer == p) %>%
      arrange(desc(dateGame)) %>%
      head(5)
  )
}
compute_player_hit_rates_ten          <- function(p) {
  .compute_hit_rates(
    p,
    df_enriched_all %>%
      filter(namePlayer == p) %>%
      arrange(desc(dateGame)) %>%
      head(10)
  )
}

compute_player_hit_rates_opponent <- function(player_id) {
  library(dplyr)
  
  slug_team <- all_rosters_raw %>%
    filter(PLAYER == player_id) %>%
    rename(slugTeam = team_abbreviation) %>%
    pull(slugTeam) %>%
    head(1)
  
  # Get this player's opponent info
  opp_info <- player_opponent_lookup %>%
    filter(player == player_id) %>%
    head(1)
  
  if (nrow(opp_info) == 0) return(NULL)
  
  opp_abbrev <- opp_info$opponent_abbrev
  type_label <- opp_info$type_label
  name_val   <- player_id
  
  # Filter regular season games against this opponent
  player_df <- df_enriched |>
    filter(
      namePlayer   == player_id,
      slugOpponent == opp_abbrev
    )
  
  # If no games against this opponent return 0s
  if (nrow(player_df) == 0) {
    return(
      bind_rows(lapply(stat_names, function(stat) {
        bind_rows(lapply(hit_rate_seq, function(line) {
          tibble(
            namePlayer = name_val,
            idPlayer   = player_id,
            slugTeam   = slug_team,
            metric     = stat,
            OU         = line,
            hit_rate   = 0,
            Type       = type_label
          )
        }))
      }))
    )
  }
  
  results <- lapply(stat_names, function(stat) {
    amounts <- player_df |> pull(.data[[stat]])
    if (length(amounts) == 0 || all(is.na(amounts))) return(NULL)
    
    bind_rows(lapply(hit_rate_seq, function(line) {
      tibble(
        namePlayer = name_val,
        idPlayer   = player_id,
        slugTeam   = slug_team,
        metric     = stat,
        OU         = line,
        hit_rate   = mean(amounts > line, na.rm = TRUE),
        Type       = type_label
      )
    }))
  })
  
  bind_rows(results)
}

# ── Spin up cluster ────────────────────────────────────────────────────────────
n_cores <- max(1L, parallelly::availableCores() - 2L)
message("Launching cluster with ", n_cores, " workers...")

cl <- makeCluster(n_cores, type = "PSOCK")
on.exit(stopCluster(cl), add = TRUE)

# Export all datasets and shared objects once
clusterExport(cl, varlist = c(
  "all_rosters_raw", "stat_names", "hit_rate_seq",
  "df_enriched", "df_enriched_home", "df_enriched_away",
  "df_enriched_playoffs", "df_enriched_playoffs_home", "df_enriched_playoffs_away",
  "df_enriched_all", ".compute_hit_rates", "player_opponent_lookup"
))



# ── Run all splits ─────────────────────────────────────────────────────────────
run_split <- function(fn_name, label) {
  message("Computing hit rates — ", label, "...")
  parLapply(cl, next_team_batch$PLAYER, get(fn_name))
}

results_raw              <- run_split("compute_player_hit_rates",              "Regular Season")
results_raw_home         <- run_split("compute_player_hit_rates_home",         "Regular Season Home")
results_raw_away         <- run_split("compute_player_hit_rates_away",         "Regular Season Away")
results_raw_playoffs     <- run_split("compute_player_hit_rates_playoffs",     "Playoffs")
results_raw_playoff_home <- run_split("compute_player_hit_rates_playoff_home", "Playoff Home")
results_raw_playoff_away <- run_split("compute_player_hit_rates_playoff_away", "Playoff Away")
results_raw_five         <- run_split("compute_player_hit_rates_five",         "Last 5")
results_raw_ten          <- run_split("compute_player_hit_rates_ten",          "Last 10")
results_raw_opponent     <- run_split("compute_player_hit_rates_opponent", "vs Opponent")


# ── Collect ────────────────────────────────────────────────────────────────────
results              <- bind_rows(results_raw)
results_home         <- bind_rows(results_raw_home)
results_away         <- bind_rows(results_raw_away)
results_playoffs     <- bind_rows(results_raw_playoffs)
results_playoff_home <- bind_rows(results_raw_playoff_home)
results_playoff_away <- bind_rows(results_raw_playoff_away)
results_five         <- bind_rows(results_raw_five)
results_ten          <- bind_rows(results_raw_ten)
results_opponent     <- bind_rows(results_raw_opponent)

message("Done. Rows: ", nrow(results), " | Players: ", n_distinct(results$idPlayer))

# ── Pivot all eight splits ─────────────────────────────────────────────────────
pivot_and_tag <- function(results_df, type_label) {
  # Guard — if no data return empty named list
  if (nrow(results_df) == 0 || !"metric" %in% names(results_df)) {
    message("No data for split: ", type_label, " — skipping")
    return(setNames(
      lapply(stat_names, function(s) tibble()),
      stat_names
    ))
  }
  
  results_df |>
    split(~metric) |>
    imap(function(df, metric_name) {
      df |>
        pivot_wider(names_from = OU, values_from = hit_rate) |>
        unnest(cols = everything()) |>
        mutate(Type = type_label)
    })
}

pivoted <- list(
  pivot_and_tag(results,              "Regular Season"),
  pivot_and_tag(results_home,         "Home Games"),
  pivot_and_tag(results_away,         "Away Games"),
  pivot_and_tag(results_playoffs,     "Playoffs"),
  pivot_and_tag(results_playoff_home, "Playoff Home Games"),
  pivot_and_tag(results_playoff_away, "Playoff Away Games"),
  pivot_and_tag(results_five,         "Last 5"),
  pivot_and_tag(results_ten,          "Last 10"),
  pivot_and_tag(results_opponent,     "vs Opponent")
)

# ── Combine by metric ──────────────────────────────────────────────────────────
# transpose() flips list-of-lists from [split][metric] to [metric][split],
# then map() bind_rows() across the five splits for each metric.
results_combined <- pivoted |>
  purrr::transpose() |>
  purrr::map(function(split_list) {
    # Filter out empty tibbles before binding
    non_empty <- Filter(function(df) nrow(df) > 0, split_list)
    if (length(non_empty) == 0) return(tibble())
    bind_rows(non_empty)
  })


# Create standalone objects
iwalk(results_combined, function(df, metric_name) {
  assign(paste0("results_combined_", metric_name), df, envir = .GlobalEnv)
})

# ── First Quarter Data ───────────────────────────────

# ── Helper: strip accents and normalize for joining ───────────────────────────
normalize_name <- function(x) {
  x |>
    stringi::stri_trans_general("Latin-ASCII") |>  # Dončić → Doncic
    stringr::str_squish() |>                        # remove extra whitespace
    stringr::str_to_lower()                         # case insensitive
}

# ── Add normalized join key to both tables ────────────────────────────────────
playerdata <- playerdata |>
  mutate(name_key = normalize_name(namePlayer))

play_play <- play_play |>
  mutate(scorer_key = normalize_name(scorer), assister_key = normalize_name(assister), 
         rebounder_key= normalize_name(rebounder))

next_game_players <- all_rosters_raw |>
  filter(team_abbreviation %in% next_game_date_teams) |>
  dplyr::select(PLAYER, team_abbreviation) |>
  dplyr::rename(
    scorer            = PLAYER,
    scorer_team_abbrev = team_abbreviation
  ) |>
  mutate(scorer_key = normalize_name(scorer))


## First Quarter Pts

# ── Helper: build q1 scoring raw for a given game type ────────────────────────
build_q1_scoring_raw <- function(game_type_filter) {
  play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number == 1,
      scoring_play  == TRUE,
      !is.na(scorer)
    ) %>%
    inner_join(
      next_game_players,
      by = c("scorer_key" = "scorer_key")
    ) %>%
    group_by(scorer.x, game_date) %>%
    summarize(
      pts                 = sum(score_value),
      home_team_full_name = first(home_team_full_name),
      away_team_full_name = first(away_team_full_name),
      team_abbreviation   = first(scorer_team_abbrev),
      .groups             = "drop"
    ) %>%
    left_join(
      teams |> dplyr::select(slugTeam, nameTeam),
      by = c("team_abbreviation" = "slugTeam")
    ) |>
    mutate(
      game_location = case_when(
        nameTeam == home_team_full_name ~ "H",
        nameTeam == away_team_full_name ~ "A",
        TRUE                            ~ NA_character_
      )
    ) %>%
    rename(scorer = scorer.x)
}



# ── Build raw datasets ─────────────────────────────────────────────────────────
q1_scoring_raw          <- build_q1_scoring_raw("Regular Season")
q1_scoring_raw_playoffs <- build_q1_scoring_raw("Playoffs")

q1_scoring_raw_home          <- q1_scoring_raw          %>% filter(game_location == "H")
q1_scoring_raw_away          <- q1_scoring_raw          %>% filter(game_location == "A")
q1_scoring_raw_playoff_home  <- q1_scoring_raw_playoffs %>% filter(game_location == "H")
q1_scoring_raw_playoff_away  <- q1_scoring_raw_playoffs %>% filter(game_location == "A")

# Last 5/10 pull from ALL game types combined
q1_scoring_raw_all <- bind_rows(q1_scoring_raw, q1_scoring_raw_playoffs)

q1_pts_games <- q1_scoring_raw |>
  dplyr::count(scorer, name = "n_games") |>
  dplyr::rename(Player = scorer)

# ── Helper: compute hit rates from a scoring raw dataset ──────────────────────
compute_q1_hit_rates <- function(scoring_raw) {
  if (nrow(scoring_raw) == 0) return(tibble())
  scoring_raw |>
    select(scorer, game_date, pts) |>
    cross_join(tibble(OU = hit_rate_seq)) |>
    group_by(scorer, OU) |>
    summarise(
      hit_rate = mean(pts > OU, na.rm = TRUE),
      .groups  = "drop"
    )
}

compute_q1_hit_rates_last_n <- function(scoring_raw, n) {
  if (nrow(scoring_raw) == 0) return(tibble())
  scoring_raw |>
    group_by(scorer, game_date) |>
    summarize(pts = sum(pts), .groups = "drop") |>
    select(scorer, game_date, pts) |>
    group_by(scorer) |>
    arrange(desc(game_date)) |>
    slice_head(n = n) |>
    ungroup() |>
    cross_join(tibble(OU = hit_rate_seq)) |>
    group_by(scorer, OU) |>
    summarise(
      hit_rate = mean(pts > OU, na.rm = TRUE),
      .groups  = "drop"
    )
}

# ── Compute all 8 splits ───────────────────────────────────────────────────────
q1_hit_rates              <- compute_q1_hit_rates(q1_scoring_raw)
q1_hit_rates_home         <- compute_q1_hit_rates(q1_scoring_raw_home)
q1_hit_rates_away         <- compute_q1_hit_rates(q1_scoring_raw_away)
q1_hit_rates_playoffs     <- compute_q1_hit_rates(q1_scoring_raw_playoffs)
q1_hit_rates_playoff_home <- compute_q1_hit_rates(q1_scoring_raw_playoff_home)
q1_hit_rates_playoff_away <- compute_q1_hit_rates(q1_scoring_raw_playoff_away)
q1_hit_rates_five         <- compute_q1_hit_rates_last_n(q1_scoring_raw_all, 5)
q1_hit_rates_ten          <- compute_q1_hit_rates_last_n(q1_scoring_raw_all, 10)

# ── Helper: pivot and tag with guard for empty data ───────────────────────────
pivot_q1 <- function(df, type_label, metric_label) {
  if (nrow(df) == 0) {
    message("No data for Q1 split: ", type_label, " — skipping")
    return(tibble())
  }
  df |>
    pivot_wider(names_from = OU, values_from = hit_rate) |>
    mutate(metric = metric_label, Type = type_label)
}

# ── Pivot all 8 splits ─────────────────────────────────────────────────────────
q1_hit_rates_pivoted              <- pivot_q1(q1_hit_rates,              "Regular Season",      "q1_pts")
q1_hit_rates_pivoted_home         <- pivot_q1(q1_hit_rates_home,         "Home Games",          "q1_pts")
q1_hit_rates_pivoted_away         <- pivot_q1(q1_hit_rates_away,         "Away Games",          "q1_pts")
q1_hit_rates_pivoted_playoffs     <- pivot_q1(q1_hit_rates_playoffs,     "Playoffs",            "q1_pts")
q1_hit_rates_pivoted_playoff_home <- pivot_q1(q1_hit_rates_playoff_home, "Playoff Home Games",  "q1_pts")
q1_hit_rates_pivoted_playoff_away <- pivot_q1(q1_hit_rates_playoff_away, "Playoff Away Games",  "q1_pts")
q1_hit_rates_pivoted_five         <- pivot_q1(q1_hit_rates_five,         "Last 5",              "q1_pts")
q1_hit_rates_pivoted_ten          <- pivot_q1(q1_hit_rates_ten,          "Last 10",             "q1_pts")

# ── Combine — filter out empty tibbles before binding ─────────────────────────
q1_hit_rates_pivoted_combined <- list(
  q1_hit_rates_pivoted,
  q1_hit_rates_pivoted_home,
  q1_hit_rates_pivoted_away,
  q1_hit_rates_pivoted_playoffs,
  q1_hit_rates_pivoted_playoff_home,
  q1_hit_rates_pivoted_playoff_away,
  q1_hit_rates_pivoted_five,
  q1_hit_rates_pivoted_ten
) |>
  Filter(function(df) nrow(df) > 0, x = _) |>
  bind_rows()




## First Quarter Assists

next_game_players_ast <- all_rosters_raw |>
  filter(team_abbreviation %in% next_game_date_teams) |>
  mutate(assister_key = normalize_name(PLAYER)) |>
  dplyr::select(assister_key, team_abbreviation)



# ── Helper: build q1 assists raw for a given game type ────────────────────────
build_q1_assists_raw <- function(game_type_filter) {
  play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number == 1,
      scoring_play  == TRUE,
      !is.na(assister)
    ) %>%
    inner_join(
      next_game_players_ast,
      by = c("assister_key" = "assister_key")
    ) %>%
    group_by(assister, game_date) %>%
    summarize(
      ast                 = n(),
      home_team_full_name = first(home_team_full_name),
      away_team_full_name = first(away_team_full_name),
      team_abbreviation   = first(team_abbreviation),
      .groups             = "drop"
    ) %>%
    left_join(
      teams |> dplyr::select(slugTeam, nameTeam),
      by = c("team_abbreviation" = "slugTeam")
    ) |>
    mutate(
      game_location = case_when(
        nameTeam == home_team_full_name ~ "H",
        nameTeam == away_team_full_name ~ "A",
        TRUE                            ~ NA_character_
      )
    )
}

# ── Build raw datasets ─────────────────────────────────────────────────────────
q1_assists_raw          <- build_q1_assists_raw("Regular Season")
q1_assists_raw_playoffs <- build_q1_assists_raw("Playoffs")

q1_assists_raw_home          <- q1_assists_raw          %>% filter(game_location == "H")
q1_assists_raw_away          <- q1_assists_raw          %>% filter(game_location == "A")
q1_assists_raw_playoff_home  <- q1_assists_raw_playoffs %>% filter(game_location == "H")
q1_assists_raw_playoff_away  <- q1_assists_raw_playoffs %>% filter(game_location == "A")

# Last 5/10 pull from ALL game types combined
q1_assists_raw_all <- bind_rows(q1_assists_raw, q1_assists_raw_playoffs)

q1_ast_games <- q1_assists_raw |>
  dplyr::count(assister, name = "n_games") |>
  dplyr::rename(Player = assister)

# ── Helper: compute hit rates ─────────────────────────────────────────────────
compute_q1_ast_hit_rates <- function(assists_raw) {
  if (nrow(assists_raw) == 0) return(tibble())
  assists_raw |>
    select(assister, game_date, ast) |>
    cross_join(tibble(OU = hit_rate_seq)) |>
    group_by(assister, OU) |>
    summarise(
      hit_rate = mean(ast > OU, na.rm = TRUE),
      .groups  = "drop"
    )
}

compute_q1_ast_hit_rates_last_n <- function(assists_raw, n) {
  if (nrow(assists_raw) == 0) return(tibble())
  assists_raw |>
    filter(!str_detect(assister, "team")) |>
    group_by(assister, game_date) |>
    summarize(ast = sum(ast), .groups = "drop") |>
    select(assister, game_date, ast) |>
    group_by(assister) |>
    arrange(desc(game_date)) |>
    slice_head(n = n) |>
    ungroup() |>
    cross_join(tibble(OU = hit_rate_seq)) |>
    group_by(assister, OU) |>
    summarise(
      hit_rate = mean(ast > OU, na.rm = TRUE),
      .groups  = "drop"
    )
}

# ── Compute all 8 splits ───────────────────────────────────────────────────────
q1_hit_rates_ast              <- compute_q1_ast_hit_rates(q1_assists_raw)
q1_hit_rates_ast_home         <- compute_q1_ast_hit_rates(q1_assists_raw_home)
q1_hit_rates_ast_away         <- compute_q1_ast_hit_rates(q1_assists_raw_away)
q1_hit_rates_ast_playoffs     <- compute_q1_ast_hit_rates(q1_assists_raw_playoffs)
q1_hit_rates_ast_playoff_home <- compute_q1_ast_hit_rates(q1_assists_raw_playoff_home)
q1_hit_rates_ast_playoff_away <- compute_q1_ast_hit_rates(q1_assists_raw_playoff_away)
q1_hit_rates_ast_five         <- compute_q1_ast_hit_rates_last_n(q1_assists_raw_all, 5)
q1_hit_rates_ast_ten          <- compute_q1_ast_hit_rates_last_n(q1_assists_raw_all, 10)

# ── Helper: pivot and tag with guard for empty data ───────────────────────────
pivot_q1_ast <- function(df, type_label) {
  if (nrow(df) == 0) {
    message("No data for Q1 assists split: ", type_label, " — skipping")
    return(tibble())
  }
  df |>
    pivot_wider(names_from = OU, values_from = hit_rate) |>
    mutate(metric = "q1_ast", Type = type_label)
}

# ── Pivot all 8 splits ─────────────────────────────────────────────────────────
q1_hit_rates_ast_pivoted              <- pivot_q1_ast(q1_hit_rates_ast,              "Regular Season")
q1_hit_rates_ast_pivoted_home         <- pivot_q1_ast(q1_hit_rates_ast_home,         "Home Games")
q1_hit_rates_ast_pivoted_away         <- pivot_q1_ast(q1_hit_rates_ast_away,         "Away Games")
q1_hit_rates_ast_pivoted_playoffs     <- pivot_q1_ast(q1_hit_rates_ast_playoffs,     "Playoffs")
q1_hit_rates_ast_pivoted_playoff_home <- pivot_q1_ast(q1_hit_rates_ast_playoff_home, "Playoff Home Games")
q1_hit_rates_ast_pivoted_playoff_away <- pivot_q1_ast(q1_hit_rates_ast_playoff_away, "Playoff Away Games")
q1_hit_rates_ast_pivoted_five         <- pivot_q1_ast(q1_hit_rates_ast_five,         "Last 5")
q1_hit_rates_ast_pivoted_ten          <- pivot_q1_ast(q1_hit_rates_ast_ten,          "Last 10")

# ── Combine ────────────────────────────────────────────────────────────────────
q1_hit_rates_ast_pivoted_combined <- list(
  q1_hit_rates_ast_pivoted,
  q1_hit_rates_ast_pivoted_home,
  q1_hit_rates_ast_pivoted_away,
  q1_hit_rates_ast_pivoted_playoffs,
  q1_hit_rates_ast_pivoted_playoff_home,
  q1_hit_rates_ast_pivoted_playoff_away,
  q1_hit_rates_ast_pivoted_five,
  q1_hit_rates_ast_pivoted_ten
) |>
  Filter(function(df) nrow(df) > 0, x = _) |>
  bind_rows()



## First Quarter Rebounds

next_game_players_reb <- all_rosters_raw |>
  filter(team_abbreviation %in% next_game_date_teams) |>
  mutate(rebounder_key = normalize_name(PLAYER)) |>
  dplyr::select(rebounder_key, team_abbreviation)

# ── Helper: build q1 rebounds raw for a given game type ───────────────────────
build_q1_rebounds_raw <- function(game_type_filter) {
  play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number == 1,
      !is.na(rebounder),
      !str_detect(rebounder, "team")
    ) %>%
    inner_join(
      next_game_players_reb,
      by = c("rebounder_key" = "rebounder_key")
    ) %>%
    group_by(rebounder, game_date) %>%
    summarize(
      treb                = n(),
      home_team_full_name = first(home_team_full_name),
      away_team_full_name = first(away_team_full_name),
      team_abbreviation   = first(team_abbreviation),
      .groups             = "drop"
    ) %>%
    left_join(
      teams |> dplyr::select(slugTeam, nameTeam),
      by = c("team_abbreviation" = "slugTeam")
    ) |>
    mutate(
      game_location = case_when(
        nameTeam == home_team_full_name ~ "H",
        nameTeam == away_team_full_name ~ "A",
        TRUE                            ~ NA_character_
      )
    )
}

# ── Build raw datasets ─────────────────────────────────────────────────────────
q1_rebounds_raw          <- build_q1_rebounds_raw("Regular Season")
q1_rebounds_raw_playoffs <- build_q1_rebounds_raw("Playoffs")

q1_rebounds_raw_home         <- q1_rebounds_raw          %>% filter(game_location == "H")
q1_rebounds_raw_away         <- q1_rebounds_raw          %>% filter(game_location == "A")
q1_rebounds_raw_playoff_home <- q1_rebounds_raw_playoffs %>% filter(game_location == "H")
q1_rebounds_raw_playoff_away <- q1_rebounds_raw_playoffs %>% filter(game_location == "A")

# Last 5/10 pull from ALL game types combined
q1_rebounds_raw_all <- bind_rows(q1_rebounds_raw, q1_rebounds_raw_playoffs)

q1_reb_games <- q1_rebounds_raw |>
  dplyr::count(rebounder, name = "n_games") |>
  dplyr::rename(Player = rebounder)

# ── Helper: compute hit rates ─────────────────────────────────────────────────
compute_q1_reb_hit_rates <- function(rebounds_raw) {
  if (nrow(rebounds_raw) == 0) return(tibble())
  rebounds_raw |>
    select(rebounder, game_date, treb) |>
    cross_join(tibble(OU = hit_rate_seq)) |>
    group_by(rebounder, OU) |>
    summarise(
      hit_rate = mean(treb > OU, na.rm = TRUE),
      .groups  = "drop"
    )
}

compute_q1_reb_hit_rates_last_n <- function(rebounds_raw, n) {
  if (nrow(rebounds_raw) == 0) return(tibble())
  rebounds_raw |>
    filter(!str_detect(rebounder, "team")) |>
    group_by(rebounder, game_date) |>
    summarize(treb = sum(treb), .groups = "drop") |>
    select(rebounder, game_date, treb) |>
    group_by(rebounder) |>
    arrange(desc(game_date)) |>
    slice_head(n = n) |>
    ungroup() |>
    cross_join(tibble(OU = hit_rate_seq)) |>
    group_by(rebounder, OU) |>
    summarise(
      hit_rate = mean(treb > OU, na.rm = TRUE),
      .groups  = "drop"
    )
}

# ── Compute all 8 splits ───────────────────────────────────────────────────────
q1_hit_rates_reb              <- compute_q1_reb_hit_rates(q1_rebounds_raw)
q1_hit_rates_reb_home         <- compute_q1_reb_hit_rates(q1_rebounds_raw_home)
q1_hit_rates_reb_away         <- compute_q1_reb_hit_rates(q1_rebounds_raw_away)
q1_hit_rates_reb_playoffs     <- compute_q1_reb_hit_rates(q1_rebounds_raw_playoffs)
q1_hit_rates_reb_playoff_home <- compute_q1_reb_hit_rates(q1_rebounds_raw_playoff_home)
q1_hit_rates_reb_playoff_away <- compute_q1_reb_hit_rates(q1_rebounds_raw_playoff_away)
q1_hit_rates_reb_five         <- compute_q1_reb_hit_rates_last_n(q1_rebounds_raw_all, 5)
q1_hit_rates_reb_ten          <- compute_q1_reb_hit_rates_last_n(q1_rebounds_raw_all, 10)

# ── Helper: pivot and tag with guard for empty data ───────────────────────────
pivot_q1_reb <- function(df, type_label) {
  if (nrow(df) == 0) {
    message("No data for Q1 rebounds split: ", type_label, " — skipping")
    return(tibble())
  }
  df |>
    pivot_wider(names_from = OU, values_from = hit_rate) |>
    mutate(metric = "q1_reb", Type = type_label)
}

# ── Pivot all 8 splits ─────────────────────────────────────────────────────────
q1_hit_rates_reb_pivoted              <- pivot_q1_reb(q1_hit_rates_reb,              "Regular Season")
q1_hit_rates_reb_pivoted_home         <- pivot_q1_reb(q1_hit_rates_reb_home,         "Home Games")
q1_hit_rates_reb_pivoted_away         <- pivot_q1_reb(q1_hit_rates_reb_away,         "Away Games")
q1_hit_rates_reb_pivoted_playoffs     <- pivot_q1_reb(q1_hit_rates_reb_playoffs,     "Playoffs")
q1_hit_rates_reb_pivoted_playoff_home <- pivot_q1_reb(q1_hit_rates_reb_playoff_home, "Playoff Home Games")
q1_hit_rates_reb_pivoted_playoff_away <- pivot_q1_reb(q1_hit_rates_reb_playoff_away, "Playoff Away Games")
q1_hit_rates_reb_pivoted_five         <- pivot_q1_reb(q1_hit_rates_reb_five,         "Last 5")
q1_hit_rates_reb_pivoted_ten          <- pivot_q1_reb(q1_hit_rates_reb_ten,          "Last 10")

# ── Combine ────────────────────────────────────────────────────────────────────
q1_hit_rates_reb_pivoted_combined <- list(
  q1_hit_rates_reb_pivoted,
  q1_hit_rates_reb_pivoted_home,
  q1_hit_rates_reb_pivoted_away,
  q1_hit_rates_reb_pivoted_playoffs,
  q1_hit_rates_reb_pivoted_playoff_home,
  q1_hit_rates_reb_pivoted_playoff_away,
  q1_hit_rates_reb_pivoted_five,
  q1_hit_rates_reb_pivoted_ten
) |>
  Filter(function(df) nrow(df) > 0, x = _) |>
  bind_rows()


   # updated to include n_games column
# ── First Quarter Data Joined with Whole Game Data ────────────


# ── Join on normalized key, keep original names from both sides ───────────────

# ══════════════════════════════════════════════════════════════════════════════
# Points
# ══════════════════════════════════════════════════════════════════════════════
build_joined_pts <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      game_type     != "Play-In",
      period_number == 1,
      !is.na(scorer)
    ) %>%
    # Filter to next game players only via normalized key join
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    )
  
  player_filtered <- playerdata %>%
    filter(
      typeSeason == game_type_filter,
      yearSeason == season_current
    ) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(pts = sum(pts), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(scorer, scorer_key, game_date) %>%
    summarize(fqpts = sum(score_value), .groups = "drop") %>%
    left_join(
      player_filtered,
      by = c("scorer_key" = "name_key", "game_date" = "dateGame")
    )
  
  if (!is.null(location_filter)) {
    joined <- joined %>% filter(locationGame == location_filter)
  }
  
  joined
}

# ── Helper: build joined_pts for last N games (all game types combined) ────────
build_joined_pts_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(period_number == 1, !is.na(scorer),
           game_type     != "Play-In") %>%
    # Filter to next game players only via normalized key join
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    ) %>%
    group_by(scorer, scorer_key, game_date) %>%
    summarize(fqpts = sum(score_value), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(pts = sum(pts), .groups = "drop")
  
  pbp_all %>%
    left_join(
      player_all,
      by = c("scorer_key" = "name_key", "game_date" = "dateGame")
    ) %>%
    group_by(scorer) %>%
    arrange(desc(game_date)) %>%
    slice_head(n = n) %>%
    ungroup()
}

# ── Helper: compute pts_conditional from a joined dataset ─────────────────────
compute_pts_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$pts))) {
    message("No data for pts conditional split: ", type_label, " — skipping")
    return(tibble())
  }
  
  joined |>
    cross_join(tibble(full_game_line = 1:40)) |>
    group_by(scorer, fqpts, full_game_line) |>
    summarise(
      pct_hit = mean(pts >= full_game_line, na.rm = TRUE),
      n_games = n(),
      .groups = "drop"
    ) |>
    pivot_wider(
      names_from   = full_game_line,
      values_from  = pct_hit,
      names_prefix = "total_pts_"
    ) |>
    mutate(Type = type_label)
}

# ── Build all splits ───────────────────────────────────────────────────────────
pts_conditional <- bind_rows(
  Filter(function(df) nrow(df) > 0, list(
    compute_pts_conditional(
      build_joined_pts("Regular Season"),
      "Regular Season"
    ),
    compute_pts_conditional(
      build_joined_pts("Regular Season", location_filter = "H"),
      "Home Games"
    ),
    compute_pts_conditional(
      build_joined_pts("Regular Season", location_filter = "A"),
      "Away Games"
    ),
    compute_pts_conditional(
      build_joined_pts("Playoffs"),
      "Playoffs"
    ),
    compute_pts_conditional(
      build_joined_pts("Playoffs", location_filter = "H"),
      "Playoff Home Games"
    ),
    compute_pts_conditional(
      build_joined_pts("Playoffs", location_filter = "A"),
      "Playoff Away Games"
    ),
    compute_pts_conditional(
      build_joined_pts_last_n(5),
      "Last 5"
    ),
    compute_pts_conditional(
      build_joined_pts_last_n(10),
      "Last 10"
    )
  ))
)

# ══════════════════════════════════════════════════════════════════════════════
# ASSISTS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_ast <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number == 1,
      scoring_play  == TRUE,
      !is.na(assister)
    ) %>%
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    )
  
  player_filtered <- playerdata %>%
    filter(
      typeSeason == game_type_filter,
      yearSeason == season_current
    ) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(ast = sum(ast), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(assister, assister_key, game_date) %>%
    summarize(fqast = n(), .groups = "drop") %>%
    left_join(
      player_filtered,
      by = c("assister_key" = "name_key", "game_date" = "dateGame")
    )
  
  if (!is.null(location_filter)) {
    joined <- joined %>% filter(locationGame == location_filter)
  }
  
  joined
}

build_joined_ast_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(
      period_number == 1,
      scoring_play  == TRUE,
      !is.na(assister),
      game_type     != "Play-In"
    ) %>%
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    ) %>%
    group_by(assister, assister_key, game_date) %>%
    summarize(fqast = n(), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(ast = sum(ast), .groups = "drop")
  
  pbp_all %>%
    left_join(
      player_all,
      by = c("assister_key" = "name_key", "game_date" = "dateGame")
    ) %>%
    group_by(assister) %>%
    arrange(desc(game_date)) %>%
    slice_head(n = n) %>%
    ungroup()
}

compute_ast_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$ast))) {
    message("No data for ast conditional split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:20)) |>
    group_by(assister, fqast, full_game_line) |>
    summarise(
      pct_hit = mean(ast >= full_game_line, na.rm = TRUE),
      n_games = n(),
      .groups = "drop"
    ) |>
    pivot_wider(
      names_from   = full_game_line,
      values_from  = pct_hit,
      names_prefix = "total_ast_"
    ) |>
    mutate(Type = type_label)
}

ast_conditional <- bind_rows(
  Filter(function(df) nrow(df) > 0, list(
    compute_ast_conditional(build_joined_ast("Regular Season"),                    "Regular Season"),
    compute_ast_conditional(build_joined_ast("Regular Season", location_filter = "H"), "Home Games"),
    compute_ast_conditional(build_joined_ast("Regular Season", location_filter = "A"), "Away Games"),
    compute_ast_conditional(build_joined_ast("Playoffs"),                          "Playoffs"),
    compute_ast_conditional(build_joined_ast("Playoffs", location_filter = "H"),   "Playoff Home Games"),
    compute_ast_conditional(build_joined_ast("Playoffs", location_filter = "A"),   "Playoff Away Games"),
    compute_ast_conditional(build_joined_ast_last_n(5),                            "Last 5"),
    compute_ast_conditional(build_joined_ast_last_n(10),                           "Last 10")
  ))
)


# ══════════════════════════════════════════════════════════════════════════════
# REBOUNDS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_reb <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number == 1,
      !is.na(rebounder),
      !str_detect(rebounder, "team")
    ) %>%
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    )
  
  player_filtered <- playerdata %>%
    filter(
      typeSeason == game_type_filter,
      yearSeason == season_current
    ) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(reb = sum(treb), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(rebounder, rebounder_key, game_date) %>%
    summarize(fqreb = n(), .groups = "drop") %>%
    left_join(
      player_filtered,
      by = c("rebounder_key" = "name_key", "game_date" = "dateGame")
    )
  
  if (!is.null(location_filter)) {
    joined <- joined %>% filter(locationGame == location_filter)
  }
  
  joined
}

build_joined_reb_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(
      period_number == 1,
      !is.na(rebounder),
      !str_detect(rebounder, "team"),
      game_type     != "Play-In"
    ) %>%
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    ) %>%
    group_by(rebounder, rebounder_key, game_date) %>%
    summarize(fqreb = n(), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(reb = sum(treb), .groups = "drop")
  
  pbp_all %>%
    left_join(
      player_all,
      by = c("rebounder_key" = "name_key", "game_date" = "dateGame")
    ) %>%
    group_by(rebounder) %>%
    arrange(desc(game_date)) %>%
    slice_head(n = n) %>%
    ungroup()
}

compute_reb_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$reb))) {
    message("No data for reb conditional split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:15)) |>
    group_by(rebounder, fqreb, full_game_line) |>
    summarise(
      pct_hit = mean(reb >= full_game_line, na.rm = TRUE),
      n_games = n(),
      .groups = "drop"
    ) |>
    pivot_wider(
      names_from   = full_game_line,
      values_from  = pct_hit,
      names_prefix = "total_reb_"
    ) |>
    mutate(Type = type_label)
}

reb_conditional <- bind_rows(
  Filter(function(df) nrow(df) > 0, list(
    compute_reb_conditional(build_joined_reb("Regular Season"),                    "Regular Season"),
    compute_reb_conditional(build_joined_reb("Regular Season", location_filter = "H"), "Home Games"),
    compute_reb_conditional(build_joined_reb("Regular Season", location_filter = "A"), "Away Games"),
    compute_reb_conditional(build_joined_reb("Playoffs"),                          "Playoffs"),
    compute_reb_conditional(build_joined_reb("Playoffs", location_filter = "H"),   "Playoff Home Games"),
    compute_reb_conditional(build_joined_reb("Playoffs", location_filter = "A"),   "Playoff Away Games"),
    compute_reb_conditional(build_joined_reb_last_n(5),                            "Last 5"),
    compute_reb_conditional(build_joined_reb_last_n(10),                           "Last 10")
  ))
)




# ══════════════════════════════════════════════════════════════════════════════
# BUCKET HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# ── Points bucket labels ───────────────────────────────────────────────────────
bucket_pts <- function(x) {
  dplyr::case_when(
    x <= 4  ~ "0-4 pts",
    x <= 9  ~ "5-9 pts",
    x <= 14 ~ "10-14 pts",
    TRUE    ~ "15+ pts"
  )
}

bucket_pts_order <- c("0-4 pts", "5-9 pts", "10-14 pts", "15+ pts")

# ── Assists bucket labels ──────────────────────────────────────────────────────
bucket_ast <- function(x) {
  dplyr::case_when(
    x <= 1  ~ "0-1 ast",
    x <= 3  ~ "2-3 ast",
    x <= 5  ~ "4-5 ast",
    TRUE    ~ "6+ ast"
  )
}

bucket_ast_order <- c("0-1 ast", "2-3 ast", "4-5 ast", "6+ ast")

# ── Rebounds bucket labels ─────────────────────────────────────────────────────
bucket_reb <- function(x) {
  dplyr::case_when(
    x <= 1  ~ "0-1 reb",
    x <= 4  ~ "2-4 reb",
    x <= 8  ~ "5-8 reb",
    TRUE    ~ "9+ reb"
  )
}

bucket_reb_order <- c("0-1 reb", "2-4 reb", "5-8 reb", "9+ reb")

# ── Generic bucketing function ─────────────────────────────────────────────────
# Takes a conditional table, buckets the half_col, and re-summarises
bucket_conditional <- function(data, half_col, bucket_fn, bucket_order, prefix) {
  
  # Get player column
  ou_cols    <- names(data)[grepl(paste0("^", prefix), names(data))]
  player_col <- setdiff(names(data), c(ou_cols, half_col, "n_games", "Type"))
  
  # Pivot back to long format
  data_long <- data |>
    tidyr::pivot_longer(
      cols      = all_of(ou_cols),
      names_to  = "full_game_line",
      values_to = "pct_hit"
    ) |>
    dplyr::mutate(
      bucket = bucket_fn(.data[[half_col]]),
      bucket = factor(bucket, levels = bucket_order)
    )
  
  # Summarise in long format — weighted mean weighted by n_games
  data_bucketed_long <- data_long |>
    dplyr::group_by(across(all_of(c(player_col, "bucket", "Type", "full_game_line")))) |>
    dplyr::summarise(
      pct_hit = weighted.mean(pct_hit, w = n_games, na.rm = TRUE),
      n_games = sum(n_games, na.rm = TRUE),
      .groups = "drop"
    )
  
  # Pivot back to wide — then reorder columns numerically
  result <- data_bucketed_long |>
    tidyr::pivot_wider(
      names_from  = full_game_line,
      values_from = pct_hit
    ) |>
    dplyr::rename(!!half_col := bucket)
  
  # ── Reorder OU columns numerically to match original ──────────────────────
  ou_cols_result <- names(result)[grepl(paste0("^", prefix), names(result))]
  ou_cols_sorted <- ou_cols_result[order(as.numeric(gsub(prefix, "", ou_cols_result)))]
  
  non_ou_cols <- setdiff(names(result), ou_cols_result)
  
  result |> dplyr::select(all_of(non_ou_cols), all_of(ou_cols_sorted))
}

# ── Build bucketed versions ────────────────────────────────────────────────────
pts_conditional_bucketed  <- bucket_conditional(
  pts_conditional,  "fqpts", bucket_pts, bucket_pts_order, "total_pts_"
)
ast_conditional_bucketed  <- bucket_conditional(
  ast_conditional,  "fqast", bucket_ast, bucket_ast_order, "total_ast_"
)
reb_conditional_bucketed  <- bucket_conditional(
  reb_conditional,  "fqreb", bucket_reb, bucket_reb_order, "total_reb_"
)



# ══════════════════════════════════════════════════════════════════════════════
# FIRST HALF (Q1 + Q2) POINTS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_h1pts <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number %in% c(1, 2),    # Q1 + Q2
      !is.na(scorer)
    ) %>%
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    )
  
  player_filtered <- playerdata %>%
    filter(typeSeason == game_type_filter, yearSeason == season_current) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(pts = sum(pts), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(scorer, scorer_key, game_date) %>%
    summarize(h1pts = sum(score_value), .groups = "drop") %>%
    left_join(player_filtered, by = c("scorer_key" = "name_key", "game_date" = "dateGame"))
  
  if (!is.null(location_filter)) joined <- joined %>% filter(locationGame == location_filter)
  joined
}

build_joined_h1pts_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(period_number %in% c(1, 2), !is.na(scorer),
           game_type     != "Play-In") %>%
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    ) %>%
    group_by(scorer, scorer_key, game_date) %>%
    summarize(h1pts = sum(score_value), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(pts = sum(pts), .groups = "drop")
  
  pbp_all %>%
    left_join(player_all, by = c("scorer_key" = "name_key", "game_date" = "dateGame")) %>%
    group_by(scorer) %>% arrange(desc(game_date)) %>% slice_head(n = n) %>% ungroup()
}

compute_h1pts_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$pts))) {
    message("No data for H1 pts split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:40)) |>
    group_by(scorer, h1pts, full_game_line) |>
    summarise(pct_hit = mean(pts >= full_game_line, na.rm = TRUE), n_games = n(), .groups = "drop") |>
    pivot_wider(names_from = full_game_line, values_from = pct_hit, names_prefix = "total_pts_") |>
    mutate(Type = type_label)
}

h1pts_conditional <- bind_rows(Filter(function(df) nrow(df) > 0, list(
  compute_h1pts_conditional(build_joined_h1pts("Regular Season"),                     "Regular Season"),
  compute_h1pts_conditional(build_joined_h1pts("Regular Season", location_filter="H"),"Home Games"),
  compute_h1pts_conditional(build_joined_h1pts("Regular Season", location_filter="A"),"Away Games"),
  compute_h1pts_conditional(build_joined_h1pts("Playoffs"),                           "Playoffs"),
  compute_h1pts_conditional(build_joined_h1pts("Playoffs",   location_filter="H"),    "Playoff Home Games"),
  compute_h1pts_conditional(build_joined_h1pts("Playoffs",   location_filter="A"),    "Playoff Away Games"),
  compute_h1pts_conditional(build_joined_h1pts_last_n(5),                             "Last 5"),
  compute_h1pts_conditional(build_joined_h1pts_last_n(10),                            "Last 10")
)))


# ══════════════════════════════════════════════════════════════════════════════
# FIRST HALF (Q1 + Q2) ASSISTS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_h1ast <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number %in% c(1, 2),
      scoring_play  == TRUE,
      !is.na(assister)
    ) %>%
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    )
  
  player_filtered <- playerdata %>%
    filter(typeSeason == game_type_filter, yearSeason == season_current) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(ast = sum(ast), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(assister, assister_key, game_date) %>%
    summarize(h1ast = n(), .groups = "drop") %>%
    left_join(player_filtered, by = c("assister_key" = "name_key", "game_date" = "dateGame"))
  
  if (!is.null(location_filter)) joined <- joined %>% filter(locationGame == location_filter)
  joined
}

build_joined_h1ast_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(period_number %in% c(1, 2), scoring_play == TRUE, !is.na(assister),
           game_type     != "Play-In") %>%
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    ) %>%
    group_by(assister, assister_key, game_date) %>%
    summarize(h1ast = n(), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(ast = sum(ast), .groups = "drop")
  
  pbp_all %>%
    left_join(player_all, by = c("assister_key" = "name_key", "game_date" = "dateGame")) %>%
    group_by(assister) %>% arrange(desc(game_date)) %>% slice_head(n = n) %>% ungroup()
}

compute_h1ast_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$ast))) {
    message("No data for H1 ast split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:20)) |>
    group_by(assister, h1ast, full_game_line) |>
    summarise(pct_hit = mean(ast >= full_game_line, na.rm = TRUE), n_games = n(), .groups = "drop") |>
    pivot_wider(names_from = full_game_line, values_from = pct_hit, names_prefix = "total_ast_") |>
    mutate(Type = type_label)
}

h1ast_conditional <- bind_rows(Filter(function(df) nrow(df) > 0, list(
  compute_h1ast_conditional(build_joined_h1ast("Regular Season"),                     "Regular Season"),
  compute_h1ast_conditional(build_joined_h1ast("Regular Season", location_filter="H"),"Home Games"),
  compute_h1ast_conditional(build_joined_h1ast("Regular Season", location_filter="A"),"Away Games"),
  compute_h1ast_conditional(build_joined_h1ast("Playoffs"),                           "Playoffs"),
  compute_h1ast_conditional(build_joined_h1ast("Playoffs",   location_filter="H"),    "Playoff Home Games"),
  compute_h1ast_conditional(build_joined_h1ast("Playoffs",   location_filter="A"),    "Playoff Away Games"),
  compute_h1ast_conditional(build_joined_h1ast_last_n(5),                             "Last 5"),
  compute_h1ast_conditional(build_joined_h1ast_last_n(10),                            "Last 10")
)))


# ══════════════════════════════════════════════════════════════════════════════
# FIRST HALF (Q1 + Q2) REBOUNDS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_h1reb <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number %in% c(1, 2),
      !is.na(rebounder),
      !str_detect(rebounder, "team"),
      game_type     != "Play-In"
    ) %>%
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    )
  
  player_filtered <- playerdata %>%
    filter(typeSeason == game_type_filter, yearSeason == season_current) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(reb = sum(treb), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(rebounder, rebounder_key, game_date) %>%
    summarize(h1reb = n(), .groups = "drop") %>%
    left_join(player_filtered, by = c("rebounder_key" = "name_key", "game_date" = "dateGame"))
  
  if (!is.null(location_filter)) joined <- joined %>% filter(locationGame == location_filter)
  joined
}

build_joined_h1reb_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(period_number %in% c(1, 2), !is.na(rebounder), !str_detect(rebounder, "team")) %>%
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    ) %>%
    group_by(rebounder, rebounder_key, game_date) %>%
    summarize(h1reb = n(), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(reb = sum(treb), .groups = "drop")
  
  pbp_all %>%
    left_join(player_all, by = c("rebounder_key" = "name_key", "game_date" = "dateGame")) %>%
    group_by(rebounder) %>% arrange(desc(game_date)) %>% slice_head(n = n) %>% ungroup()
}

compute_h1reb_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$reb))) {
    message("No data for H1 reb split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:15)) |>
    group_by(rebounder, h1reb, full_game_line) |>
    summarise(pct_hit = mean(reb >= full_game_line, na.rm = TRUE), n_games = n(), .groups = "drop") |>
    pivot_wider(names_from = full_game_line, values_from = pct_hit, names_prefix = "total_reb_") |>
    mutate(Type = type_label)
}

h1reb_conditional <- bind_rows(Filter(function(df) nrow(df) > 0, list(
  compute_h1reb_conditional(build_joined_h1reb("Regular Season"),                     "Regular Season"),
  compute_h1reb_conditional(build_joined_h1reb("Regular Season", location_filter="H"),"Home Games"),
  compute_h1reb_conditional(build_joined_h1reb("Regular Season", location_filter="A"),"Away Games"),
  compute_h1reb_conditional(build_joined_h1reb("Playoffs"),                           "Playoffs"),
  compute_h1reb_conditional(build_joined_h1reb("Playoffs",   location_filter="H"),    "Playoff Home Games"),
  compute_h1reb_conditional(build_joined_h1reb("Playoffs",   location_filter="A"),    "Playoff Away Games"),
  compute_h1reb_conditional(build_joined_h1reb_last_n(5),                             "Last 5"),
  compute_h1reb_conditional(build_joined_h1reb_last_n(10),                            "Last 10")
)))

# ══════════════════════════════════════════════════════════════════════════════
# H1 BUCKET HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# ── H1 Points bucket labels ───────────────────────────────────────────────────
bucket_h1pts <- function(x) {
  dplyr::case_when(
    x <= 9  ~ "0-9 pts",
    x <= 19 ~ "10-19 pts",
    x <= 29 ~ "20-29 pts",
    TRUE    ~ "30+ pts"
  )
}

bucket_h1pts_order <- c("0-9 pts", "10-19 pts", "20-29 pts", "30+ pts")

# ── H1 Assists bucket labels ──────────────────────────────────────────────────
bucket_h1ast <- function(x) {
  dplyr::case_when(
    x <= 2  ~ "0-2 ast",
    x <= 5  ~ "3-5 ast",
    x <= 8  ~ "6-8 ast",
    TRUE    ~ "9+ ast"
  )
}

bucket_h1ast_order <- c("0-2 ast", "3-5 ast", "6-8 ast", "9+ ast")

# ── H1 Rebounds bucket labels ─────────────────────────────────────────────────
bucket_h1reb <- function(x) {
  dplyr::case_when(
    x <= 3  ~ "0-3 reb",
    x <= 7  ~ "4-7 reb",
    x <= 12 ~ "8-12 reb",
    TRUE    ~ "13+ reb"
  )
}

bucket_h1reb_order <- c("0-3 reb", "4-7 reb", "8-12 reb", "13+ reb")

# ── Build H1 bucketed versions ────────────────────────────────────────────────
h1pts_conditional_bucketed <- bucket_conditional(
  h1pts_conditional, "h1pts", bucket_h1pts, bucket_h1pts_order, "total_pts_"
)
h1ast_conditional_bucketed <- bucket_conditional(
  h1ast_conditional, "h1ast", bucket_h1ast, bucket_h1ast_order, "total_ast_"
)
h1reb_conditional_bucketed <- bucket_conditional(
  h1reb_conditional, "h1reb", bucket_h1reb, bucket_h1reb_order, "total_reb_"
)

# ══════════════════════════════════════════════════════════════════════════════
# FIRST THREE QUARTERS (Q1 + Q2 + Q3) POINTS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_q3pts <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number %in% c(1, 2, 3),
      !is.na(scorer)
    ) %>%
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    )
  
  player_filtered <- playerdata %>%
    filter(typeSeason == game_type_filter, yearSeason == season_current) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(pts = sum(pts), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(scorer, scorer_key, game_date) %>%
    summarize(q3pts = sum(score_value), .groups = "drop") %>%
    left_join(player_filtered, by = c("scorer_key" = "name_key", "game_date" = "dateGame"))
  
  if (!is.null(location_filter)) joined <- joined %>% filter(locationGame == location_filter)
  joined
}

build_joined_q3pts_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(period_number %in% c(1, 2, 3), !is.na(scorer),
           game_type     != "Play-In") %>%
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    ) %>%
    group_by(scorer, scorer_key, game_date) %>%
    summarize(q3pts = sum(score_value), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(pts = sum(pts), .groups = "drop")
  
  pbp_all %>%
    left_join(player_all, by = c("scorer_key" = "name_key", "game_date" = "dateGame")) %>%
    group_by(scorer) %>% arrange(desc(game_date)) %>% slice_head(n = n) %>% ungroup()
}

compute_q3pts_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$pts))) {
    message("No data for Q3 pts split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:40)) |>
    group_by(scorer, q3pts, full_game_line) |>
    summarise(pct_hit = mean(pts >= full_game_line, na.rm = TRUE), n_games = n(), .groups = "drop") |>
    pivot_wider(names_from = full_game_line, values_from = pct_hit, names_prefix = "total_pts_") |>
    mutate(Type = type_label)
}

q3pts_conditional <- bind_rows(Filter(function(df) nrow(df) > 0, list(
  compute_q3pts_conditional(build_joined_q3pts("Regular Season"),                     "Regular Season"),
  compute_q3pts_conditional(build_joined_q3pts("Regular Season", location_filter="H"),"Home Games"),
  compute_q3pts_conditional(build_joined_q3pts("Regular Season", location_filter="A"),"Away Games"),
  compute_q3pts_conditional(build_joined_q3pts("Playoffs"),                           "Playoffs"),
  compute_q3pts_conditional(build_joined_q3pts("Playoffs",   location_filter="H"),    "Playoff Home Games"),
  compute_q3pts_conditional(build_joined_q3pts("Playoffs",   location_filter="A"),    "Playoff Away Games"),
  compute_q3pts_conditional(build_joined_q3pts_last_n(5),                             "Last 5"),
  compute_q3pts_conditional(build_joined_q3pts_last_n(10),                            "Last 10")
)))


# ══════════════════════════════════════════════════════════════════════════════
# FIRST THREE QUARTERS (Q1 + Q2 + Q3) ASSISTS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_q3ast <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number %in% c(1, 2, 3),
      scoring_play  == TRUE,
      !is.na(assister)
    ) %>%
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    )
  
  player_filtered <- playerdata %>%
    filter(typeSeason == game_type_filter, yearSeason == season_current) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(ast = sum(ast), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(assister, assister_key, game_date) %>%
    summarize(q3ast = n(), .groups = "drop") %>%
    left_join(player_filtered, by = c("assister_key" = "name_key", "game_date" = "dateGame"))
  
  if (!is.null(location_filter)) joined <- joined %>% filter(locationGame == location_filter)
  joined
}

build_joined_q3ast_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(period_number %in% c(1, 2, 3), scoring_play == TRUE, !is.na(assister),
           game_type     != "Play-In") %>%
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    ) %>%
    group_by(assister, assister_key, game_date) %>%
    summarize(q3ast = n(), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(ast = sum(ast), .groups = "drop")
  
  pbp_all %>%
    left_join(player_all, by = c("assister_key" = "name_key", "game_date" = "dateGame")) %>%
    group_by(assister) %>% arrange(desc(game_date)) %>% slice_head(n = n) %>% ungroup()
}

compute_q3ast_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$ast))) {
    message("No data for Q3 ast split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:20)) |>
    group_by(assister, q3ast, full_game_line) |>
    summarise(pct_hit = mean(ast >= full_game_line, na.rm = TRUE), n_games = n(), .groups = "drop") |>
    pivot_wider(names_from = full_game_line, values_from = pct_hit, names_prefix = "total_ast_") |>
    mutate(Type = type_label)
}

q3ast_conditional <- bind_rows(Filter(function(df) nrow(df) > 0, list(
  compute_q3ast_conditional(build_joined_q3ast("Regular Season"),                     "Regular Season"),
  compute_q3ast_conditional(build_joined_q3ast("Regular Season", location_filter="H"),"Home Games"),
  compute_q3ast_conditional(build_joined_q3ast("Regular Season", location_filter="A"),"Away Games"),
  compute_q3ast_conditional(build_joined_q3ast("Playoffs"),                           "Playoffs"),
  compute_q3ast_conditional(build_joined_q3ast("Playoffs",   location_filter="H"),    "Playoff Home Games"),
  compute_q3ast_conditional(build_joined_q3ast("Playoffs",   location_filter="A"),    "Playoff Away Games"),
  compute_q3ast_conditional(build_joined_q3ast_last_n(5),                             "Last 5"),
  compute_q3ast_conditional(build_joined_q3ast_last_n(10),                            "Last 10")
)))


# ══════════════════════════════════════════════════════════════════════════════
# FIRST THREE QUARTERS (Q1 + Q2 + Q3) REBOUNDS
# ══════════════════════════════════════════════════════════════════════════════

build_joined_q3reb <- function(game_type_filter, location_filter = NULL) {
  
  pbp_filtered <- play_play %>%
    filter(
      game_type     == game_type_filter,
      period_number %in% c(1, 2, 3),
      !is.na(rebounder),
      !str_detect(rebounder, "team")
    ) %>%
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    )
  
  player_filtered <- playerdata %>%
    filter(typeSeason == game_type_filter, yearSeason == season_current) %>%
    group_by(name_key, dateGame, locationGame) %>%
    summarize(reb = sum(treb), .groups = "drop")
  
  joined <- pbp_filtered %>%
    group_by(rebounder, rebounder_key, game_date) %>%
    summarize(q3reb = n(), .groups = "drop") %>%
    left_join(player_filtered, by = c("rebounder_key" = "name_key", "game_date" = "dateGame"))
  
  if (!is.null(location_filter)) joined <- joined %>% filter(locationGame == location_filter)
  joined
}

build_joined_q3reb_last_n <- function(n) {
  
  pbp_all <- play_play %>%
    filter(period_number %in% c(1, 2, 3), !is.na(rebounder), !str_detect(rebounder, "team"),
           game_type     != "Play-In") %>%
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    ) %>%
    group_by(rebounder, rebounder_key, game_date) %>%
    summarize(q3reb = n(), .groups = "drop")
  
  player_all <- playerdata %>%
    filter(yearSeason == season_current) %>%
    group_by(name_key, dateGame) %>%
    summarize(reb = sum(treb), .groups = "drop")
  
  pbp_all %>%
    left_join(player_all, by = c("rebounder_key" = "name_key", "game_date" = "dateGame")) %>%
    group_by(rebounder) %>% arrange(desc(game_date)) %>% slice_head(n = n) %>% ungroup()
}

compute_q3reb_conditional <- function(joined, type_label) {
  if (nrow(joined) == 0 || all(is.na(joined$reb))) {
    message("No data for Q3 reb split: ", type_label, " — skipping")
    return(tibble())
  }
  joined |>
    cross_join(tibble(full_game_line = 1:15)) |>
    group_by(rebounder, q3reb, full_game_line) |>
    summarise(pct_hit = mean(reb >= full_game_line, na.rm = TRUE), n_games = n(), .groups = "drop") |>
    pivot_wider(names_from = full_game_line, values_from = pct_hit, names_prefix = "total_reb_") |>
    mutate(Type = type_label)
}

q3reb_conditional <- bind_rows(Filter(function(df) nrow(df) > 0, list(
  compute_q3reb_conditional(build_joined_q3reb("Regular Season"),                     "Regular Season"),
  compute_q3reb_conditional(build_joined_q3reb("Regular Season", location_filter="H"),"Home Games"),
  compute_q3reb_conditional(build_joined_q3reb("Regular Season", location_filter="A"),"Away Games"),
  compute_q3reb_conditional(build_joined_q3reb("Playoffs"),                           "Playoffs"),
  compute_q3reb_conditional(build_joined_q3reb("Playoffs",   location_filter="H"),    "Playoff Home Games"),
  compute_q3reb_conditional(build_joined_q3reb("Playoffs",   location_filter="A"),    "Playoff Away Games"),
  compute_q3reb_conditional(build_joined_q3reb_last_n(5),                             "Last 5"),
  compute_q3reb_conditional(build_joined_q3reb_last_n(10),                            "Last 10")
)))


# ══════════════════════════════════════════════════════════════════════════════
# Q3 BUCKET HELPERS
# ══════════════════════════════════════════════════════════════════════════════

bucket_q3pts <- function(x) {
  dplyr::case_when(
    x <= 14 ~ "0-14 pts",
    x <= 24 ~ "15-24 pts",
    x <= 34 ~ "25-34 pts",
    TRUE    ~ "35+ pts"
  )
}
bucket_q3pts_order <- c("0-14 pts", "15-24 pts", "25-34 pts", "35+ pts")

bucket_q3ast <- function(x) {
  dplyr::case_when(
    x <= 3  ~ "0-3 ast",
    x <= 7  ~ "4-7 ast",
    x <= 11 ~ "8-11 ast",
    TRUE    ~ "12+ ast"
  )
}
bucket_q3ast_order <- c("0-3 ast", "4-7 ast", "8-11 ast", "12+ ast")

bucket_q3reb <- function(x) {
  dplyr::case_when(
    x <= 5  ~ "0-5 reb",
    x <= 10 ~ "6-10 reb",
    x <= 15 ~ "11-15 reb",
    TRUE    ~ "16+ reb"
  )
}
bucket_q3reb_order <- c("0-5 reb", "6-10 reb", "11-15 reb", "16+ reb")

# ── Build Q3 bucketed versions ─────────────────────────────────────────────────
q3pts_conditional_bucketed <- bucket_conditional(
  q3pts_conditional, "q3pts", bucket_q3pts, bucket_q3pts_order, "total_pts_"
)
q3ast_conditional_bucketed <- bucket_conditional(
  q3ast_conditional, "q3ast", bucket_q3ast, bucket_q3ast_order, "total_ast_"
)
q3reb_conditional_bucketed <- bucket_conditional(
  q3reb_conditional, "q3reb", bucket_q3reb, bucket_q3reb_order, "total_reb_"
)


# ── Minimum games filter for conditional bucketed tables ───────────────────────
# Players need at least this many games across all splits to be included
# This removes garbage sample sizes and reduces render memory significantly

season_start_date <- as.Date(scheduleDate)
days_into_season  <- as.numeric(Sys.Date() - season_start_date)

MIN_GAMES_CONDITIONAL <- dplyr::case_when(
  days_into_season < 30  ~ 5,    # October — early season, small samples ok
  days_into_season < 60  ~ 10,   # November
  days_into_season < 90  ~ 20,   # December
  days_into_season < 120 ~ 30,   # January
  days_into_season < 150 ~ 40,   # February
  days_into_season < 180 ~ 50,   # March — where you are now
  TRUE                   ~ 60    # April playoffs — high bar
)

# ── Points ─────────────────────────────────────────────────────────────────────
pts_eligible_scorers <- pts_conditional_bucketed |>
  group_by(scorer) |>
  summarise(total_games = sum(n_games), .groups = "drop") |>
  filter(total_games >= MIN_GAMES_CONDITIONAL) |>
  pull(scorer)

pts_conditional_bucketed  <- pts_conditional_bucketed  |> filter(scorer   %in% pts_eligible_scorers)
pts_conditional           <- pts_conditional           |> filter(scorer   %in% pts_eligible_scorers)

# ── Assists ────────────────────────────────────────────────────────────────────
ast_eligible_assisters <- ast_conditional_bucketed |>
  group_by(assister) |>
  summarise(total_games = sum(n_games), .groups = "drop") |>
  filter(total_games >= MIN_GAMES_CONDITIONAL) |>
  pull(assister)

ast_conditional_bucketed  <- ast_conditional_bucketed  |> filter(assister %in% ast_eligible_assisters)
ast_conditional           <- ast_conditional           |> filter(assister %in% ast_eligible_assisters)

# ── Rebounds ───────────────────────────────────────────────────────────────────
reb_eligible_rebounders <- reb_conditional_bucketed |>
  group_by(rebounder) |>
  summarise(total_games = sum(n_games), .groups = "drop") |>
  filter(total_games >= MIN_GAMES_CONDITIONAL) |>
  pull(rebounder)

reb_conditional_bucketed  <- reb_conditional_bucketed  |> filter(rebounder %in% reb_eligible_rebounders)
reb_conditional           <- reb_conditional           |> filter(rebounder %in% reb_eligible_rebounders)

# ── Apply same filter to H1 and Q3 ────────────────────────────────────────────
h1pts_conditional_bucketed <- h1pts_conditional_bucketed |> filter(scorer   %in% pts_eligible_scorers)
h1pts_conditional          <- h1pts_conditional          |> filter(scorer   %in% pts_eligible_scorers)
h1ast_conditional_bucketed <- h1ast_conditional_bucketed |> filter(assister %in% ast_eligible_assisters)
h1ast_conditional          <- h1ast_conditional          |> filter(assister %in% ast_eligible_assisters)
h1reb_conditional_bucketed <- h1reb_conditional_bucketed |> filter(rebounder %in% reb_eligible_rebounders)
h1reb_conditional          <- h1reb_conditional          |> filter(rebounder %in% reb_eligible_rebounders)

q3pts_conditional_bucketed <- q3pts_conditional_bucketed |> filter(scorer   %in% pts_eligible_scorers)
q3pts_conditional          <- q3pts_conditional          |> filter(scorer   %in% pts_eligible_scorers)
q3ast_conditional_bucketed <- q3ast_conditional_bucketed |> filter(assister %in% ast_eligible_assisters)
q3ast_conditional          <- q3ast_conditional          |> filter(assister %in% ast_eligible_assisters)
q3reb_conditional_bucketed <- q3reb_conditional_bucketed |> filter(rebounder %in% reb_eligible_rebounders)
q3reb_conditional          <- q3reb_conditional          |> filter(rebounder %in% reb_eligible_rebounders)

# ══════════════════════════════════════════════════════════════════════════════
# VS OPPONENT SPLIT — Q1 / H1 / Q3
# Adds "vs [OPP]" rows to pts_conditional, ast_conditional, reb_conditional
# and all H1/Q3 equivalents
# ══════════════════════════════════════════════════════════════════════════════

# ── Build opponent lookup from player_opponent_lookup ─────────────────────────
# Maps each scorer_key to their opponent abbreviation tonight
opp_lookup_pts <- player_opponent_lookup |>
  dplyr::mutate(scorer_key = normalize_name(player)) |>
  dplyr::select(scorer_key, opponent_abbrev)

opp_lookup_ast <- player_opponent_lookup |>
  dplyr::mutate(assister_key = normalize_name(player)) |>
  dplyr::select(assister_key, opponent_abbrev)

opp_lookup_reb <- player_opponent_lookup |>
  dplyr::mutate(rebounder_key = normalize_name(player)) |>
  dplyr::select(rebounder_key, opponent_abbrev)

# ── VS Opponent builder — Points ──────────────────────────────────────────────
build_joined_pts_vs_opp <- function() {
  
  pbp_filtered <- play_play |>
    dplyr::filter(
      game_type     == "Regular Season",
      game_type     != "Play-In",
      period_number == 1,
      !is.na(scorer)
    ) |>
    dplyr::inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    ) |>
    dplyr::group_by(scorer, scorer_key, game_date) |>
    dplyr::summarise(fqpts = sum(score_value), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(pts = sum(pts), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(
      player_filtered,
      by = c("scorer_key" = "name_key", "game_date" = "dateGame")
    ) |>
    # Join to get tonight's opponent per player
    dplyr::left_join(opp_lookup_pts, by = "scorer_key") |>
    # Only keep games where they played tonight's opponent
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

# ── VS Opponent builder — Assists ─────────────────────────────────────────────
build_joined_ast_vs_opp <- function() {
  
  pbp_filtered <- play_play |>
    dplyr::filter(
      game_type     == "Regular Season",
      period_number == 1,
      scoring_play  == TRUE,
      !is.na(assister)
    ) |>
    dplyr::inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    ) |>
    dplyr::group_by(assister, assister_key, game_date) |>
    dplyr::summarise(fqast = n(), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(ast = sum(ast), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(
      player_filtered,
      by = c("assister_key" = "name_key", "game_date" = "dateGame")
    ) |>
    dplyr::left_join(opp_lookup_ast, by = "assister_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

# ── VS Opponent builder — Rebounds ────────────────────────────────────────────
build_joined_reb_vs_opp <- function() {
  
  pbp_filtered <- play_play |>
    dplyr::filter(
      game_type     == "Regular Season",
      period_number == 1,
      !is.na(rebounder),
      !stringr::str_detect(rebounder, "team")
    ) |>
    dplyr::inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    ) |>
    dplyr::group_by(rebounder, rebounder_key, game_date) |>
    dplyr::summarise(fqreb = n(), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(reb = sum(treb), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(
      player_filtered,
      by = c("rebounder_key" = "name_key", "game_date" = "dateGame")
    ) |>
    dplyr::left_join(opp_lookup_reb, by = "rebounder_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

# ── Compute and append vs opponent rows ───────────────────────────────────────
# The Type label is dynamic per player — "vs OKC", "vs BOS" etc.
# We compute per-player so each gets their own opponent label

append_vs_opp_pts <- function(joined) {
  if (nrow(joined) == 0) return(tibble())
  # Build a per-player type label using opponent_abbrev
  joined |>
    dplyr::mutate(opp_label = paste0("vs ", opponent_abbrev)) |>
    dplyr::group_by(scorer, scorer_key, fqpts, opp_label) |>
    dplyr::group_modify(~ {
      compute_pts_conditional(.x |> dplyr::mutate(pts = pts), .y$opp_label[1])
    }) |>
    dplyr::ungroup()
}

# Simpler approach — use compute functions directly with dynamic label
vs_opp_pts_joined <- build_joined_pts_vs_opp()

vs_opp_pts <- if (nrow(vs_opp_pts_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_pts_joined$scorer_key), function(key) {
    player_data <- vs_opp_pts_joined |> dplyr::filter(scorer_key == key)
    if (nrow(player_data) == 0) return(NULL)
    opp   <- player_data$opponent_abbrev[1]
    label <- paste0("vs ", opp)
    compute_pts_conditional(player_data, label)
  })
} else tibble()

vs_opp_ast_joined <- build_joined_ast_vs_opp()

vs_opp_ast <- if (nrow(vs_opp_ast_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_ast_joined$assister_key), function(key) {
    player_data <- vs_opp_ast_joined |> dplyr::filter(assister_key == key)
    if (nrow(player_data) == 0) return(NULL)
    opp   <- player_data$opponent_abbrev[1]
    label <- paste0("vs ", opp)
    compute_ast_conditional(player_data, label)
  })
} else tibble()

vs_opp_reb_joined <- build_joined_reb_vs_opp()

vs_opp_reb <- if (nrow(vs_opp_reb_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_reb_joined$rebounder_key), function(key) {
    player_data <- vs_opp_reb_joined |> dplyr::filter(rebounder_key == key)
    if (nrow(player_data) == 0) return(NULL)
    opp   <- player_data$opponent_abbrev[1]
    label <- paste0("vs ", opp)
    compute_reb_conditional(player_data, label)
  })
} else tibble()

# ── Append to existing conditional tables ─────────────────────────────────────
if (nrow(vs_opp_pts) > 0) {
  pts_conditional <- dplyr::bind_rows(pts_conditional, vs_opp_pts)
  cat("Added vs opponent pts rows:", nrow(vs_opp_pts), "\n")
}

if (nrow(vs_opp_ast) > 0) {
  ast_conditional <- dplyr::bind_rows(ast_conditional, vs_opp_ast)
  cat("Added vs opponent ast rows:", nrow(vs_opp_ast), "\n")
}

if (nrow(vs_opp_reb) > 0) {
  reb_conditional <- dplyr::bind_rows(reb_conditional, vs_opp_reb)
  cat("Added vs opponent reb rows:", nrow(vs_opp_reb), "\n")
}

# Verify Type values now include vs opponent
cat("Updated Type values in pts_conditional:\n")
print(sort(unique(pts_conditional$Type)))

# ── H1 vs Opponent ────────────────────────────────────────────────────────────
build_joined_h1pts_vs_opp <- function() {
  pbp_filtered <- play_play |>
    dplyr::filter(game_type == "Regular Season", game_type != "Play-In",
                  period_number %in% c(1,2), !is.na(scorer)) |>
    dplyr::inner_join(next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
                      by = "scorer_key") |>
    dplyr::group_by(scorer, scorer_key, game_date) |>
    dplyr::summarise(h1pts = sum(score_value), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(pts = sum(pts), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(player_filtered,
                     by = c("scorer_key" = "name_key", "game_date" = "dateGame")) |>
    dplyr::left_join(opp_lookup_pts, by = "scorer_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

build_joined_h1ast_vs_opp <- function() {
  pbp_filtered <- play_play |>
    dplyr::filter(game_type == "Regular Season", period_number %in% c(1,2),
                  scoring_play == TRUE, !is.na(assister)) |>
    dplyr::inner_join(next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
                      by = "assister_key") |>
    dplyr::group_by(assister, assister_key, game_date) |>
    dplyr::summarise(h1ast = n(), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(ast = sum(ast), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(player_filtered,
                     by = c("assister_key" = "name_key", "game_date" = "dateGame")) |>
    dplyr::left_join(opp_lookup_ast, by = "assister_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

build_joined_h1reb_vs_opp <- function() {
  pbp_filtered <- play_play |>
    dplyr::filter(game_type == "Regular Season", period_number %in% c(1,2),
                  !is.na(rebounder), !stringr::str_detect(rebounder, "team")) |>
    dplyr::inner_join(next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
                      by = "rebounder_key") |>
    dplyr::group_by(rebounder, rebounder_key, game_date) |>
    dplyr::summarise(h1reb = n(), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(reb = sum(treb), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(player_filtered,
                     by = c("rebounder_key" = "name_key", "game_date" = "dateGame")) |>
    dplyr::left_join(opp_lookup_reb, by = "rebounder_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

# Compute and append H1 vs opponent
vs_opp_h1pts_joined <- build_joined_h1pts_vs_opp()
vs_opp_h1pts <- if (nrow(vs_opp_h1pts_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_h1pts_joined$scorer_key), function(key) {
    player_data <- vs_opp_h1pts_joined |> dplyr::filter(scorer_key == key)
    if (nrow(player_data) == 0) return(NULL)
    compute_h1pts_conditional(player_data, paste0("vs ", player_data$opponent_abbrev[1]))
  })
} else tibble()

vs_opp_h1ast_joined <- build_joined_h1ast_vs_opp()
vs_opp_h1ast <- if (nrow(vs_opp_h1ast_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_h1ast_joined$assister_key), function(key) {
    player_data <- vs_opp_h1ast_joined |> dplyr::filter(assister_key == key)
    if (nrow(player_data) == 0) return(NULL)
    compute_h1ast_conditional(player_data, paste0("vs ", player_data$opponent_abbrev[1]))
  })
} else tibble()

vs_opp_h1reb_joined <- build_joined_h1reb_vs_opp()
vs_opp_h1reb <- if (nrow(vs_opp_h1reb_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_h1reb_joined$rebounder_key), function(key) {
    player_data <- vs_opp_h1reb_joined |> dplyr::filter(rebounder_key == key)
    if (nrow(player_data) == 0) return(NULL)
    compute_h1reb_conditional(player_data, paste0("vs ", player_data$opponent_abbrev[1]))
  })
} else tibble()

if (nrow(vs_opp_h1pts) > 0) h1pts_conditional <- dplyr::bind_rows(h1pts_conditional, vs_opp_h1pts)
if (nrow(vs_opp_h1ast) > 0) h1ast_conditional <- dplyr::bind_rows(h1ast_conditional, vs_opp_h1ast)
if (nrow(vs_opp_h1reb) > 0) h1reb_conditional <- dplyr::bind_rows(h1reb_conditional, vs_opp_h1reb)

# ── Q3 vs Opponent ────────────────────────────────────────────────────────────
build_joined_q3pts_vs_opp <- function() {
  pbp_filtered <- play_play |>
    dplyr::filter(game_type == "Regular Season", game_type != "Play-In",
                  period_number %in% c(1,2,3), !is.na(scorer)) |>
    dplyr::inner_join(next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
                      by = "scorer_key") |>
    dplyr::group_by(scorer, scorer_key, game_date) |>
    dplyr::summarise(q3pts = sum(score_value), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(pts = sum(pts), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(player_filtered,
                     by = c("scorer_key" = "name_key", "game_date" = "dateGame")) |>
    dplyr::left_join(opp_lookup_pts, by = "scorer_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

build_joined_q3ast_vs_opp <- function() {
  pbp_filtered <- play_play |>
    dplyr::filter(game_type == "Regular Season", period_number %in% c(1,2,3),
                  scoring_play == TRUE, !is.na(assister)) |>
    dplyr::inner_join(next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
                      by = "assister_key") |>
    dplyr::group_by(assister, assister_key, game_date) |>
    dplyr::summarise(q3ast = n(), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(ast = sum(ast), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(player_filtered,
                     by = c("assister_key" = "name_key", "game_date" = "dateGame")) |>
    dplyr::left_join(opp_lookup_ast, by = "assister_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

build_joined_q3reb_vs_opp <- function() {
  pbp_filtered <- play_play |>
    dplyr::filter(game_type == "Regular Season", period_number %in% c(1,2,3),
                  !is.na(rebounder), !stringr::str_detect(rebounder, "team")) |>
    dplyr::inner_join(next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
                      by = "rebounder_key") |>
    dplyr::group_by(rebounder, rebounder_key, game_date) |>
    dplyr::summarise(q3reb = n(), .groups = "drop")
  
  player_filtered <- playerdata |>
    dplyr::filter(typeSeason == "Regular Season", yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame, slugOpponent) |>
    dplyr::summarize(reb = sum(treb), .groups = "drop")
  
  pbp_filtered |>
    dplyr::left_join(player_filtered,
                     by = c("rebounder_key" = "name_key", "game_date" = "dateGame")) |>
    dplyr::left_join(opp_lookup_reb, by = "rebounder_key") |>
    dplyr::filter(!is.na(opponent_abbrev), slugOpponent == opponent_abbrev)
}

# Compute and append Q3 vs opponent
vs_opp_q3pts_joined <- build_joined_q3pts_vs_opp()
vs_opp_q3pts <- if (nrow(vs_opp_q3pts_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_q3pts_joined$scorer_key), function(key) {
    player_data <- vs_opp_q3pts_joined |> dplyr::filter(scorer_key == key)
    if (nrow(player_data) == 0) return(NULL)
    compute_q3pts_conditional(player_data, paste0("vs ", player_data$opponent_abbrev[1]))
  })
} else tibble()

vs_opp_q3ast_joined <- build_joined_q3ast_vs_opp()
vs_opp_q3ast <- if (nrow(vs_opp_q3ast_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_q3ast_joined$assister_key), function(key) {
    player_data <- vs_opp_q3ast_joined |> dplyr::filter(assister_key == key)
    if (nrow(player_data) == 0) return(NULL)
    compute_q3ast_conditional(player_data, paste0("vs ", player_data$opponent_abbrev[1]))
  })
} else tibble()

vs_opp_q3reb_joined <- build_joined_q3reb_vs_opp()
vs_opp_q3reb <- if (nrow(vs_opp_q3reb_joined) > 0) {
  purrr::map_dfr(unique(vs_opp_q3reb_joined$rebounder_key), function(key) {
    player_data <- vs_opp_q3reb_joined |> dplyr::filter(rebounder_key == key)
    if (nrow(player_data) == 0) return(NULL)
    compute_q3reb_conditional(player_data, paste0("vs ", player_data$opponent_abbrev[1]))
  })
} else tibble()

if (nrow(vs_opp_q3pts) > 0) q3pts_conditional <- dplyr::bind_rows(q3pts_conditional, vs_opp_q3pts)
if (nrow(vs_opp_q3ast) > 0) q3ast_conditional <- dplyr::bind_rows(q3ast_conditional, vs_opp_q3ast)
if (nrow(vs_opp_q3reb) > 0) q3reb_conditional <- dplyr::bind_rows(q3reb_conditional, vs_opp_q3reb)

cat("Final Type values in pts_conditional:\n")
print(sort(unique(pts_conditional$Type)))


# ── Normalized team lookup using name_key ──────────────────────────────────────

team_logo_lookup <- teams |>
  dplyr::select(slugTeam, urlThumbnailTeam) |>
  dplyr::rename(team = slugTeam, team_logo = urlThumbnailTeam)


roster_normalized <- all_rosters_raw |>
  dplyr::mutate(name_key = normalize_name(PLAYER)) |>
  dplyr::select(name_key, team_abbreviation) |>
  dplyr::rename(team = team_abbreviation) |>
  left_join(team_logo_lookup, by = "team")

# Join pts_conditional via scorer_key instead of scorer
pts_conditional_bucketed <- pts_conditional_bucketed |>
  dplyr::mutate(scorer_key_join = normalize_name(scorer)) |>
  left_join(roster_normalized, by = c("scorer_key_join" = "name_key")) |>
  dplyr::select(-scorer_key_join)

pts_conditional <- pts_conditional |>
  dplyr::mutate(scorer_key_join = normalize_name(scorer)) |>
  left_join(roster_normalized, by = c("scorer_key_join" = "name_key")) |>
  dplyr::select(-scorer_key_join)

# Repeat for ast using assister
ast_conditional_bucketed <- ast_conditional_bucketed |>
  dplyr::mutate(assister_key_join = normalize_name(assister)) |>
  left_join(roster_normalized, by = c("assister_key_join" = "name_key")) |>
  dplyr::select(-assister_key_join)

ast_conditional <- ast_conditional |>
  dplyr::mutate(assister_key_join = normalize_name(assister)) |>
  left_join(roster_normalized, by = c("assister_key_join" = "name_key")) |>
  dplyr::select(-assister_key_join)

# Repeat for reb using rebounder
reb_conditional_bucketed <- reb_conditional_bucketed |>
  dplyr::mutate(rebounder_key_join = normalize_name(rebounder)) |>
  left_join(roster_normalized, by = c("rebounder_key_join" = "name_key")) |>
  dplyr::select(-rebounder_key_join)

reb_conditional <- reb_conditional |>
  dplyr::mutate(rebounder_key_join = normalize_name(rebounder)) |>
  left_join(roster_normalized, by = c("rebounder_key_join" = "name_key")) |>
  dplyr::select(-rebounder_key_join)

# Apply same pattern to all H1 and Q3 tables
for (tbl_name in c(
  "h1pts_conditional_bucketed", "h1pts_conditional",
  "h1ast_conditional_bucketed", "h1ast_conditional",
  "h1reb_conditional_bucketed", "h1reb_conditional",
  "q3pts_conditional_bucketed", "q3pts_conditional",
  "q3ast_conditional_bucketed", "q3ast_conditional",
  "q3reb_conditional_bucketed", "q3reb_conditional"
)) {
  tbl <- get(tbl_name)
  
  # Detect player column
  player_col <- intersect(c("scorer", "assister", "rebounder"), names(tbl))[1]
  join_key   <- paste0(player_col, "_key_join")
  
  tbl <- tbl |>
    dplyr::mutate(!!join_key := normalize_name(.data[[player_col]])) |>
    left_join(roster_normalized, by = setNames("name_key", join_key)) |>
    dplyr::select(-all_of(join_key))
  
  assign(tbl_name, tbl)
}

# ══════════════════════════════════════════════════════════════════════════════
# ACTIONABLE SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

# ── Last game Q1 stats ─────────────────────────────────────────────────────────
# ── Last game Q1 stats — includes playoffs ─────────────────────────────────────
last_game_q1pts <- play_play |>
  filter(
    game_type %in% c("Regular Season", "Playoffs"),
    period_number == 1,
    !is.na(scorer)
  ) |>
  inner_join(
    next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
    by = "scorer_key"
  ) |>
  group_by(scorer, game_date) |>
  summarise(q1pts = sum(score_value), .groups = "drop") |>
  group_by(scorer) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>                          # last 5 games
  summarise(
    last_game_q1pts = round(mean(q1pts), 1),    # average of last 5
    last_game_date  = max(game_date),            # most recent game date
    .groups         = "drop"
  )

# ── Build actionable — AT LEAST threshold logic ────────────────────────────────
# For each player take their last game Q1 pts as the threshold
# Then compute hit rates for all games where they scored >= that threshold
build_actionable_q1pts <- function() {
  q1_all <- play_play |>
    filter(
      game_type     %in% c("Regular Season", "Playoffs"),
      period_number == 1,
      !is.na(scorer)
    ) |>
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    ) |>
    group_by(scorer, scorer_key, game_date) |>
    summarise(fqpts = sum(score_value), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(pts = sum(pts), .groups = "drop"),
      by = c("scorer_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_q1pts$scorer), function(player) {
    
    threshold <- last_game_q1pts |>
      filter(scorer == player) |>
      pull(last_game_q1pts)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    # Use floor so threshold is a whole number
    threshold_int <- floor(threshold)
    
    player_data <- q1_all |>
      filter(scorer == player, fqpts >= threshold_int, !is.na(pts))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:40, function(line) {
      tibble(!!paste0("total_pts_", line) := mean(player_data$pts >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        scorer          = player,
        threshold_label = paste0(threshold_int, "+ Q1 Pts (L5 avg)"),
        last_game_q1pts = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_q1pts |> dplyr::select(scorer, last_game_date),
      by = "scorer"
    )
}

actionable_q1pts <- build_actionable_q1pts()

# ── Last game Q1 stats: AST ─────────────────────────────────────────────────────────
last_game_q1ast <- play_play |>
  filter(
    game_type %in% c("Regular Season", "Playoffs"),
    period_number == 1,
    scoring_play  == TRUE,
    !is.na(assister)
  ) |>
  inner_join(
    next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
    by = "assister_key"
  ) |>
  group_by(assister, game_date) |>
  summarise(q1ast = n(), .groups = "drop") |>
  group_by(assister) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>                          # last 5 games
  summarise(
    last_game_q1ast = round(mean(q1ast), 1),    # average of last 5
    last_game_date  = max(game_date),            # most recent game date
    .groups         = "drop"
  )

build_actionable_q1ast <- function() {
  
  q1_all_ast <- play_play |>
    filter(
      game_type %in% c("Regular Season", "Playoffs"),
      period_number == 1,
      scoring_play  == TRUE,
      !is.na(assister)
    ) |>
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    ) |>
    group_by(assister, assister_key, game_date) |>
    summarise(fqast = n(), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(ast = sum(ast), .groups = "drop"),
      by = c("assister_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_q1ast$assister), function(player) {
    
    threshold <- last_game_q1ast |>
      filter(assister == player) |>
      pull(last_game_q1ast)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    # Use floor so threshold is a whole number
    threshold_int <- floor(threshold)
    
    player_data <- q1_all_ast |>
      filter(assister == player, fqast >= threshold_int, !is.na(ast))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:20, function(line) {
      tibble(!!paste0("total_ast_", line) := mean(player_data$ast >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        assister        = player,
        threshold_label = paste0(threshold, "+ Q1 Ast (L5 avg)"),
        last_game_q1ast = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_q1ast |> dplyr::select(assister, last_game_date),
      by = "assister"
    )
}

actionable_q1ast <- build_actionable_q1ast()

# ── Last game Q1 stats: REB ─────────────────────────────────────────────────────────
last_game_q1reb <- play_play |>
  filter(
    game_type %in% c("Regular Season", "Playoffs"),
    period_number == 1,
    !is.na(rebounder),
    !str_detect(rebounder, "team")
  ) |>
  inner_join(
    next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
    by = "rebounder_key"
  ) |>
  group_by(rebounder, game_date) |>
  summarise(q1reb = n(), .groups = "drop") |>
  group_by(rebounder) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>                          # last 5 games
  summarise(
    last_game_q1reb = round(mean(q1reb), 1),    # average of last 5
    last_game_date  = max(game_date),            # most recent game date
    .groups         = "drop"
  )

build_actionable_q1reb <- function() {
  
  q1_all_reb <- play_play |>
    filter(
      game_type %in% c("Regular Season", "Playoffs"),
      period_number == 1,
      !is.na(rebounder),
      !str_detect(rebounder, "team")
    ) |>
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    ) |>
    group_by(rebounder, rebounder_key, game_date) |>
    summarise(fqreb = n(), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(reb = sum(treb), .groups = "drop"),
      by = c("rebounder_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_q1reb$rebounder), function(player) {
    
    threshold <- last_game_q1reb |>
      filter(rebounder == player) |>
      pull(last_game_q1reb)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    # Use floor so threshold is a whole number
    threshold_int <- floor(threshold)
    
    player_data <- q1_all_reb |>
      filter(rebounder == player, fqreb >= threshold_int, !is.na(reb))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:15, function(line) {
      tibble(!!paste0("total_reb_", line) := mean(player_data$reb >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        rebounder       = player,
        threshold_label = paste0(threshold, "+ Q1 Reb (L5 avg)"),
        last_game_q1reb = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_q1reb |> dplyr::select(rebounder, last_game_date),
      by = "rebounder"
    )
}

actionable_q1reb <- build_actionable_q1reb()


# ── Matrix File Tables ───────────────────────────────

pt_pivoted <- results_combined_pts  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:4,6:44)

ast_pivoted <- results_combined_ast  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:17)

reb_pivoted <- results_combined_treb  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:21)

fg3m_pivoted <- results_combined_fg3m  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:10)

fg3a_pivoted <- results_combined_fg3a  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:13)

stl_pivoted <- results_combined_stl  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:10)

blk_pivoted <- results_combined_blk  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:10)

tov_pivoted <- results_combined_tov  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:10)

ftm_pivoted <- results_combined_ftm  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:10)

fgm_pivoted <- results_combined_fgm  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:15)

ptrebast_pivoted <- results_combined_pts_reb_ast  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:4,13:64)

pt_ast_pivoted <- results_combined_pts_ast  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:4,13:64)

pt_reb_pivoted <- results_combined_pts_reb  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:4,13:64)

ast_reb_pivoted <- results_combined_ast_reb  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:4,8:29)

stl_blk_pivoted <- results_combined_stl_blk  %>% 
  left_join(teams[c("slugTeam","urlThumbnailTeam")], by = "slugTeam") %>% select(!c(metric,idPlayer,slugTeam)) %>%
  rename(Player = namePlayer) %>% relocate(c(Type,urlThumbnailTeam), .after =Player) %>% select(1:11)

firstqpoints_pivoted <- q1_hit_rates_pivoted_combined %>%
  left_join(
    all_rosters_raw[c("PLAYER", "team_abbreviation")] %>%
      rename(scorer = PLAYER, slugTeam = team_abbreviation),
    by = "scorer"
  ) %>%
  left_join(teams[c("slugTeam", "urlThumbnailTeam")], by = "slugTeam") %>%
  select(!c(slugTeam, metric)) %>%
  rename(Player = scorer) %>%
  left_join(q1_pts_games, by = "Player") %>%
  relocate(c(Type, urlThumbnailTeam, n_games), .after = Player) %>%
  select(1:14)

firstqassists_pivoted <- q1_hit_rates_ast_pivoted_combined %>%
  left_join(
    all_rosters_raw[c("PLAYER", "team_abbreviation")] %>%
      rename(assister = PLAYER, slugTeam = team_abbreviation),
    by = "assister"
  ) %>%
  left_join(teams[c("slugTeam", "urlThumbnailTeam")], by = "slugTeam") %>%
  select(!c(slugTeam, metric)) %>%
  rename(Player = assister) %>%
  left_join(q1_ast_games, by = "Player") %>%
  relocate(c(Type, urlThumbnailTeam, n_games), .after = Player) %>%
  select(1:12)

firstqrebounds_pivoted <- q1_hit_rates_reb_pivoted_combined %>%
  left_join(
    all_rosters_raw[c("PLAYER", "team_abbreviation")] %>%
      rename(rebounder = PLAYER, slugTeam = team_abbreviation),
    by = "rebounder"
  ) %>%
  left_join(teams[c("slugTeam", "urlThumbnailTeam")], by = "slugTeam") %>%
  select(!c(slugTeam, metric)) %>%
  rename(Player = rebounder) %>%
  left_join(q1_reb_games, by = "Player") %>%
  relocate(c(Type, urlThumbnailTeam, n_games), .after = Player) %>%
  select(1:11)

# ══════════════════════════════════════════════════════════════════════════════
# FEATURED PLAYERS FOR EDGE CARDS
# ══════════════════════════════════════════════════════════════════════════════

# ── Helper: scrape ESPN for recent injuries ────────────────────────────────────
get_recent_injuries_espn <- function() {
  tryCatch({
    url  <- "https://www.espn.com/nba/injuries"
    page <- rvest::read_html(url)
    
    rows <- page |> rvest::html_nodes(".Table__TR--sm")
    
    if (length(rows) == 0) {
      message("ESPN injury scrape returned no rows — skipping")
      return(character())
    }
    
    injured_df <- purrr::map_dfr(rows, function(row) {
      cols <- row |>
        rvest::html_nodes("td") |>
        rvest::html_text(trim = TRUE)
      if (length(cols) < 4) return(NULL)
      tibble(
        player  = cols[1],
        status  = cols[3],
        comment = cols[4]
      )
    })
    
    injured_df |>
      filter(
        stringr::str_detect(
          status,
          regex("out|injured reserve|suspended|day-to-day", ignore_case = TRUE)
        )
      ) |>
      mutate(player_key = normalize_name(player)) |>
      pull(player_key)
    
  }, error = function(e) {
    message("ESPN injury scrape failed: ", conditionMessage(e))
    character()
  })
}

# ══════════════════════════════════════════════════════════════════════════════
# FEATURED PLAYERS FOR EDGE CARDS
# ══════════════════════════════════════════════════════════════════════════════

# ── Helper: scrape ESPN for recent injuries ────────────────────────────────────
get_recent_injuries_espn <- function() {
  tryCatch({
    url  <- "https://www.espn.com/nba/injuries"
    page <- rvest::read_html(url)
    
    rows <- page |> rvest::html_nodes(".Table__TR--sm")
    
    if (length(rows) == 0) {
      message("ESPN injury scrape returned no rows — skipping")
      return(character())
    }
    
    injured_df <- purrr::map_dfr(rows, function(row) {
      cols <- row |>
        rvest::html_nodes("td") |>
        rvest::html_text(trim = TRUE)
      if (length(cols) < 4) return(NULL)
      tibble(
        player  = cols[1],
        status  = cols[3],
        comment = cols[4]
      )
    })
    
    injured_df |>
      filter(
        stringr::str_detect(
          status,
          regex("out|injured reserve|suspended|day-to-day", ignore_case = TRUE)
        )
      ) |>
      mutate(player_key = normalize_name(player)) |>
      pull(player_key)
    
  }, error = function(e) {
    message("ESPN injury scrape failed: ", conditionMessage(e))
    character()
  })
}

# ── Step 1: Get recently injured players from ESPN ────────────────────────────
espn_injured_keys <- get_recent_injuries_espn()
cat("ESPN injured players found:", length(espn_injured_keys), "\n")

# ── Step 2: Pull headshot URLs from playerdata ────────────────────────────────
headshot_lookup <- playerdata |>
  filter(yearSeason == season_current) |>
  group_by(namePlayer, name_key) |>
  summarise(
    headshot_url = first(urlPlayerHeadshot),
    .groups      = "drop"
  )

# ── Step 3: Build active player pool ──────────────────────────────────────────
recent_cutoff <- Sys.Date() - 7

recent_players_clean <- playerdata |>
  filter(
    typeSeason %in% c("Regular Season", "Playoffs"),
    yearSeason == season_current,
    namePlayer %in% next_team_batch$PLAYER,
    dateGame   >= recent_cutoff
  ) |>
  group_by(namePlayer, name_key) |>
  summarise(
    avg_pts   = mean(pts,     na.rm = TRUE),
    avg_ast   = mean(ast,     na.rm = TRUE),
    avg_reb   = mean(treb,    na.rm = TRUE),
    avg_min   = mean(minutes, na.rm = TRUE),
    last_game = max(dateGame),
    n_recent  = n(),
    .groups   = "drop"
  ) |>
  filter(
    last_game >= recent_cutoff,
    avg_min   >= 20
  ) |>
  filter(!name_key %in% espn_injured_keys) |>
  mutate(
    is_pts_worthy = avg_pts >= 15 & avg_min >= 28,
    is_ast_worthy = avg_ast >= 4  & avg_min >= 28,
    is_reb_worthy = avg_reb >= 6  & avg_min >= 25
  )

cat("Active players after both filters:", nrow(recent_players_clean), "\n")

# ── Step 4: Select top 4 per stat ─────────────────────────────────────────────
featured_pts_players <- recent_players_clean |>
  filter(is_pts_worthy) |>
  arrange(desc(avg_pts)) |>
  slice_head(n = 4) |>
  pull(namePlayer)

featured_ast_players <- recent_players_clean |>
  filter(is_ast_worthy) |>
  arrange(desc(avg_ast)) |>
  slice_head(n = 4) |>
  pull(namePlayer)

featured_reb_players <- recent_players_clean |>
  filter(is_reb_worthy) |>
  arrange(desc(avg_reb)) |>
  slice_head(n = 4) |>
  pull(namePlayer)


# ── Step 5: Filter actionable tables and join headshots ───────────────────────
actionable_q1pts_top <- actionable_q1pts |>
  filter(scorer %in% featured_pts_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(scorer = namePlayer),
    by = "scorer"
  )

actionable_q1ast_top <- actionable_q1ast |>
  filter(assister %in% featured_ast_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(assister = namePlayer),
    by = "assister"
  )

actionable_q1reb_top <- actionable_q1reb |>
  filter(rebounder %in% featured_reb_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(rebounder = namePlayer),
    by = "rebounder"
  )

# ══════════════════════════════════════════════════════════════════════════════
# H1 ACTIONABLE TABLES — Last 5 Average Threshold
# ══════════════════════════════════════════════════════════════════════════════

# ── H1 Points ─────────────────────────────────────────────────────────────────
last_game_h1pts <- play_play |>
  filter(
    game_type     %in% c("Regular Season", "Playoffs"),
    period_number %in% c(1, 2),
    !is.na(scorer)
  ) |>
  inner_join(
    next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
    by = "scorer_key"
  ) |>
  group_by(scorer, game_date) |>
  summarise(h1pts = sum(score_value), .groups = "drop") |>
  group_by(scorer) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>
  summarise(
    last_game_h1pts = round(mean(h1pts), 1),
    last_game_date  = max(game_date),
    .groups         = "drop"
  )

build_actionable_h1pts <- function() {
  h1_all <- play_play |>
    filter(
      game_type     %in% c("Regular Season", "Playoffs"),
      period_number %in% c(1, 2),
      !is.na(scorer)
    ) |>
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    ) |>
    group_by(scorer, scorer_key, game_date) |>
    summarise(h1pts = sum(score_value), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(pts = sum(pts), .groups = "drop"),
      by = c("scorer_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_h1pts$scorer), function(player) {
    threshold <- last_game_h1pts |>
      filter(scorer == player) |>
      pull(last_game_h1pts)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    threshold_int <- floor(threshold)
    
    player_data <- h1_all |>
      filter(scorer == player, h1pts >= threshold_int, !is.na(pts))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:40, function(line) {
      tibble(!!paste0("total_pts_", line) := mean(player_data$pts >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        scorer          = player,
        threshold_label = paste0(threshold_int, "+ H1 Pts (L5 avg)"),
        last_game_h1pts = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_h1pts |> dplyr::select(scorer, last_game_date),
      by = "scorer"
    )
}

actionable_h1pts <- build_actionable_h1pts()

# ── H1 Assists ────────────────────────────────────────────────────────────────
last_game_h1ast <- play_play |>
  filter(
    game_type     %in% c("Regular Season", "Playoffs"),
    period_number %in% c(1, 2),
    scoring_play  == TRUE,
    !is.na(assister)
  ) |>
  inner_join(
    next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
    by = "assister_key"
  ) |>
  group_by(assister, game_date) |>
  summarise(h1ast = n(), .groups = "drop") |>
  group_by(assister) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>
  summarise(
    last_game_h1ast = round(mean(h1ast), 1),
    last_game_date  = max(game_date),
    .groups         = "drop"
  )

build_actionable_h1ast <- function() {
  h1_all_ast <- play_play |>
    filter(
      game_type     %in% c("Regular Season", "Playoffs"),
      period_number %in% c(1, 2),
      scoring_play  == TRUE,
      !is.na(assister)
    ) |>
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    ) |>
    group_by(assister, assister_key, game_date) |>
    summarise(h1ast = n(), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(ast = sum(ast), .groups = "drop"),
      by = c("assister_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_h1ast$assister), function(player) {
    threshold <- last_game_h1ast |>
      filter(assister == player) |>
      pull(last_game_h1ast)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    threshold_int <- floor(threshold)
    
    player_data <- h1_all_ast |>
      filter(assister == player, h1ast >= threshold_int, !is.na(ast))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:15, function(line) {
      tibble(!!paste0("total_ast_", line) := mean(player_data$ast >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        assister        = player,
        threshold_label = paste0(threshold_int, "+ H1 Ast (L5 avg)"),
        last_game_h1ast = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_h1ast |> dplyr::select(assister, last_game_date),
      by = "assister"
    )
}

actionable_h1ast <- build_actionable_h1ast()

# ── H1 Rebounds ───────────────────────────────────────────────────────────────
last_game_h1reb <- play_play |>
  filter(
    game_type     %in% c("Regular Season", "Playoffs"),
    period_number %in% c(1, 2),
    !is.na(rebounder),
    !str_detect(rebounder, "team")
  ) |>
  inner_join(
    next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
    by = "rebounder_key"
  ) |>
  group_by(rebounder, game_date) |>
  summarise(h1reb = n(), .groups = "drop") |>
  group_by(rebounder) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>
  summarise(
    last_game_h1reb = round(mean(h1reb), 1),
    last_game_date  = max(game_date),
    .groups         = "drop"
  )

build_actionable_h1reb <- function() {
  h1_all_reb <- play_play |>
    filter(
      game_type     %in% c("Regular Season", "Playoffs"),
      period_number %in% c(1, 2),
      !is.na(rebounder),
      !str_detect(rebounder, "team")
    ) |>
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    ) |>
    group_by(rebounder, rebounder_key, game_date) |>
    summarise(h1reb = n(), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(reb = sum(treb), .groups = "drop"),
      by = c("rebounder_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_h1reb$rebounder), function(player) {
    threshold <- last_game_h1reb |>
      filter(rebounder == player) |>
      pull(last_game_h1reb)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    threshold_int <- floor(threshold)
    
    player_data <- h1_all_reb |>
      filter(rebounder == player, h1reb >= threshold_int, !is.na(reb))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:15, function(line) {
      tibble(!!paste0("total_reb_", line) := mean(player_data$reb >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        rebounder       = player,
        threshold_label = paste0(threshold_int, "+ H1 Reb (L5 avg)"),
        last_game_h1reb = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_h1reb |> dplyr::select(rebounder, last_game_date),
      by = "rebounder"
    )
}

actionable_h1reb <- build_actionable_h1reb()


# ══════════════════════════════════════════════════════════════════════════════
# Q3 ACTIONABLE TABLES — Last 5 Average Threshold
# ══════════════════════════════════════════════════════════════════════════════

# ── Q3 Points ─────────────────────────────────────────────────────────────────
last_game_q3pts <- play_play |>
  filter(
    game_type     %in% c("Regular Season", "Playoffs"),
    period_number %in% c(1, 2, 3),
    !is.na(scorer)
  ) |>
  inner_join(
    next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
    by = "scorer_key"
  ) |>
  group_by(scorer, game_date) |>
  summarise(q3pts = sum(score_value), .groups = "drop") |>
  group_by(scorer) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>
  summarise(
    last_game_q3pts = round(mean(q3pts), 1),
    last_game_date  = max(game_date),
    .groups         = "drop"
  )

build_actionable_q3pts <- function() {
  q3_all <- play_play |>
    filter(
      game_type     %in% c("Regular Season", "Playoffs"),
      period_number %in% c(1, 2, 3),
      !is.na(scorer)
    ) |>
    inner_join(
      next_game_players |> dplyr::select(scorer_key, scorer_team_abbrev),
      by = "scorer_key"
    ) |>
    group_by(scorer, scorer_key, game_date) |>
    summarise(q3pts = sum(score_value), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(pts = sum(pts), .groups = "drop"),
      by = c("scorer_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_q3pts$scorer), function(player) {
    threshold <- last_game_q3pts |>
      filter(scorer == player) |>
      pull(last_game_q3pts)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    threshold_int <- floor(threshold)
    
    player_data <- q3_all |>
      filter(scorer == player, q3pts >= threshold_int, !is.na(pts))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:40, function(line) {
      tibble(!!paste0("total_pts_", line) := mean(player_data$pts >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        scorer          = player,
        threshold_label = paste0(threshold_int, "+ Q3 Pts (L5 avg)"),
        last_game_q3pts = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_q3pts |> dplyr::select(scorer, last_game_date),
      by = "scorer"
    )
}

actionable_q3pts <- build_actionable_q3pts()

# ── Q3 Assists ────────────────────────────────────────────────────────────────
last_game_q3ast <- play_play |>
  filter(
    game_type     %in% c("Regular Season", "Playoffs"),
    period_number %in% c(1, 2, 3),
    scoring_play  == TRUE,
    !is.na(assister)
  ) |>
  inner_join(
    next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
    by = "assister_key"
  ) |>
  group_by(assister, game_date) |>
  summarise(q3ast = n(), .groups = "drop") |>
  group_by(assister) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>
  summarise(
    last_game_q3ast = round(mean(q3ast), 1),
    last_game_date  = max(game_date),
    .groups         = "drop"
  )

build_actionable_q3ast <- function() {
  q3_all_ast <- play_play |>
    filter(
      game_type     %in% c("Regular Season", "Playoffs"),
      period_number %in% c(1, 2, 3),
      scoring_play  == TRUE,
      !is.na(assister)
    ) |>
    inner_join(
      next_game_players_ast |> dplyr::select(assister_key, team_abbreviation),
      by = "assister_key"
    ) |>
    group_by(assister, assister_key, game_date) |>
    summarise(q3ast = n(), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(ast = sum(ast), .groups = "drop"),
      by = c("assister_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_q3ast$assister), function(player) {
    threshold <- last_game_q3ast |>
      filter(assister == player) |>
      pull(last_game_q3ast)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    threshold_int <- floor(threshold)
    
    player_data <- q3_all_ast |>
      filter(assister == player, q3ast >= threshold_int, !is.na(ast))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:15, function(line) {
      tibble(!!paste0("total_ast_", line) := mean(player_data$ast >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        assister        = player,
        threshold_label = paste0(threshold_int, "+ Q3 Ast (L5 avg)"),
        last_game_q3ast = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_q3ast |> dplyr::select(assister, last_game_date),
      by = "assister"
    )
}

actionable_q3ast <- build_actionable_q3ast()

# ── Q3 Rebounds ───────────────────────────────────────────────────────────────
last_game_q3reb <- play_play |>
  filter(
    game_type     %in% c("Regular Season", "Playoffs"),
    period_number %in% c(1, 2, 3),
    !is.na(rebounder),
    !str_detect(rebounder, "team")
  ) |>
  inner_join(
    next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
    by = "rebounder_key"
  ) |>
  group_by(rebounder, game_date) |>
  summarise(q3reb = n(), .groups = "drop") |>
  group_by(rebounder) |>
  arrange(desc(game_date)) |>
  slice_head(n = 5) |>
  summarise(
    last_game_q3reb = round(mean(q3reb), 1),
    last_game_date  = max(game_date),
    .groups         = "drop"
  )

build_actionable_q3reb <- function() {
  q3_all_reb <- play_play |>
    filter(
      game_type     %in% c("Regular Season", "Playoffs"),
      period_number %in% c(1, 2, 3),
      !is.na(rebounder),
      !str_detect(rebounder, "team")
    ) |>
    inner_join(
      next_game_players_reb |> dplyr::select(rebounder_key, team_abbreviation),
      by = "rebounder_key"
    ) |>
    group_by(rebounder, rebounder_key, game_date) |>
    summarise(q3reb = n(), .groups = "drop") |>
    left_join(
      playerdata |>
        filter(yearSeason == season_current) |>
        group_by(name_key, dateGame) |>
        summarize(reb = sum(treb), .groups = "drop"),
      by = c("rebounder_key" = "name_key", "game_date" = "dateGame")
    )
  
  purrr::map_dfr(unique(last_game_q3reb$rebounder), function(player) {
    threshold <- last_game_q3reb |>
      filter(rebounder == player) |>
      pull(last_game_q3reb)
    
    if (length(threshold) == 0 || is.na(threshold)) return(NULL)
    
    threshold_int <- floor(threshold)
    
    player_data <- q3_all_reb |>
      filter(rebounder == player, q3reb >= threshold_int, !is.na(reb))
    
    if (nrow(player_data) == 0) return(NULL)
    
    n <- nrow(player_data)
    
    hit_rates <- purrr::map_dfc(1:15, function(line) {
      tibble(!!paste0("total_reb_", line) := mean(player_data$reb >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        rebounder       = player,
        threshold_label = paste0(threshold_int, "+ Q3 Reb (L5 avg)"),
        last_game_q3reb = threshold,
        n_games         = n
      ),
      hit_rates
    )
  }) |>
    left_join(
      last_game_q3reb |> dplyr::select(rebounder, last_game_date),
      by = "rebounder"
    )
}

actionable_q3reb <- build_actionable_q3reb()


# ══════════════════════════════════════════════════════════════════════════════
# JOIN HEADSHOTS AND FILTER TO FEATURED PLAYERS
# ══════════════════════════════════════════════════════════════════════════════

# H1
actionable_h1pts_top <- actionable_h1pts |>
  filter(scorer %in% featured_pts_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(scorer = namePlayer),
    by = "scorer"
  )

actionable_h1ast_top <- actionable_h1ast |>
  filter(assister %in% featured_ast_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(assister = namePlayer),
    by = "assister"
  )

actionable_h1reb_top <- actionable_h1reb |>
  filter(rebounder %in% featured_reb_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(rebounder = namePlayer),
    by = "rebounder"
  )

# Q3
actionable_q3pts_top <- actionable_q3pts |>
  filter(scorer %in% featured_pts_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(scorer = namePlayer),
    by = "scorer"
  )

actionable_q3ast_top <- actionable_q3ast |>
  filter(assister %in% featured_ast_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(assister = namePlayer),
    by = "assister"
  )

actionable_q3reb_top <- actionable_q3reb |>
  filter(rebounder %in% featured_reb_players) |>
  left_join(
    headshot_lookup |> dplyr::select(namePlayer, headshot_url) |>
      dplyr::rename(rebounder = namePlayer),
    by = "rebounder"
  )


# ══════════════════════════════════════════════════════════════════════════════
# Player Specific Total Matrix
# ══════════════════════════════════════════════════════════════════════════════

get_player_q1_threshold <- function(player_name,
                                    period_filter   = c(1),
                                    stat_col        = "pts",
                                    thresholds      = NULL,
                                    full_game_lines = NULL) {  # ← remove default here
  
  if (is.null(thresholds)) {
    thresholds <- switch(stat_col,
                         "pts" = 2:20,
                         "ast" = 1:10,
                         "reb" = 1:12
    )
  }
  
  # ── Set full_game_lines per stat if not provided ───────────────────────────
  if (is.null(full_game_lines)) {
    full_game_lines <- switch(stat_col,
                              "pts" = 10:35,       # points — leave as is
                              "ast" = seq(2.5, 20.5, by = 1),   
                              "reb" = seq(2.5, 20.5, by = 1)    
    )
  }
  
  full_stat_col  <- switch(stat_col, "pts" = "pts", "ast" = "ast", "reb" = "treb")
  player_key_val <- normalize_name(player_name)
  
  # ── Filter PBP using normalized key ───────────────────────────────────────
  if (stat_col == "pts") {
    pbp_raw <- play_play |>
      dplyr::filter(
        game_type     %in% c("Regular Season", "Playoffs"),
        period_number %in% period_filter,
        !is.na(scorer_key),
        scorer_key == player_key_val
      ) |>
      dplyr::group_by(scorer, scorer_key, game_date) |>
      dplyr::summarise(period_stat = sum(score_value), .groups = "drop") |>
      dplyr::rename(player = scorer, player_key = scorer_key)
    
  } else if (stat_col == "ast") {
    pbp_raw <- play_play |>
      dplyr::filter(
        game_type     %in% c("Regular Season", "Playoffs"),
        period_number %in% period_filter,
        scoring_play  == TRUE,
        !is.na(assister_key),
        assister_key == player_key_val
      ) |>
      dplyr::group_by(assister, assister_key, game_date) |>
      dplyr::summarise(period_stat = n(), .groups = "drop") |>
      dplyr::rename(player = assister, player_key = assister_key)
    
  } else if (stat_col == "reb") {
    pbp_raw <- play_play |>
      dplyr::filter(
        game_type     %in% c("Regular Season", "Playoffs"),
        period_number %in% period_filter,
        !is.na(rebounder_key),
        !stringr::str_detect(rebounder, "team"),
        rebounder_key == player_key_val
      ) |>
      dplyr::group_by(rebounder, rebounder_key, game_date) |>
      dplyr::summarise(period_stat = n(), .groups = "drop") |>
      dplyr::rename(player = rebounder, player_key = rebounder_key)
  }
  
  if (nrow(pbp_raw) == 0) {
    message("No PBP data found for: ", player_name,
            " (key: ", player_key_val, ")")
    return(tibble())
  }
  
  # ── Join full game stats using name_key ───────────────────────────────────
  full_game <- playerdata |>
    dplyr::filter(yearSeason == season_current) |>
    dplyr::group_by(name_key, dateGame) |>
    dplyr::summarise(
      full_stat = sum(.data[[full_stat_col]], na.rm = TRUE),
      .groups   = "drop"
    )
  
  joined <- pbp_raw |>
    dplyr::left_join(
      full_game,
      by = c("player_key" = "name_key", "game_date" = "dateGame")
    ) |>
    dplyr::filter(!is.na(full_stat))
  
  if (nrow(joined) == 0) {
    message("No matched full game data for: ", player_name,
            " — check name_key alignment")
    return(tibble())
  }
  
  # ── Compute hit rates ──────────────────────────────────────────────────────
  purrr::map_dfr(thresholds, function(thresh) {
    player_data <- joined |> dplyr::filter(period_stat >= thresh)
    n <- nrow(player_data)
    if (n == 0) return(NULL)
    
    hit_rates <- purrr::map_dfc(full_game_lines, function(line) {
      col_label <- if (line == round(line)) line - 0.5 else line
      tibble(!!paste0("o", col_label) :=
               mean(player_data$full_stat >= line, na.rm = TRUE))
    })
    
    bind_cols(
      tibble(
        player    = player_name,
        threshold = paste0(thresh, "+"),
        n_games   = n
      ),
      hit_rates
    )
  })
}





# ── Helper: run get_player_q1_threshold safely with message on failure ─────────
safe_threshold <- function(player_name, ...) {
  tryCatch(
    get_player_q1_threshold(player_name, ...),
    error = function(e) {
      message("Failed for: ", player_name, " — ", conditionMessage(e))
      NULL
    }
  )
}

# Use normalized names from next_team_batch to handle accents
# next_team_batch$PLAYER may have accented names — normalize for lookup
# but pass the original name through so display names stay correct
player_list <- next_team_batch$PLAYER

# ── Q1 ─────────────────────────────────────────────────────────────────────────
all_players_q1_pts <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1), stat_col = "pts")

all_players_q1_ast <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1), stat_col = "ast")

all_players_q1_reb <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1), stat_col = "reb")

# ── H1 ─────────────────────────────────────────────────────────────────────────
all_players_h1_pts <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1,2), stat_col = "pts")

all_players_h1_ast <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1,2), stat_col = "ast")

all_players_h1_reb <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1,2), stat_col = "reb")

# ── Q3 ─────────────────────────────────────────────────────────────────────────
all_players_q3_pts <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1,2,3), stat_col = "pts")

all_players_q3_ast <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1,2,3), stat_col = "ast")

all_players_q3_reb <- purrr::map_dfr(player_list, safe_threshold,
                                     period_filter = c(1,2,3), stat_col = "reb")












# ── Matrix File Output ───────────────────────────────

library(reactablefmtr)


# ══════════════════════════════════════════════════════════════════════════════
# HTML OUTPUT — wrapped in tryCatch for error email
# ══════════════════════════════════════════════════════════════════════════════

# Ensure output folders exist
dir.create("Starter", showWarnings = FALSE)
dir.create("Pro",     showWarnings = FALSE)
dir.create("Sharp",   showWarnings = FALSE)
source("save_self_contained.R", local = FALSE)

tryCatch({
  
  # ── Stats HTML ───────────────────────────────────────────────────────────────
  message("\nBuilding propiq_stats.html...")
  source("build_propiq_stats.R", local = FALSE)
  build_propiq_stats("Starter/propiq_stats.html")
  gc(); gc()
  message("✓ propiq_stats.html complete")
  
  # ── Gameday threshold HTML ────────────────────────────────────────────────────
  message("\nBuilding propiq_gameday.html...")
  source("build_propiq_html.R", local = FALSE)
  build_propiq_page("Pro/propiq_gameday.html")
  gc(); gc()
  message("✓ propiq_gameday.html complete")
  
  # ── Split HTML files ──────────────────────────────────────────────────────────
  message("\nBuilding split HTML files...")
  source("build_propiq_html_5.R", local = FALSE)
  
  matchup_str <- tryCatch({
    n_games <- matchup |>
      dplyr::filter(location == "Home") |>
      dplyr::distinct(Team) |>
      nrow()
    paste0(n_games, " games tonight")
  }, error = function(e) format(Sys.Date(), "%B %d, %Y"))
  
  build_split_page(
    output_file    = "Sharp/propiq_reg_season.html",
    split_filter   = "Regular Season",
    split_label    = "Regular Season",
    split_sublabel = "Full regular season sample",
    matchup_str    = matchup_str
  )
  gc()
  
  build_split_page(
    output_file    = "Sharp/propiq_home.html",
    split_filter   = "Home Games",
    split_label    = "Home Games",
    split_sublabel = "Regular season home games only",
    matchup_str    = matchup_str
  )
  gc()
  
  build_split_page(
    output_file    = "Sharp/propiq_away.html",
    split_filter   = "Away Games",
    split_label    = "Away Games",
    split_sublabel = "Regular season away games only",
    matchup_str    = matchup_str
  )
  gc()
  
  build_split_page(
    output_file    = "Sharp/propiq_playoffs.html",
    split_filter   = "Playoffs",
    split_label    = "Playoffs",
    split_sublabel = "Playoff games only",
    matchup_str    = matchup_str
  )
  gc()
  
  build_split_page(
    output_file    = "Sharp/propiq_last10.html",
    split_filter   = "Last 10",
    split_label    = "Last 10 Games",
    split_sublabel = "Most recent 10 games regardless of game type",
    matchup_str    = matchup_str
  )
  gc()
  
  build_split_page(
    output_file    = "Sharp/propiq_vs_opponent.html",
    vs_opponent    = TRUE,
    vs_type_values = vs_type_values,
    split_label    = "vs Tonight's Opponent",
    split_sublabel = "Regular season games against tonight's specific opponent",
    matchup_str    = matchup_str
  )
  gc(); gc()
  message("✓ All split HTML files complete")
  
  # ── Build index pages ────────────────────────────────────────────────────────
  build_propiq_index <- function(tier, output_file, links, matchup_str = "") {
    game_date <- tryCatch(
      format(as.Date(next_team_batch_date), "%A, %B %d %Y"),
      error = function(e) format(Sys.Date(), "%A, %B %d %Y")
    )
    link_cards <- paste(sapply(links, function(l) {
      glue::glue('
        <a href="{l$url}" target="_blank" style="display:block;background:#1C0F3A;
           border:1px solid #2D1A5A;border-radius:12px;padding:18px 24px;
           text-decoration:none;margin-bottom:12px;"
           onmouseover="this.style.background=\'#2A1650\'"
           onmouseout="this.style.background=\'#1C0F3A\'">
          <div style="font-family:Barlow Condensed,sans-serif;font-size:18px;
                      font-weight:800;color:#FFFFFF;margin-bottom:4px;">{l$label}</div>
          <div style="font-family:JetBrains Mono,monospace;font-size:11px;
                      color:#9B86C0;">{l$description}</div>
        </a>')
    }), collapse = "\n")
    html <- paste0('<!DOCTYPE html><html lang="en"><head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>PropIQ ', tier, ' \u2014 ', game_date, '</title>
      <link href="https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@700;800;900&family=Barlow:wght@400;500&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
      <style>
        *{box-sizing:border-box;margin:0;padding:0;}
        body{background:#0A0518;color:#FFF;font-family:Barlow,sans-serif;
             min-height:100vh;display:flex;flex-direction:column;
             align-items:center;padding:40px 20px;}
        .container{width:100%;max-width:540px;}
        .logo{font-family:Barlow Condensed,sans-serif;font-size:36px;
              font-weight:900;text-align:center;}
        .logo span{color:#F5A623;}
        .badge{display:block;text-align:center;font-family:JetBrains Mono,monospace;
               font-size:11px;color:#F5A623;background:rgba(245,166,35,0.15);
               border:1px solid rgba(245,166,35,0.4);border-radius:4px;
               padding:3px 10px;letter-spacing:0.1em;margin:8px auto 4px;
               width:fit-content;}
        .date{font-family:JetBrains Mono,monospace;font-size:12px;
              color:#6B4DA0;text-align:center;margin-bottom:4px;}
        .matchups{font-size:13px;color:#9B86C0;text-align:center;margin-bottom:28px;}
        .section{font-family:JetBrains Mono,monospace;font-size:10px;
                 letter-spacing:0.15em;color:#6B4DA0;text-transform:uppercase;
                 margin-bottom:12px;}
        .footer{margin-top:28px;font-family:JetBrains Mono,monospace;
                font-size:11px;color:#3D2A6B;text-align:center;}
      </style></head><body><div class="container">
      <div class="logo">Prop<span>IQ</span></div>
      <div class="badge">', toupper(tier), ' TIER</div>
      <div class="date">', game_date, '</div>
      <div class="matchups">', matchup_str, '</div>
      <div class="section">Select a report</div>',
                   link_cards,
                   '<div class="footer">Updated nightly \u00b7 Data through today\'s games</div>
      </div></body></html>')
    writeLines(html, output_file)
    message("  Index page saved: ", output_file)
    invisible(output_file)
  }
  
  base_url <- "https://backironanalytics.github.io/propiq-data"
  
  build_propiq_index(
    tier        = "Sharp",
    output_file = "Sharp/index.html",
    matchup_str = matchup_str,
    links = list(
      list(url="https://backironanalytics.github.io/propiq-data/Sharp/propiq_reg_season.html",
           label="Regular Season",        description="Full season hit rates for tonight's players"),
      list(url="https://backironanalytics.github.io/propiq-data/Sharp/propiq_home.html",
           label="Home Games",            description="Regular season home game splits"),
      list(url="https://backironanalytics.github.io/propiq-data/Sharp/propiq_away.html",
           label="Away Games",            description="Regular season away game splits"),
      list(url="https://backironanalytics.github.io/propiq-data/Sharp/propiq_playoffs.html",
           label="Playoffs",              description="Playoff game splits"),
      list(url="https://backironanalytics.github.io/propiq-data/Sharp/propiq_last10.html",
           label="Last 10 Games",         description="Recent form — last 10 games"),
      list(url="https://backironanalytics.github.io/propiq-data/Sharp/propiq_vs_opponent.html",
           label="vs Tonight's Opponent", description="Head-to-head splits against tonight's matchup")
    )
  )
  
  build_propiq_index(
    tier        = "Pro",
    output_file = "Pro/index.html",
    matchup_str = matchup_str,
    links = list(
      list(url="https://backironanalytics.github.io/propiq-data/Pro/propiq_gameday.html",
           label="Game Day Thresholds",
           description="Q1 / H1 / Q3 threshold analysis for tonight's players")
    )
  )
  
  build_propiq_index(
    tier        = "Starter",
    output_file = "Starter/index.html",
    matchup_str = matchup_str,
    links = list(
      list(url="https://backironanalytics.github.io/propiq-data/Starter/propiq_stats.html",
           label="Hit Rate Stats",
           description="Full game hit rates — Regular Season and Playoffs")
    )
  )
  
  message("✓ Index pages complete")
  
  # ── Success email ─────────────────────────────────────────────────────────────
  duration <- round(difftime(Sys.time(), script_start_time, units = "mins"), 1)
  
  output_files <- c(
    "Starter/propiq_stats.html",
    "Pro/propiq_gameday.html",
    "Sharp/propiq_reg_season.html",
    "Sharp/propiq_home.html",
    "Sharp/propiq_away.html",
    "Sharp/propiq_playoffs.html",
    "Sharp/propiq_last10.html",
    "Sharp/propiq_vs_opponent.html"
  )
  
  file_rows <- paste(
    sapply(output_files, function(f) {
      if (file.exists(f)) {
        size_mb <- round(file.size(f) / 1024 / 1024, 1)
        glue::glue("| {f} | {size_mb} MB \u2705 |")
      } else {
        glue::glue("| {f} | Not found \u274c |")
      }
    }),
    collapse = "\n"
  )
  
  game_date_str <- tryCatch(
    format(as.Date(next_team_batch_date), "%A, %B %d %Y"),
    error = function(e) format(Sys.Date(), "%A, %B %d %Y")
  )
  
  success_email <- blastula::compose_email(
    body = blastula::md(glue::glue(
      "## PropIQ \u2014 Nightly Run Complete \u2705

      | | |
      |---|---|
      | **Game Date** | {game_date_str} |
      | **Started** | {format(script_start_time, '%I:%M %p')} |
      | **Finished** | {format(Sys.time(), '%I:%M %p')} |
      | **Duration** | {duration} minutes |

      ## Output Files

      | File | Size |
      |---|---|
      {file_rows}
      "
    ))
  )
  
  blastula::smtp_send(
    success_email,
    to          = "ccraig2435@gmail.com",
    from        = "ccraig2435@gmail.com",
    subject     = glue::glue("\u2705 PropIQ Ready \u2014 {game_date_str}"),
    credentials = blastula::creds_file("gmail_creds")
  )
  
  message("\u2705 All done. Total time: ", duration, " minutes.")
  
}, error = function(e) {
  
  duration <- round(difftime(Sys.time(), script_start_time, units = "mins"), 1)
  
  error_email <- blastula::compose_email(
    body = blastula::md(glue::glue(
      "## PropIQ \u2014 Nightly Run FAILED \u274c

      | | |
      |---|---|
      | **Date** | {format(Sys.Date(), '%A, %B %d %Y')} |
      | **Started** | {format(script_start_time, '%I:%M %p')} |
      | **Failed after** | {duration} minutes |

      ## Error

      {conditionMessage(e)}
      "
    ))
  )
  
  blastula::smtp_send(
    error_email,
    to          = "ccraig2435@gmail.com",
    from        = "ccraig2435@gmail.com",
    subject     = glue::glue("\u274c PropIQ FAILED \u2014 {format(Sys.Date(), '%b %d %Y')}"),
    credentials = blastula::creds_file("gmail_creds")
  )
  
  message("\u274c Failed after ", duration, " minutes: ", conditionMessage(e))
  stop(e)
  
})

