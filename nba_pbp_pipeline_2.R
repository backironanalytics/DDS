# ============================================================
# NBA Play-by-Play Daily Refresh Pipeline
# Uses: hoopR (ESPN/NBA Stats API wrapper)
# Storage: Parquet files via arrow
# Parallel: furrr + future (multisession workers)
# ============================================================

library(hoopR)
library(dplyr)
library(purrr)
library(furrr)       # parallel map — drop-in for purrr
library(future)      # backend for furrr
library(arrow)
library(lubridate)
library(glue)
library(logger)
library(progressr)

library(httr)
library(jsonlite)

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
    attempt <- attempt + 1
    schedule <- tryCatch({
      s <- hoopR::load_nba_schedule(seasons = season)
      
      # Validate the response actually has the column we need
      if (!"status_type_completed" %in% names(s)) {
        log_warn("Schedule response missing status_type_completed — attempt {attempt}")
        Sys.sleep(10 * attempt)   # wait longer each retry
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
  
  # ── Fall back to cached parquet files if all retries fail ─────────────────
  if (is.null(schedule)) {
    log_warn("Schedule unavailable after {max_retries} attempts — using cached game IDs")
    
    existing_files <- list.files(DATA_DIR, pattern = "\\.parquet$", full.names = FALSE)
    existing_ids   <- sub("\\.parquet$", "", existing_files)
    
    if (length(existing_ids) == 0) {
      stop("No schedule data available and no cached parquet files to fall back on")
    }
    
    # Return empty data frame so games_needing_update returns nothing to fetch
    # Pipeline will skip fetching and just use what's already cached
    log_info("Returning empty schedule — no new games will be fetched today")
    return(data.frame(
      game_id           = integer(),
      game_date         = as.Date(character()),
      home_abbreviation = character(),
      away_abbreviation = character(),
      season_type       = integer()
    ))
  }
  
  completed <- schedule |>
    dplyr::filter(status_type_completed == TRUE) |>
    dplyr::select(game_id, game_date, home_abbreviation,
                  away_abbreviation, season_type)
  
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
    select(game_id, game_date, home_abbreviation, away_abbreviation, season_type)
  
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
  meta <- meta_lookup |> filter(game_id == gid)
  pbp  <- pbp_data |>
    mutate(
      game_id          = gid,
      game_date        = meta$game_date[[1]],
      home_team_abbrev = meta$home_abbreviation[[1]],
      away_team_abbrev = meta$away_abbreviation[[1]],
      season_type      = meta$season_type[[1]],
      fetched_at       = Sys.time(),
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
  out_path <- file.path(DATA_DIR, glue("{gid}.parquet"))
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
