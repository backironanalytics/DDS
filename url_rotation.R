# ══════════════════════════════════════════════════════════════════════════════
# URL ROTATION MODULE — PropBetIQ
# Per-tier HTML slugs + shared per-sport JSON slug
# ══════════════════════════════════════════════════════════════════════════════
#
# State structure:
#   nfl_basic:   {slug, created}  → NFL/index-basic-<slug>.html
#   nfl_edge:    {slug, created}  → NFL/index-edge-<slug>.html
#   nfl_json:    {slug, created}  → NFL/propbetiq_nfl-<slug>.json (shared)
#   nba_starter: {slug, created}  → NBA/index-starter-<slug>.html
#   nba_edge:    {slug, created}  → NBA/index-edge-<slug>.html
#   nba_json:    {slug, created}  → NBA/propbetiq_nba-<slug>.json (shared)
#
# All 6 slugs rotate together on the 1st of month (or if any is >30 days old).
# Each tier has its own unique HTML slug — Basic sub can't guess Edge slug.
# JSON slug is shared per sport since data is identical across tiers.
#
# USAGE:
#   source("C:/Users/ccrai/OneDrive/Documents/DDS/url_rotation.R")
#   slugs <- get_current_slugs(sport = "nfl")
#   # slugs$html_basic, slugs$html_edge, slugs$json — all include current slugs
#
# ══════════════════════════════════════════════════════════════════════════════

library(jsonlite)

SLUGS_FILE <- Sys.getenv("PROPBETIQ_SLUGS_FILE", "C:/Users/ccrai/OneDrive/Documents/DDS/.current_slugs.json")

SPORT_TIERS <- list(
  nfl = c("nfl_basic", "nfl_edge", "nfl_json"),
  nba = c("nba_starter", "nba_edge", "nba_json")
)

generate_slug <- function() {
  paste0(sample(c(letters, 0:9), 8, replace = TRUE), collapse = "")
}

load_slug_state <- function() {
  if (!file.exists(SLUGS_FILE)) return(list())
  fromJSON(SLUGS_FILE, simplifyVector = FALSE)
}

save_slug_state <- function(state) {
  write_json(state, SLUGS_FILE, auto_unbox = TRUE, pretty = TRUE)
}

needs_rotation_for_sport <- function(state, sport) {
  tiers <- SPORT_TIERS[[sport]]
  
  for (t in tiers) {
    if (is.null(state[[t]]) || is.null(state[[t]]$slug) || is.null(state[[t]]$created)) {
      return(TRUE)
    }
  }
  
  created_str <- as.character(unlist(state[[tiers[1]]]$created))[1]
  if (is.na(created_str) || nchar(created_str) == 0) return(TRUE)
  
  created_date <- tryCatch(as.Date(created_str), error = function(e) NA)
  if (is.na(created_date)) return(TRUE)
  
  age_days <- as.integer(Sys.Date() - created_date)
  if (age_days > 30) return(TRUE)
  
  today_day <- format(Sys.Date(), "%d")
  created_month <- format(created_date, "%Y-%m")
  today_month <- format(Sys.Date(), "%Y-%m")
  
  if (today_day == "01" && created_month != today_month) return(TRUE)
  
  return(FALSE)
}

get_current_slugs <- function(sport = c("nfl", "nba")) {
  sport <- match.arg(sport)
  state <- load_slug_state()
  tiers <- SPORT_TIERS[[sport]]
  
  if (needs_rotation_for_sport(state, sport)) {
    today <- as.character(Sys.Date())
    for (t in tiers) {
      state[[t]] <- list(slug = generate_slug(), created = today)
    }
    save_slug_state(state)
    
    message("")
    message("═══════════════════════════════════════════════════════════════")
    message("  URL ROTATION — ", toupper(sport), " — ", Sys.Date())
    message("═══════════════════════════════════════════════════════════════")
    for (t in tiers) {
      message("  ", t, " slug: ", state[[t]]$slug)
    }
    message("  Update Patreon pinned posts with the URLs printed below.")
    message("═══════════════════════════════════════════════════════════════")
    message("")
  }
  
  if (sport == "nfl") {
    basic_slug <- as.character(unlist(state$nfl_basic$slug))[1]
    edge_slug  <- as.character(unlist(state$nfl_edge$slug))[1]
    json_slug  <- as.character(unlist(state$nfl_json$slug))[1]
    return(list(
      basic_slug = basic_slug,
      edge_slug  = edge_slug,
      json_slug  = json_slug,
      html_basic = paste0("index-basic-", basic_slug, ".html"),
      html_edge  = paste0("index-edge-",  edge_slug,  ".html"),
      json       = paste0("propbetiq_nfl-", json_slug, ".json"),
      json_local = "propbetiq_nfl.json"
    ))
  } else {
    starter_slug <- as.character(unlist(state$nba_starter$slug))[1]
    edge_slug    <- as.character(unlist(state$nba_edge$slug))[1]
    json_slug    <- as.character(unlist(state$nba_json$slug))[1]
    return(list(
      starter_slug = starter_slug,
      edge_slug    = edge_slug,
      json_slug    = json_slug,
      html_starter = paste0("index-starter-", starter_slug, ".html"),
      html_edge    = paste0("index-edge-",    edge_slug,    ".html"),
      json         = paste0("propbetiq_nba-", json_slug, ".json"),
      json_local   = "propbetiq_nba.json"
    ))
  }
}

rewrite_html_json_ref <- function(html_local_path, old_json_name, new_json_name) {
  if (!file.exists(html_local_path)) return(invisible(NULL))
  content <- readLines(html_local_path, warn = FALSE, encoding = "UTF-8")
  content <- gsub(
    paste0("'./", old_json_name, "'"),
    paste0("'./", new_json_name, "'"),
    content, fixed = TRUE
  )
  tmp_path <- tempfile(fileext = ".html")
  writeLines(content, tmp_path, useBytes = TRUE)
  return(tmp_path)
}

# Deletes any slugged file on GitHub that doesn't match one of the current slugs
# SAFETY: Refuses to delete if it can't find AT LEAST ONE current-slug file
# (protects against a scenario where slug extraction fails and everything looks orphan)
cleanup_old_slugs <- function(sport = c("nfl", "nba"),
                              github_user = "backironanalytics",
                              github_repo = "DDS",
                              github_token,
                              current_slugs,
                              dry_run = FALSE) {
  sport <- match.arg(sport)
  folder <- toupper(sport)
  
  headers <- httr::add_headers(
    Authorization = paste("token", github_token),
    Accept        = "application/vnd.github.v3+json"
  )
  
  list_url <- paste0("https://api.github.com/repos/", github_user, "/",
                     github_repo, "/contents/", folder)
  resp <- httr::GET(list_url, headers)
  
  if (httr::status_code(resp) != 200) {
    message("  ⚠ Could not list ", folder, "/ folder on GitHub for cleanup — skipping")
    return(invisible(NULL))
  }
  
  files <- httr::content(resp)
  
  # Build safe_slugs list defensively
  safe_slugs <- unique(as.character(unlist(current_slugs)))
  safe_slugs <- safe_slugs[nchar(safe_slugs) == 8 & !is.na(safe_slugs)]
  
  if (length(safe_slugs) == 0) {
    message("  ⚠ SAFETY ABORT: current_slugs list is empty. Refusing to delete anything.")
    return(invisible(NULL))
  }
  
  message("  Current safe slugs for ", folder, ": ", paste(safe_slugs, collapse = ", "))
  
  # First pass: categorize every file
  files_to_delete <- list()
  files_to_keep <- list()
  
  for (f in files) {
    fname <- f$name
    
    # Only consider files matching our slugged pattern
    if (!grepl("-[a-z0-9]{8}\\.(html|json)$", fname)) {
      message("  Skipping (not a slugged file): ", fname)
      next
    }
    
    # Extract slug: find last "-XXXXXXXX." before extension
    slug_extract <- sub(".*-([a-z0-9]{8})\\.(html|json)$", "\\1", fname)
    
    if (nchar(slug_extract) != 8 || slug_extract == fname) {
      message("  Skipping (slug extraction failed): ", fname)
      next
    }
    
    if (slug_extract %in% safe_slugs) {
      files_to_keep[[length(files_to_keep) + 1]] <- list(name = fname, slug = slug_extract)
    } else {
      files_to_delete[[length(files_to_delete) + 1]] <- list(name = fname, sha = f$sha, slug = slug_extract)
    }
  }
  
  message("  Files matching current slugs (KEEP): ", length(files_to_keep))
  for (kept in files_to_keep) {
    message("    ✓ ", kept$name)
  }
  message("  Files not matching current slugs (DELETE): ", length(files_to_delete))
  for (del in files_to_delete) {
    message("    ✗ ", del$name, " (slug ", del$slug, " not in current)")
  }
  
  # SAFETY CHECK: If we're about to delete files but NOT keep any, abort.
  # This would indicate the safe_slugs list doesn't match anything, which means
  # the state and GitHub are out of sync — a normal cleanup should always keep
  # the 3 files we just pushed.
  if (length(files_to_delete) > 0 && length(files_to_keep) == 0) {
    message("  ⚠⚠⚠ SAFETY ABORT: About to delete files but current slugs match NOTHING on GitHub.")
    message("  This means slug state is out of sync with deployed files. Not deleting anything.")
    message("  If this is expected (e.g. fresh push), rerun the push script to upload current slugs first.")
    return(invisible(NULL))
  }
  
  if (dry_run) {
    message("  DRY RUN MODE — no files were actually deleted.")
    return(invisible(NULL))
  }
  
  # Actually delete
  deleted_count <- 0
  for (del in files_to_delete) {
    delete_url <- paste0("https://api.github.com/repos/", github_user, "/",
                         github_repo, "/contents/", folder, "/", del$name)
    body <- list(
      message = paste0("Rotation cleanup: remove orphan ", del$name),
      sha     = del$sha
    )
    del_resp <- httr::DELETE(
      url    = delete_url,
      config = headers,
      body   = jsonlite::toJSON(body, auto_unbox = TRUE),
      httr::content_type_json()
    )
    if (httr::status_code(del_resp) %in% c(200, 204)) {
      message("  ✓ Deleted orphan: ", del$name)
      deleted_count <- deleted_count + 1
    } else {
      message("  ✗ Failed to delete ", del$name, " (status ", httr::status_code(del_resp), ")")
    }
    Sys.sleep(0.3)
  }
  
  if (deleted_count > 0) {
    message("  Cleanup complete — removed ", deleted_count, " orphan file(s)")
  } else {
    message("  No orphan files to clean up")
  }
}

print_current_urls <- function() {
  state <- load_slug_state()
  
  cat("\n")
  cat("═══════════════════════════════════════════════════════════════\n")
  cat("  CURRENT DASHBOARD URLS — UPDATE PATREON PINNED POSTS\n")
  cat("═══════════════════════════════════════════════════════════════\n")
  
  if (!is.null(state$nfl_basic$slug)) {
    basic  <- as.character(unlist(state$nfl_basic$slug))[1]
    edge   <- as.character(unlist(state$nfl_edge$slug))[1]
    created <- as.character(unlist(state$nfl_basic$created))[1]
    cat("\n  NFL (created ", created, ")\n", sep = "")
    cat("    Basic: https://backironanalytics.github.io/DDS/NFL/index-basic-", basic, ".html\n", sep = "")
    cat("    Edge:  https://backironanalytics.github.io/DDS/NFL/index-edge-",  edge,  ".html\n", sep = "")
  }
  
  if (!is.null(state$nba_starter$slug)) {
    starter <- as.character(unlist(state$nba_starter$slug))[1]
    edge    <- as.character(unlist(state$nba_edge$slug))[1]
    created <- as.character(unlist(state$nba_starter$created))[1]
    cat("\n  NBA (created ", created, ")\n", sep = "")
    cat("    Starter: https://backironanalytics.github.io/DDS/NBA/index-starter-", starter, ".html\n", sep = "")
    cat("    Edge:    https://backironanalytics.github.io/DDS/NBA/index-edge-",    edge,    ".html\n", sep = "")
  }
  
  cat("\n═══════════════════════════════════════════════════════════════\n\n")
}
