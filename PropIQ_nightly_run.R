# ══════════════════════════════════════════════════════════════════════════════
# PropIQ — Master Nightly Run
# Runs in order:
#   1. Compiled_daily_run.R  (data pipeline)
#   2. build_propiq_stats.R  (hit rate HTML)
#   3. build_propiq_html.R   (gameday threshold HTML)
#   4. build_propiq_html_5.R (split HTML files x6)
#
# Sends start email immediately, success/failure email when complete
# ══════════════════════════════════════════════════════════════════════════════

# ── Set working directory to script location ───────────────────────────────────
# This ensures all relative file paths work correctly when run by Task Scheduler
setwd("C:/Users/ccrai/OneDrive/Documents/DDS")

# ── Record start time ──────────────────────────────────────────────────────────
script_start_time <- Sys.time()

# ── Load email library early so start email can fire ──────────────────────────
library(blastula)
library(glue)

# ── Send start email ───────────────────────────────────────────────────────────
tryCatch({
  start_email <- blastula::compose_email(
    body = blastula::md(glue::glue("
      <h2 style='color:#2e6d9e; font-family:sans-serif;'>
        PropIQ — Nightly Run Started
      </h2>
      <table style='border-collapse:collapse; font-family:sans-serif; font-size:14px;'>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Date</b></td>
          <td style='padding:8px 0;'>{format(Sys.Date(), '%A, %B %d %Y')}</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Started</b></td>
          <td style='padding:8px 0;'>{format(script_start_time, '%I:%M %p')}</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Pipeline</b></td>
          <td style='padding:8px 0;'>
            Data → Stats HTML → Gameday HTML → Split HTMLs
          </td>
        </tr>
      </table>
    "))
  )
  blastula::smtp_send(
    start_email,
    to          = "ccraig2435@gmail.com",
    from        = "ccraig2435@gmail.com",
    subject     = glue::glue("🏀 PropIQ Starting — {format(Sys.Date(), '%b %d %Y')}"),
    credentials = blastula::creds_file("gmail_creds")
  )
  message("Start email sent.")
}, error = function(e) {
  message("Start email failed (non-fatal): ", conditionMessage(e))
})

# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE — wrapped in tryCatch for error email
# ══════════════════════════════════════════════════════════════════════════════
tryCatch({

  # ── Step 1: Data pipeline ──────────────────────────────────────────────────
  message("\n========================================")
  message("STEP 1: Running Compiled_daily_run.R...")
  message("========================================")
  
  # Run in its own environment to prevent on.exit conflicts
  data_env <- new.env(parent = globalenv())
  source("Compiled_daily_run.R", local = data_env)
  
  for (obj_name in ls(data_env)) {
    assign(obj_name, get(obj_name, envir = data_env), envir = globalenv())
  }
  rm(data_env)

  gc(); gc()
  message("✓ Compiled_daily_run.R complete")

  # ── Step 2: Stats HTML ────────────────────────────────────────────────────
  message("\n========================================")
  message("STEP 2: Building propiq_stats.html...")
  message("========================================")

  source("build_propiq_stats.R", local = FALSE)
  gc(); gc()
  message("✓ propiq_stats.html complete")

  # ── Step 3: Gameday threshold HTML ────────────────────────────────────────
  message("\n========================================")
  message("STEP 3: Building propiq_gameday.html...")
  message("========================================")

  source("build_propiq_html.R", local = FALSE)
  gc(); gc()
  message("✓ propiq_gameday.html complete")

  # ── Step 4: Split HTML files ──────────────────────────────────────────────
  message("\n========================================")
  message("STEP 4: Building split HTML files...")
  message("========================================")

  source("build_propiq_html_5.R", local = FALSE)
  gc(); gc()
  message("✓ Split HTML files complete")

  # ── Build output summary ──────────────────────────────────────────────────
  duration <- round(difftime(Sys.time(), script_start_time, units = "mins"), 1)

  output_files <- c(
    "propiq_stats.html",
    "propiq_gameday.html",
    "propiq_reg_season.html",
    "propiq_home.html",
    "propiq_away.html",
    "propiq_playoffs.html",
    "propiq_last10.html",
    "propiq_vs_opponent.html"
  )

  file_rows <- paste(
    sapply(output_files, function(f) {
      if (file.exists(f)) {
        size_mb <- round(file.size(f) / 1024 / 1024, 1)
        glue::glue(
          "<tr>
             <td style='padding:6px 16px 6px 0; color:#666;'>{f}</td>
             <td style='padding:6px 0; color:#429460;'>{size_mb} MB ✅</td>
           </tr>"
        )
      } else {
        glue::glue(
          "<tr>
             <td style='padding:6px 16px 6px 0; color:#666;'>{f}</td>
             <td style='padding:6px 0; color:#c0392b;'>Not found ❌</td>
           </tr>"
        )
      }
    }),
    collapse = "\n"
  )

  matchup_summary <- tryCatch({
    n_games <- matchup |>
      dplyr::filter(location == "Home") |>
      dplyr::distinct(Team) |>
      nrow()
    game_date_str <- format(as.Date(next_team_batch_date), "%A, %B %d %Y")
    paste0(n_games, " games \u00b7 ", game_date_str)
  }, error = function(e) format(Sys.Date(), "%A, %B %d %Y"))

  # ── Success email ─────────────────────────────────────────────────────────
  success_email <- blastula::compose_email(
    body = blastula::md(glue::glue("
      <h2 style='color:#2e6d9e; font-family:sans-serif;'>
        PropIQ — Nightly Run Complete ✅
      </h2>

      <table style='border-collapse:collapse; font-family:sans-serif; font-size:14px;'>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Game Date</b></td>
          <td style='padding:8px 0;'>{matchup_summary}</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Started</b></td>
          <td style='padding:8px 0;'>{format(script_start_time, '%I:%M %p')}</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Finished</b></td>
          <td style='padding:8px 0;'>{format(Sys.time(), '%I:%M %p')}</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Duration</b></td>
          <td style='padding:8px 0;'>{duration} minutes</td>
        </tr>
      </table>

      <br>
      <h3 style='font-family:sans-serif; color:#444; font-size:14px;'>Output Files</h3>
      <table style='border-collapse:collapse; font-family:sans-serif; font-size:13px;'>
        {file_rows}
      </table>
    "))
  )

  blastula::smtp_send(
    success_email,
    to          = "ccraig2435@gmail.com",
    from        = "ccraig2435@gmail.com",
    subject     = glue::glue("✅ PropIQ Ready — {format(as.Date(next_team_batch_date), '%b %d %Y')}"),
    credentials = blastula::creds_file("gmail_creds")
  )

  message("\n✅ All done. Total time: ", duration, " minutes.")

}, error = function(e) {

  duration <- round(difftime(Sys.time(), script_start_time, units = "mins"), 1)

  error_email <- blastula::compose_email(
    body = blastula::md(glue::glue("
      <h2 style='color:#c0392b; font-family:sans-serif;'>
        PropIQ — Nightly Run FAILED ❌
      </h2>

      <table style='border-collapse:collapse; font-family:sans-serif; font-size:14px;'>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Date</b></td>
          <td style='padding:8px 0;'>{format(Sys.Date(), '%A, %B %d %Y')}</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Started</b></td>
          <td style='padding:8px 0;'>{format(script_start_time, '%I:%M %p')}</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666;'><b>Failed after</b></td>
          <td style='padding:8px 0;'>{duration} minutes</td>
        </tr>
        <tr>
          <td style='padding:8px 16px 8px 0; color:#666; vertical-align:top;'>
            <b>Error</b>
          </td>
          <td style='padding:8px 0; color:#c0392b;'>
            <pre style='font-size:12px; white-space:pre-wrap;'>{conditionMessage(e)}</pre>
          </td>
        </tr>
      </table>
    "))
  )

  blastula::smtp_send(
    error_email,
    to          = "ccraig2435@gmail.com",
    from        = "ccraig2435@gmail.com",
    subject     = glue::glue("❌ PropIQ FAILED — {format(Sys.Date(), '%b %d %Y')}"),
    credentials = blastula::creds_file("gmail_creds")
  )

  message("❌ Pipeline failed after ", duration, " minutes.")
  message("Error: ", conditionMessage(e))
  stop(e)
})
