# ============================================================
# NBA PBP — Daily Refresh Scheduler
# Supports: cronR (Linux/Mac) or manual Windows Task Scheduler
# Run this script ONCE to register the cron job.
# ============================================================

# ── Option A: cronR (Linux / Mac) ─────────────────────────────────────────────
# if (requireNamespace("cronR", quietly = TRUE)) {
#   library(cronR)
#   
#   PIPELINE_SCRIPT <- normalizePath("nba_pbp_pipeline.R")  # full path
#   RSCRIPT_PATH    <- normalizePath(file.path(R.home("bin"), "Rscript"))
#   
#   cmd <- cron_rscript(
#     rscript      = PIPELINE_SCRIPT,
#     rscript_bin  = RSCRIPT_PATH,
#     log_append   = TRUE,
#     log_file     = normalizePath("logs/cron_refresh.log", mustWork = FALSE)
#   )
#   
#   # Run daily at 6:00 AM — NBA games usually finish by ~1–2 AM ET
#   cron_add(
#     command   = cmd,
#     frequency = "daily",
#     at        = "06:00",
#     id        = "nba_pbp_daily",
#     description = "NBA PBP daily refresh"
#   )
#   
#   cron_ls()   # verify it's registered
#   message("✅ cronR job registered — runs daily at 06:00")
#   
# } else {
#   message("cronR not installed. Run: install.packages('cronR')")
#   message("")
#   message("── Windows Task Scheduler alternative ────────────────────────────")
#   message("1. Open Task Scheduler → Create Basic Task")
#   message("2. Trigger: Daily at 6:00 AM")
#   message("3. Action: Start a Program")
#   message("   Program: ", normalizePath(file.path(R.home("bin"), "Rscript.exe")))
#   message("   Arguments: \"", normalizePath("nba_pbp_pipeline.R"), "\"")
#   message("   Start in: ", getwd())
# }


# ── Option B: taskscheduleR (Windows) ─────────────────────────────────────────
# Uncomment the block below if on Windows and taskscheduleR is installed:

library(taskscheduleR)
taskscheduler_create(
  taskname   = "NBA_PBP_Daily_Refresh",
  rscript    = normalizePath("nba_pbp_pipeline_2.R"),
  schedule   = "DAILY",
  starttime  = "17:34",
  startdate  = format(Sys.Date(), "%m/%d/%Y")
)
message("✅ Windows Task Scheduler job registered")
