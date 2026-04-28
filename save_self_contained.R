# ══════════════════════════════════════════════════════════════════════════════
# PropIQ — Self-Contained HTML Save Helper v3
# Uses a fixed local staging folder — more reliable on Windows
# Usage: save_self_contained(page, "output/path/file.html")
# ══════════════════════════════════════════════════════════════════════════════

save_self_contained <- function(page, output_file) {
  
  # ── Step 1: Save to a fixed staging location in working directory ─────────
  # Using a fixed path is more reliable than tempdir() on Windows
  staging_dir  <- file.path(getwd(), "_propiq_staging")
  staging_file <- file.path(staging_dir, "staged.html")
  staging_libs <- file.path(staging_dir, "libs")
  
  dir.create(staging_dir, showWarnings = FALSE, recursive = TRUE)
  
  htmltools::save_html(page, staging_file, libdir = "libs")
  
  # ── Step 2: Read the staged HTML ─────────────────────────────────────────
  if (!file.exists(staging_file)) {
    stop("save_self_contained: staged file not created at ", staging_file)
  }
  
  html_text <- paste(
    readLines(staging_file, warn = FALSE, encoding = "UTF-8"),
    collapse = "\n"
  )
  
  message("  Inlining ", sum(grepl("src=|href=", strsplit(html_text, "\n")[[1]])),
          " external dependencies...")
  
  # ── Step 3: Inline all <script src="libs/..."> tags ──────────────────────
  # Extract all script src values
  script_srcs <- regmatches(
    html_text,
    gregexpr('<script src="[^"]*"[^>]*></script>', html_text, perl = TRUE)
  )[[1]]
  
  for (tag in script_srcs) {
    src <- gsub('.*src="([^"]+)".*', "\\1", tag)
    
    # Skip external URLs
    if (grepl("^http", src)) next
    
    full_path <- file.path(staging_dir, src)
    
    if (!file.exists(full_path)) {
      message("    Skipping missing file: ", full_path)
      next
    }
    
    js  <- paste(readLines(full_path, warn = FALSE, encoding = "UTF-8"),
                 collapse = "\n")
    new_tag <- paste0("<script>", js, "</script>")
    html_text <- gsub(tag, new_tag, html_text, fixed = TRUE)
    message("    Inlined: ", src)
  }
  
  # ── Step 4: Inline all <link href="libs/..." rel="stylesheet"> tags ───────
  link_tags <- regmatches(
    html_text,
    gregexpr('<link href="[^"]*"[^>]*/>', html_text, perl = TRUE)
  )[[1]]
  
  for (tag in link_tags) {
    # Only process stylesheet links
    if (!grepl('rel="stylesheet"', tag)) next
    
    href <- gsub('.*href="([^"]+)".*', "\\1", tag)
    
    if (grepl("^http", href)) next
    
    full_path <- file.path(staging_dir, href)
    
    if (!file.exists(full_path)) {
      message("    Skipping missing file: ", full_path)
      next
    }
    
    css <- paste(readLines(full_path, warn = FALSE, encoding = "UTF-8"),
                 collapse = "\n")
    new_tag   <- paste0("<style>", css, "</style>")
    html_text <- gsub(tag, new_tag, html_text, fixed = TRUE)
    message("    Inlined: ", href)
  }
  
  # ── Step 5: Remove remaining external src/href references check ───────────
  remaining <- length(grep('src="libs/', html_text)) +
    length(grep('href="libs/', html_text))
  if (remaining > 0) {
    message("  Warning: ", remaining, " external references remain")
  } else {
    message("  All dependencies inlined successfully")
  }
  
  # ── Step 6: Remove htmltools white background injection ───────────────────
  html_text <- gsub(
    "<style>body\\{background-color:white;\\}</style>",
    "", html_text, fixed = TRUE
  )
  
  # ── Step 7: Write final self-contained output file ────────────────────────
  out_dir <- dirname(output_file)
  if (nchar(out_dir) > 0 && out_dir != "." && !dir.exists(out_dir)) {
    dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  writeLines(html_text, output_file, useBytes = TRUE)
  
  # ── Step 8: Clean up staging folder ──────────────────────────────────────
  unlink(staging_dir, recursive = TRUE)
  
  # ── Report ────────────────────────────────────────────────────────────────
  size_mb <- round(file.size(output_file) / 1024 / 1024, 1)
  message("  Saved self-contained: ", output_file, " (", size_mb, " MB)")
  
  # Verify no external refs remain
  final_check <- readLines(output_file, warn = FALSE, encoding = "UTF-8")
  ext_refs <- sum(grepl('src="libs/|href="libs/', final_check))
  if (ext_refs > 0) {
    message("  WARNING: ", ext_refs, " external references still in output file!")
  } else {
    message("  Verified: no external references — file is self-contained")
  }
  
  invisible(output_file)
}
