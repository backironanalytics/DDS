# ══════════════════════════════════════════════════════════════════════════════
# PropIQ — HTML Dashboard Builder (Multi-Split Version)
# Produces 6 separate HTML files, one per split
# No pandoc. No flexdashboard. No RMarkdown.
#
# Requires: pts_conditional, ast_conditional, reb_conditional,
#           h1pts_conditional, h1ast_conditional, h1reb_conditional,
#           q3pts_conditional, q3ast_conditional, q3reb_conditional
#           matchup, Sys.Date()
# ══════════════════════════════════════════════════════════════════════════════

library(htmltools)
library(reactable)
library(scales)
library(dplyr)
library(stringr)

# ── Diagnostic: print available Type values ────────────────────────────────────
message("Available Type values in pts_conditional:")
print(sort(unique(pts_conditional$Type)))

# ── Color theme ────────────────────────────────────────────────────────────────
color_set <- c("red2", "#f7c844", "#1a9e5c")

propiq_theme <- reactableTheme(
  color           = "#1A0F3A",
  backgroundColor = "#FFFFFF",
  borderColor     = "#C9BEE8",
  stripedColor    = "#EDE8F7",
  highlightColor  = "#E2DAF5",
  cellPadding     = "8px 12px",
  headerStyle = list(
    background    = "#EDE8F7",
    color         = "#7B6B9E",
    fontSize      = "11px",
    fontWeight    = "600",
    letterSpacing = "0.05em",
    textTransform = "uppercase",
    borderBottom  = "1px solid #C9BEE8"
  ),
  groupHeaderStyle = list(
    background   = "#F4F1FB",
    color        = "#7B6B9E",
    borderBottom = "1px solid #C9BEE8"
  ),
  paginationStyle = list(
    background = "#EDE8F7",
    color      = "#7B6B9E",
    borderTop  = "1px solid #C9BEE8"
  ),
  pageButtonStyle = list(
    background   = "#E2DAF5",
    color        = "#7B6B9E",
    border       = "1px solid #C9BEE8",
    borderRadius = "4px"
  ),
  pageButtonHoverStyle  = list(background = "#534AB7", color = "#FFFFFF"),
  pageButtonActiveStyle = list(background = "#D4870A", color = "#FFFFFF",
                               fontWeight = "800")
)

# ── Prep conditional table ─────────────────────────────────────────────────────
prep_conditional <- function(data, player_col, period_stat_col, ou_prefix,
                             split_filter  = NULL,
                             vs_opponent   = FALSE,
                             vs_type_values = NULL) {
  
  if (is.null(data) || nrow(data) == 0) return(tibble())
  
  # Rename player col
  data <- data |> dplyr::rename(player = !!player_col)
  
  # Rename period stat to threshold
  data <- data |>
    dplyr::rename(threshold = !!period_stat_col) |>
    dplyr::mutate(threshold_label = paste0(threshold, "+"))
  
  # Apply split filter
  if (vs_opponent) {
    if (!is.null(vs_type_values) && length(vs_type_values) > 0) {
      # Use exact Type values discovered at runtime
      data <- data |> dplyr::filter(Type %in% vs_type_values)
    } else {
      # Fallback: any Type starting with "vs" (case insensitive)
      data <- data |>
        dplyr::filter(stringr::str_detect(Type,
                                          stringr::regex("^vs", ignore_case = TRUE)))
    }
  } else if (!is.null(split_filter)) {
    data <- data |> dplyr::filter(Type == split_filter)
  }
  
  if (nrow(data) == 0) return(tibble())
  
  # Keep opponent_abbrev for display in vs opponent file
  if (!vs_opponent && "opponent_abbrev" %in% names(data)) {
    data <- data |> dplyr::select(-opponent_abbrev)
  }
  
  # Rename OU cols from total_pts_N to oN-0.5 format
  ou_cols_old <- names(data)[grepl(paste0("^", ou_prefix), names(data))]
  ou_cols_new <- sapply(ou_cols_old, function(col) {
    num <- as.numeric(gsub(ou_prefix, "", col))
    paste0("o", num - 0.5)
  })
  names(data)[names(data) %in% ou_cols_old] <- ou_cols_new
  
  data
}

# ── Build one colored reactable ────────────────────────────────────────────────
build_table <- function(data, table_id, period_label, stat_label) {
  
  if (is.null(data) || nrow(data) == 0) return(htmltools::p(
    style = "color:#7B6B9E;padding:20px;font-family:'JetBrains Mono',monospace;",
    "No data available for this split."
  ))
  
  # ── Identify OU columns ───────────────────────────────────────────────────
  fixed_cols <- c("player", "Type","opponent_abbrev", "threshold", "threshold_label", "n_games")
  ou_cols    <- setdiff(names(data), fixed_cols)
  ou_cols    <- ou_cols[grepl("^o[0-9]", ou_cols)]
  ou_cols    <- ou_cols[order(as.numeric(gsub("^o", "", ou_cols)))]
  
  if (length(ou_cols) == 0) return(htmltools::p("No O/U columns found."))
  
  # ── Color scale ───────────────────────────────────────────────────────────
  all_vals <- unlist(data[, ou_cols], use.names = FALSE)
  all_vals <- all_vals[!is.na(all_vals) & !is.nan(all_vals)]
  if (length(all_vals) == 0) all_vals <- c(0, 1)
  dmin <- min(all_vals); dmax <- max(all_vals)
  if (dmin == dmax) { dmin <- dmin - 0.01; dmax <- dmax + 0.01 }
  color_fn <- scales::col_numeric(palette = color_set, domain = c(dmin, dmax))
  
  # ── OU col defs ───────────────────────────────────────────────────────────
  ou_col_defs <- setNames(lapply(ou_cols, function(col) {
    colDef(
      name  = col,
      align = "center",
      width = 58,
      style = function(value) {
        if (is.na(value) || is.nan(value)) return(list())
        bg  <- color_fn(value)
        rgb <- grDevices::col2rgb(bg) / 255
        lum <- 0.299 * rgb[1] + 0.587 * rgb[2] + 0.114 * rgb[3]
        list(background   = bg,
             color        = if (lum > 0.5) "#111111" else "#FFFFFF",
             fontWeight   = "700",
             borderRadius = "4px",
             textAlign    = "center")
      },
      cell = function(value, index) {
        if (is.na(value) || is.nan(value)) return("\u2014")
        n   <- data$n_games[index]
        num <- if (!is.na(n)) round(value * n) else "?"
        htmltools::span(
          title = paste0(num, "/", n, " games"),
          style = "cursor:help;border-bottom:1px dotted rgba(0,0,0,0.25);",
          paste0(round(value * 100), "%")
        )
      }
    )
  }), ou_cols)
  
  # ── Fixed col defs — no team column ──────────────────────────────────────
  fixed_col_defs <- list(
    
    player = colDef(
      name        = "Player",
      sticky      = "left",
      width       = 155,
      filterable  = TRUE,
      style       = list(fontWeight = "700", color = "#1A0F3A"),
      headerStyle = list(textAlign = "left")
    ),
    

    
    threshold_label = colDef(
      name        = paste(period_label, stat_label, "\u2265"),
      sticky      = "left",
      width       = 80,
      filterable  = TRUE,
      align       = "center",
      style       = list(color = "#D4870A", fontWeight = "800",
                         fontFamily = "'JetBrains Mono',monospace",
                         fontSize   = "13px")
    ),
    
    threshold = colDef(show = FALSE),
    
    n_games = colDef(
      name        = "Games",
      sticky      = "left",
      width       = 60,
      filterable  = FALSE,
      align       = "center",
      style       = list(color = "#7B6B9E", fontSize = "12px")
    )
  )
  
  if ("opponent_abbrev" %in% names(data)) {
    fixed_col_defs[["opponent_abbrev"]] <- colDef(
      name        = "Opponent",
      sticky      = "left",
      width       = 80,
      filterable  = TRUE,
      align       = "center",
      style       = list(
        color      = "#534AB7",
        fontWeight = "700",
        fontSize   = "12px",
        fontFamily = "'JetBrains Mono',monospace"
      )
    )
  }
  
  # ── Column group ──────────────────────────────────────────────────────────
  col_group <- list(
    colGroup(
      name        = paste("Full Game", stat_label, "\u2014 O/U Lines"),
      columns     = ou_cols,
      headerStyle = list(
        background   = "#534AB7",
        color        = "#FFFFFF",
        fontWeight   = "700",
        fontSize     = "12px",
        textAlign    = "center",
        borderRadius = "4px"
      )
    )
  )
  
  # ── Reorder columns ───────────────────────────────────────────────────────
  col_order <- c("player", "opponent_abbrev", "Type", "threshold_label", "threshold",
                 "n_games", ou_cols)
  col_order <- col_order[col_order %in% names(data)]
  data      <- data[, col_order]
  
  reactable(
    data,
    elementId           = table_id,
    columns             = c(fixed_col_defs, ou_col_defs),
    columnGroups        = col_group,
    filterable          = FALSE,
    searchable          = FALSE,
    highlight           = TRUE,
    striped             = TRUE,
    fullWidth           = TRUE,
    defaultPageSize     = 15,
    showPageSizeOptions = TRUE,
    pageSizeOptions     = c(15, 25, 50, 100),
    paginationType      = "jump",
    showPageInfo        = TRUE,
    defaultSorted       = list(player = "asc"),
    theme               = propiq_theme,
    language            = reactableLang(
      pageInfo     = "{rowStart}\u2013{rowEnd} of {rows} rows",
      pagePrevious = "\u2190 Prev",
      pageNext     = "Next \u2192"
    )
  )
}

# ── CSS ────────────────────────────────────────────────────────────────────────
make_css <- function() {
  htmltools::tags$style(HTML("
    @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@600;700;800;900&family=Barlow:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap');

    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg:         #F4F1FB;
      --bg-card:    #EDE8F7;
      --bg-surface: #E2DAF5;
      --border:     #C9BEE8;
      --navy:       #1A0F3A;
      --purple-mid: #534AB7;
      --muted:      #7B6B9E;
      --text-dim:   #4A3880;
      --gold:       #D4870A;
    }

    html, body {
      background: var(--bg);
      color: var(--navy);
      font-family: 'Barlow', sans-serif;
      min-height: 100vh;
    }

    .piq-header {
      padding: 20px 32px;
      background: linear-gradient(135deg, #1A0F3A 0%, #2D1A5A 100%);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }
    .piq-logo { display: flex; align-items: baseline; gap: 10px; }
    .piq-logo-text {
      font-family: 'Barlow Condensed', sans-serif;
      font-size: 30px;
      font-weight: 900;
      letter-spacing: -0.5px;
      color: #FFFFFF;
    }
    .piq-logo-text span { color: #F5A623; }
    .piq-split-badge {
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      font-weight: 700;
      color: #FFFFFF;
      background: rgba(245,166,35,0.2);
      border: 1px solid rgba(245,166,35,0.5);
      border-radius: 6px;
      padding: 3px 10px;
      letter-spacing: 0.08em;
    }
    .piq-header-right { text-align: right; }
    .piq-date {
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px;
      color: #9B86C0;
    }
    .piq-matchups { font-size: 13px; color: #C4AEE8; margin-top: 2px; }

    .piq-nav {
      padding: 0 32px;
      background: #1A0F3A;
      border-bottom: 3px solid #F5A623;
      position: sticky;
      top: 0;
      z-index: 100;
    }
    .piq-nav-groups { display: flex; gap: 0; }
    .piq-nav-group  { display: flex; flex-direction: column; padding: 10px 24px 0 0; }
    .piq-nav-group-label {
      font-family: 'JetBrains Mono', monospace;
      font-size: 9px;
      font-weight: 600;
      letter-spacing: 0.15em;
      color: #6B4DA0;
      text-transform: uppercase;
      padding-left: 2px;
      margin-bottom: 4px;
    }
    .piq-nav-tabs { display: flex; gap: 2px; }
    .piq-tab {
      font-family: 'Barlow Condensed', sans-serif;
      font-size: 14px;
      font-weight: 700;
      padding: 8px 14px 10px;
      border: none;
      border-radius: 4px 4px 0 0;
      background: transparent;
      color: #9B86C0;
      cursor: pointer;
      border-bottom: 3px solid transparent;
      transition: all 0.15s ease;
      white-space: nowrap;
    }
    .piq-tab:hover { color: #FFFFFF; background: rgba(255,255,255,0.08); }
    .piq-tab.active {
      color: #F5A623;
      background: rgba(245,166,35,0.1);
      border-bottom-color: #F5A623;
    }

    .piq-content { padding: 24px 32px; background: var(--bg); }
    .piq-panel        { display: none; }
    .piq-panel.active { display: block; }

    .piq-panel-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
      padding-bottom: 14px;
      border-bottom: 1px solid var(--border);
    }
    .piq-panel-title {
      font-family: 'Barlow Condensed', sans-serif;
      font-size: 22px;
      font-weight: 800;
      color: var(--navy);
    }
    .piq-panel-subtitle {
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      color: var(--muted);
      margin-top: 2px;
    }
    .piq-dot {
      width: 10px; height: 10px;
      border-radius: 50%;
      background: #D4870A;
      flex-shrink: 0;
      box-shadow: 0 0 8px rgba(212,135,10,0.5);
    }

    .piq-info {
      background: rgba(83,74,183,0.06);
      border: 1px solid rgba(83,74,183,0.2);
      border-left: 3px solid #534AB7;
      border-radius: 0 8px 8px 0;
      padding: 12px 16px;
      margin-bottom: 12px;
      font-size: 13px;
      color: var(--text-dim);
      line-height: 1.6;
    }
    .piq-info strong { color: var(--navy); }
    .piq-info .gold  { color: #D4870A; font-weight: 700; }

    .piq-hint {
      background: rgba(83,74,183,0.03);
      border: 1px solid rgba(83,74,183,0.1);
      border-left: 3px solid #C9BEE8;
      border-radius: 0 6px 6px 0;
      padding: 10px 14px;
      margin-bottom: 12px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      color: var(--muted);
    }
    .piq-hint strong { color: var(--navy); }

    .piq-legend {
      display: flex;
      gap: 20px;
      margin-bottom: 12px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      color: var(--muted);
      align-items: center;
      flex-wrap: wrap;
    }
    .piq-legend-dot {
      width: 8px; height: 8px;
      border-radius: 50%;
      display: inline-block;
      margin-right: 5px;
    }

    .rt-th {
      background-color: #EDE8F7 !important;
      color: #7B6B9E !important;
      font-size: 11px !important;
      font-weight: 600 !important;
      letter-spacing: 0.05em !important;
      text-transform: uppercase !important;
      border-right: 1px solid #C9BEE8 !important;
      border-bottom: 1px solid #C9BEE8 !important;
    }
    .rt-column-group-header {
      background-color: #F4F1FB !important;
      border-bottom: 1px solid #C9BEE8 !important;
    }
    .rt-tbody .rt-tr-group { border-bottom: 1px solid #E8E3F0 !important; }
    .rt-td-sticky-left {
      background-color: #FFFFFF !important;
      color: #1A0F3A !important;
      border-right: 2px solid #C9BEE8 !important;
    }
    .rt-th-sticky-left {
      background-color: #EDE8F7 !important;
      border-right: 2px solid #C9BEE8 !important;
    }
    .rt-pagination {
      background-color: #EDE8F7 !important;
      color: #7B6B9E !important;
      border-top: 1px solid #C9BEE8 !important;
    }
    .rt-page-button {
      background-color: #E2DAF5 !important;
      color: #7B6B9E !important;
      border: 1px solid #C9BEE8 !important;
      border-radius: 4px !important;
    }
    .rt-page-button:hover { background-color: #534AB7 !important; color: #FFFFFF !important; }
    .rt-page-button-current {
      background-color: #D4870A !important;
      color: #FFFFFF !important;
      font-weight: 800 !important;
    }
    input[type='text'].rt-filter {
      background-color: #F4F1FB !important;
      border: 1px solid #C9BEE8 !important;
      color: #1A0F3A !important;
      border-radius: 4px !important;
      font-size: 11px !important;
      padding: 4px 6px !important;
    }

    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: var(--bg); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--purple-mid); }

    @media (max-width: 768px) {
      .piq-header  { padding: 16px; }
      .piq-nav     { padding: 0 16px; }
      .piq-content { padding: 16px; }
      .piq-nav-groups { flex-wrap: wrap; }
    }
  "))
}

# ── Build one HTML file for one split ─────────────────────────────────────────
build_split_page <- function(output_file,
                             split_filter   = NULL,
                             vs_opponent    = FALSE,
                             vs_type_values = NULL,
                             split_label    = "Regular Season",
                             split_sublabel = NULL,
                             matchup_str    = "") {
  
  message("Building: ", output_file, " [", split_label, "]")
  
  q1_pts <- prep_conditional(pts_conditional,   "scorer",    "fqpts", "total_pts_", split_filter, vs_opponent, vs_type_values)
  q1_ast <- prep_conditional(ast_conditional,   "assister",  "fqast", "total_ast_", split_filter, vs_opponent, vs_type_values)
  q1_reb <- prep_conditional(reb_conditional,   "rebounder", "fqreb", "total_reb_", split_filter, vs_opponent, vs_type_values)
  h1_pts <- prep_conditional(h1pts_conditional, "scorer",    "h1pts", "total_pts_", split_filter, vs_opponent, vs_type_values)
  h1_ast <- prep_conditional(h1ast_conditional, "assister",  "h1ast", "total_ast_", split_filter, vs_opponent, vs_type_values)
  h1_reb <- prep_conditional(h1reb_conditional, "rebounder", "h1reb", "total_reb_", split_filter, vs_opponent, vs_type_values)
  q3_pts <- prep_conditional(q3pts_conditional, "scorer",    "q3pts", "total_pts_", split_filter, vs_opponent, vs_type_values)
  q3_ast <- prep_conditional(q3ast_conditional, "assister",  "q3ast", "total_ast_", split_filter, vs_opponent, vs_type_values)
  q3_reb <- prep_conditional(q3reb_conditional, "rebounder", "q3reb", "total_reb_", split_filter, vs_opponent, vs_type_values)
  
  tabs <- list(
    list(id="q1pts", label="Q1 Pts",  data=q1_pts, period="Q1", stat="Points"),
    list(id="q1ast", label="Q1 Ast",  data=q1_ast, period="Q1", stat="Assists"),
    list(id="q1reb", label="Q1 Reb",  data=q1_reb, period="Q1", stat="Rebounds"),
    list(id="h1pts", label="H1 Pts",  data=h1_pts, period="H1", stat="Points"),
    list(id="h1ast", label="H1 Ast",  data=h1_ast, period="H1", stat="Assists"),
    list(id="h1reb", label="H1 Reb",  data=h1_reb, period="H1", stat="Rebounds"),
    list(id="q3pts", label="Q3 Pts",  data=q3_pts, period="Q3", stat="Points"),
    list(id="q3ast", label="Q3 Ast",  data=q3_ast, period="Q3", stat="Assists"),
    list(id="q3reb", label="Q3 Reb",  data=q3_reb, period="Q3", stat="Rebounds")
  )
  
  tab_groups <- list(
    list(label = "FIRST QUARTER", ids = c("q1pts","q1ast","q1reb")),
    list(label = "FIRST HALF",    ids = c("h1pts","h1ast","h1reb")),
    list(label = "THRU Q3",       ids = c("q3pts","q3ast","q3reb"))
  )
  
  css <- make_css()
  
  js <- htmltools::tags$script(HTML("
    function showTab(id) {
      document.querySelectorAll('.piq-panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.piq-tab').forEach(t => t.classList.remove('active'));
      document.getElementById('panel-' + id).classList.add('active');
      document.getElementById('tab-' + id).classList.add('active');
    }
    document.addEventListener('DOMContentLoaded', function() { showTab('q1pts'); });
  "))
  
  header <- htmltools::tags$div(
    class = "piq-header",
    htmltools::tags$div(
      class = "piq-logo",
      htmltools::tags$div(class = "piq-logo-text",
                          "Prop", htmltools::tags$span("IQ")),
      htmltools::tags$div(class = "piq-split-badge", toupper(split_label))
    ),
    htmltools::tags$div(
      class = "piq-header-right",
      htmltools::tags$div(class = "piq-date",     format(as.Date(next_team_batch_date), "%A, %B %d %Y")),
      htmltools::tags$div(class = "piq-matchups", matchup_str)
    )
  )
  
  nav_groups_html <- lapply(tab_groups, function(grp) {
    group_tab_buttons <- lapply(grp$ids, function(tab_id) {
      tab_info <- Filter(function(t) t$id == tab_id, tabs)[[1]]
      htmltools::tags$button(
        id      = paste0("tab-", tab_id),
        class   = paste0("piq-tab", if (tab_id == "q1pts") " active" else ""),
        onclick = paste0("showTab('", tab_id, "')"),
        tab_info$label
      )
    })
    htmltools::tags$div(
      class = "piq-nav-group",
      htmltools::tags$div(class = "piq-nav-group-label", grp$label),
      htmltools::tags$div(class = "piq-nav-tabs", group_tab_buttons)
    )
  })
  
  nav <- htmltools::tags$nav(
    class = "piq-nav",
    htmltools::tags$div(class = "piq-nav-groups", nav_groups_html)
  )
  
  info_text <- list(
    Q1 = "in the <span class='gold'>first quarter</span>.",
    H1 = "in the <span class='gold'>first half</span>.",
    Q3 = "<span class='gold'>through three quarters</span>."
  )
  
  panels <- lapply(tabs, function(t) {
    tbl <- build_table(t$data, paste0("rt-", t$id), t$period, t$stat)
    
    n_rows    <- if (!is.null(t$data) && nrow(t$data) > 0) nrow(t$data) else 0
    n_players <- if (!is.null(t$data) && "player" %in% names(t$data))
      dplyr::n_distinct(t$data$player) else 0
    
    info <- htmltools::tags$div(
      class = "piq-info",
      htmltools::HTML(paste0(
        "<strong>How to read:</strong> Each row = one player at one threshold. ",
        "Columns show % of games hitting each O/U line when scoring at or above ",
        "that threshold ", info_text[[t$period]],
        " Split: <span class='gold'>", split_label, ".</span>",
        " <strong>Hover any % for the exact fraction.</strong>"
      ))
    )
    
    hint <- htmltools::tags$div(
      class = "piq-hint",
      htmltools::HTML(paste0(
        "<strong>", n_rows, " rows</strong> \u00b7 ",
        "<strong>", n_players, " players</strong> \u00b7 ",
        "Filter <em>Player</em> or <em>Threshold</em> using column filters"
      ))
    )
    
    legend <- htmltools::tags$div(
      class = "piq-legend",
      htmltools::HTML("
        <span><span class='piq-legend-dot' style='background:#1a9e5c'></span>65%+ Strong edge</span>
        <span><span class='piq-legend-dot' style='background:#f7c844'></span>50\u201364% Borderline</span>
        <span><span class='piq-legend-dot' style='background:#cc2222'></span>&lt;50% Avoid</span>
        <span style='margin-left:8px;color:#7B6B9E;'>\u00b7 Hover % for fraction</span>
      ")
    )
    
    subtitle <- if (!is.null(split_sublabel)) split_sublabel else split_label
    
    htmltools::tags$div(
      id    = paste0("panel-", t$id),
      class = paste0("piq-panel", if (t$id == "q1pts") " active" else ""),
      htmltools::tags$div(
        class = "piq-panel-header",
        htmltools::tags$div(class = "piq-dot"),
        htmltools::tags$div(
          htmltools::tags$div(class = "piq-panel-title",
                              paste(t$period, t$stat, "\u2014", split_label)),
          htmltools::tags$div(class = "piq-panel-subtitle",
                              paste0(subtitle, " \u00b7 ",
                                     format(Sys.Date(), "%b %d %Y")))
        )
      ),
      info, hint, legend, tbl
    )
  })
  
  content <- htmltools::tags$div(class = "piq-content", panels)
  
  page <- htmltools::tags$html(
    lang = "en",
    htmltools::tags$head(
      htmltools::tags$meta(charset = "UTF-8"),
      htmltools::tags$meta(name = "viewport",
                           content = "width=device-width, initial-scale=1.0"),
      htmltools::tags$title(paste0("PropBetIQ \u2014 ", split_label, " \u2014 ",
                                   format(as.Date(next_team_batch_date), "%b %d %Y"))),
      css
    ),
    htmltools::tags$body(header, nav, content, js)
  )
  
  save_self_contained(page, output_file)
  
  size_mb <- round(file.size(output_file) / 1024 / 1024, 1)
  message("  \u2713 ", output_file, " (", size_mb, " MB)")
  invisible(output_file)
}

# ══════════════════════════════════════════════════════════════════════════════
# DETECT VS OPPONENT TYPE VALUES AT RUNTIME
# ══════════════════════════════════════════════════════════════════════════════
# Rather than guessing the format, detect it from the actual data
vs_type_values <- pts_conditional |>
  dplyr::pull(Type) |>
  unique() |>
  stringr::str_subset(stringr::regex("^vs", ignore_case = TRUE))

message("Detected vs opponent Type values: ",
        paste(vs_type_values, collapse = ", "))

# ══════════════════════════════════════════════════════════════════════════════
# RENDER ALL 6 SPLIT FILES
# ══════════════════════════════════════════════════════════════════════════════

matchup_str <- tryCatch({
  n_games <- matchup |>
    dplyr::filter(location == "Home") |>
    dplyr::distinct(Team) |>
    nrow()
  paste0(n_games, " games tonight")
}, error = function(e) format(Sys.Date(), "%B %d, %Y"))

message("\n=== PropIQ Dashboard Builder ===")
message("Rendering 6 split files...\n")

# build_split_page(
#   output_file    = "propiq_reg_season.html",
#   split_filter   = "Regular Season",
#   split_label    = "Regular Season",
#   split_sublabel = "Full regular season sample",
#   matchup_str    = matchup_str
# )
# 
# build_split_page(
#   output_file    = "propiq_home.html",
#   split_filter   = "Home Games",
#   split_label    = "Home Games",
#   split_sublabel = "Regular season home games only",
#   matchup_str    = matchup_str
# )
# 
# build_split_page(
#   output_file    = "propiq_away.html",
#   split_filter   = "Away Games",
#   split_label    = "Away Games",
#   split_sublabel = "Regular season away games only",
#   matchup_str    = matchup_str
# )
# 
# build_split_page(
#   output_file    = "propiq_playoffs.html",
#   split_filter   = "Playoffs",
#   split_label    = "Playoffs",
#   split_sublabel = "Playoff games only",
#   matchup_str    = matchup_str
# )
# 
# build_split_page(
#   output_file    = "propiq_last10.html",
#   split_filter   = "Last 10",
#   split_label    = "Last 10 Games",
#   split_sublabel = "Most recent 10 games regardless of game type",
#   matchup_str    = matchup_str
# )
# 
# build_split_page(
#   output_file    = "propiq_vs_opponent.html",
#   vs_opponent    = TRUE,
#   vs_type_values = vs_type_values,
#   split_label    = "vs Tonight's Opponent",
#   split_sublabel = "Regular season games against tonight's specific opponent",
#   matchup_str    = matchup_str
# )

message("\n=== All files rendered ===")
for (f in c("propiq_reg_season.html", "propiq_home.html", "propiq_away.html",
            "propiq_playoffs.html",   "propiq_last10.html",
            "propiq_vs_opponent.html")) {
  if (file.exists(f)) {
    size_mb <- round(file.size(f) / 1024 / 1024, 1)
    message("  ", f, " \u2014 ", size_mb, " MB")
  }
}
