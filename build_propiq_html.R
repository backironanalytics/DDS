# ══════════════════════════════════════════════════════════════════════════════
# PropIQ — HTML Dashboard Builder (Light Theme)
# Dark chrome navbar + light content canvas
# No pandoc. No flexdashboard. No RMarkdown.
# Requires: all_players_q1_pts, all_players_q1_ast, all_players_q1_reb,
#           all_players_h1_pts, all_players_h1_ast, all_players_h1_reb,
#           all_players_q3_pts, all_players_q3_ast, all_players_q3_reb
#           matchup, Sys.Date()
# ══════════════════════════════════════════════════════════════════════════════

library(htmltools)
library(reactable)
library(scales)

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

# ── Build one colored reactable ────────────────────────────────────────────────
build_table <- function(data, table_id, period_label, stat_label) {
  
  if (is.null(data) || nrow(data) == 0) return(htmltools::p(
    style = "color:#7B6B9E;padding:20px;font-family:'JetBrains Mono',monospace;",
    "No data available."
  ))
  
  fixed_cols <- c("player", "threshold", "n_games")
  ou_cols    <- setdiff(names(data), fixed_cols)
  ou_cols    <- ou_cols[grepl("^o[0-9]", ou_cols)]
  ou_cols    <- ou_cols[order(as.numeric(gsub("^o", "", ou_cols)))]
  
  if (length(ou_cols) == 0) return(htmltools::p("No O/U columns found."))
  
  all_vals <- unlist(data[, ou_cols], use.names = FALSE)
  all_vals <- all_vals[!is.na(all_vals) & !is.nan(all_vals)]
  if (length(all_vals) == 0) all_vals <- c(0, 1)
  dmin <- min(all_vals); dmax <- max(all_vals)
  if (dmin == dmax) { dmin <- dmin - 0.01; dmax <- dmax + 0.01 }
  color_fn <- scales::col_numeric(palette = color_set, domain = c(dmin, dmax))
  
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
        list(
          background   = bg,
          color        = if (lum > 0.5) "#111111" else "#FFFFFF",
          fontWeight   = "700",
          borderRadius = "4px",
          textAlign    = "center"
        )
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
  
  fixed_col_defs <- list(
    player = colDef(
      name        = "Player",
      sticky      = "left",
      width       = 160,
      style       = list(fontWeight = "700", color = "#1A0F3A"),
      headerStyle = list(textAlign = "left")
    ),
    threshold = colDef(
      name  = paste(period_label, stat_label, "\u2265"),
      width = 85,
      align = "center",
      style = list(
        color      = "#D4870A",
        fontWeight = "800",
        fontFamily = "'JetBrains Mono',monospace",
        fontSize   = "13px"
      )
    ),
    n_games = colDef(
      name  = "Games",
      width = 65,
      align = "center",
      style = list(color = "#7B6B9E", fontSize = "12px")
    )
  )
  
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
  
  reactable(
    data,
    elementId           = table_id,
    columns             = c(fixed_col_defs, ou_col_defs),
    columnGroups        = col_group,
    filterable          = TRUE,
    searchable          = FALSE,
    highlight           = TRUE,
    striped             = TRUE,
    fullWidth           = TRUE,
    defaultPageSize     = 15,
    showPageSizeOptions = TRUE,
    pageSizeOptions     = c(15, 25, 50, 100),
    paginationType      = "jump",
    showPageInfo        = TRUE,
    theme               = propiq_theme,
    language            = reactableLang(
      pageInfo     = "{rowStart}\u2013{rowEnd} of {rows} rows",
      pagePrevious = "\u2190 Prev",
      pageNext     = "Next \u2192"
    )
  )
}

# ── Full page builder ──────────────────────────────────────────────────────────
build_propiq_page <- function(output_file = NULL) {
  
  if (is.null(output_file)) {
    output_file <- paste0("propiq_", Sys.Date(), ".html")
  }
  
  matchup_str <- tryCatch({
    n_games <- matchup |>
      dplyr::filter(location == "Home") |>
      dplyr::distinct(Team) |>
      nrow()
    paste0(n_games, " games tonight")
  }, error = function(e) format(Sys.Date(), "%B %d, %Y"))
  
  tabs <- list(
    list(id="q1pts", label="Q1 Pts",  data=all_players_q1_pts, period="Q1", stat="Points"),
    list(id="q1ast", label="Q1 Ast",  data=all_players_q1_ast, period="Q1", stat="Assists"),
    list(id="q1reb", label="Q1 Reb",  data=all_players_q1_reb, period="Q1", stat="Rebounds"),
    list(id="h1pts", label="H1 Pts",  data=all_players_h1_pts, period="H1", stat="Points"),
    list(id="h1ast", label="H1 Ast",  data=all_players_h1_ast, period="H1", stat="Assists"),
    list(id="h1reb", label="H1 Reb",  data=all_players_h1_reb, period="H1", stat="Rebounds"),
    list(id="q3pts", label="Q3 Pts",  data=all_players_q3_pts, period="Q3", stat="Points"),
    list(id="q3ast", label="Q3 Ast",  data=all_players_q3_ast, period="Q3", stat="Assists"),
    list(id="q3reb", label="Q3 Reb",  data=all_players_q3_reb, period="Q3", stat="Rebounds")
  )
  
  tab_groups <- list(
    list(label = "FIRST QUARTER", ids = c("q1pts","q1ast","q1reb")),
    list(label = "FIRST HALF",    ids = c("h1pts","h1ast","h1reb")),
    list(label = "THRU Q3",       ids = c("q3pts","q3ast","q3reb"))
  )
  
  # ── CSS ───────────────────────────────────────────────────────────────────────
  css <- htmltools::tags$style(HTML("
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
      --green:      #1A9E5C;
      --navbar-bg:  #1A0F3A;
      --navbar-border: #2D1A5A;
    }

    html, body {
      background: var(--bg);
      color: var(--navy);
      font-family: 'Barlow', sans-serif;
      min-height: 100vh;
    }

    /* ── Header ── */
    .piq-header {
      padding: 20px 32px;
      border-bottom: 1px solid var(--navbar-border);
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
    .piq-badge {
      font-family: 'JetBrains Mono', monospace;
      font-size: 10px;
      font-weight: 600;
      color: #F5A623;
      background: rgba(245,166,35,0.15);
      border: 1px solid rgba(245,166,35,0.4);
      border-radius: 4px;
      padding: 2px 8px;
      letter-spacing: 0.1em;
    }
    .piq-header-right { text-align: right; }
    .piq-date {
      font-family: 'JetBrains Mono', monospace;
      font-size: 12px;
      color: #9B86C0;
    }
    .piq-matchups {
      font-size: 13px;
      color: #C4AEE8;
      margin-top: 2px;
    }

    /* ── Nav ── */
    .piq-nav {
      padding: 0 32px;
      background: var(--navbar-bg);
      border-bottom: 3px solid #F5A623;
      position: sticky;
      top: 0;
      z-index: 100;
    }
    .piq-nav-groups {
      display: flex;
      gap: 0;
    }
    .piq-nav-group {
      display: flex;
      flex-direction: column;
      padding: 10px 24px 0 0;
    }
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
      letter-spacing: 0.03em;
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
    .piq-tab:hover {
      color: #FFFFFF;
      background: rgba(255,255,255,0.08);
    }
    .piq-tab.active {
      color: #F5A623;
      background: rgba(245,166,35,0.1);
      border-bottom-color: #F5A623;
    }

    /* ── Content ── */
    .piq-content {
      padding: 24px 32px;
      background: var(--bg);
    }
    .piq-panel        { display: none; }
    .piq-panel.active { display: block; }

    /* ── Panel header ── */
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
      letter-spacing: 0.02em;
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

    /* ── Info box ── */
    .piq-info {
      background: rgba(83,74,183,0.06);
      border: 1px solid rgba(83,74,183,0.2);
      border-left: 3px solid #534AB7;
      border-radius: 0 8px 8px 0;
      padding: 12px 16px;
      margin-bottom: 14px;
      font-size: 13px;
      color: var(--text-dim);
      line-height: 1.6;
    }
    .piq-info strong { color: var(--navy); }
    .piq-info .gold  { color: #D4870A; font-weight: 700; }

    /* ── Legend ── */
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

    /* ── Reactable light overrides ── */
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
    .rt-tbody .rt-tr-group {
      border-bottom: 1px solid #E8E3F0 !important;
    }
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
    .rt-page-button:hover {
      background-color: #534AB7 !important;
      color: #FFFFFF !important;
    }
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

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: var(--bg); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--purple-mid); }

    /* ── Responsive ── */
    @media (max-width: 768px) {
      .piq-header  { padding: 16px; }
      .piq-nav     { padding: 0 16px; }
      .piq-content { padding: 16px; }
      .piq-nav-groups { flex-wrap: wrap; }
    }
  "))
  
  # ── JavaScript ────────────────────────────────────────────────────────────────
  js <- htmltools::tags$script(HTML("
    function showTab(id) {
      document.querySelectorAll('.piq-panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.piq-tab').forEach(t => t.classList.remove('active'));
      document.getElementById('panel-' + id).classList.add('active');
      document.getElementById('tab-' + id).classList.add('active');
    }
    document.addEventListener('DOMContentLoaded', function() {
      showTab('q1pts');
    });
  "))
  
  # ── Header ───────────────────────────────────────────────────────────────────
  header <- htmltools::tags$div(
    class = "piq-header",
    htmltools::tags$div(
      class = "piq-logo",
      htmltools::tags$div(
        class = "piq-logo-text",
        "Prop", htmltools::tags$span("IQ")
      ),
      htmltools::tags$div(class = "piq-badge", "THRESHOLD ANALYSIS")
    ),
    htmltools::tags$div(
      class = "piq-header-right",
      htmltools::tags$div(class = "piq-date",     format(as.Date(next_team_batch_date), "%A, %B %d %Y")),
      htmltools::tags$div(class = "piq-matchups", matchup_str)
    )
  )
  
  # ── Navigation ────────────────────────────────────────────────────────────────
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
  
  # ── Info text per period ──────────────────────────────────────────────────────
  info_text <- list(
    Q1 = "Shows how often players hit full game O/U lines when they score at or above the threshold <span class='gold'>in the first quarter</span>. Use this during or after Q1 to assess full game potential.",
    H1 = "Shows how often players hit full game O/U lines when they score at or above the threshold <span class='gold'>in the first half</span>. Strong half-time predictor for full game totals.",
    Q3 = "Shows how often players hit full game O/U lines when they score at or above the threshold <span class='gold'>through three quarters</span>. Highest predictive power \u2014 only one quarter remains."
  )
  
  # ── Content panels ────────────────────────────────────────────────────────────
  panels <- lapply(tabs, function(t) {
    
    tbl <- build_table(t$data, paste0("rt-", t$id), t$period, t$stat)
    
    info <- htmltools::tags$div(
      class = "piq-info",
      htmltools::HTML(paste0(
        "<strong>How to read:</strong> Each row shows a player at a given threshold ",
        "(e.g. <span style='color:#D4870A;font-weight:700;'>5+</span> means they scored ",
        "at least 5 in the ", t$period, "). Columns show the % of qualifying games where ",
        "they hit that full game O/U line. ",
        info_text[[t$period]],
        " <strong>Hover any % to see the exact fraction.</strong>"
      ))
    )
    
    legend <- htmltools::tags$div(
      class = "piq-legend",
      htmltools::HTML("
        <span><span class='piq-legend-dot' style='background:#1a9e5c'></span>65%+ Strong edge</span>
        <span><span class='piq-legend-dot' style='background:#f7c844'></span>50\u201364% Borderline</span>
        <span><span class='piq-legend-dot' style='background:#cc2222'></span>&lt;50% Avoid</span>
        <span style='margin-left:8px;color:#7B6B9E;'>\u00b7 Use column filters to search by player, threshold or any value</span>
      ")
    )
    
    htmltools::tags$div(
      id    = paste0("panel-", t$id),
      class = paste0("piq-panel", if (t$id == "q1pts") " active" else ""),
      htmltools::tags$div(
        class = "piq-panel-header",
        htmltools::tags$div(class = "piq-dot"),
        htmltools::tags$div(
          htmltools::tags$div(
            class = "piq-panel-title",
            paste(t$period, t$stat, "Threshold Analysis")
          ),
          htmltools::tags$div(
            class = "piq-panel-subtitle",
            paste0("Regular Season + Playoffs \u00b7 ",
                   format(Sys.Date(), "%b %d %Y"))
          )
        )
      ),
      info,
      legend,
      tbl
    )
  })
  
  content <- htmltools::tags$div(
    class = "piq-content",
    panels
  )
  
  # ── Assemble full page ────────────────────────────────────────────────────────
  page <- htmltools::tags$html(
    lang = "en",
    htmltools::tags$head(
      htmltools::tags$meta(charset = "UTF-8"),
      htmltools::tags$meta(
        name    = "viewport",
        content = "width=device-width, initial-scale=1.0"
      ),
      htmltools::tags$title(
        paste0("PropBetIQ \u2014 ",format(as.Date(next_team_batch_date), "%b %d %Y"))
      ),
      css
    ),
    htmltools::tags$body(
      header,
      nav,
      content,
      js
    )
  )
  
  save_self_contained(page, output_file)
  
  message("PropIQ dashboard saved to: ", output_file)
  invisible(output_file)
}

# # ── Run ────────────────────────────────────────────────────────────────────────
# build_propiq_page(
#   output_file = paste0("propiq_gameday",".html")
# )
