# ══════════════════════════════════════════════════════════════════════════════
# PropIQ — Stats HTML Builder
# Hit rate tables — Regular Season and Playoffs
# Sub nav tabs live INSIDE each main panel (not duplicated)
# Requires: pt_pivoted, ast_pivoted, reb_pivoted, fg3m_pivoted, fg3a_pivoted,
#           stl_pivoted, blk_pivoted, tov_pivoted, ftm_pivoted, fgm_pivoted,
#           ptrebast_pivoted, pt_ast_pivoted, pt_reb_pivoted,
#           ast_reb_pivoted, stl_blk_pivoted,
#           firstqpoints_pivoted, firstqassists_pivoted, firstqrebounds_pivoted
#           next_team_batch_date, matchup
# ══════════════════════════════════════════════════════════════════════════════

library(htmltools)
library(reactable)
library(scales)
library(dplyr)

# ── Color theme ────────────────────────────────────────────────────────────────
color_set <- c("red2", "#f7c844", "#1a9e5c")

propiq_theme <- reactableTheme(
  color           = "#1A0F3A",
  backgroundColor = "#FFFFFF",
  borderColor     = "#C9BEE8",
  stripedColor    = "#EDE8F7",
  highlightColor  = "#E2DAF5",
  cellPadding     = "8px 10px",
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

# ── Filter pivoted table to one split ─────────────────────────────────────────
filter_split <- function(data, split) {
  if (is.null(data) || nrow(data) == 0) return(tibble())
  if (!"Type" %in% names(data)) return(data)
  data |> dplyr::filter(Type == split)
}

# ── Build hit rate reactable ───────────────────────────────────────────────────
build_stats_table <- function(data, table_id, metric_label,
                              player_col = "Player") {
  
  if (is.null(data) || nrow(data) == 0) return(htmltools::p(
    style = "color:#7B6B9E;padding:20px;font-family:'JetBrains Mono',monospace;",
    "No data available for this split."
  ))
  
  # Identify OU columns — numeric names like "19.5", "20.5"
  fixed_cols <- c(player_col, "Type", "urlThumbnailTeam", "n_games",
                  "scorer", "assister", "rebounder")
  ou_cols <- setdiff(names(data), fixed_cols)
  ou_cols <- ou_cols[!is.na(suppressWarnings(as.numeric(ou_cols)))]
  ou_cols <- ou_cols[order(as.numeric(ou_cols))]
  
  if (length(ou_cols) == 0) return(htmltools::p(
    style = "color:#7B6B9E;padding:20px;",
    "No O/U columns found."
  ))
  
  # Color scale
  all_vals <- unlist(data[, ou_cols], use.names = FALSE)
  all_vals <- all_vals[!is.na(all_vals) & !is.nan(all_vals)]
  if (length(all_vals) == 0) all_vals <- c(0, 1)
  dmin <- min(all_vals); dmax <- max(all_vals)
  if (dmin == dmax) { dmin <- dmin - 0.01; dmax <- dmax + 0.01 }
  color_fn <- scales::col_numeric(palette = color_set, domain = c(dmin, dmax))
  
  # OU col defs
  ou_col_defs <- setNames(lapply(ou_cols, function(col) {
    colDef(
      name  = col,
      align = "center",
      width = 56,
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
        if ("n_games" %in% names(data)) {
          n   <- data$n_games[index]
          num <- if (!is.na(n)) round(value * n) else "?"
          htmltools::span(
            title = paste0(num, "/", n, " games"),
            style = "cursor:help;border-bottom:1px dotted rgba(0,0,0,0.25);",
            paste0(round(value * 100), "%")
          )
        } else {
          htmltools::span(paste0(round(value * 100), "%"))
        }
      }
    )
  }), ou_cols)
  
  # Fixed col defs
  fixed_col_defs <- list()
  
  fixed_col_defs[[player_col]] <- colDef(
    name        = "Player",
    sticky      = "left",
    width       = 155,
    filterable  = TRUE,
    style       = list(fontWeight = "700", color = "#1A0F3A"),
    headerStyle = list(textAlign = "left")
  )
  
  if ("urlThumbnailTeam" %in% names(data)) {
    fixed_col_defs[["urlThumbnailTeam"]] <- colDef(
      name       = "",
      sticky     = "left",
      width      = 32,
      sortable   = FALSE,
      filterable = FALSE,
      cell       = function(value) {
        if (is.na(value) || nchar(as.character(value)) == 0) return("")
        htmltools::img(src = value, height = "18px", width = "18px",
                       style = "object-fit:contain;")
      }
    )
  }
  
  if ("Type" %in% names(data)) {
    fixed_col_defs[["Type"]] <- colDef(show = FALSE)
  }
  
  if ("n_games" %in% names(data)) {
    fixed_col_defs[["n_games"]] <- colDef(
      name        = "Games",
      sticky      = "left",
      width       = 60,
      filterable  = FALSE,
      align       = "center",
      style       = list(color = "#7B6B9E", fontSize = "12px")
    )
  }
  
  # Column group
  col_group <- list(
    colGroup(
      name        = paste(metric_label, "\u2014 O/U Hit Rates"),
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
  
  # Reorder columns
  col_order <- c(player_col, "urlThumbnailTeam", "Type", "n_games", ou_cols)
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
    defaultSorted       = setNames(list("asc"), player_col),
    theme               = propiq_theme,
    language            = reactableLang(
      pageInfo     = "{rowStart}\u2013{rowEnd} of {rows} rows",
      pagePrevious = "\u2190 Prev",
      pageNext     = "Next \u2192"
    )
  )
}

# ── Build full stats page ──────────────────────────────────────────────────────
build_propiq_stats <- function(output_file = "propiq_stats.html") {
  
  game_date  <- tryCatch(as.Date(next_team_batch_date), error = function(e) Sys.Date())
  date_label <- format(game_date, "%A, %B %d %Y")
  date_short <- format(game_date, "%b %d %Y")
  
  matchup_str <- tryCatch({
    n <- matchup |> dplyr::filter(location == "Home") |>
      dplyr::distinct(Team) |> nrow()
    paste0(n, " games tonight")
  }, error = function(e) date_label)
  
  # ── Metric definitions ────────────────────────────────────────────────────
  metrics <- list(
    list(id="pts",      label="Points",       data=pt_pivoted,             player_col="Player"),
    list(id="ast",      label="Assists",      data=ast_pivoted,            player_col="Player"),
    list(id="reb",      label="Rebounds",     data=reb_pivoted,            player_col="Player"),
    list(id="ptrebast", label="Pts+Reb+Ast",  data=ptrebast_pivoted,       player_col="Player"),
    list(id="ptast",    label="Pts+Ast",      data=pt_ast_pivoted,         player_col="Player"),
    list(id="ptreb",    label="Pts+Reb",      data=pt_reb_pivoted,         player_col="Player"),
    list(id="astreb",   label="Ast+Reb",      data=ast_reb_pivoted,        player_col="Player"),
    list(id="fg3m",     label="3-Pt Made",    data=fg3m_pivoted,           player_col="Player"),
    list(id="fg3a",     label="3-Pt Att",     data=fg3a_pivoted,           player_col="Player"),
    list(id="ftm",      label="Free Throws",  data=ftm_pivoted,            player_col="Player"),
    list(id="fgm",      label="FG Made",      data=fgm_pivoted,            player_col="Player"),
    list(id="stl",      label="Steals",       data=stl_pivoted,            player_col="Player"),
    list(id="blk",      label="Blocks",       data=blk_pivoted,            player_col="Player"),
    list(id="stlblk",   label="Stl+Blk",      data=stl_blk_pivoted,        player_col="Player"),
    list(id="tov",      label="Turnovers",    data=tov_pivoted,            player_col="Player"),
    list(id="q1pts",    label="Q1 Points",    data=firstqpoints_pivoted,   player_col="Player"),
    list(id="q1ast",    label="Q1 Assists",   data=firstqassists_pivoted,  player_col="Player"),
    list(id="q1reb",    label="Q1 Rebounds",  data=firstqrebounds_pivoted, player_col="Player")
  )
  
  splits <- list(
    list(id="reg",      label="Regular Season", filter="Regular Season"),
    list(id="playoffs", label="Playoffs",       filter="Playoffs")
  )
  
  # ── Legend ───────────────────────────────────────────────────────────────────
  legend <- htmltools::tags$div(
    style = "display:flex;gap:16px;margin-bottom:10px;font-family:'JetBrains Mono',
             monospace;font-size:11px;color:#7B6B9E;align-items:center;flex-wrap:wrap;",
    htmltools::HTML("
      <span><span style='width:7px;height:7px;border-radius:50%;background:#1a9e5c;
            display:inline-block;margin-right:4px;'></span>65%+ Strong edge</span>
      <span><span style='width:7px;height:7px;border-radius:50%;background:#f7c844;
            display:inline-block;margin-right:4px;'></span>50\u201364% Borderline</span>
      <span><span style='width:7px;height:7px;border-radius:50%;background:#cc2222;
            display:inline-block;margin-right:4px;'></span>&lt;50% Avoid</span>
      <span style='margin-left:8px;'>\u00b7 Filter Player column \u00b7 Hover % for fraction</span>
    ")
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

    /* ── Main nav (Regular Season / Playoffs) ── */
    .piq-main-nav {
      background: #1A0F3A;
      border-bottom: 3px solid #F5A623;
      padding: 0 32px;
      position: sticky;
      top: 0;
      z-index: 100;
      display: flex;
      gap: 4px;
    }
    .piq-main-tab {
      font-family: 'Barlow Condensed', sans-serif;
      font-size: 16px;
      font-weight: 800;
      letter-spacing: 0.05em;
      padding: 12px 24px;
      border: none;
      background: transparent;
      color: #9B86C0;
      cursor: pointer;
      border-bottom: 3px solid transparent;
      transition: all 0.15s ease;
      white-space: nowrap;
    }
    .piq-main-tab:hover { color: #FFFFFF; background: rgba(255,255,255,0.05); }
    .piq-main-tab.active {
      color: #F5A623;
      border-bottom-color: #F5A623;
      background: rgba(245,166,35,0.08);
    }

    /* ── Main panels ── */
    .piq-main-panel        { display: none; }
    .piq-main-panel.active { display: block; }

    /* ── Sub nav — INSIDE each main panel, sticky below main nav ── */
    .piq-sub-nav {
      background: var(--bg-card);
      border-bottom: 1px solid var(--border);
      padding: 8px 32px;
      display: flex;
      gap: 4px;
      flex-wrap: wrap;
      position: sticky;
      top: 51px;
      z-index: 99;
    }
    .piq-sub-tab {
      font-family: 'Barlow Condensed', sans-serif;
      font-size: 13px;
      font-weight: 700;
      padding: 5px 12px;
      border: 1px solid var(--border);
      border-radius: 4px;
      background: #FFFFFF;
      color: var(--muted);
      cursor: pointer;
      transition: all 0.12s ease;
      white-space: nowrap;
    }
    .piq-sub-tab:hover {
      background: var(--bg-surface);
      color: var(--navy);
      border-color: var(--purple-mid);
    }
    .piq-sub-tab.active {
      background: #534AB7;
      color: #FFFFFF;
      border-color: #534AB7;
    }

    /* ── Sub panels ── */
    .piq-sub-panel        { display: none; }
    .piq-sub-panel.active { display: block; }

    /* ── Content ── */
    .piq-content { padding: 20px 32px; background: var(--bg); }

    /* ── Panel header ── */
    .piq-panel-header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--border);
    }
    .piq-panel-title {
      font-family: 'Barlow Condensed', sans-serif;
      font-size: 20px;
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
      width: 8px; height: 8px;
      border-radius: 50%;
      background: #D4870A;
      flex-shrink: 0;
    }

    /* ── Reactable ── */
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
      .piq-header   { padding: 16px; }
      .piq-main-nav { padding: 0 16px; }
      .piq-sub-nav  { padding: 8px 16px; top: 44px; }
      .piq-content  { padding: 16px; }
    }
  "))
  
  # ── JavaScript ────────────────────────────────────────────────────────────────
  # Sub nav lives INSIDE each main panel so only one is ever visible
  js <- htmltools::tags$script(HTML("
    function showMain(id) {
      document.querySelectorAll('.piq-main-panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.piq-main-tab').forEach(t => t.classList.remove('active'));
      document.getElementById('main-' + id).classList.add('active');
      document.getElementById('main-tab-' + id).classList.add('active');
      // Activate the first sub tab within this panel
      var panel    = document.getElementById('main-' + id);
      var subPanels = panel.querySelectorAll('.piq-sub-panel');
      var subTabs   = panel.querySelectorAll('.piq-sub-tab');
      subPanels.forEach(p => p.classList.remove('active'));
      subTabs.forEach(t => t.classList.remove('active'));
      if (subPanels[0]) subPanels[0].classList.add('active');
      if (subTabs[0])   subTabs[0].classList.add('active');
    }

    function showSub(mainId, subId) {
      var panel = document.getElementById('main-' + mainId);
      panel.querySelectorAll('.piq-sub-panel').forEach(p => p.classList.remove('active'));
      panel.querySelectorAll('.piq-sub-tab').forEach(t => t.classList.remove('active'));
      document.getElementById('sub-' + mainId + '-' + subId).classList.add('active');
      document.getElementById('sub-tab-' + mainId + '-' + subId).classList.add('active');
    }

    document.addEventListener('DOMContentLoaded', function() {
      showMain('reg');
    });
  "))
  
  # ── Header ───────────────────────────────────────────────────────────────────
  header <- htmltools::tags$div(
    class = "piq-header",
    htmltools::tags$div(
      class = "piq-logo",
      htmltools::tags$div(class = "piq-logo-text",
                          "Prop", htmltools::tags$span("IQ")),
      htmltools::tags$div(class = "piq-badge", "HIT RATE STATS")
    ),
    htmltools::tags$div(
      class = "piq-header-right",
      htmltools::tags$div(class = "piq-date",     date_label),
      htmltools::tags$div(class = "piq-matchups", matchup_str)
    )
  )
  
  # ── Main nav (Regular Season / Playoffs) ──────────────────────────────────────
  main_nav <- htmltools::tags$div(
    class = "piq-main-nav",
    lapply(splits, function(s) {
      htmltools::tags$button(
        id      = paste0("main-tab-", s$id),
        class   = "piq-main-tab",
        onclick = paste0("showMain('", s$id, "')"),
        s$label
      )
    })
  )
  
  # ── Main panels — sub nav INSIDE each panel ───────────────────────────────────
  main_panels <- lapply(splits, function(s) {
    
    # Sub nav for this split — lives inside the main panel
    sub_nav <- htmltools::tags$div(
      class = "piq-sub-nav",
      lapply(metrics, function(m) {
        htmltools::tags$button(
          id      = paste0("sub-tab-", s$id, "-", m$id),
          class   = "piq-sub-tab",
          onclick = paste0("showSub('", s$id, "','", m$id, "')"),
          m$label
        )
      })
    )
    
    # Sub panels — one per metric
    sub_panels <- lapply(metrics, function(m) {
      
      filtered_data <- tryCatch(
        filter_split(m$data, s$filter),
        error = function(e) tibble()
      )
      
      n_rows    <- if (nrow(filtered_data) > 0) nrow(filtered_data) else 0
      n_players <- if (n_rows > 0 && m$player_col %in% names(filtered_data))
        dplyr::n_distinct(filtered_data[[m$player_col]]) else 0
      
      tbl <- build_stats_table(
        filtered_data,
        table_id     = paste0("rt-", s$id, "-", m$id),
        metric_label = m$label,
        player_col   = m$player_col
      )
      
      htmltools::tags$div(
        id    = paste0("sub-", s$id, "-", m$id),
        class = "piq-sub-panel",
        htmltools::tags$div(class = "piq-content",
                            htmltools::tags$div(
                              class = "piq-panel-header",
                              htmltools::tags$div(class = "piq-dot"),
                              htmltools::tags$div(
                                htmltools::tags$div(class = "piq-panel-title",
                                                    paste(s$label, "\u2014", m$label)),
                                htmltools::tags$div(class = "piq-panel-subtitle",
                                                    paste0(n_players, " players \u00b7 ",
                                                           n_rows, " rows \u00b7 ", date_short))
                              )
                            ),
                            legend,
                            tbl
        )
      )
    })
    
    # Main panel contains sub nav + all sub panels
    htmltools::tags$div(
      id    = paste0("main-", s$id),
      class = "piq-main-panel",
      sub_nav,
      sub_panels
    )
  })
  
  # ── Assemble page ─────────────────────────────────────────────────────────────
  page <- htmltools::tags$html(
    lang = "en",
    htmltools::tags$head(
      htmltools::tags$meta(charset = "UTF-8"),
      htmltools::tags$meta(name = "viewport",
                           content = "width=device-width, initial-scale=1.0"),
      htmltools::tags$title(paste0("PropBetIQ Stats \u2014 ", date_short)),
      css
    ),
    htmltools::tags$body(
      header,
      main_nav,
      main_panels,  # sub navs are inside here — not duplicated
      js
    )
  )
  
  save_self_contained(page, output_file)
  
  size_mb <- round(file.size(output_file) / 1024 / 1024, 1)
  message("PropIQ Stats saved to: ", output_file, " (", size_mb, " MB)")
  invisible(output_file)
}

# # ── Run ────────────────────────────────────────────────────────────────────────
# build_propiq_stats("propiq_stats.html")
