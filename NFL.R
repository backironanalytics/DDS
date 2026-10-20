pkgs <- c(
  "tidyverse", "nflfastR","nflreadr","nflplotR", "dplyr", "flexdashboard", "parallel",
  "rvest", "stringr", "caret", "ggplot2", "ggpubr", "knitr",
  "broom", "grid", "gridExtra", "formattable", "markdown",
  "magick", "highcharter", "extrafont", "cowplot", "fmsb",
  "shiny", "fontawesome", "bslib", "plotly", "ggbreak",
  "hoopR", "crosstalk", "htmltools","httr","jsonlite","progressr",
  "logger","glue","lubridate","arrow","future","furrr","stringi"
)

invisible(lapply(pkgs, library, character.only = TRUE))

# ── Config ────────────────────────────────────────────────────────────────────

current_season <- 2026
prior_season   <- 2025
seasontype     <- "REG"  # Regular season only — no preseason, no playoffs

# ── CHANGE 1: Add near the top of NFL.R ────────────────────────────────────

source(Sys.getenv("PROPBETIQ_ROTATION_FILE", "C:/Users/ccrai/OneDrive/Documents/DDS/url_rotation.R"))


# ── Load PBP with graceful handling of not-yet-available current season ─────
message("Loading prior season (", prior_season, ") PBP data...")
pbp_prior <- load_pbp(prior_season) %>% filter(season_type == "REG")

message("Attempting to load current season (", current_season, ") PBP data...")
pbp_current <- tryCatch({
  data <- load_pbp(current_season) %>% filter(season_type == "REG")
  if (nrow(data) == 0) {
    message("  Current season data exists but has 0 REG plays — treating as unavailable")
    NULL
  } else {
    message("  Loaded ", nrow(data), " current season plays")
    data
  }
}, error = function(e) {
  message("  Current season data not yet available: ", conditionMessage(e))
  NULL
})

if (!is.null(pbp_current)) {
  pbp_nfl <- bind_rows(pbp_prior, pbp_current)
  message("✓ Combined PBP: ", nrow(pbp_nfl), " REG plays across ", prior_season, " + ", current_season)
} else {
  pbp_nfl <- pbp_prior
  message("✓ Using only ", prior_season, " REG PBP data (", nrow(pbp_nfl), " plays) — current season not yet started")
}

# Recent form uses current season if it has data, else falls back to prior
recent_season <- if (any(pbp_nfl$season == current_season, na.rm = TRUE)) current_season else prior_season
message("Recent form calculations will use season: ", recent_season)

# ── Player identity: single source of truth ──────────────────────────────────

# Load rosters — always use most recent available for team assignments
rosters_current <- tryCatch(load_rosters(current_season), error = function(e) NULL)
rosters_source_season <- if (!is.null(rosters_current) && nrow(rosters_current) > 0) current_season else prior_season
message("Using ", rosters_source_season, " rosters for team assignments")


current_team <- load_rosters(rosters_source_season) %>%
  filter(!is.na(gsis_id)) %>%
  select(gsis_id, full_name, position, team, headshot_url) %>%
  distinct() %>%
  rename(
    player_id     = gsis_id,
    display_name  = full_name,
    current_team  = team
  ) %>%
  group_by(display_name) %>%
  mutate(
    display_name = ifelse(
      n_distinct(player_id) > 1,
      paste0(display_name, " · ", current_team),
      display_name
    )
  ) %>%
  ungroup()

# ── Team change detection — flag players whose team differs vs prior season ─
prior_team <- load_rosters(prior_season) %>%
  filter(!is.na(gsis_id)) %>%
  select(player_id = gsis_id, prior_team = team) %>%
  distinct(player_id, .keep_all = TRUE)

current_team <- current_team %>%
  left_join(prior_team, by = "player_id") %>%
  mutate(
    team_changed = !is.na(prior_team) & prior_team != current_team,
    team_change_note = ifelse(team_changed, paste0(prior_team, " → ", current_team), NA_character_)
  )

# ── Rookie detection ────────────────────────────────────────────────────────
# A rookie in the current season has no PBP appearances in the prior season
prior_season_players <- unique(c(
  pbp_prior$passer_player_id,
  pbp_prior$rusher_player_id,
  pbp_prior$receiver_id,
  pbp_prior$solo_tackle_1_player_id,
  pbp_prior$solo_tackle_2_player_id,
  pbp_prior$assist_tackle_1_player_id,
  pbp_prior$assist_tackle_2_player_id,
  pbp_prior$assist_tackle_3_player_id,
  pbp_prior$assist_tackle_4_player_id,
  pbp_prior$sack_player_id,
  pbp_prior$half_sack_1_player_id,
  pbp_prior$half_sack_2_player_id,
  pbp_prior$interception_player_id,
  pbp_prior$pass_defense_1_player_id,
  pbp_prior$pass_defense_2_player_id,
  pbp_prior$forced_fumble_player_1_player_id,
  pbp_prior$fumble_recovery_1_player_id
))
prior_season_players <- prior_season_players[!is.na(prior_season_players)]

# Fallback if `gather` deprecated:
# prior_season_players <- unique(c(
#   pbp_prior$passer_player_id,
#   pbp_prior$rusher_player_id,
#   pbp_prior$receiver_id
# ))
# prior_season_players <- prior_season_players[!is.na(prior_season_players)]

current_team <- current_team %>%
  mutate(
    is_rookie = !(player_id %in% prior_season_players)
  )

# ── Sacks ────────────────────────────────────────────────────────────────────


sack_attribution <- pbp_nfl %>%
  filter(sack == 1, season_type == seasontype) %>%
  select(
    game_id, play_id, week, season, posteam, defteam,
    sack_player_id, sack_player_name,
    half_sack_1_player_id, half_sack_1_player_name,
    half_sack_2_player_id, half_sack_2_player_name
  ) %>%
  
  # ── Full sacks (only when half_sack cols are both empty) ──────────────────
  mutate(
    is_full_sack = !is.na(sack_player_id) &
      is.na(half_sack_1_player_id) &
      is.na(half_sack_2_player_id)
  ) %>%
  
  # ── Pivot to long format ──────────────────────────────────────────────────
  {
    full_sacks <- filter(., is_full_sack) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id   = sack_player_id,
        player_name = sack_player_name,
        sack_credit = 1.0
      )
    
    half_sack_1 <- filter(., !is.na(half_sack_1_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id   = half_sack_1_player_id,
        player_name = half_sack_1_player_name,
        sack_credit = 0.5
      )
    
    half_sack_2 <- filter(., !is.na(half_sack_2_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id   = half_sack_2_player_id,
        player_name = half_sack_2_player_name,
        sack_credit = 0.5
      )
    
    bind_rows(full_sacks, half_sack_1, half_sack_2)
  } %>%
  arrange(week, game_id, play_id)

sack_attribution <- sack_attribution %>%
  left_join(
    pbp_nfl %>% select(game_id, home_team, away_team) %>% distinct(),
    by = "game_id"
  ) %>%
  mutate(
    location = case_when(
      defteam == home_team ~ "home",
      defteam == away_team ~ "away",
      TRUE                 ~ NA_character_
    )
  )


# ── Season totals by player ───────────────────────────────────────────────────
sack_totals <- sack_attribution %>%
  group_by(player_id, player_name) %>%
  summarise(
    games          = n_distinct(game_id),
    total_sacks    = sum(sack_credit),
    full_sacks     = sum(sack_credit == 1.0),
    half_sacks     = sum(sack_credit == 0.5),
    sacks_per_game = round(total_sacks / n_distinct(game_id), 2),
    .groups = "drop"
  ) %>%
  arrange(desc(total_sacks))

# ── Weekly breakdown by player ────────────────────────────────────────────────
sack_weekly <- sack_attribution %>%
  group_by(player_id, player_name, week, location) %>%
  summarise(
    sacks = sum(sack_credit),
    .groups = "drop"
  ) %>%
  arrange(player_name, week)


# ── Participation ────────────────────────────────────────────────


# Load participation for both seasons — combine to match PBP data
part_prior <- tryCatch(load_participation(prior_season, include_pbp = FALSE), error = function(e) NULL)
part_current <- tryCatch(load_participation(current_season, include_pbp = FALSE), error = function(e) NULL)

if (!is.null(part_current) && nrow(part_current) > 0) {
  participation <- bind_rows(part_prior, part_current)
} else {
  participation <- part_prior
}

# Get distinct player-game appearances (played at least 1 snap on defense)
# ── Step 1: Parse defense_players into long format (one row per player per play) ──
player_weeks_played <- participation %>%
  select(nflverse_game_id, play_id, defense_players, defense_names, possession_team) %>%
  mutate(
    player_id_list   = str_split(defense_players, ";"),
    player_name_list = str_split(defense_names, ";")
  ) %>%
  unnest(cols = c(player_id_list, player_name_list)) %>%
  rename(
    player_id   = player_id_list,
    player_name = player_name_list
  ) %>%
  left_join(
    pbp_nfl %>% 
      select(game_id, week, season, season_type, defteam) %>% 
      group_by(game_id) %>%
      slice(1) %>%
      ungroup(),
    by = c("nflverse_game_id" = "game_id")
  ) %>%
  select(nflverse_game_id, week, season, player_id, player_name, 
         defteam, season_type) %>%
  distinct(nflverse_game_id, player_id, .keep_all = TRUE)




# ── Step 2: Cross join with sack data ─────────────────────────────────────────
sack_weekly_full <- player_weeks_played %>%
  filter(season_type == seasontype) %>%
  left_join(
    sack_weekly %>% select(player_id, week, sacks, location),
    by = c("player_id", "week")
  ) %>%
  mutate(sacks = replace_na(sacks, 0), played = TRUE)

# ── Hit Rate Matrix ───────────────────────────────────────────────────────────
sack_hit_rates <- sack_weekly_full %>%
  filter(season_type == seasontype) %>%
  group_by(player_id, player_name) %>%
  summarise(
    weeks_played     = n_distinct(nflverse_game_id),
    total_sacks      = sum(sacks),
    sacks_per_game   = round(total_sacks / weeks_played, 3),
    over_0.5         = round(mean(sacks >= 0.5), 3),
    over_1.5         = round(mean(sacks >= 1.5), 3),
    over_2.5         = round(mean(sacks >= 2.5), 3),
    over_3.5         = round(mean(sacks >= 3.5), 3),
    zero_sack_games  = sum(sacks == 0),
    half_sack_games  = sum(sacks == 0.5),
    one_sack_games   = sum(sacks == 1.0),
    multi_sack_games = sum(sacks >= 2.0),
    max_sacks        = max(sacks),
    consistency      = round(mean(sacks > 0), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_sacks >= 1) %>%
  arrange(desc(total_sacks))


# ── Rolling form window (last 4 weeks) ────────────────────────────────────────
recent_form <- sack_weekly_full %>%
  group_by(player_id, player_name) %>%
  slice_max(order_by = week, n = 4) %>%
  summarise(
    last4_weeks    = n_distinct(nflverse_game_id),
    last4_sacks    = sum(sacks),
    last4_per_game = round(sum(sacks) / n_distinct(nflverse_game_id), 3),
    last4_over_0.5 = round(mean(sacks >= 0.5), 3),
    last4_over_1.5 = round(mean(sacks >= 1.5), 3),
    last4_over_2.5 = round(mean(sacks >= 2.5), 3),
    last4_over_3.5 = round(mean(sacks >= 3.5), 3),
    .groups = "drop"
  )

# ── Join season rates with recent form ────────────────────────────────────────
sack_matrix <- sack_hit_rates %>%
  left_join(
    recent_form %>% select(player_id, last4_sacks, last4_per_game,
                           last4_over_0.5, last4_over_1.5,
                           last4_over_2.5, last4_over_3.5),
    by = "player_id"
  ) %>%
  mutate(
    trend = case_when(
      last4_per_game > sacks_per_game * 1.25 ~ "heating_up",
      last4_per_game < sacks_per_game * 0.75 ~ "cooling_down",
      TRUE                                   ~ "stable"
    )
  )

# Get distinct player-game appearances (played at least 1 snap on offense)
# ── Step 1: Parse doffense_players into long format (one row per player per play) ──
player_weeks_played_offense <- participation %>%
  select(nflverse_game_id, play_id, offense_players, offense_names, possession_team) %>%
  mutate(
    player_id_list   = str_split(offense_players, ";"),
    player_name_list = str_split(offense_names, ";")
  ) %>%
  unnest(cols = c(player_id_list, player_name_list)) %>%
  rename(
    player_id   = player_id_list,
    player_name = player_name_list
  ) %>%
  left_join(
    pbp_nfl %>% select(game_id, week, season, season_type) %>% distinct(),
    by = c("nflverse_game_id" = "game_id")
  ) %>%
  select(nflverse_game_id, week, season, player_id, player_name, 
         posteam = possession_team, season_type) %>%
  distinct()

# ── Supplement from PBP when participation lags behind (common early in season) ──
missing_seasons <- setdiff(unique(pbp_nfl$season), unique(player_weeks_played_offense$season))
missing_seasons <- missing_seasons[!is.na(missing_seasons)]

if (length(missing_seasons) > 0) {
  message("⚠ Participation data missing for season(s): ", paste(missing_seasons, collapse = ", "),
          " — supplementing from PBP")
  
  # Get all offensive touches from PBP for missing seasons
  pbp_supplement <- pbp_nfl %>%
    filter(season %in% missing_seasons)
  
  # Passers
  passers_supp <- pbp_supplement %>%
    filter(!is.na(passer_player_id)) %>%
    transmute(nflverse_game_id = game_id, week, season, season_type,
              player_id = passer_player_id, player_name = passer_player_name,
              posteam)
  
  # Rushers
  rushers_supp <- pbp_supplement %>%
    filter(!is.na(rusher_player_id)) %>%
    transmute(nflverse_game_id = game_id, week, season, season_type,
              player_id = rusher_player_id, player_name = rusher_player_name,
              posteam)
  
  # Receivers
  receivers_supp <- pbp_supplement %>%
    filter(!is.na(receiver_player_id)) %>%
    transmute(nflverse_game_id = game_id, week, season, season_type,
              player_id = receiver_player_id, player_name = receiver_player_name,
              posteam)
  
  offense_supp <- bind_rows(passers_supp, rushers_supp, receivers_supp) %>%
    distinct()
  
  player_weeks_played_offense <- bind_rows(player_weeks_played_offense, offense_supp)
  
  message("  ✓ Added ", nrow(offense_supp), " player-week rows from PBP for missing seasons")
}

# ── Fallback: parse season/week from nflverse_game_id when the join failed ─────
# nflverse game_id format: "SEASON_WEEK_AWAY_HOME" e.g. "2025_01_DAL_PHI"
player_weeks_played_offense <- player_weeks_played_offense %>%
  mutate(
    parsed_season = as.integer(substr(nflverse_game_id, 1, 4)),
    parsed_week   = as.integer(substr(nflverse_game_id, 6, 7)),
    season = coalesce(season, parsed_season),
    week   = coalesce(week, parsed_week)
  ) %>%
  select(-parsed_season, -parsed_week)

# Verify no more NAs
na_after <- sum(is.na(player_weeks_played_offense$season))
message("Rows still missing season after parse fallback: ", na_after)
# ── QB Completions / Attempts / Pass TDS / Interceptions ────────────────────────────────────────────────────────────────────

passing_stats_weekly_total <- pbp_nfl %>% filter(season_type == seasontype, sack == 0, !is.na(passer_player_id)) %>%
  group_by(
    game_id, week, season, posteam, defteam,
    passer_player_id,passer_player_name
  ) %>% summarize(attempts = sum(pass_attempt),completions = sum(complete_pass),
                  touchdowns = sum(pass_touchdown), interceptions = sum(interception), 
                  long_comp = max(ifelse(is.na(passing_yards),0,passing_yards)),
                  pass_yards = sum(ifelse(is.na(passing_yards),0,passing_yards)),
                  .groups = "drop")

passing_stats_weekly_total <- passing_stats_weekly_total %>%
  left_join(
    pbp_nfl %>% select(game_id, home_team, away_team) %>% distinct(),
    by = "game_id"
  ) %>%
  mutate(
    location = case_when(
      posteam == home_team ~ "home",
      posteam == away_team ~ "away",
      TRUE                 ~ NA_character_
    )
  )

# ── Load rosters and filter to QBs ───────────────────────────────────────────
rosters <- load_rosters(rosters_source_season) %>%
  filter(position == "QB") %>%
  select(gsis_id, full_name, position) %>%
  distinct()

# ── Filter passing_stats_weekly_total to QBs only ────────────────────────────
passing_stats_weekly_total_qb <- passing_stats_weekly_total %>%
  inner_join(
    rosters %>% select(gsis_id, position),
    by = c("passer_player_id" = "gsis_id")
  )

# ── Overlay player_weeks_played_offense for zero-fill ────────────────────────
passing_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  inner_join(
    rosters %>% select(gsis_id, position),
    by = c("player_id" = "gsis_id")
  ) %>%
  left_join(
    passing_stats_weekly_total_qb %>%
      select(passer_player_id, week, attempts, completions,
             touchdowns, interceptions, pass_yards, location),
    by = c("player_id" = "passer_player_id", "week")
  ) %>%
  mutate(
    attempts      = replace_na(attempts, 0),
    completions   = replace_na(completions, 0),
    touchdowns    = replace_na(touchdowns, 0),
    interceptions = replace_na(interceptions, 0),
    pass_yards    = replace_na(pass_yards, 0)
  )

# ── Helper function to build hit rate columns from a sequence ─────────────────
make_hit_rates <- function(data, col, lines) {
  col_sym <- sym(col)
  map_dfc(lines, function(line) {
    col_name <- paste0("over_", gsub("\\.", "p", as.character(line)))
    data %>%
      summarise(!!col_name := round(mean(!!col_sym >= line), 3)) %>%
      pull(!!col_name) %>%
      tibble(!!col_name := .)
  })
}

# ── Passing hit rate matrix ───────────────────────────────────────────────────
passing_hit_rates <- passing_weekly_full %>%
  group_by(player_id, player_name) %>%
  summarise(
    weeks_played        = n_distinct(nflverse_game_id),
    total_attempts      = sum(attempts),
    total_completions   = sum(completions),
    total_touchdowns    = sum(touchdowns),
    total_interceptions = sum(interceptions),
    total_pass_yards    = sum(pass_yards),
    avg_attempts        = round(total_attempts      / weeks_played, 2),
    avg_completions     = round(total_completions   / weeks_played, 2),
    avg_touchdowns      = round(total_touchdowns    / weeks_played, 2),
    avg_interceptions   = round(total_interceptions / weeks_played, 2),
    avg_pass_yards      = round(total_pass_yards    / weeks_played, 2),
    over_td_0p5         = round(mean(touchdowns >= 0.5), 3),
    over_td_1p5         = round(mean(touchdowns >= 1.5), 3),
    over_td_2p5         = round(mean(touchdowns >= 2.5), 3),
    over_td_3p5         = round(mean(touchdowns >= 3.5), 3),
    over_td_4p5         = round(mean(touchdowns >= 4.5), 3),
    over_int_0p5        = round(mean(interceptions >= 0.5), 3),
    over_int_1p5        = round(mean(interceptions >= 1.5), 3),
    over_int_2p5        = round(mean(interceptions >= 2.5), 3),
    over_int_3p5        = round(mean(interceptions >= 3.5), 3),
    over_cmp_13p5       = round(mean(completions >= 13.5), 3),
    over_cmp_14p5       = round(mean(completions >= 14.5), 3),
    over_cmp_15p5       = round(mean(completions >= 15.5), 3),
    over_cmp_16p5       = round(mean(completions >= 16.5), 3),
    over_cmp_17p5       = round(mean(completions >= 17.5), 3),
    over_cmp_18p5       = round(mean(completions >= 18.5), 3),
    over_cmp_19p5       = round(mean(completions >= 19.5), 3),
    over_cmp_20p5       = round(mean(completions >= 20.5), 3),
    over_cmp_21p5       = round(mean(completions >= 21.5), 3),
    over_cmp_22p5       = round(mean(completions >= 22.5), 3),
    over_cmp_23p5       = round(mean(completions >= 23.5), 3),
    over_cmp_24p5       = round(mean(completions >= 24.5), 3),
    over_cmp_25p5       = round(mean(completions >= 25.5), 3),
    over_cmp_26p5       = round(mean(completions >= 26.5), 3),
    over_cmp_27p5       = round(mean(completions >= 27.5), 3),
    over_cmp_28p5       = round(mean(completions >= 28.5), 3),
    over_cmp_29p5       = round(mean(completions >= 29.5), 3),
    over_cmp_30p5       = round(mean(completions >= 30.5), 3),
    over_cmp_31p5       = round(mean(completions >= 31.5), 3),
    over_cmp_32p5       = round(mean(completions >= 32.5), 3),
    over_cmp_33p5       = round(mean(completions >= 33.5), 3),
    over_cmp_34p5       = round(mean(completions >= 34.5), 3),
    over_cmp_35p5       = round(mean(completions >= 35.5), 3),
    over_cmp_36p5       = round(mean(completions >= 36.5), 3),
    over_cmp_37p5       = round(mean(completions >= 37.5), 3),
    over_cmp_38p5       = round(mean(completions >= 38.5), 3),
    over_cmp_39p5       = round(mean(completions >= 39.5), 3),
    over_att_19p5       = round(mean(attempts >= 19.5), 3),
    over_att_20p5       = round(mean(attempts >= 20.5), 3),
    over_att_21p5       = round(mean(attempts >= 21.5), 3),
    over_att_22p5       = round(mean(attempts >= 22.5), 3),
    over_att_23p5       = round(mean(attempts >= 23.5), 3),
    over_att_24p5       = round(mean(attempts >= 24.5), 3),
    over_att_25p5       = round(mean(attempts >= 25.5), 3),
    over_att_26p5       = round(mean(attempts >= 26.5), 3),
    over_att_27p5       = round(mean(attempts >= 27.5), 3),
    over_att_28p5       = round(mean(attempts >= 28.5), 3),
    over_att_29p5       = round(mean(attempts >= 29.5), 3),
    over_att_30p5       = round(mean(attempts >= 30.5), 3),
    over_att_31p5       = round(mean(attempts >= 31.5), 3),
    over_att_32p5       = round(mean(attempts >= 32.5), 3),
    over_att_33p5       = round(mean(attempts >= 33.5), 3),
    over_att_34p5       = round(mean(attempts >= 34.5), 3),
    over_att_35p5       = round(mean(attempts >= 35.5), 3),
    over_att_36p5       = round(mean(attempts >= 36.5), 3),
    over_att_37p5       = round(mean(attempts >= 37.5), 3),
    over_att_38p5       = round(mean(attempts >= 38.5), 3),
    over_att_39p5       = round(mean(attempts >= 39.5), 3),
    over_att_40p5       = round(mean(attempts >= 40.5), 3),
    over_att_41p5       = round(mean(attempts >= 41.5), 3),
    over_att_42p5       = round(mean(attempts >= 42.5), 3),
    over_att_43p5       = round(mean(attempts >= 43.5), 3),
    over_att_44p5       = round(mean(attempts >= 44.5), 3),
    over_att_45p5       = round(mean(attempts >= 45.5), 3),
    over_att_46p5       = round(mean(attempts >= 46.5), 3),
    over_att_47p5       = round(mean(attempts >= 47.5), 3),
    over_att_48p5       = round(mean(attempts >= 48.5), 3),
    over_att_49p5       = round(mean(attempts >= 49.5), 3),
    over_yds_149p5      = round(mean(pass_yards >= 149.5), 3),
    over_yds_154p5      = round(mean(pass_yards >= 154.5), 3),
    over_yds_159p5      = round(mean(pass_yards >= 159.5), 3),
    over_yds_164p5      = round(mean(pass_yards >= 164.5), 3),
    over_yds_169p5      = round(mean(pass_yards >= 169.5), 3),
    over_yds_174p5      = round(mean(pass_yards >= 174.5), 3),
    over_yds_179p5      = round(mean(pass_yards >= 179.5), 3),
    over_yds_184p5      = round(mean(pass_yards >= 184.5), 3),
    over_yds_189p5      = round(mean(pass_yards >= 189.5), 3),
    over_yds_194p5      = round(mean(pass_yards >= 194.5), 3),
    over_yds_199p5      = round(mean(pass_yards >= 199.5), 3),
    over_yds_204p5      = round(mean(pass_yards >= 204.5), 3),
    over_yds_209p5      = round(mean(pass_yards >= 209.5), 3),
    over_yds_214p5      = round(mean(pass_yards >= 214.5), 3),
    over_yds_219p5      = round(mean(pass_yards >= 219.5), 3),
    over_yds_224p5      = round(mean(pass_yards >= 224.5), 3),
    over_yds_229p5      = round(mean(pass_yards >= 229.5), 3),
    over_yds_234p5      = round(mean(pass_yards >= 234.5), 3),
    over_yds_239p5      = round(mean(pass_yards >= 239.5), 3),
    over_yds_244p5      = round(mean(pass_yards >= 244.5), 3),
    over_yds_249p5      = round(mean(pass_yards >= 249.5), 3),
    over_yds_254p5      = round(mean(pass_yards >= 254.5), 3),
    over_yds_259p5      = round(mean(pass_yards >= 259.5), 3),
    over_yds_264p5      = round(mean(pass_yards >= 264.5), 3),
    over_yds_269p5      = round(mean(pass_yards >= 269.5), 3),
    over_yds_274p5      = round(mean(pass_yards >= 274.5), 3),
    over_yds_279p5      = round(mean(pass_yards >= 279.5), 3),
    over_yds_284p5      = round(mean(pass_yards >= 284.5), 3),
    over_yds_289p5      = round(mean(pass_yards >= 289.5), 3),
    over_yds_294p5      = round(mean(pass_yards >= 294.5), 3),
    over_yds_299p5      = round(mean(pass_yards >= 299.5), 3),
    over_yds_304p5      = round(mean(pass_yards >= 304.5), 3),
    over_yds_309p5      = round(mean(pass_yards >= 309.5), 3),
    over_yds_314p5      = round(mean(pass_yards >= 314.5), 3),
    over_yds_319p5      = round(mean(pass_yards >= 319.5), 3),
    over_yds_324p5      = round(mean(pass_yards >= 324.5), 3),
    over_yds_329p5      = round(mean(pass_yards >= 329.5), 3),
    over_yds_334p5      = round(mean(pass_yards >= 334.5), 3),
    over_yds_339p5      = round(mean(pass_yards >= 339.5), 3),
    over_yds_344p5      = round(mean(pass_yards >= 344.5), 3),
    over_yds_349p5      = round(mean(pass_yards >= 349.5), 3),
    over_yds_354p5      = round(mean(pass_yards >= 354.5), 3),
    over_yds_359p5      = round(mean(pass_yards >= 359.5), 3),
    over_yds_364p5      = round(mean(pass_yards >= 364.5), 3),
    over_yds_369p5      = round(mean(pass_yards >= 369.5), 3),
    over_yds_374p5      = round(mean(pass_yards >= 374.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  arrange(desc(avg_pass_yards))

# ── Loop through quarters 1-4 for QB Stats ───────────────────────────────
qb_stats_by_qtr <- map(1:4, function(q) {
  pbp_nfl %>%
    filter(season_type == seasontype, sack == 0, !is.na(passer_player_id), qtr == q) %>%
    group_by(
      game_id, week, season, posteam, defteam,
      passer_player_id, passer_player_name
    ) %>%
    summarize(attempts = sum(pass_attempt),completions = sum(complete_pass),
              touchdowns = sum(pass_touchdown), interceptions = sum(interception), 
              long_comp = max(ifelse(is.na(passing_yards),0,passing_yards)),
              pass_yards = sum(ifelse(is.na(passing_yards),0,passing_yards)),
              .groups = "drop") %>%
    mutate(quarter = q)
}) %>%
  set_names(paste0("qtr_", 1:4))

qb_stats_all_qtrs <- bind_rows(qb_stats_by_qtr)



# ── Rushing Yards (including QBs) ────────────────────────────────────────────────────────────────────

rushing_weekly_total <- pbp_nfl %>% filter(season_type == seasontype, !is.na(rushing_yards)) %>%
  group_by(
    game_id, week, season, posteam, defteam,
    rusher_player_id,rusher_player_name
  ) %>% summarize(rushing_yards = sum(rushing_yards),carries = sum(rush_attempt),
                  longest_rush = max(rushing_yards), .groups = "drop") 

rushing_weekly_total <- rushing_weekly_total %>%
  left_join(
    pbp_nfl %>% select(game_id, home_team, away_team) %>% distinct(),
    by = "game_id"
  ) %>%
  mutate(
    location = case_when(
      posteam == home_team ~ "home",
      posteam == away_team ~ "away",
      TRUE                 ~ NA_character_
    )
  )


# ── Rushing weekly full (with zero-fill) ──────────────────────────────────────
rushing_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  left_join(
    rushing_weekly_total %>%
      select(rusher_player_id, week, rushing_yards, carries, location),
    by = c("player_id" = "rusher_player_id", "week")
  ) %>%
  mutate(
    rushing_yards = replace_na(rushing_yards, 0),
    carries       = replace_na(carries, 0)
  )

# ── Rushing hit rate matrix ───────────────────────────────────────────────────
rushing_hit_rates <- rushing_weekly_full %>%
  group_by(player_id, player_name) %>%
  summarise(
    weeks_played     = n_distinct(nflverse_game_id),
    total_rush_yards = sum(rushing_yards),
    total_carries    = sum(carries),
    avg_rush_yards   = round(total_rush_yards / weeks_played, 2),
    avg_carries      = round(total_carries    / weeks_played, 2),
    over_ryd_9p5     = round(mean(rushing_yards >= 9.5),   3),
    over_ryd_14p5    = round(mean(rushing_yards >= 14.5),  3),
    over_ryd_19p5    = round(mean(rushing_yards >= 19.5),  3),
    over_ryd_24p5    = round(mean(rushing_yards >= 24.5),  3),
    over_ryd_29p5    = round(mean(rushing_yards >= 29.5),  3),
    over_ryd_34p5    = round(mean(rushing_yards >= 34.5),  3),
    over_ryd_39p5    = round(mean(rushing_yards >= 39.5),  3),
    over_ryd_44p5    = round(mean(rushing_yards >= 44.5),  3),
    over_ryd_49p5    = round(mean(rushing_yards >= 49.5),  3),
    over_ryd_54p5    = round(mean(rushing_yards >= 54.5),  3),
    over_ryd_59p5    = round(mean(rushing_yards >= 59.5),  3),
    over_ryd_64p5    = round(mean(rushing_yards >= 64.5),  3),
    over_ryd_69p5    = round(mean(rushing_yards >= 69.5),  3),
    over_ryd_74p5    = round(mean(rushing_yards >= 74.5),  3),
    over_ryd_79p5    = round(mean(rushing_yards >= 79.5),  3),
    over_ryd_84p5    = round(mean(rushing_yards >= 84.5),  3),
    over_ryd_89p5    = round(mean(rushing_yards >= 89.5),  3),
    over_ryd_94p5    = round(mean(rushing_yards >= 94.5),  3),
    over_ryd_99p5    = round(mean(rushing_yards >= 99.5),  3),
    over_ryd_104p5   = round(mean(rushing_yards >= 104.5), 3),
    over_ryd_109p5   = round(mean(rushing_yards >= 109.5), 3),
    over_ryd_114p5   = round(mean(rushing_yards >= 114.5), 3),
    over_ryd_119p5   = round(mean(rushing_yards >= 119.5), 3),
    over_ryd_124p5   = round(mean(rushing_yards >= 124.5), 3),
    over_ryd_129p5   = round(mean(rushing_yards >= 129.5), 3),
    over_ryd_134p5   = round(mean(rushing_yards >= 134.5), 3),
    over_ryd_139p5   = round(mean(rushing_yards >= 139.5), 3),
    over_ryd_144p5   = round(mean(rushing_yards >= 144.5), 3),
    over_ryd_149p5   = round(mean(rushing_yards >= 149.5), 3),
    over_car_4p5     = round(mean(carries >= 4.5),  3),
    over_car_5p5     = round(mean(carries >= 5.5),  3),
    over_car_6p5     = round(mean(carries >= 6.5),  3),
    over_car_7p5     = round(mean(carries >= 7.5),  3),
    over_car_8p5     = round(mean(carries >= 8.5),  3),
    over_car_9p5     = round(mean(carries >= 9.5),  3),
    over_car_10p5    = round(mean(carries >= 10.5), 3),
    over_car_11p5    = round(mean(carries >= 11.5), 3),
    over_car_12p5    = round(mean(carries >= 12.5), 3),
    over_car_13p5    = round(mean(carries >= 13.5), 3),
    over_car_14p5    = round(mean(carries >= 14.5), 3),
    over_car_15p5    = round(mean(carries >= 15.5), 3),
    over_car_16p5    = round(mean(carries >= 16.5), 3),
    over_car_17p5    = round(mean(carries >= 17.5), 3),
    over_car_18p5    = round(mean(carries >= 18.5), 3),
    over_car_19p5    = round(mean(carries >= 19.5), 3),
    over_car_20p5    = round(mean(carries >= 20.5), 3),
    over_car_21p5    = round(mean(carries >= 21.5), 3),
    over_car_22p5    = round(mean(carries >= 22.5), 3),
    over_car_23p5    = round(mean(carries >= 23.5), 3),
    over_car_24p5    = round(mean(carries >= 24.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_carries >= 4) %>%
  arrange(desc(avg_rush_yards))


# ── Loop through quarters 1-4 for rushing yards ───────────────────────────────
rushing_yards_by_qtr <- map(1:4, function(q) {
  pbp_nfl %>%
    filter(season_type == seasontype, !is.na(rushing_yards), qtr == q) %>%
    group_by(
      game_id, week, season, posteam, defteam,
      rusher_player_id, rusher_player_name
    ) %>%
    summarize(rushing_yards = sum(rushing_yards),carries = sum(rush_attempt),longest_rush = max(rushing_yards),
              .groups = "drop") %>%
    mutate(quarter = q)
}) %>%
  set_names(paste0("qtr_", 1:4))

rushing_yards_all_qtrs <- bind_rows(rushing_yards_by_qtr)



# ── Receiving Yards ───────────────────────────────

receiving_weekly_total <- pbp_nfl %>% filter(season_type == seasontype, !is.na(receiver_id)) %>%
  group_by(
    game_id, week, season, posteam, defteam,
    receiver_player_id,receiver_player_name
  ) %>% summarize(rec_yrds = sum(ifelse(is.na(receiving_yards),0,receiving_yards)), receptions = sum(complete_pass),
                  long_reception = max(ifelse(is.na(receiving_yards),0,receiving_yards)),
                  targets = sum(pass_attempt),tds = sum(pass_touchdown),.groups = "drop")

receiving_weekly_total <- receiving_weekly_total %>%
  left_join(
    pbp_nfl %>% select(game_id, home_team, away_team) %>% distinct(),
    by = "game_id"
  ) %>%
  mutate(
    location = case_when(
      posteam == home_team ~ "home",
      posteam == away_team ~ "away",
      TRUE                 ~ NA_character_
    )
  )

# ── Receiving weekly full (with zero-fill) ────────────────────────────────────
receiving_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  left_join(
    receiving_weekly_total %>%
      select(receiver_player_id, week, rec_yrds, receptions, targets, location),
    by = c("player_id" = "receiver_player_id", "week")
  ) %>%
  mutate(
    rec_yrds   = replace_na(rec_yrds, 0),
    receptions = replace_na(receptions, 0),
    targets    = replace_na(targets, 0)
  )

# ── Receiving hit rate matrix ─────────────────────────────────────────────────
receiving_hit_rates <- receiving_weekly_full %>%
  group_by(player_id, player_name) %>%
  summarise(
    weeks_played     = n_distinct(nflverse_game_id),
    total_rec_yards  = sum(rec_yrds),
    total_receptions = sum(receptions),
    total_targets    = sum(targets),
    avg_rec_yards    = round(total_rec_yards  / weeks_played, 2),
    avg_receptions   = round(total_receptions / weeks_played, 2),
    avg_targets      = round(total_targets    / weeks_played, 2),
    over_ryd_9p5     = round(mean(rec_yrds >= 9.5),   3),
    over_ryd_14p5    = round(mean(rec_yrds >= 14.5),  3),
    over_ryd_19p5    = round(mean(rec_yrds >= 19.5),  3),
    over_ryd_24p5    = round(mean(rec_yrds >= 24.5),  3),
    over_ryd_29p5    = round(mean(rec_yrds >= 29.5),  3),
    over_ryd_34p5    = round(mean(rec_yrds >= 34.5),  3),
    over_ryd_39p5    = round(mean(rec_yrds >= 39.5),  3),
    over_ryd_44p5    = round(mean(rec_yrds >= 44.5),  3),
    over_ryd_49p5    = round(mean(rec_yrds >= 49.5),  3),
    over_ryd_54p5    = round(mean(rec_yrds >= 54.5),  3),
    over_ryd_59p5    = round(mean(rec_yrds >= 59.5),  3),
    over_ryd_64p5    = round(mean(rec_yrds >= 64.5),  3),
    over_ryd_69p5    = round(mean(rec_yrds >= 69.5),  3),
    over_ryd_74p5    = round(mean(rec_yrds >= 74.5),  3),
    over_ryd_79p5    = round(mean(rec_yrds >= 79.5),  3),
    over_ryd_84p5    = round(mean(rec_yrds >= 84.5),  3),
    over_ryd_89p5    = round(mean(rec_yrds >= 89.5),  3),
    over_ryd_94p5    = round(mean(rec_yrds >= 94.5),  3),
    over_ryd_99p5    = round(mean(rec_yrds >= 99.5),  3),
    over_ryd_104p5   = round(mean(rec_yrds >= 104.5), 3),
    over_ryd_109p5   = round(mean(rec_yrds >= 109.5), 3),
    over_ryd_114p5   = round(mean(rec_yrds >= 114.5), 3),
    over_ryd_119p5   = round(mean(rec_yrds >= 119.5), 3),
    over_ryd_124p5   = round(mean(rec_yrds >= 124.5), 3),
    over_ryd_129p5   = round(mean(rec_yrds >= 129.5), 3),
    over_ryd_134p5   = round(mean(rec_yrds >= 134.5), 3),
    over_ryd_139p5   = round(mean(rec_yrds >= 139.5), 3),
    over_ryd_144p5   = round(mean(rec_yrds >= 144.5), 3),
    over_ryd_149p5   = round(mean(rec_yrds >= 149.5), 3),
    over_rec_0p5     = round(mean(receptions >= 0.5),  3),
    over_rec_1p5     = round(mean(receptions >= 1.5),  3),
    over_rec_2p5     = round(mean(receptions >= 2.5),  3),
    over_rec_3p5     = round(mean(receptions >= 3.5),  3),
    over_rec_4p5     = round(mean(receptions >= 4.5),  3),
    over_rec_5p5     = round(mean(receptions >= 5.5),  3),
    over_rec_6p5     = round(mean(receptions >= 6.5),  3),
    over_rec_7p5     = round(mean(receptions >= 7.5),  3),
    over_rec_8p5     = round(mean(receptions >= 8.5),  3),
    over_rec_9p5     = round(mean(receptions >= 9.5),  3),
    over_rec_10p5    = round(mean(receptions >= 10.5), 3),
    over_rec_11p5    = round(mean(receptions >= 11.5), 3),
    over_rec_12p5    = round(mean(receptions >= 12.5), 3),
    over_tgt_0p5     = round(mean(targets >= 0.5),  3),
    over_tgt_1p5     = round(mean(targets >= 1.5),  3),
    over_tgt_2p5     = round(mean(targets >= 2.5),  3),
    over_tgt_3p5     = round(mean(targets >= 3.5),  3),
    over_tgt_4p5     = round(mean(targets >= 4.5),  3),
    over_tgt_5p5     = round(mean(targets >= 5.5),  3),
    over_tgt_6p5     = round(mean(targets >= 6.5),  3),
    over_tgt_7p5     = round(mean(targets >= 7.5),  3),
    over_tgt_8p5     = round(mean(targets >= 8.5),  3),
    over_tgt_9p5     = round(mean(targets >= 9.5),  3),
    over_tgt_10p5    = round(mean(targets >= 10.5), 3),
    over_tgt_11p5    = round(mean(targets >= 11.5), 3),
    over_tgt_12p5    = round(mean(targets >= 12.5), 3),
    over_tgt_13p5    = round(mean(targets >= 13.5), 3),
    over_tgt_14p5    = round(mean(targets >= 14.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_targets >= 4) %>%
  arrange(desc(avg_rec_yards))


# ── Loop through quarters 1-4 for receiving yards ───────────────────────────────
receiving_yards_by_qtr <- map(1:4, function(q) {
  pbp_nfl %>%
    filter(season_type == seasontype, !is.na(receiver_id), qtr == q) %>%
    group_by(
      game_id, week, season, posteam, defteam,
      receiver_player_id,receiver_player_name
    ) %>%
    summarize(rec_yrds = sum(ifelse(is.na(receiving_yards),0,receiving_yards)), receptions = sum(complete_pass),
              long_reception = max(ifelse(is.na(receiving_yards),0,receiving_yards)),
              targets = sum(pass_attempt),tds = sum(pass_touchdown),.groups = "drop") %>%
    mutate(quarter = q)
}) %>%
  set_names(paste0("qtr_", 1:4))

receiving_yards_all_qtrs <- bind_rows(receiving_yards_by_qtr)



# ── Tackles ───────────────────────────────

# ── Tackle Attribution ────────────────────────────────────────────────────────
tackle_attribution <- pbp_nfl %>%
  filter(season_type == seasontype) %>%
  filter(solo_tackle == 1 | tackle_with_assist == 1 | assist_tackle == 1) %>%
  select(
    game_id, play_id, week, season, posteam, defteam,
    solo_tackle,
    solo_tackle_1_player_id, solo_tackle_1_player_name, solo_tackle_1_team,
    solo_tackle_2_player_id, solo_tackle_2_player_name, solo_tackle_2_team,
    tackle_with_assist,
    tackle_with_assist_1_player_id, tackle_with_assist_1_player_name, tackle_with_assist_1_team,
    tackle_with_assist_2_player_id, tackle_with_assist_2_player_name, tackle_with_assist_2_team,
    assist_tackle,
    assist_tackle_1_player_id, assist_tackle_1_player_name, assist_tackle_1_team,
    assist_tackle_2_player_id, assist_tackle_2_player_name, assist_tackle_2_team,
    assist_tackle_3_player_id, assist_tackle_3_player_name, assist_tackle_3_team,
    assist_tackle_4_player_id, assist_tackle_4_player_name, assist_tackle_4_team
  ) %>%
  {
    solo_1 <- filter(., !is.na(solo_tackle_1_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = solo_tackle_1_player_id,
        player_name   = solo_tackle_1_player_name,
        team          = solo_tackle_1_team,
        tackle_credit = 1.0,
        is_solo       = TRUE
      )
    
    solo_2 <- filter(., !is.na(solo_tackle_2_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = solo_tackle_2_player_id,
        player_name   = solo_tackle_2_player_name,
        team          = solo_tackle_2_team,
        tackle_credit = 1.0,
        is_solo       = TRUE
      )
    
    # ── tackle_with_assist = 1.0 each (counts as a full tackle) ──────────────
    twa_1 <- filter(., !is.na(tackle_with_assist_1_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = tackle_with_assist_1_player_id,
        player_name   = tackle_with_assist_1_player_name,
        team          = tackle_with_assist_1_team,
        tackle_credit = 1.0,
        is_solo       = FALSE
      )
    
    twa_2 <- filter(., !is.na(tackle_with_assist_2_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = tackle_with_assist_2_player_id,
        player_name   = tackle_with_assist_2_player_name,
        team          = tackle_with_assist_2_team,
        tackle_credit = 1.0,
        is_solo       = FALSE
      )
    
    # ── assist_tackle = 1.0 each (counts as a full tackle) ───────────────────
    ast_1 <- filter(., !is.na(assist_tackle_1_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = assist_tackle_1_player_id,
        player_name   = assist_tackle_1_player_name,
        team          = assist_tackle_1_team,
        tackle_credit = 1.0,
        is_solo       = FALSE
      )
    
    ast_2 <- filter(., !is.na(assist_tackle_2_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = assist_tackle_2_player_id,
        player_name   = assist_tackle_2_player_name,
        team          = assist_tackle_2_team,
        tackle_credit = 1.0,
        is_solo       = FALSE
      )
    
    ast_3 <- filter(., !is.na(assist_tackle_3_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = assist_tackle_3_player_id,
        player_name   = assist_tackle_3_player_name,
        team          = assist_tackle_3_team,
        tackle_credit = 1.0,
        is_solo       = FALSE
      )
    
    ast_4 <- filter(., !is.na(assist_tackle_4_player_id)) %>%
      transmute(
        game_id, play_id, week, season, posteam, defteam,
        player_id     = assist_tackle_4_player_id,
        player_name   = assist_tackle_4_player_name,
        team          = assist_tackle_4_team,
        tackle_credit = 1.0,
        is_solo       = FALSE
      )
    
    bind_rows(solo_1, solo_2, twa_1, twa_2, ast_1, ast_2, ast_3, ast_4)
  } %>%
  
  # Deduplicate — same player appearing in multiple slots on the same play
  group_by(game_id, play_id, player_id) %>%
  slice_max(order_by = is_solo, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  arrange(week, game_id, play_id)

tackle_attribution <- tackle_attribution %>%
  left_join(
    pbp_nfl %>% select(game_id, home_team, away_team) %>% distinct(),
    by = "game_id"
  ) %>%
  mutate(
    location = case_when(
      defteam == home_team ~ "home",
      defteam == away_team ~ "away",
      TRUE                 ~ NA_character_
    )
  )


# ── Weekly summary by player ──────────────────────────────────────────────────
tackle_weekly <- tackle_attribution %>%
  group_by(player_id, player_name, week, location) %>%
  summarise(
    total_tackles  = sum(tackle_credit),
    solo_tackles   = sum(tackle_credit[is_solo == TRUE]),
    assist_tackles = sum(tackle_credit[is_solo == FALSE]),
    .groups = "drop"
  ) %>%
  arrange(player_name, week)

# ── Tackle weekly full (with zero-fill) ───────────────────────────────────────
tackle_weekly_full <- player_weeks_played %>%
  filter(season_type == seasontype) %>%
  left_join(
    tackle_weekly %>% select(player_id, week, total_tackles, solo_tackles, assist_tackles, location),
    by = c("player_id", "week")
  ) %>%
  mutate(
    total_tackles  = replace_na(total_tackles, 0),
    solo_tackles   = replace_na(solo_tackles, 0),
    assist_tackles = replace_na(assist_tackles, 0)
  )


# ── Tackle hit rate matrix ────────────────────────────────────────────────────
tackle_hit_rates <- tackle_weekly_full %>%
  group_by(player_id, player_name) %>%
  summarise(
    weeks_played   = n_distinct(nflverse_game_id),
    total_tackles  = sum(total_tackles),
    total_solo     = sum(solo_tackles),
    total_assists  = sum(assist_tackles),
    avg_tackles    = round(total_tackles / weeks_played, 2),
    avg_solo       = round(total_solo    / weeks_played, 2),
    avg_assists    = round(total_assists / weeks_played, 2),
    over_tkl_0p5   = round(mean(total_tackles >= 0.5),  3),
    over_tkl_1p5   = round(mean(total_tackles >= 1.5),  3),
    over_tkl_2p5   = round(mean(total_tackles >= 2.5),  3),
    over_tkl_3p5   = round(mean(total_tackles >= 3.5),  3),
    over_tkl_4p5   = round(mean(total_tackles >= 4.5),  3),
    over_tkl_5p5   = round(mean(total_tackles >= 5.5),  3),
    over_tkl_6p5   = round(mean(total_tackles >= 6.5),  3),
    over_tkl_7p5   = round(mean(total_tackles >= 7.5),  3),
    over_tkl_8p5   = round(mean(total_tackles >= 8.5),  3),
    over_tkl_9p5   = round(mean(total_tackles >= 9.5),  3),
    over_tkl_10p5  = round(mean(total_tackles >= 10.5), 3),
    over_tkl_11p5  = round(mean(total_tackles >= 11.5), 3),
    over_tkl_12p5  = round(mean(total_tackles >= 12.5), 3),
    over_tkl_13p5  = round(mean(total_tackles >= 13.5), 3),
    over_tkl_14p5  = round(mean(total_tackles >= 14.5), 3),
    over_solo_0p5  = round(mean(solo_tackles >= 0.5),   3),
    over_solo_1p5  = round(mean(solo_tackles >= 1.5),   3),
    over_solo_2p5  = round(mean(solo_tackles >= 2.5),   3),
    over_solo_3p5  = round(mean(solo_tackles >= 3.5),   3),
    over_solo_4p5  = round(mean(solo_tackles >= 4.5),   3),
    over_solo_5p5  = round(mean(solo_tackles >= 5.5),   3),
    over_solo_6p5  = round(mean(solo_tackles >= 6.5),   3),
    over_solo_7p5  = round(mean(solo_tackles >= 7.5),   3),
    over_solo_8p5  = round(mean(solo_tackles >= 8.5),   3),
    over_solo_9p5  = round(mean(solo_tackles >= 9.5),   3),
    over_solo_10p5 = round(mean(solo_tackles >= 10.5),  3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_tackles >= 4) %>%
  arrange(desc(avg_tackles))


# ── Interceptions ───────────────────────────────

interception_attribution <- pbp_nfl %>% filter(season_type == seasontype, !is.na(interception_player_id)) %>%
  group_by(
    game_id, week, season, posteam, defteam,
    interception_player_id,interception_player_name
  ) %>% summarize (interceptions = sum(interception), .groups = "drop")

interception_attribution <- interception_attribution %>%
  left_join(
    pbp_nfl %>% select(game_id, home_team, away_team) %>% distinct(),
    by = "game_id"
  ) %>%
  mutate(
    location = case_when(
      defteam == home_team ~ "home",
      defteam == away_team ~ "away",
      TRUE                 ~ NA_character_
    )
  )


# ── Step 2: Cross join with interception data ─────────────────────────────────────────
interception_weekly_full <- player_weeks_played %>%
  left_join(
    interception_attribution %>% select(interception_player_id, week, interceptions, location),
    by = c("player_id" = "interception_player_id", "week")
  ) %>%
  mutate(interceptions = replace_na(interceptions, 0))

# ── Hit Rate Matrix ───────────────────────────────────────────────────────────
interception_hit_rates <- interception_weekly_full %>%
  filter(season_type == seasontype) %>%
  group_by(player_id, player_name) %>%
  summarise(
    weeks_played            = n_distinct(nflverse_game_id),
    total_interceptions     = sum(interceptions),
    interceptions_per_game  = round(total_interceptions / weeks_played, 3),
    over_0.5                = round(mean(interceptions >= 0.5), 3),
    over_1.5                = round(mean(interceptions >= 1.5), 3),
    over_2.5                = round(mean(interceptions >= 2.5), 3),
    over_3.5                = round(mean(interceptions >= 3.5), 3),
    zero_interception_games = sum(interceptions == 0),
    one_interception_games  = sum(interceptions == 1.0),
    multi_interception_games = sum(interceptions >= 2.0),
    max_interception        = max(interceptions),
    consistency             = round(mean(interceptions > 0), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_interceptions >= 1) %>%
  arrange(desc(total_interceptions))




# ── Attach current team to all matrices ───────────────────────────────────────
sack_matrix            <- sack_matrix            %>% left_join(current_team, by = "player_id")
tackle_hit_rates       <- tackle_hit_rates       %>% left_join(current_team, by = "player_id")
passing_hit_rates      <- passing_hit_rates      %>% left_join(current_team, by = "player_id")
rushing_hit_rates      <- rushing_hit_rates      %>% left_join(current_team, by = "player_id")
receiving_hit_rates    <- receiving_hit_rates    %>% left_join(current_team, by = "player_id")
interception_hit_rates <- interception_hit_rates %>% left_join(current_team, by = "player_id")



# ══════════════════════════════════════════════════════════════════════════════
# ── Home / Away Split Hit Rates ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Sacks home/away ───────────────────────────────────────────────────────────
sacks_home_away <- sack_weekly_full %>%
  filter(season_type == seasontype, !is.na(location)) %>%
  group_by(player_id, player_name, location) %>%
  summarise(
    weeks_played = n_distinct(nflverse_game_id),
    total_sacks  = sum(sacks),
    avg_sacks    = round(total_sacks / weeks_played, 3),
    over_0p5     = round(mean(sacks >= 0.5), 3),
    over_1p5     = round(mean(sacks >= 1.5), 3),
    over_2p5     = round(mean(sacks >= 2.5), 3),
    over_3p5     = round(mean(sacks >= 3.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_sacks >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, location)

# ── Tackles home/away ─────────────────────────────────────────────────────────
tackles_home_away <- tackle_weekly_full %>%
  filter(!is.na(location)) %>%
  group_by(player_id, player_name, location) %>%
  summarise(
    weeks_played   = n_distinct(nflverse_game_id),
    avg_tackles    = round(sum(total_tackles)  / weeks_played, 2),
    avg_solo       = round(sum(solo_tackles)   / weeks_played, 2),
    avg_assists    = round(sum(assist_tackles) / weeks_played, 2),
    over_tkl_0p5   = round(mean(total_tackles >= 0.5),  3),
    over_tkl_1p5   = round(mean(total_tackles >= 1.5),  3),
    over_tkl_2p5   = round(mean(total_tackles >= 2.5),  3),
    over_tkl_3p5   = round(mean(total_tackles >= 3.5),  3),
    over_tkl_4p5   = round(mean(total_tackles >= 4.5),  3),
    over_tkl_5p5   = round(mean(total_tackles >= 5.5),  3),
    over_tkl_6p5   = round(mean(total_tackles >= 6.5),  3),
    over_tkl_7p5   = round(mean(total_tackles >= 7.5),  3),
    over_tkl_8p5   = round(mean(total_tackles >= 8.5),  3),
    over_tkl_9p5   = round(mean(total_tackles >= 9.5),  3),
    over_tkl_10p5  = round(mean(total_tackles >= 10.5), 3),
    over_tkl_11p5  = round(mean(total_tackles >= 11.5), 3),
    over_tkl_12p5  = round(mean(total_tackles >= 12.5), 3),
    over_tkl_13p5  = round(mean(total_tackles >= 13.5), 3),
    over_tkl_14p5  = round(mean(total_tackles >= 14.5), 3),
    over_solo_0p5  = round(mean(solo_tackles >= 0.5),   3),
    over_solo_1p5  = round(mean(solo_tackles >= 1.5),   3),
    over_solo_2p5  = round(mean(solo_tackles >= 2.5),   3),
    over_solo_3p5  = round(mean(solo_tackles >= 3.5),   3),
    over_solo_4p5  = round(mean(solo_tackles >= 4.5),   3),
    over_solo_5p5  = round(mean(solo_tackles >= 5.5),   3),
    over_solo_6p5  = round(mean(solo_tackles >= 6.5),   3),
    over_solo_7p5  = round(mean(solo_tackles >= 7.5),   3),
    over_solo_8p5  = round(mean(solo_tackles >= 8.5),   3),
    over_solo_9p5  = round(mean(solo_tackles >= 9.5),   3),
    over_solo_10p5 = round(mean(solo_tackles >= 10.5),  3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, location)

# ── Interceptions home/away ───────────────────────────────────────────────────
interceptions_home_away <- interception_weekly_full %>%
  filter(season_type == seasontype, !is.na(location)) %>%
  group_by(player_id, player_name, location) %>%
  summarise(
    weeks_played        = n_distinct(nflverse_game_id),
    total_interceptions = sum(interceptions),
    avg_interceptions   = round(total_interceptions / weeks_played, 3),
    over_0p5            = round(mean(interceptions >= 0.5), 3),
    over_1p5            = round(mean(interceptions >= 1.5), 3),
    over_2p5            = round(mean(interceptions >= 2.5), 3),
    over_3p5            = round(mean(interceptions >= 3.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_interceptions >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, location)

# ── Passing home/away ─────────────────────────────────────────────────────────
passing_home_away <- passing_weekly_full %>%
  filter(!is.na(location)) %>%
  group_by(player_id, player_name, location) %>%
  summarise(
    weeks_played        = n_distinct(nflverse_game_id),
    avg_pass_yards      = round(sum(pass_yards)    / weeks_played, 2),
    avg_completions     = round(sum(completions)   / weeks_played, 2),
    avg_attempts        = round(sum(attempts)      / weeks_played, 2),
    avg_touchdowns      = round(sum(touchdowns)    / weeks_played, 2),
    avg_interceptions   = round(sum(interceptions) / weeks_played, 2),
    over_td_0p5         = round(mean(touchdowns >= 0.5), 3),
    over_td_1p5         = round(mean(touchdowns >= 1.5), 3),
    over_td_2p5         = round(mean(touchdowns >= 2.5), 3),
    over_td_3p5         = round(mean(touchdowns >= 3.5), 3),
    over_td_4p5         = round(mean(touchdowns >= 4.5), 3),
    over_int_0p5        = round(mean(interceptions >= 0.5), 3),
    over_int_1p5        = round(mean(interceptions >= 1.5), 3),
    over_int_2p5        = round(mean(interceptions >= 2.5), 3),
    over_int_3p5        = round(mean(interceptions >= 3.5), 3),
    over_cmp_13p5       = round(mean(completions >= 13.5), 3),
    over_cmp_14p5       = round(mean(completions >= 14.5), 3),
    over_cmp_15p5       = round(mean(completions >= 15.5), 3),
    over_cmp_16p5       = round(mean(completions >= 16.5), 3),
    over_cmp_17p5       = round(mean(completions >= 17.5), 3),
    over_cmp_18p5       = round(mean(completions >= 18.5), 3),
    over_cmp_19p5       = round(mean(completions >= 19.5), 3),
    over_cmp_20p5       = round(mean(completions >= 20.5), 3),
    over_cmp_21p5       = round(mean(completions >= 21.5), 3),
    over_cmp_22p5       = round(mean(completions >= 22.5), 3),
    over_cmp_23p5       = round(mean(completions >= 23.5), 3),
    over_cmp_24p5       = round(mean(completions >= 24.5), 3),
    over_cmp_25p5       = round(mean(completions >= 25.5), 3),
    over_cmp_26p5       = round(mean(completions >= 26.5), 3),
    over_cmp_27p5       = round(mean(completions >= 27.5), 3),
    over_cmp_28p5       = round(mean(completions >= 28.5), 3),
    over_cmp_29p5       = round(mean(completions >= 29.5), 3),
    over_cmp_30p5       = round(mean(completions >= 30.5), 3),
    over_cmp_31p5       = round(mean(completions >= 31.5), 3),
    over_cmp_32p5       = round(mean(completions >= 32.5), 3),
    over_cmp_33p5       = round(mean(completions >= 33.5), 3),
    over_cmp_34p5       = round(mean(completions >= 34.5), 3),
    over_cmp_35p5       = round(mean(completions >= 35.5), 3),
    over_cmp_36p5       = round(mean(completions >= 36.5), 3),
    over_cmp_37p5       = round(mean(completions >= 37.5), 3),
    over_cmp_38p5       = round(mean(completions >= 38.5), 3),
    over_cmp_39p5       = round(mean(completions >= 39.5), 3),
    over_att_19p5       = round(mean(attempts >= 19.5), 3),
    over_att_20p5       = round(mean(attempts >= 20.5), 3),
    over_att_21p5       = round(mean(attempts >= 21.5), 3),
    over_att_22p5       = round(mean(attempts >= 22.5), 3),
    over_att_23p5       = round(mean(attempts >= 23.5), 3),
    over_att_24p5       = round(mean(attempts >= 24.5), 3),
    over_att_25p5       = round(mean(attempts >= 25.5), 3),
    over_att_26p5       = round(mean(attempts >= 26.5), 3),
    over_att_27p5       = round(mean(attempts >= 27.5), 3),
    over_att_28p5       = round(mean(attempts >= 28.5), 3),
    over_att_29p5       = round(mean(attempts >= 29.5), 3),
    over_att_30p5       = round(mean(attempts >= 30.5), 3),
    over_att_31p5       = round(mean(attempts >= 31.5), 3),
    over_att_32p5       = round(mean(attempts >= 32.5), 3),
    over_att_33p5       = round(mean(attempts >= 33.5), 3),
    over_att_34p5       = round(mean(attempts >= 34.5), 3),
    over_att_35p5       = round(mean(attempts >= 35.5), 3),
    over_att_36p5       = round(mean(attempts >= 36.5), 3),
    over_att_37p5       = round(mean(attempts >= 37.5), 3),
    over_att_38p5       = round(mean(attempts >= 38.5), 3),
    over_att_39p5       = round(mean(attempts >= 39.5), 3),
    over_att_40p5       = round(mean(attempts >= 40.5), 3),
    over_att_41p5       = round(mean(attempts >= 41.5), 3),
    over_att_42p5       = round(mean(attempts >= 42.5), 3),
    over_att_43p5       = round(mean(attempts >= 43.5), 3),
    over_att_44p5       = round(mean(attempts >= 44.5), 3),
    over_att_45p5       = round(mean(attempts >= 45.5), 3),
    over_att_46p5       = round(mean(attempts >= 46.5), 3),
    over_att_47p5       = round(mean(attempts >= 47.5), 3),
    over_att_48p5       = round(mean(attempts >= 48.5), 3),
    over_att_49p5       = round(mean(attempts >= 49.5), 3),
    over_yds_149p5      = round(mean(pass_yards >= 149.5), 3),
    over_yds_154p5      = round(mean(pass_yards >= 154.5), 3),
    over_yds_159p5      = round(mean(pass_yards >= 159.5), 3),
    over_yds_164p5      = round(mean(pass_yards >= 164.5), 3),
    over_yds_169p5      = round(mean(pass_yards >= 169.5), 3),
    over_yds_174p5      = round(mean(pass_yards >= 174.5), 3),
    over_yds_179p5      = round(mean(pass_yards >= 179.5), 3),
    over_yds_184p5      = round(mean(pass_yards >= 184.5), 3),
    over_yds_189p5      = round(mean(pass_yards >= 189.5), 3),
    over_yds_194p5      = round(mean(pass_yards >= 194.5), 3),
    over_yds_199p5      = round(mean(pass_yards >= 199.5), 3),
    over_yds_204p5      = round(mean(pass_yards >= 204.5), 3),
    over_yds_209p5      = round(mean(pass_yards >= 209.5), 3),
    over_yds_214p5      = round(mean(pass_yards >= 214.5), 3),
    over_yds_219p5      = round(mean(pass_yards >= 219.5), 3),
    over_yds_224p5      = round(mean(pass_yards >= 224.5), 3),
    over_yds_229p5      = round(mean(pass_yards >= 229.5), 3),
    over_yds_234p5      = round(mean(pass_yards >= 234.5), 3),
    over_yds_239p5      = round(mean(pass_yards >= 239.5), 3),
    over_yds_244p5      = round(mean(pass_yards >= 244.5), 3),
    over_yds_249p5      = round(mean(pass_yards >= 249.5), 3),
    over_yds_254p5      = round(mean(pass_yards >= 254.5), 3),
    over_yds_259p5      = round(mean(pass_yards >= 259.5), 3),
    over_yds_264p5      = round(mean(pass_yards >= 264.5), 3),
    over_yds_269p5      = round(mean(pass_yards >= 269.5), 3),
    over_yds_274p5      = round(mean(pass_yards >= 274.5), 3),
    over_yds_279p5      = round(mean(pass_yards >= 279.5), 3),
    over_yds_284p5      = round(mean(pass_yards >= 284.5), 3),
    over_yds_289p5      = round(mean(pass_yards >= 289.5), 3),
    over_yds_294p5      = round(mean(pass_yards >= 294.5), 3),
    over_yds_299p5      = round(mean(pass_yards >= 299.5), 3),
    over_yds_304p5      = round(mean(pass_yards >= 304.5), 3),
    over_yds_309p5      = round(mean(pass_yards >= 309.5), 3),
    over_yds_314p5      = round(mean(pass_yards >= 314.5), 3),
    over_yds_319p5      = round(mean(pass_yards >= 319.5), 3),
    over_yds_324p5      = round(mean(pass_yards >= 324.5), 3),
    over_yds_329p5      = round(mean(pass_yards >= 329.5), 3),
    over_yds_334p5      = round(mean(pass_yards >= 334.5), 3),
    over_yds_339p5      = round(mean(pass_yards >= 339.5), 3),
    over_yds_344p5      = round(mean(pass_yards >= 344.5), 3),
    over_yds_349p5      = round(mean(pass_yards >= 349.5), 3),
    over_yds_354p5      = round(mean(pass_yards >= 354.5), 3),
    over_yds_359p5      = round(mean(pass_yards >= 359.5), 3),
    over_yds_364p5      = round(mean(pass_yards >= 364.5), 3),
    over_yds_369p5      = round(mean(pass_yards >= 369.5), 3),
    over_yds_374p5      = round(mean(pass_yards >= 374.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, location)

# ── Rushing home/away ─────────────────────────────────────────────────────────
rushing_home_away <- rushing_weekly_full %>%
  filter(!is.na(location)) %>%
  group_by(player_id, player_name, location) %>%
  summarise(
    weeks_played     = n_distinct(nflverse_game_id),
    avg_rush_yards   = round(sum(rushing_yards) / weeks_played, 2),
    avg_carries      = round(sum(carries)       / weeks_played, 2),
    over_ryd_9p5     = round(mean(rushing_yards >= 9.5),   3),
    over_ryd_14p5    = round(mean(rushing_yards >= 14.5),  3),
    over_ryd_19p5    = round(mean(rushing_yards >= 19.5),  3),
    over_ryd_24p5    = round(mean(rushing_yards >= 24.5),  3),
    over_ryd_29p5    = round(mean(rushing_yards >= 29.5),  3),
    over_ryd_34p5    = round(mean(rushing_yards >= 34.5),  3),
    over_ryd_39p5    = round(mean(rushing_yards >= 39.5),  3),
    over_ryd_44p5    = round(mean(rushing_yards >= 44.5),  3),
    over_ryd_49p5    = round(mean(rushing_yards >= 49.5),  3),
    over_ryd_54p5    = round(mean(rushing_yards >= 54.5),  3),
    over_ryd_59p5    = round(mean(rushing_yards >= 59.5),  3),
    over_ryd_64p5    = round(mean(rushing_yards >= 64.5),  3),
    over_ryd_69p5    = round(mean(rushing_yards >= 69.5),  3),
    over_ryd_74p5    = round(mean(rushing_yards >= 74.5),  3),
    over_ryd_79p5    = round(mean(rushing_yards >= 79.5),  3),
    over_ryd_84p5    = round(mean(rushing_yards >= 84.5),  3),
    over_ryd_89p5    = round(mean(rushing_yards >= 89.5),  3),
    over_ryd_94p5    = round(mean(rushing_yards >= 94.5),  3),
    over_ryd_99p5    = round(mean(rushing_yards >= 99.5),  3),
    over_ryd_104p5   = round(mean(rushing_yards >= 104.5), 3),
    over_ryd_109p5   = round(mean(rushing_yards >= 109.5), 3),
    over_ryd_114p5   = round(mean(rushing_yards >= 114.5), 3),
    over_ryd_119p5   = round(mean(rushing_yards >= 119.5), 3),
    over_ryd_124p5   = round(mean(rushing_yards >= 124.5), 3),
    over_ryd_129p5   = round(mean(rushing_yards >= 129.5), 3),
    over_ryd_134p5   = round(mean(rushing_yards >= 134.5), 3),
    over_ryd_139p5   = round(mean(rushing_yards >= 139.5), 3),
    over_ryd_144p5   = round(mean(rushing_yards >= 144.5), 3),
    over_ryd_149p5   = round(mean(rushing_yards >= 149.5), 3),
    over_car_4p5     = round(mean(carries >= 4.5),  3),
    over_car_5p5     = round(mean(carries >= 5.5),  3),
    over_car_6p5     = round(mean(carries >= 6.5),  3),
    over_car_7p5     = round(mean(carries >= 7.5),  3),
    over_car_8p5     = round(mean(carries >= 8.5),  3),
    over_car_9p5     = round(mean(carries >= 9.5),  3),
    over_car_10p5    = round(mean(carries >= 10.5), 3),
    over_car_11p5    = round(mean(carries >= 11.5), 3),
    over_car_12p5    = round(mean(carries >= 12.5), 3),
    over_car_13p5    = round(mean(carries >= 13.5), 3),
    over_car_14p5    = round(mean(carries >= 14.5), 3),
    over_car_15p5    = round(mean(carries >= 15.5), 3),
    over_car_16p5    = round(mean(carries >= 16.5), 3),
    over_car_17p5    = round(mean(carries >= 17.5), 3),
    over_car_18p5    = round(mean(carries >= 18.5), 3),
    over_car_19p5    = round(mean(carries >= 19.5), 3),
    over_car_20p5    = round(mean(carries >= 20.5), 3),
    over_car_21p5    = round(mean(carries >= 21.5), 3),
    over_car_22p5    = round(mean(carries >= 22.5), 3),
    over_car_23p5    = round(mean(carries >= 23.5), 3),
    over_car_24p5    = round(mean(carries >= 24.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, location)

# ── Receiving home/away ───────────────────────────────────────────────────────
receiving_home_away <- receiving_weekly_full %>%
  filter(!is.na(location)) %>%
  group_by(player_id, player_name, location) %>%
  summarise(
    weeks_played     = n_distinct(nflverse_game_id),
    avg_rec_yards    = round(sum(rec_yrds)    / weeks_played, 2),
    avg_receptions   = round(sum(receptions)  / weeks_played, 2),
    avg_targets      = round(sum(targets)     / weeks_played, 2),
    over_ryd_9p5     = round(mean(rec_yrds >= 9.5),   3),
    over_ryd_14p5    = round(mean(rec_yrds >= 14.5),  3),
    over_ryd_19p5    = round(mean(rec_yrds >= 19.5),  3),
    over_ryd_24p5    = round(mean(rec_yrds >= 24.5),  3),
    over_ryd_29p5    = round(mean(rec_yrds >= 29.5),  3),
    over_ryd_34p5    = round(mean(rec_yrds >= 34.5),  3),
    over_ryd_39p5    = round(mean(rec_yrds >= 39.5),  3),
    over_ryd_44p5    = round(mean(rec_yrds >= 44.5),  3),
    over_ryd_49p5    = round(mean(rec_yrds >= 49.5),  3),
    over_ryd_54p5    = round(mean(rec_yrds >= 54.5),  3),
    over_ryd_59p5    = round(mean(rec_yrds >= 59.5),  3),
    over_ryd_64p5    = round(mean(rec_yrds >= 64.5),  3),
    over_ryd_69p5    = round(mean(rec_yrds >= 69.5),  3),
    over_ryd_74p5    = round(mean(rec_yrds >= 74.5),  3),
    over_ryd_79p5    = round(mean(rec_yrds >= 79.5),  3),
    over_ryd_84p5    = round(mean(rec_yrds >= 84.5),  3),
    over_ryd_89p5    = round(mean(rec_yrds >= 89.5),  3),
    over_ryd_94p5    = round(mean(rec_yrds >= 94.5),  3),
    over_ryd_99p5    = round(mean(rec_yrds >= 99.5),  3),
    over_ryd_104p5   = round(mean(rec_yrds >= 104.5), 3),
    over_ryd_109p5   = round(mean(rec_yrds >= 109.5), 3),
    over_ryd_114p5   = round(mean(rec_yrds >= 114.5), 3),
    over_ryd_119p5   = round(mean(rec_yrds >= 119.5), 3),
    over_ryd_124p5   = round(mean(rec_yrds >= 124.5), 3),
    over_ryd_129p5   = round(mean(rec_yrds >= 129.5), 3),
    over_ryd_134p5   = round(mean(rec_yrds >= 134.5), 3),
    over_ryd_139p5   = round(mean(rec_yrds >= 139.5), 3),
    over_ryd_144p5   = round(mean(rec_yrds >= 144.5), 3),
    over_ryd_149p5   = round(mean(rec_yrds >= 149.5), 3),
    over_rec_0p5     = round(mean(receptions >= 0.5),  3),
    over_rec_1p5     = round(mean(receptions >= 1.5),  3),
    over_rec_2p5     = round(mean(receptions >= 2.5),  3),
    over_rec_3p5     = round(mean(receptions >= 3.5),  3),
    over_rec_4p5     = round(mean(receptions >= 4.5),  3),
    over_rec_5p5     = round(mean(receptions >= 5.5),  3),
    over_rec_6p5     = round(mean(receptions >= 6.5),  3),
    over_rec_7p5     = round(mean(receptions >= 7.5),  3),
    over_rec_8p5     = round(mean(receptions >= 8.5),  3),
    over_rec_9p5     = round(mean(receptions >= 9.5),  3),
    over_rec_10p5    = round(mean(receptions >= 10.5), 3),
    over_rec_11p5    = round(mean(receptions >= 11.5), 3),
    over_rec_12p5    = round(mean(receptions >= 12.5), 3),
    over_tgt_0p5     = round(mean(targets >= 0.5),  3),
    over_tgt_1p5     = round(mean(targets >= 1.5),  3),
    over_tgt_2p5     = round(mean(targets >= 2.5),  3),
    over_tgt_3p5     = round(mean(targets >= 3.5),  3),
    over_tgt_4p5     = round(mean(targets >= 4.5),  3),
    over_tgt_5p5     = round(mean(targets >= 5.5),  3),
    over_tgt_6p5     = round(mean(targets >= 6.5),  3),
    over_tgt_7p5     = round(mean(targets >= 7.5),  3),
    over_tgt_8p5     = round(mean(targets >= 8.5),  3),
    over_tgt_9p5     = round(mean(targets >= 9.5),  3),
    over_tgt_10p5    = round(mean(targets >= 10.5), 3),
    over_tgt_11p5    = round(mean(targets >= 11.5), 3),
    over_tgt_12p5    = round(mean(targets >= 12.5), 3),
    over_tgt_13p5    = round(mean(targets >= 13.5), 3),
    over_tgt_14p5    = round(mean(targets >= 14.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, location)



# ══════════════════════════════════════════════════════════════════════════════
# ── Weather Parsing & Bucketing ───────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Parse weather string into structured fields ───────────────────────────────

game_weather <- pbp_nfl %>%
  filter(season_type == seasontype) %>%
  select(game_id, weather, roof) %>%
  distinct() %>%
  mutate(
    temp_f = as.numeric(str_extract(weather, "(?<=Temp:\\s)\\d+")),
    
    # First try: extract number after "Wind: [direction(s)] " — direction can be any letters
    wind_mph = as.numeric(str_extract(weather, regex("(?<=Wind:\\s)[A-Za-z]+\\s+\\d+", ignore_case = TRUE)) %>%
                            str_extract("\\d+")),
    
    # Fallback for "Wind: 9 mph" (no direction at all)
    wind_mph = if_else(
      is.na(wind_mph),
      as.numeric(str_extract(weather, "(?<=Wind:\\s)\\d+")),
      wind_mph
    ),
    
    has_rain  = str_detect(tolower(coalesce(weather, "")), "rain|shower|drizzle"),
    has_snow  = str_detect(tolower(coalesce(weather, "")), "snow|flurries|sleet"),
    is_indoor = roof %in% c("dome", "closed")
  ) %>%
  mutate(
    weather_bucket = case_when(
      is_indoor                              ~ "dome",
      has_snow                               ~ "snow",
      has_rain                               ~ "rain",
      !is.na(wind_mph) & wind_mph >= 20      ~ "heavy_wind",
      !is.na(wind_mph) & wind_mph >= 10      ~ "windy",
      !is.na(temp_f)   & temp_f   <= 25      ~ "cold_extreme",
      !is.na(wind_mph) & wind_mph < 10       ~ "clear",
      !is.na(temp_f)   & temp_f   <= 32      ~ "cold_extreme",
      !is.na(temp_f)                         ~ "clear",
      TRUE                                   ~ "unknown"
    )
  ) %>%
  select(game_id, weather_bucket, temp_f, wind_mph)

# ── Verify bucket distribution ────────────────────────────────────────────────
game_weather %>% count(weather_bucket)



# ── Add weather_bucket to every attribution table ─────────────────────────────
sack_attribution         <- sack_attribution         %>% left_join(game_weather %>% select(game_id, weather_bucket), by = "game_id")
tackle_attribution       <- tackle_attribution       %>% left_join(game_weather %>% select(game_id, weather_bucket), by = "game_id")
interception_attribution <- interception_attribution %>% left_join(game_weather %>% select(game_id, weather_bucket), by = "game_id")
passing_stats_weekly_total <- passing_stats_weekly_total %>% left_join(game_weather %>% select(game_id, weather_bucket), by = "game_id")
rushing_weekly_total     <- rushing_weekly_total     %>% left_join(game_weather %>% select(game_id, weather_bucket), by = "game_id")
receiving_weekly_total   <- receiving_weekly_total   %>% left_join(game_weather %>% select(game_id, weather_bucket), by = "game_id")


# ── Rebuild weekly tables to carry weather_bucket through ─────────────────────

# Sacks
sack_weekly <- sack_attribution %>%
  group_by(player_id, player_name, week, location, weather_bucket) %>%
  summarise(sacks = sum(sack_credit), .groups = "drop")

sack_weekly_full <- player_weeks_played %>%
  filter(season_type == seasontype) %>%
  left_join(sack_weekly %>% select(player_id, week, sacks, location, weather_bucket),
            by = c("player_id", "week")) %>%
  mutate(sacks = replace_na(sacks, 0))

# Tackles
tackle_weekly <- tackle_attribution %>%
  group_by(player_id, player_name, week, location, weather_bucket) %>%
  summarise(
    total_tackles  = sum(tackle_credit),
    solo_tackles   = sum(tackle_credit[is_solo == TRUE]),
    assist_tackles = sum(tackle_credit[is_solo == FALSE]),
    .groups = "drop"
  )

tackle_weekly_full <- player_weeks_played %>%
  filter(season_type == seasontype) %>%
  left_join(tackle_weekly %>% select(player_id, week, total_tackles, solo_tackles, assist_tackles, location, weather_bucket),
            by = c("player_id", "week")) %>%
  mutate(
    total_tackles  = replace_na(total_tackles, 0),
    solo_tackles   = replace_na(solo_tackles, 0),
    assist_tackles = replace_na(assist_tackles, 0)
  )

# Interceptions
interception_weekly_full <- player_weeks_played %>%
  left_join(interception_attribution %>% select(interception_player_id, week, interceptions, location, weather_bucket),
            by = c("player_id" = "interception_player_id", "week")) %>%
  mutate(interceptions = replace_na(interceptions, 0))

# Passing
passing_stats_weekly_total_qb <- passing_stats_weekly_total %>%
  inner_join(
    rosters %>% select(gsis_id, position),
    by = c("passer_player_id" = "gsis_id")
  )


passing_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  inner_join(rosters %>% select(gsis_id, position), by = c("player_id" = "gsis_id")) %>%
  left_join(
    passing_stats_weekly_total_qb %>%
      select(passer_player_id, week, attempts, completions,
             touchdowns, interceptions, pass_yards, location, weather_bucket),
    by = c("player_id" = "passer_player_id", "week")
  ) %>%
  mutate(
    attempts      = replace_na(attempts, 0),
    completions   = replace_na(completions, 0),
    touchdowns    = replace_na(touchdowns, 0),
    interceptions = replace_na(interceptions, 0),
    pass_yards    = replace_na(pass_yards, 0)
  )

# Rushing
rushing_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  left_join(rushing_weekly_total %>%
              select(rusher_player_id, week, rushing_yards, carries, location, weather_bucket),
            by = c("player_id" = "rusher_player_id", "week")) %>%
  mutate(
    rushing_yards = replace_na(rushing_yards, 0),
    carries       = replace_na(carries, 0)
  )

# Receiving
receiving_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  left_join(receiving_weekly_total %>%
              select(receiver_player_id, week, rec_yrds, receptions, targets, location, weather_bucket),
            by = c("player_id" = "receiver_player_id", "week")) %>%
  mutate(
    rec_yrds   = replace_na(rec_yrds, 0),
    receptions = replace_na(receptions, 0),
    targets    = replace_na(targets, 0)
  )



# ══════════════════════════════════════════════════════════════════════════════
# ── Weather Split Hit Rates ───────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Sacks by weather ──────────────────────────────────────────────────────────
sacks_weather <- sack_weekly_full %>%
  filter(season_type == seasontype, !is.na(weather_bucket), weather_bucket != "unknown") %>%
  group_by(player_id, player_name, weather_bucket) %>%
  summarise(
    weeks_played = n_distinct(nflverse_game_id),
    total_sacks  = sum(sacks),
    avg_sacks    = round(total_sacks / weeks_played, 3),
    over_0p5     = round(mean(sacks >= 0.5), 3),
    over_1p5     = round(mean(sacks >= 1.5), 3),
    over_2p5     = round(mean(sacks >= 2.5), 3),
    over_3p5     = round(mean(sacks >= 3.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_sacks >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, weather_bucket)

# ── Tackles by weather ────────────────────────────────────────────────────────
tackles_weather <- tackle_weekly_full %>%
  filter(!is.na(weather_bucket), weather_bucket != "unknown") %>%
  group_by(player_id, player_name, weather_bucket) %>%
  summarise(
    weeks_played   = n_distinct(nflverse_game_id),
    avg_tackles    = round(sum(total_tackles)  / weeks_played, 2),
    avg_solo       = round(sum(solo_tackles)   / weeks_played, 2),
    avg_assists    = round(sum(assist_tackles) / weeks_played, 2),
    over_tkl_0p5   = round(mean(total_tackles >= 0.5),  3),
    over_tkl_1p5   = round(mean(total_tackles >= 1.5),  3),
    over_tkl_2p5   = round(mean(total_tackles >= 2.5),  3),
    over_tkl_3p5   = round(mean(total_tackles >= 3.5),  3),
    over_tkl_4p5   = round(mean(total_tackles >= 4.5),  3),
    over_tkl_5p5   = round(mean(total_tackles >= 5.5),  3),
    over_tkl_6p5   = round(mean(total_tackles >= 6.5),  3),
    over_tkl_7p5   = round(mean(total_tackles >= 7.5),  3),
    over_tkl_8p5   = round(mean(total_tackles >= 8.5),  3),
    over_tkl_9p5   = round(mean(total_tackles >= 9.5),  3),
    over_tkl_10p5  = round(mean(total_tackles >= 10.5), 3),
    over_tkl_11p5  = round(mean(total_tackles >= 11.5), 3),
    over_tkl_12p5  = round(mean(total_tackles >= 12.5), 3),
    over_tkl_13p5  = round(mean(total_tackles >= 13.5), 3),
    over_tkl_14p5  = round(mean(total_tackles >= 14.5), 3),
    over_solo_0p5  = round(mean(solo_tackles >= 0.5),   3),
    over_solo_1p5  = round(mean(solo_tackles >= 1.5),   3),
    over_solo_2p5  = round(mean(solo_tackles >= 2.5),   3),
    over_solo_3p5  = round(mean(solo_tackles >= 3.5),   3),
    over_solo_4p5  = round(mean(solo_tackles >= 4.5),   3),
    over_solo_5p5  = round(mean(solo_tackles >= 5.5),   3),
    over_solo_6p5  = round(mean(solo_tackles >= 6.5),   3),
    over_solo_7p5  = round(mean(solo_tackles >= 7.5),   3),
    over_solo_8p5  = round(mean(solo_tackles >= 8.5),   3),
    over_solo_9p5  = round(mean(solo_tackles >= 9.5),   3),
    over_solo_10p5 = round(mean(solo_tackles >= 10.5),  3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, weather_bucket)

# ── Interceptions by weather ──────────────────────────────────────────────────
interceptions_weather <- interception_weekly_full %>%
  filter(season_type == seasontype, !is.na(weather_bucket), weather_bucket != "unknown") %>%
  group_by(player_id, player_name, weather_bucket) %>%
  summarise(
    weeks_played        = n_distinct(nflverse_game_id),
    total_interceptions = sum(interceptions),
    avg_interceptions   = round(total_interceptions / weeks_played, 3),
    over_0p5            = round(mean(interceptions >= 0.5), 3),
    over_1p5            = round(mean(interceptions >= 1.5), 3),
    over_2p5            = round(mean(interceptions >= 2.5), 3),
    over_3p5            = round(mean(interceptions >= 3.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_interceptions >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, weather_bucket)

# ── Passing by weather ────────────────────────────────────────────────────────
passing_weather <- passing_weekly_full %>%
  filter(!is.na(weather_bucket), weather_bucket != "unknown") %>%
  group_by(player_id, player_name, weather_bucket) %>%
  summarise(
    weeks_played        = n_distinct(nflverse_game_id),
    avg_pass_yards      = round(sum(pass_yards)    / weeks_played, 2),
    avg_completions     = round(sum(completions)   / weeks_played, 2),
    avg_attempts        = round(sum(attempts)      / weeks_played, 2),
    avg_touchdowns      = round(sum(touchdowns)    / weeks_played, 2),
    avg_interceptions   = round(sum(interceptions) / weeks_played, 2),
    over_td_0p5         = round(mean(touchdowns >= 0.5), 3),
    over_td_1p5         = round(mean(touchdowns >= 1.5), 3),
    over_td_2p5         = round(mean(touchdowns >= 2.5), 3),
    over_td_3p5         = round(mean(touchdowns >= 3.5), 3),
    over_td_4p5         = round(mean(touchdowns >= 4.5), 3),
    over_int_0p5        = round(mean(interceptions >= 0.5), 3),
    over_int_1p5        = round(mean(interceptions >= 1.5), 3),
    over_int_2p5        = round(mean(interceptions >= 2.5), 3),
    over_int_3p5        = round(mean(interceptions >= 3.5), 3),
    over_cmp_13p5       = round(mean(completions >= 13.5), 3),
    over_cmp_14p5       = round(mean(completions >= 14.5), 3),
    over_cmp_15p5       = round(mean(completions >= 15.5), 3),
    over_cmp_16p5       = round(mean(completions >= 16.5), 3),
    over_cmp_17p5       = round(mean(completions >= 17.5), 3),
    over_cmp_18p5       = round(mean(completions >= 18.5), 3),
    over_cmp_19p5       = round(mean(completions >= 19.5), 3),
    over_cmp_20p5       = round(mean(completions >= 20.5), 3),
    over_cmp_21p5       = round(mean(completions >= 21.5), 3),
    over_cmp_22p5       = round(mean(completions >= 22.5), 3),
    over_cmp_23p5       = round(mean(completions >= 23.5), 3),
    over_cmp_24p5       = round(mean(completions >= 24.5), 3),
    over_cmp_25p5       = round(mean(completions >= 25.5), 3),
    over_cmp_26p5       = round(mean(completions >= 26.5), 3),
    over_cmp_27p5       = round(mean(completions >= 27.5), 3),
    over_cmp_28p5       = round(mean(completions >= 28.5), 3),
    over_cmp_29p5       = round(mean(completions >= 29.5), 3),
    over_cmp_30p5       = round(mean(completions >= 30.5), 3),
    over_cmp_31p5       = round(mean(completions >= 31.5), 3),
    over_cmp_32p5       = round(mean(completions >= 32.5), 3),
    over_cmp_33p5       = round(mean(completions >= 33.5), 3),
    over_cmp_34p5       = round(mean(completions >= 34.5), 3),
    over_cmp_35p5       = round(mean(completions >= 35.5), 3),
    over_cmp_36p5       = round(mean(completions >= 36.5), 3),
    over_cmp_37p5       = round(mean(completions >= 37.5), 3),
    over_cmp_38p5       = round(mean(completions >= 38.5), 3),
    over_cmp_39p5       = round(mean(completions >= 39.5), 3),
    over_att_19p5       = round(mean(attempts >= 19.5), 3),
    over_att_20p5       = round(mean(attempts >= 20.5), 3),
    over_att_21p5       = round(mean(attempts >= 21.5), 3),
    over_att_22p5       = round(mean(attempts >= 22.5), 3),
    over_att_23p5       = round(mean(attempts >= 23.5), 3),
    over_att_24p5       = round(mean(attempts >= 24.5), 3),
    over_att_25p5       = round(mean(attempts >= 25.5), 3),
    over_att_26p5       = round(mean(attempts >= 26.5), 3),
    over_att_27p5       = round(mean(attempts >= 27.5), 3),
    over_att_28p5       = round(mean(attempts >= 28.5), 3),
    over_att_29p5       = round(mean(attempts >= 29.5), 3),
    over_att_30p5       = round(mean(attempts >= 30.5), 3),
    over_att_31p5       = round(mean(attempts >= 31.5), 3),
    over_att_32p5       = round(mean(attempts >= 32.5), 3),
    over_att_33p5       = round(mean(attempts >= 33.5), 3),
    over_att_34p5       = round(mean(attempts >= 34.5), 3),
    over_att_35p5       = round(mean(attempts >= 35.5), 3),
    over_att_36p5       = round(mean(attempts >= 36.5), 3),
    over_att_37p5       = round(mean(attempts >= 37.5), 3),
    over_att_38p5       = round(mean(attempts >= 38.5), 3),
    over_att_39p5       = round(mean(attempts >= 39.5), 3),
    over_att_40p5       = round(mean(attempts >= 40.5), 3),
    over_att_41p5       = round(mean(attempts >= 41.5), 3),
    over_att_42p5       = round(mean(attempts >= 42.5), 3),
    over_att_43p5       = round(mean(attempts >= 43.5), 3),
    over_att_44p5       = round(mean(attempts >= 44.5), 3),
    over_att_45p5       = round(mean(attempts >= 45.5), 3),
    over_att_46p5       = round(mean(attempts >= 46.5), 3),
    over_att_47p5       = round(mean(attempts >= 47.5), 3),
    over_att_48p5       = round(mean(attempts >= 48.5), 3),
    over_att_49p5       = round(mean(attempts >= 49.5), 3),
    over_yds_149p5      = round(mean(pass_yards >= 149.5), 3),
    over_yds_154p5      = round(mean(pass_yards >= 154.5), 3),
    over_yds_159p5      = round(mean(pass_yards >= 159.5), 3),
    over_yds_164p5      = round(mean(pass_yards >= 164.5), 3),
    over_yds_169p5      = round(mean(pass_yards >= 169.5), 3),
    over_yds_174p5      = round(mean(pass_yards >= 174.5), 3),
    over_yds_179p5      = round(mean(pass_yards >= 179.5), 3),
    over_yds_184p5      = round(mean(pass_yards >= 184.5), 3),
    over_yds_189p5      = round(mean(pass_yards >= 189.5), 3),
    over_yds_194p5      = round(mean(pass_yards >= 194.5), 3),
    over_yds_199p5      = round(mean(pass_yards >= 199.5), 3),
    over_yds_204p5      = round(mean(pass_yards >= 204.5), 3),
    over_yds_209p5      = round(mean(pass_yards >= 209.5), 3),
    over_yds_214p5      = round(mean(pass_yards >= 214.5), 3),
    over_yds_219p5      = round(mean(pass_yards >= 219.5), 3),
    over_yds_224p5      = round(mean(pass_yards >= 224.5), 3),
    over_yds_229p5      = round(mean(pass_yards >= 229.5), 3),
    over_yds_234p5      = round(mean(pass_yards >= 234.5), 3),
    over_yds_239p5      = round(mean(pass_yards >= 239.5), 3),
    over_yds_244p5      = round(mean(pass_yards >= 244.5), 3),
    over_yds_249p5      = round(mean(pass_yards >= 249.5), 3),
    over_yds_254p5      = round(mean(pass_yards >= 254.5), 3),
    over_yds_259p5      = round(mean(pass_yards >= 259.5), 3),
    over_yds_264p5      = round(mean(pass_yards >= 264.5), 3),
    over_yds_269p5      = round(mean(pass_yards >= 269.5), 3),
    over_yds_274p5      = round(mean(pass_yards >= 274.5), 3),
    over_yds_279p5      = round(mean(pass_yards >= 279.5), 3),
    over_yds_284p5      = round(mean(pass_yards >= 284.5), 3),
    over_yds_289p5      = round(mean(pass_yards >= 289.5), 3),
    over_yds_294p5      = round(mean(pass_yards >= 294.5), 3),
    over_yds_299p5      = round(mean(pass_yards >= 299.5), 3),
    over_yds_304p5      = round(mean(pass_yards >= 304.5), 3),
    over_yds_309p5      = round(mean(pass_yards >= 309.5), 3),
    over_yds_314p5      = round(mean(pass_yards >= 314.5), 3),
    over_yds_319p5      = round(mean(pass_yards >= 319.5), 3),
    over_yds_324p5      = round(mean(pass_yards >= 324.5), 3),
    over_yds_329p5      = round(mean(pass_yards >= 329.5), 3),
    over_yds_334p5      = round(mean(pass_yards >= 334.5), 3),
    over_yds_339p5      = round(mean(pass_yards >= 339.5), 3),
    over_yds_344p5      = round(mean(pass_yards >= 344.5), 3),
    over_yds_349p5      = round(mean(pass_yards >= 349.5), 3),
    over_yds_354p5      = round(mean(pass_yards >= 354.5), 3),
    over_yds_359p5      = round(mean(pass_yards >= 359.5), 3),
    over_yds_364p5      = round(mean(pass_yards >= 364.5), 3),
    over_yds_369p5      = round(mean(pass_yards >= 369.5), 3),
    over_yds_374p5      = round(mean(pass_yards >= 374.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, weather_bucket)

# ── Rushing by weather ────────────────────────────────────────────────────────
rushing_weather <- rushing_weekly_full %>%
  filter(!is.na(weather_bucket), weather_bucket != "unknown") %>%
  group_by(player_id, player_name, weather_bucket) %>%
  summarise(
    weeks_played     = n_distinct(nflverse_game_id),
    avg_rush_yards   = round(sum(rushing_yards) / weeks_played, 2),
    avg_carries      = round(sum(carries)       / weeks_played, 2),
    over_ryd_9p5     = round(mean(rushing_yards >= 9.5),   3),
    over_ryd_14p5    = round(mean(rushing_yards >= 14.5),  3),
    over_ryd_19p5    = round(mean(rushing_yards >= 19.5),  3),
    over_ryd_24p5    = round(mean(rushing_yards >= 24.5),  3),
    over_ryd_29p5    = round(mean(rushing_yards >= 29.5),  3),
    over_ryd_34p5    = round(mean(rushing_yards >= 34.5),  3),
    over_ryd_39p5    = round(mean(rushing_yards >= 39.5),  3),
    over_ryd_44p5    = round(mean(rushing_yards >= 44.5),  3),
    over_ryd_49p5    = round(mean(rushing_yards >= 49.5),  3),
    over_ryd_54p5    = round(mean(rushing_yards >= 54.5),  3),
    over_ryd_59p5    = round(mean(rushing_yards >= 59.5),  3),
    over_ryd_64p5    = round(mean(rushing_yards >= 64.5),  3),
    over_ryd_69p5    = round(mean(rushing_yards >= 69.5),  3),
    over_ryd_74p5    = round(mean(rushing_yards >= 74.5),  3),
    over_ryd_79p5    = round(mean(rushing_yards >= 79.5),  3),
    over_ryd_84p5    = round(mean(rushing_yards >= 84.5),  3),
    over_ryd_89p5    = round(mean(rushing_yards >= 89.5),  3),
    over_ryd_94p5    = round(mean(rushing_yards >= 94.5),  3),
    over_ryd_99p5    = round(mean(rushing_yards >= 99.5),  3),
    over_ryd_104p5   = round(mean(rushing_yards >= 104.5), 3),
    over_ryd_109p5   = round(mean(rushing_yards >= 109.5), 3),
    over_ryd_114p5   = round(mean(rushing_yards >= 114.5), 3),
    over_ryd_119p5   = round(mean(rushing_yards >= 119.5), 3),
    over_ryd_124p5   = round(mean(rushing_yards >= 124.5), 3),
    over_ryd_129p5   = round(mean(rushing_yards >= 129.5), 3),
    over_ryd_134p5   = round(mean(rushing_yards >= 134.5), 3),
    over_ryd_139p5   = round(mean(rushing_yards >= 139.5), 3),
    over_ryd_144p5   = round(mean(rushing_yards >= 144.5), 3),
    over_ryd_149p5   = round(mean(rushing_yards >= 149.5), 3),
    over_car_4p5     = round(mean(carries >= 4.5),  3),
    over_car_5p5     = round(mean(carries >= 5.5),  3),
    over_car_6p5     = round(mean(carries >= 6.5),  3),
    over_car_7p5     = round(mean(carries >= 7.5),  3),
    over_car_8p5     = round(mean(carries >= 8.5),  3),
    over_car_9p5     = round(mean(carries >= 9.5),  3),
    over_car_10p5    = round(mean(carries >= 10.5), 3),
    over_car_11p5    = round(mean(carries >= 11.5), 3),
    over_car_12p5    = round(mean(carries >= 12.5), 3),
    over_car_13p5    = round(mean(carries >= 13.5), 3),
    over_car_14p5    = round(mean(carries >= 14.5), 3),
    over_car_15p5    = round(mean(carries >= 15.5), 3),
    over_car_16p5    = round(mean(carries >= 16.5), 3),
    over_car_17p5    = round(mean(carries >= 17.5), 3),
    over_car_18p5    = round(mean(carries >= 18.5), 3),
    over_car_19p5    = round(mean(carries >= 19.5), 3),
    over_car_20p5    = round(mean(carries >= 20.5), 3),
    over_car_21p5    = round(mean(carries >= 21.5), 3),
    over_car_22p5    = round(mean(carries >= 22.5), 3),
    over_car_23p5    = round(mean(carries >= 23.5), 3),
    over_car_24p5    = round(mean(carries >= 24.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, weather_bucket)

# ── Receiving by weather ──────────────────────────────────────────────────────
receiving_weather <- receiving_weekly_full %>%
  filter(!is.na(weather_bucket), weather_bucket != "unknown") %>%
  group_by(player_id, player_name, weather_bucket) %>%
  summarise(
    weeks_played     = n_distinct(nflverse_game_id),
    avg_rec_yards    = round(sum(rec_yrds)    / weeks_played, 2),
    avg_receptions   = round(sum(receptions)  / weeks_played, 2),
    avg_targets      = round(sum(targets)     / weeks_played, 2),
    over_ryd_9p5     = round(mean(rec_yrds >= 9.5),   3),
    over_ryd_14p5    = round(mean(rec_yrds >= 14.5),  3),
    over_ryd_19p5    = round(mean(rec_yrds >= 19.5),  3),
    over_ryd_24p5    = round(mean(rec_yrds >= 24.5),  3),
    over_ryd_29p5    = round(mean(rec_yrds >= 29.5),  3),
    over_ryd_34p5    = round(mean(rec_yrds >= 34.5),  3),
    over_ryd_39p5    = round(mean(rec_yrds >= 39.5),  3),
    over_ryd_44p5    = round(mean(rec_yrds >= 44.5),  3),
    over_ryd_49p5    = round(mean(rec_yrds >= 49.5),  3),
    over_ryd_54p5    = round(mean(rec_yrds >= 54.5),  3),
    over_ryd_59p5    = round(mean(rec_yrds >= 59.5),  3),
    over_ryd_64p5    = round(mean(rec_yrds >= 64.5),  3),
    over_ryd_69p5    = round(mean(rec_yrds >= 69.5),  3),
    over_ryd_74p5    = round(mean(rec_yrds >= 74.5),  3),
    over_ryd_79p5    = round(mean(rec_yrds >= 79.5),  3),
    over_ryd_84p5    = round(mean(rec_yrds >= 84.5),  3),
    over_ryd_89p5    = round(mean(rec_yrds >= 89.5),  3),
    over_ryd_94p5    = round(mean(rec_yrds >= 94.5),  3),
    over_ryd_99p5    = round(mean(rec_yrds >= 99.5),  3),
    over_ryd_104p5   = round(mean(rec_yrds >= 104.5), 3),
    over_ryd_109p5   = round(mean(rec_yrds >= 109.5), 3),
    over_ryd_114p5   = round(mean(rec_yrds >= 114.5), 3),
    over_ryd_119p5   = round(mean(rec_yrds >= 119.5), 3),
    over_ryd_124p5   = round(mean(rec_yrds >= 124.5), 3),
    over_ryd_129p5   = round(mean(rec_yrds >= 129.5), 3),
    over_ryd_134p5   = round(mean(rec_yrds >= 134.5), 3),
    over_ryd_139p5   = round(mean(rec_yrds >= 139.5), 3),
    over_ryd_144p5   = round(mean(rec_yrds >= 144.5), 3),
    over_ryd_149p5   = round(mean(rec_yrds >= 149.5), 3),
    over_rec_0p5     = round(mean(receptions >= 0.5),  3),
    over_rec_1p5     = round(mean(receptions >= 1.5),  3),
    over_rec_2p5     = round(mean(receptions >= 2.5),  3),
    over_rec_3p5     = round(mean(receptions >= 3.5),  3),
    over_rec_4p5     = round(mean(receptions >= 4.5),  3),
    over_rec_5p5     = round(mean(receptions >= 5.5),  3),
    over_rec_6p5     = round(mean(receptions >= 6.5),  3),
    over_rec_7p5     = round(mean(receptions >= 7.5),  3),
    over_rec_8p5     = round(mean(receptions >= 8.5),  3),
    over_rec_9p5     = round(mean(receptions >= 9.5),  3),
    over_rec_10p5    = round(mean(receptions >= 10.5), 3),
    over_rec_11p5    = round(mean(receptions >= 11.5), 3),
    over_rec_12p5    = round(mean(receptions >= 12.5), 3),
    over_tgt_0p5     = round(mean(targets >= 0.5),  3),
    over_tgt_1p5     = round(mean(targets >= 1.5),  3),
    over_tgt_2p5     = round(mean(targets >= 2.5),  3),
    over_tgt_3p5     = round(mean(targets >= 3.5),  3),
    over_tgt_4p5     = round(mean(targets >= 4.5),  3),
    over_tgt_5p5     = round(mean(targets >= 5.5),  3),
    over_tgt_6p5     = round(mean(targets >= 6.5),  3),
    over_tgt_7p5     = round(mean(targets >= 7.5),  3),
    over_tgt_8p5     = round(mean(targets >= 8.5),  3),
    over_tgt_9p5     = round(mean(targets >= 9.5),  3),
    over_tgt_10p5    = round(mean(targets >= 10.5), 3),
    over_tgt_11p5    = round(mean(targets >= 11.5), 3),
    over_tgt_12p5    = round(mean(targets >= 12.5), 3),
    over_tgt_13p5    = round(mean(targets >= 13.5), 3),
    over_tgt_14p5    = round(mean(targets >= 14.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, weather_bucket)


# ══════════════════════════════════════════════════════════════════════════════
# ── Opponent Strength Tiers (Season-to-Date) ──────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# Recalculated whenever pbp_nfl is refreshed. During offseason this represents
# the full prior season; during the season it represents season-to-date.

# ── 1. Pass defense strength (yards per attempt allowed) ──────────────────────
# Lower YPA allowed = stronger pass defense
pass_def_rankings <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(passing_yards), sack == 0) %>%
  group_by(defteam) %>%
  summarise(
    pass_attempts_faced = sum(pass_attempt, na.rm = TRUE),
    pass_yards_allowed  = sum(passing_yards, na.rm = TRUE),
    pass_ypa_allowed    = round(pass_yards_allowed / pass_attempts_faced, 2),
    .groups = "drop"
  ) %>%
  arrange(pass_ypa_allowed) %>%   # lower = better defense
  mutate(
    pass_def_rank = row_number(),
    pass_def_tier = case_when(
      pass_def_rank <= 10 ~ "top_pass_def",
      pass_def_rank >= 23 ~ "bottom_pass_def",
      TRUE                ~ "mid_pass_def"
    )
  )

# ── 2. Run defense strength (yards per carry allowed) ─────────────────────────
# Lower YPC allowed = stronger run defense
run_def_rankings <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(rushing_yards), rush_attempt == 1) %>%
  group_by(defteam) %>%
  summarise(
    rush_attempts_faced = n(),
    rush_yards_allowed  = sum(rushing_yards, na.rm = TRUE),
    rush_ypc_allowed    = round(rush_yards_allowed / rush_attempts_faced, 2),
    .groups = "drop"
  ) %>%
  arrange(rush_ypc_allowed) %>%
  mutate(
    run_def_rank = row_number(),
    run_def_tier = case_when(
      run_def_rank <= 10 ~ "top_run_def",
      run_def_rank >= 23 ~ "bottom_run_def",
      TRUE               ~ "mid_run_def"
    )
  )

# ── 3. Offensive volume (plays per game) — for defensive props ────────────────
# Higher plays/game = more tackle/sack opportunities for opposing defense
off_volume_rankings <- pbp_nfl %>%
  filter(season_type == seasontype, play_type %in% c("pass", "run")) %>%
  group_by(posteam, game_id) %>%
  summarise(plays = n(), .groups = "drop") %>%
  group_by(posteam) %>%
  summarise(
    games_played    = n_distinct(game_id),
    total_plays     = sum(plays),
    plays_per_game  = round(total_plays / games_played, 2),
    .groups = "drop"
  ) %>%
  arrange(desc(plays_per_game)) %>%
  mutate(
    off_volume_rank = row_number(),
    off_volume_tier = case_when(
      off_volume_rank <= 10 ~ "high_volume_off",
      off_volume_rank >= 23 ~ "low_volume_off",
      TRUE                  ~ "mid_volume_off"
    )
  )

# ══════════════════════════════════════════════════════════════════════════════
# ── Tag opponent strength on every attribution table ──────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# For offensive players: their opponent is defteam → join against pass/run def
# For defensive players: their opponent is posteam → join against off_volume

# ── Offensive tables: tag pass and run def tier of the opposing team ─────────
passing_stats_weekly_total <- passing_stats_weekly_total %>%
  left_join(pass_def_rankings %>% select(defteam, pass_def_tier, pass_def_rank), by = "defteam")

rushing_weekly_total <- rushing_weekly_total %>%
  left_join(run_def_rankings %>% select(defteam, run_def_tier, run_def_rank), by = "defteam")

receiving_weekly_total <- receiving_weekly_total %>%
  left_join(pass_def_rankings %>% select(defteam, pass_def_tier, pass_def_rank), by = "defteam")

# ── Defensive tables: tag offensive volume tier of the opposing team ─────────
sack_attribution <- sack_attribution %>%
  left_join(off_volume_rankings %>% select(posteam, off_volume_tier, off_volume_rank), by = "posteam")

tackle_attribution <- tackle_attribution %>%
  left_join(off_volume_rankings %>% select(posteam, off_volume_tier, off_volume_rank), by = "posteam")

interception_attribution <- interception_attribution %>%
  left_join(off_volume_rankings %>% select(posteam, off_volume_tier, off_volume_rank), by = "posteam")

# ══════════════════════════════════════════════════════════════════════════════
# ── Rebuild weekly_full tables to carry opponent tier through ─────────────────
# ══════════════════════════════════════════════════════════════════════════════

# Rebuild QB-filtered passing first
passing_stats_weekly_total_qb <- passing_stats_weekly_total %>%
  inner_join(rosters %>% select(gsis_id, position),
             by = c("passer_player_id" = "gsis_id"))

# ══════════════════════════════════════════════════════════════════════════════
# ── FIX: Supplement defensive player_weeks_played from PBP/attribution ───────
# When participation data lags (FTNData), 2026 games have no defensive rows.
# Fill in from stat attribution tables so downstream joins work.
# ══════════════════════════════════════════════════════════════════════════════
missing_seasons_def <- setdiff(unique(pbp_nfl$season), unique(player_weeks_played$season))
missing_seasons_def <- missing_seasons_def[!is.na(missing_seasons_def)]

if (length(missing_seasons_def) > 0) {
  message("⚠ Defensive participation missing for season(s): ", 
          paste(missing_seasons_def, collapse = ", "),
          " — supplementing from sack/tackle/interception attribution")
  
  def_from_sacks <- sack_attribution %>%
    filter(season %in% missing_seasons_def) %>%
    transmute(nflverse_game_id = game_id, week, season, season_type = "REG",
              player_id, player_name, defteam)
  
  def_from_tackles <- tackle_attribution %>%
    filter(season %in% missing_seasons_def) %>%
    transmute(nflverse_game_id = game_id, week, season, season_type = "REG",
              player_id, player_name, defteam)
  
  def_from_ints <- interception_attribution %>%
    filter(season %in% missing_seasons_def) %>%
    transmute(nflverse_game_id = game_id, week, season, season_type = "REG",
              player_id = interception_player_id, 
              player_name = interception_player_name, defteam)
  
  def_supp <- bind_rows(def_from_sacks, def_from_tackles, def_from_ints) %>%
    distinct(nflverse_game_id, week, season, season_type, player_id, .keep_all = TRUE)
  
  player_weeks_played <- bind_rows(player_weeks_played, def_supp) %>%
    distinct(nflverse_game_id, week, season, season_type, player_id, .keep_all = TRUE)
  
  message("  ✓ Added ", nrow(def_supp), " defensive player-week rows from attribution")
}

# ── Filter to regular season only (participation may include playoffs) ──
player_weeks_played <- player_weeks_played %>%
  mutate(parsed_week = as.integer(substr(nflverse_game_id, 6, 7))) %>%
  filter(parsed_week <= 18) %>%
  select(-parsed_week)

# ── Parse season/week from game_id for any remaining NAs ──
player_weeks_played <- player_weeks_played %>%
  mutate(
    parsed_season = as.integer(substr(nflverse_game_id, 1, 4)),
    parsed_week   = as.integer(substr(nflverse_game_id, 6, 7)),
    season = coalesce(season, parsed_season),
    week   = coalesce(week, parsed_week)
  ) %>%
  select(-parsed_season, -parsed_week)

message("✓ player_weeks_played: ", nrow(player_weeks_played), " rows, ",
        sum(is.na(player_weeks_played$season)), " still missing season")

# Sacks
sack_weekly <- sack_attribution %>%
  group_by(player_id, player_name, week,season, location, weather_bucket, off_volume_tier) %>%
  summarise(sacks = sum(sack_credit), .groups = "drop") %>%
  distinct(player_id, week,season, .keep_all = TRUE)

sack_weekly_full <- player_weeks_played %>%
  filter(season_type == seasontype) %>%
  left_join(sack_weekly %>% select(player_id, week,season, sacks, location, weather_bucket, off_volume_tier),
            by = c("player_id", "week","season")) %>%
  mutate(sacks = replace_na(sacks, 0))

# Tackles
tackle_weekly <- tackle_attribution %>%
  group_by(player_id, player_name, week,season, location, weather_bucket, off_volume_tier) %>%
  summarise(
    total_tackles  = sum(tackle_credit),
    solo_tackles   = sum(tackle_credit[is_solo == TRUE]),
    assist_tackles = sum(tackle_credit[is_solo == FALSE]),
    .groups = "drop"
  ) %>%
  distinct(player_id, week,season, .keep_all = TRUE)

tackle_weekly_full <- player_weeks_played %>%
  filter(season_type == seasontype) %>%
  left_join(tackle_weekly %>% select(player_id, week,season, total_tackles, solo_tackles, assist_tackles,
                                     location, weather_bucket, off_volume_tier),
            by = c("player_id", "week","season")) %>%
  mutate(
    total_tackles  = replace_na(total_tackles, 0),
    solo_tackles   = replace_na(solo_tackles, 0),
    assist_tackles = replace_na(assist_tackles, 0)
  )

# Interceptions
interception_weekly_full <- player_weeks_played %>%
  left_join(interception_attribution %>% select(interception_player_id, week,season, interceptions,
                                                location, weather_bucket, off_volume_tier),
            by = c("player_id" = "interception_player_id", "week","season")) %>%
  mutate(interceptions = replace_na(interceptions, 0))


# ══════════════════════════════════════════════════════════════════════════════
# ── REBUILD defensive hit_rates now that _weekly_full tables have 2026 data ──
# The initial hit_rates builds (lines 254, 1010, 1083) ran BEFORE the defensive
# supplement + weekly_full rebuilds. Rebuild here with complete 2026 data.
# ══════════════════════════════════════════════════════════════════════════════

message("↻ Rebuilding defensive hit_rates with complete 2026 data...")

sack_hit_rates <- sack_weekly_full %>%
  filter(season_type == seasontype) %>%
  group_by(player_id) %>%
  summarise(
    player_name    = first(player_name),
    weeks_played     = n_distinct(nflverse_game_id),
    total_sacks      = sum(sacks),
    sacks_per_game   = round(total_sacks / weeks_played, 3),
    over_0.5         = round(mean(sacks >= 0.5), 3),
    over_1.5         = round(mean(sacks >= 1.5), 3),
    over_2.5         = round(mean(sacks >= 2.5), 3),
    over_3.5         = round(mean(sacks >= 3.5), 3),
    zero_sack_games  = sum(sacks == 0),
    half_sack_games  = sum(sacks == 0.5),
    one_sack_games   = sum(sacks == 1.0),
    multi_sack_games = sum(sacks >= 2.0),
    max_sacks        = max(sacks),
    consistency      = round(mean(sacks > 0), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_sacks >= 1) %>%
  arrange(desc(total_sacks))

recent_form <- sack_weekly_full %>%
  group_by(player_id) %>%
  slice_max(order_by = week, n = 4) %>%
  summarise(
    last4_weeks    = n_distinct(nflverse_game_id),
    last4_sacks    = sum(sacks),
    last4_per_game = round(sum(sacks) / n_distinct(nflverse_game_id), 3),
    last4_over_0.5 = round(mean(sacks >= 0.5), 3),
    last4_over_1.5 = round(mean(sacks >= 1.5), 3),
    last4_over_2.5 = round(mean(sacks >= 2.5), 3),
    last4_over_3.5 = round(mean(sacks >= 3.5), 3),
    .groups = "drop"
  )

sack_matrix <- sack_hit_rates %>%
  left_join(
    recent_form %>% select(player_id, last4_sacks, last4_per_game,
                           last4_over_0.5, last4_over_1.5,
                           last4_over_2.5, last4_over_3.5),
    by = "player_id"
  ) %>%
  mutate(
    trend = case_when(
      last4_per_game > sacks_per_game * 1.25 ~ "heating_up",
      last4_per_game < sacks_per_game * 0.75 ~ "cooling_down",
      TRUE                                   ~ "stable"
    )
  )

tackle_hit_rates <- tackle_weekly_full %>%
  group_by(player_id) %>%
  summarise(
    player_name    = first(player_name),
    weeks_played   = n_distinct(nflverse_game_id),
    total_tackles  = sum(total_tackles),
    total_solo     = sum(solo_tackles),
    total_assists  = sum(assist_tackles),
    avg_tackles    = round(total_tackles / weeks_played, 2),
    avg_solo       = round(total_solo    / weeks_played, 2),
    avg_assists    = round(total_assists / weeks_played, 2),
    over_tkl_0p5   = round(mean(total_tackles >= 0.5),  3),
    over_tkl_1p5   = round(mean(total_tackles >= 1.5),  3),
    over_tkl_2p5   = round(mean(total_tackles >= 2.5),  3),
    over_tkl_3p5   = round(mean(total_tackles >= 3.5),  3),
    over_tkl_4p5   = round(mean(total_tackles >= 4.5),  3),
    over_tkl_5p5   = round(mean(total_tackles >= 5.5),  3),
    over_tkl_6p5   = round(mean(total_tackles >= 6.5),  3),
    over_tkl_7p5   = round(mean(total_tackles >= 7.5),  3),
    over_tkl_8p5   = round(mean(total_tackles >= 8.5),  3),
    over_tkl_9p5   = round(mean(total_tackles >= 9.5),  3),
    over_tkl_10p5  = round(mean(total_tackles >= 10.5), 3),
    over_tkl_11p5  = round(mean(total_tackles >= 11.5), 3),
    over_tkl_12p5  = round(mean(total_tackles >= 12.5), 3),
    over_tkl_13p5  = round(mean(total_tackles >= 13.5), 3),
    over_tkl_14p5  = round(mean(total_tackles >= 14.5), 3),
    over_solo_0p5  = round(mean(solo_tackles >= 0.5),   3),
    over_solo_1p5  = round(mean(solo_tackles >= 1.5),   3),
    over_solo_2p5  = round(mean(solo_tackles >= 2.5),   3),
    over_solo_3p5  = round(mean(solo_tackles >= 3.5),   3),
    over_solo_4p5  = round(mean(solo_tackles >= 4.5),   3),
    over_solo_5p5  = round(mean(solo_tackles >= 5.5),   3),
    over_solo_6p5  = round(mean(solo_tackles >= 6.5),   3),
    over_solo_7p5  = round(mean(solo_tackles >= 7.5),   3),
    over_solo_8p5  = round(mean(solo_tackles >= 8.5),   3),
    over_solo_9p5  = round(mean(solo_tackles >= 9.5),   3),
    over_solo_10p5 = round(mean(solo_tackles >= 10.5),  3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_tackles >= 4) %>%
  arrange(desc(avg_tackles))

interception_hit_rates <- interception_weekly_full %>%
  filter(season_type == seasontype) %>%
  group_by(player_id) %>%
  summarise(
    player_name    = first(player_name),
    weeks_played            = n_distinct(nflverse_game_id),
    total_interceptions     = sum(interceptions),
    interceptions_per_game  = round(total_interceptions / weeks_played, 3),
    over_0.5                = round(mean(interceptions >= 0.5), 3),
    over_1.5                = round(mean(interceptions >= 1.5), 3),
    over_2.5                = round(mean(interceptions >= 2.5), 3),
    over_3.5                = round(mean(interceptions >= 3.5), 3),
    zero_interception_games = sum(interceptions == 0),
    one_interception_games  = sum(interceptions == 1.0),
    multi_interception_games = sum(interceptions >= 2.0),
    max_interception        = max(interceptions),
    consistency             = round(mean(interceptions > 0), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_interceptions >= 1) %>%
  arrange(desc(total_interceptions))

# Re-attach current_team (was joined at line 1108-1113 to the old versions)
sack_matrix            <- sack_matrix            %>% left_join(current_team, by = "player_id")
tackle_hit_rates       <- tackle_hit_rates       %>% left_join(current_team, by = "player_id")
interception_hit_rates <- interception_hit_rates %>% left_join(current_team, by = "player_id")

message("✓ Defensive rebuilds complete: sacks=", nrow(sack_matrix), 
        " tackles=", nrow(tackle_hit_rates), 
        " interceptions=", nrow(interception_hit_rates))

# Passing
passing_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  inner_join(rosters %>% select(gsis_id, position), by = c("player_id" = "gsis_id")) %>%
  left_join(
    passing_stats_weekly_total_qb %>%
      select(passer_player_id, week,season, attempts, completions,
             touchdowns, interceptions, pass_yards, location, weather_bucket, pass_def_tier),
    by = c("player_id" = "passer_player_id", "week","season")
  ) %>%
  mutate(
    attempts      = replace_na(attempts, 0),
    completions   = replace_na(completions, 0),
    touchdowns    = replace_na(touchdowns, 0),
    interceptions = replace_na(interceptions, 0),
    pass_yards    = replace_na(pass_yards, 0)
  )

# Rushing
rushing_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  left_join(rushing_weekly_total %>%
              select(rusher_player_id, week,season, rushing_yards, carries, location, weather_bucket, run_def_tier),
            by = c("player_id" = "rusher_player_id", "week","season")) %>%
  mutate(
    rushing_yards = replace_na(rushing_yards, 0),
    carries       = replace_na(carries, 0)
  )

# Receiving
receiving_weekly_full <- player_weeks_played_offense %>%
  filter(season_type == seasontype) %>%
  left_join(receiving_weekly_total %>%
              select(receiver_player_id, week, season, rec_yrds, receptions, targets, location, weather_bucket, pass_def_tier),
            by = c("player_id" = "receiver_player_id", "week","season")) %>%
  mutate(
    rec_yrds   = replace_na(rec_yrds, 0),
    receptions = replace_na(receptions, 0),
    targets    = replace_na(targets, 0)
  )

# ══════════════════════════════════════════════════════════════════════════════
# ── Opponent tier split hit rate matrices ─────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Passing by pass defense tier ──────────────────────────────────────────────
passing_by_opp <- passing_weekly_full %>%
  filter(!is.na(pass_def_tier)) %>%
  group_by(player_id, player_name, pass_def_tier) %>%
  summarise(
    weeks_played    = n_distinct(nflverse_game_id),
    avg_pass_yards  = round(sum(pass_yards)  / weeks_played, 2),
    avg_completions = round(sum(completions) / weeks_played, 2),
    avg_attempts    = round(sum(attempts)    / weeks_played, 2),
    avg_touchdowns  = round(sum(touchdowns)  / weeks_played, 2),
    over_yds_199p5  = round(mean(pass_yards >= 199.5), 3),
    over_yds_224p5  = round(mean(pass_yards >= 224.5), 3),
    over_yds_249p5  = round(mean(pass_yards >= 249.5), 3),
    over_yds_274p5  = round(mean(pass_yards >= 274.5), 3),
    over_yds_299p5  = round(mean(pass_yards >= 299.5), 3),
    over_yds_324p5  = round(mean(pass_yards >= 324.5), 3),
    over_td_0p5     = round(mean(touchdowns >= 0.5), 3),
    over_td_1p5     = round(mean(touchdowns >= 1.5), 3),
    over_td_2p5     = round(mean(touchdowns >= 2.5), 3),
    over_cmp_19p5   = round(mean(completions >= 19.5), 3),
    over_cmp_22p5   = round(mean(completions >= 22.5), 3),
    over_cmp_25p5   = round(mean(completions >= 25.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, pass_def_tier)

# ── Rushing by run defense tier ───────────────────────────────────────────────
rushing_by_opp <- rushing_weekly_full %>%
  filter(!is.na(run_def_tier)) %>%
  group_by(player_id, player_name, run_def_tier) %>%
  summarise(
    weeks_played    = n_distinct(nflverse_game_id),
    avg_rush_yards  = round(sum(rushing_yards) / weeks_played, 2),
    avg_carries     = round(sum(carries) / weeks_played, 2),
    over_ryd_24p5   = round(mean(rushing_yards >= 24.5), 3),
    over_ryd_49p5   = round(mean(rushing_yards >= 49.5), 3),
    over_ryd_74p5   = round(mean(rushing_yards >= 74.5), 3),
    over_ryd_99p5   = round(mean(rushing_yards >= 99.5), 3),
    over_car_9p5    = round(mean(carries >= 9.5), 3),
    over_car_14p5   = round(mean(carries >= 14.5), 3),
    over_car_19p5   = round(mean(carries >= 19.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, run_def_tier)

# ── Receiving by pass defense tier ────────────────────────────────────────────
receiving_by_opp <- receiving_weekly_full %>%
  filter(!is.na(pass_def_tier)) %>%
  group_by(player_id, player_name, pass_def_tier) %>%
  summarise(
    weeks_played    = n_distinct(nflverse_game_id),
    avg_rec_yards   = round(sum(rec_yrds)   / weeks_played, 2),
    avg_receptions  = round(sum(receptions) / weeks_played, 2),
    avg_targets     = round(sum(targets)    / weeks_played, 2),
    over_ryd_24p5   = round(mean(rec_yrds >= 24.5), 3),
    over_ryd_49p5   = round(mean(rec_yrds >= 49.5), 3),
    over_ryd_74p5   = round(mean(rec_yrds >= 74.5), 3),
    over_ryd_99p5   = round(mean(rec_yrds >= 99.5), 3),
    over_rec_3p5    = round(mean(receptions >= 3.5), 3),
    over_rec_5p5    = round(mean(receptions >= 5.5), 3),
    over_rec_7p5    = round(mean(receptions >= 7.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, pass_def_tier)

# ── Sacks by offensive volume tier ────────────────────────────────────────────
sacks_by_opp <- sack_weekly_full %>%
  filter(season_type == seasontype, !is.na(off_volume_tier)) %>%
  group_by(player_id, player_name, off_volume_tier) %>%
  summarise(
    weeks_played = n_distinct(nflverse_game_id),
    total_sacks  = sum(sacks),
    avg_sacks    = round(total_sacks / weeks_played, 3),
    over_0p5     = round(mean(sacks >= 0.5), 3),
    over_1p5     = round(mean(sacks >= 1.5), 3),
    over_2p5     = round(mean(sacks >= 2.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_sacks >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, off_volume_tier)

# ── Tackles by offensive volume tier ──────────────────────────────────────────
tackles_by_opp <- tackle_weekly_full %>%
  filter(!is.na(off_volume_tier)) %>%
  group_by(player_id, player_name, off_volume_tier) %>%
  summarise(
    weeks_played  = n_distinct(nflverse_game_id),
    avg_tackles   = round(sum(total_tackles) / weeks_played, 2),
    avg_solo      = round(sum(solo_tackles)  / weeks_played, 2),
    over_tkl_3p5  = round(mean(total_tackles >= 3.5), 3),
    over_tkl_5p5  = round(mean(total_tackles >= 5.5), 3),
    over_tkl_7p5  = round(mean(total_tackles >= 7.5), 3),
    over_tkl_9p5  = round(mean(total_tackles >= 9.5), 3),
    over_solo_3p5 = round(mean(solo_tackles >= 3.5), 3),
    over_solo_5p5 = round(mean(solo_tackles >= 5.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, off_volume_tier)

# ── Interceptions by offensive volume tier ────────────────────────────────────
interceptions_by_opp <- interception_weekly_full %>%
  filter(season_type == seasontype, !is.na(off_volume_tier)) %>%
  group_by(player_id, player_name, off_volume_tier) %>%
  summarise(
    weeks_played        = n_distinct(nflverse_game_id),
    total_interceptions = sum(interceptions),
    avg_interceptions   = round(total_interceptions / weeks_played, 3),
    over_0p5            = round(mean(interceptions >= 0.5), 3),
    over_1p5            = round(mean(interceptions >= 1.5), 3),
    .groups = "drop"
  ) %>%
  filter(weeks_played >= 1, total_interceptions >= 1) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, off_volume_tier)

# ══════════════════════════════════════════════════════════════════════════════
# ── Conditional Matrix: Q1 Pace → Full Game Passing Yards ─────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Step 1: Q1 passing yards per player per game ─────────────────────────────
q1_passing <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(passer_player_id), qtr == 1, sack == 0) %>%
  group_by(game_id, week, passer_player_id, passer_player_name) %>%
  summarise(
    q1_pass_yards = sum(ifelse(is.na(passing_yards), 0, passing_yards)),
    q1_completions = sum(complete_pass, na.rm = TRUE),
    .groups = "drop"
  )

# ── Step 2: Join Q1 yards onto full-game passing data ────────────────────────
passing_conditional <- passing_stats_weekly_total_qb %>%
  select(game_id, week, passer_player_id, passer_player_name, pass_yards) %>%
  left_join(
    q1_passing %>% select(game_id, passer_player_id, q1_pass_yards),
    by = c("game_id", "passer_player_id")
  ) %>%
  mutate(
    q1_pass_yards = replace_na(q1_pass_yards, 0),
    q1_bucket = case_when(
      q1_pass_yards <  40 ~ "Q1 under 40 yds",
      q1_pass_yards <  70 ~ "Q1 40-69 yds",
      q1_pass_yards < 100 ~ "Q1 70-99 yds",
      TRUE                ~ "Q1 100+ yds"
    ),
    q1_bucket = factor(q1_bucket, levels = c(
      "Q1 under 40 yds", "Q1 40-69 yds", "Q1 70-99 yds", "Q1 100+ yds"
    ))
  )

# ── Step 3: Build the conditional hit rate matrix ─────────────────────────────
passing_yards_conditional_matrix <- passing_conditional %>%
  group_by(player_id = passer_player_id, player_name = passer_player_name, q1_bucket) %>%
  summarise(
    n_games        = n(),
    avg_full_yards = round(mean(pass_yards), 1),
    median_full    = round(median(pass_yards), 1),
    
    # Full over hit rate range: seq(149.5, 374.5, by = 5)
    over_yds_149p5 = round(mean(pass_yards >= 149.5), 3),
    over_yds_154p5 = round(mean(pass_yards >= 154.5), 3),
    over_yds_159p5 = round(mean(pass_yards >= 159.5), 3),
    over_yds_164p5 = round(mean(pass_yards >= 164.5), 3),
    over_yds_169p5 = round(mean(pass_yards >= 169.5), 3),
    over_yds_174p5 = round(mean(pass_yards >= 174.5), 3),
    over_yds_179p5 = round(mean(pass_yards >= 179.5), 3),
    over_yds_184p5 = round(mean(pass_yards >= 184.5), 3),
    over_yds_189p5 = round(mean(pass_yards >= 189.5), 3),
    over_yds_194p5 = round(mean(pass_yards >= 194.5), 3),
    over_yds_199p5 = round(mean(pass_yards >= 199.5), 3),
    over_yds_204p5 = round(mean(pass_yards >= 204.5), 3),
    over_yds_209p5 = round(mean(pass_yards >= 209.5), 3),
    over_yds_214p5 = round(mean(pass_yards >= 214.5), 3),
    over_yds_219p5 = round(mean(pass_yards >= 219.5), 3),
    over_yds_224p5 = round(mean(pass_yards >= 224.5), 3),
    over_yds_229p5 = round(mean(pass_yards >= 229.5), 3),
    over_yds_234p5 = round(mean(pass_yards >= 234.5), 3),
    over_yds_239p5 = round(mean(pass_yards >= 239.5), 3),
    over_yds_244p5 = round(mean(pass_yards >= 244.5), 3),
    over_yds_249p5 = round(mean(pass_yards >= 249.5), 3),
    over_yds_254p5 = round(mean(pass_yards >= 254.5), 3),
    over_yds_259p5 = round(mean(pass_yards >= 259.5), 3),
    over_yds_264p5 = round(mean(pass_yards >= 264.5), 3),
    over_yds_269p5 = round(mean(pass_yards >= 269.5), 3),
    over_yds_274p5 = round(mean(pass_yards >= 274.5), 3),
    over_yds_279p5 = round(mean(pass_yards >= 279.5), 3),
    over_yds_284p5 = round(mean(pass_yards >= 284.5), 3),
    over_yds_289p5 = round(mean(pass_yards >= 289.5), 3),
    over_yds_294p5 = round(mean(pass_yards >= 294.5), 3),
    over_yds_299p5 = round(mean(pass_yards >= 299.5), 3),
    over_yds_304p5 = round(mean(pass_yards >= 304.5), 3),
    over_yds_309p5 = round(mean(pass_yards >= 309.5), 3),
    over_yds_314p5 = round(mean(pass_yards >= 314.5), 3),
    over_yds_319p5 = round(mean(pass_yards >= 319.5), 3),
    over_yds_324p5 = round(mean(pass_yards >= 324.5), 3),
    over_yds_329p5 = round(mean(pass_yards >= 329.5), 3),
    over_yds_334p5 = round(mean(pass_yards >= 334.5), 3),
    over_yds_339p5 = round(mean(pass_yards >= 339.5), 3),
    over_yds_344p5 = round(mean(pass_yards >= 344.5), 3),
    over_yds_349p5 = round(mean(pass_yards >= 349.5), 3),
    over_yds_354p5 = round(mean(pass_yards >= 354.5), 3),
    over_yds_359p5 = round(mean(pass_yards >= 359.5), 3),
    over_yds_364p5 = round(mean(pass_yards >= 364.5), 3),
    over_yds_369p5 = round(mean(pass_yards >= 369.5), 3),
    over_yds_374p5 = round(mean(pass_yards >= 374.5), 3),
    
    .groups = "drop"
  ) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, q1_bucket)

# ══════════════════════════════════════════════════════════════════════════════
# ── Conditional Matrix: Q1 Pace → Full Game Completions ───────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Step 1: Q1 completions per player per game ────────────────────────────────
q1_completions <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(passer_player_id), qtr == 1, sack == 0) %>%
  group_by(game_id, week, passer_player_id, passer_player_name) %>%
  summarise(
    q1_completions = sum(complete_pass, na.rm = TRUE),
    .groups = "drop"
  )

# ── Step 2: Join Q1 completions onto full-game passing data ───────────────────
completions_conditional <- passing_stats_weekly_total_qb %>%
  select(game_id, week, passer_player_id, passer_player_name, completions) %>%
  left_join(
    q1_completions %>% select(game_id, passer_player_id, q1_completions),
    by = c("game_id", "passer_player_id")
  ) %>%
  mutate(
    q1_completions = replace_na(q1_completions, 0),
    q1_bucket = case_when(
      q1_completions <  4 ~ "Q1 under 4 cmp",
      q1_completions <  7 ~ "Q1 4-6 cmp",
      q1_completions < 10 ~ "Q1 7-9 cmp",
      TRUE                ~ "Q1 10+ cmp"
    ),
    q1_bucket = factor(q1_bucket, levels = c(
      "Q1 under 4 cmp", "Q1 4-6 cmp", "Q1 7-9 cmp", "Q1 10+ cmp"
    ))
  )

# ── Step 3: Build the conditional hit rate matrix ─────────────────────────────
completions_conditional_matrix <- completions_conditional %>%
  group_by(player_id = passer_player_id, player_name = passer_player_name, q1_bucket) %>%
  summarise(
    n_games        = n(),
    avg_full_cmp   = round(mean(completions), 1),
    median_full    = round(median(completions), 1),
    
    # Full over hit rate range: seq(13.5, 39.5, by = 1)
    over_cmp_13p5 = round(mean(completions >= 13.5), 3),
    over_cmp_14p5 = round(mean(completions >= 14.5), 3),
    over_cmp_15p5 = round(mean(completions >= 15.5), 3),
    over_cmp_16p5 = round(mean(completions >= 16.5), 3),
    over_cmp_17p5 = round(mean(completions >= 17.5), 3),
    over_cmp_18p5 = round(mean(completions >= 18.5), 3),
    over_cmp_19p5 = round(mean(completions >= 19.5), 3),
    over_cmp_20p5 = round(mean(completions >= 20.5), 3),
    over_cmp_21p5 = round(mean(completions >= 21.5), 3),
    over_cmp_22p5 = round(mean(completions >= 22.5), 3),
    over_cmp_23p5 = round(mean(completions >= 23.5), 3),
    over_cmp_24p5 = round(mean(completions >= 24.5), 3),
    over_cmp_25p5 = round(mean(completions >= 25.5), 3),
    over_cmp_26p5 = round(mean(completions >= 26.5), 3),
    over_cmp_27p5 = round(mean(completions >= 27.5), 3),
    over_cmp_28p5 = round(mean(completions >= 28.5), 3),
    over_cmp_29p5 = round(mean(completions >= 29.5), 3),
    over_cmp_30p5 = round(mean(completions >= 30.5), 3),
    over_cmp_31p5 = round(mean(completions >= 31.5), 3),
    over_cmp_32p5 = round(mean(completions >= 32.5), 3),
    over_cmp_33p5 = round(mean(completions >= 33.5), 3),
    over_cmp_34p5 = round(mean(completions >= 34.5), 3),
    over_cmp_35p5 = round(mean(completions >= 35.5), 3),
    over_cmp_36p5 = round(mean(completions >= 36.5), 3),
    over_cmp_37p5 = round(mean(completions >= 37.5), 3),
    over_cmp_38p5 = round(mean(completions >= 38.5), 3),
    over_cmp_39p5 = round(mean(completions >= 39.5), 3),
    
    .groups = "drop"
  ) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, q1_bucket)



# ══════════════════════════════════════════════════════════════════════════════
# ── Conditional Matrix: Q1 Pace → Full Game Attempts ──────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Step 1: Q1 attempts per player per game ───────────────────────────────────
q1_attempts <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(passer_player_id), qtr == 1, sack == 0) %>%
  group_by(game_id, week, passer_player_id, passer_player_name) %>%
  summarise(
    q1_attempts = sum(pass_attempt, na.rm = TRUE),
    .groups = "drop"
  )

# ── Step 2: Join Q1 attempts onto full-game passing data ──────────────────────
attempts_conditional <- passing_stats_weekly_total_qb %>%
  select(game_id, week, passer_player_id, passer_player_name, attempts) %>%
  left_join(
    q1_attempts %>% select(game_id, passer_player_id, q1_attempts),
    by = c("game_id", "passer_player_id")
  ) %>%
  mutate(
    q1_attempts = replace_na(q1_attempts, 0),
    q1_bucket = case_when(
      q1_attempts <  6 ~ "Q1 under 6 att",
      q1_attempts <  9 ~ "Q1 6-8 att",
      q1_attempts < 12 ~ "Q1 9-11 att",
      TRUE             ~ "Q1 12+ att"
    ),
    q1_bucket = factor(q1_bucket, levels = c(
      "Q1 under 6 att", "Q1 6-8 att", "Q1 9-11 att", "Q1 12+ att"
    ))
  )

# ── Step 3: Build the conditional hit rate matrix ─────────────────────────────
attempts_conditional_matrix <- attempts_conditional %>%
  group_by(player_id = passer_player_id, player_name = passer_player_name, q1_bucket) %>%
  summarise(
    n_games       = n(),
    avg_full_att  = round(mean(attempts), 1),
    median_full   = round(median(attempts), 1),
    
    # Full over hit rate range: seq(19.5, 49.5, by = 1)
    over_att_19p5 = round(mean(attempts >= 19.5), 3),
    over_att_20p5 = round(mean(attempts >= 20.5), 3),
    over_att_21p5 = round(mean(attempts >= 21.5), 3),
    over_att_22p5 = round(mean(attempts >= 22.5), 3),
    over_att_23p5 = round(mean(attempts >= 23.5), 3),
    over_att_24p5 = round(mean(attempts >= 24.5), 3),
    over_att_25p5 = round(mean(attempts >= 25.5), 3),
    over_att_26p5 = round(mean(attempts >= 26.5), 3),
    over_att_27p5 = round(mean(attempts >= 27.5), 3),
    over_att_28p5 = round(mean(attempts >= 28.5), 3),
    over_att_29p5 = round(mean(attempts >= 29.5), 3),
    over_att_30p5 = round(mean(attempts >= 30.5), 3),
    over_att_31p5 = round(mean(attempts >= 31.5), 3),
    over_att_32p5 = round(mean(attempts >= 32.5), 3),
    over_att_33p5 = round(mean(attempts >= 33.5), 3),
    over_att_34p5 = round(mean(attempts >= 34.5), 3),
    over_att_35p5 = round(mean(attempts >= 35.5), 3),
    over_att_36p5 = round(mean(attempts >= 36.5), 3),
    over_att_37p5 = round(mean(attempts >= 37.5), 3),
    over_att_38p5 = round(mean(attempts >= 38.5), 3),
    over_att_39p5 = round(mean(attempts >= 39.5), 3),
    over_att_40p5 = round(mean(attempts >= 40.5), 3),
    over_att_41p5 = round(mean(attempts >= 41.5), 3),
    over_att_42p5 = round(mean(attempts >= 42.5), 3),
    over_att_43p5 = round(mean(attempts >= 43.5), 3),
    over_att_44p5 = round(mean(attempts >= 44.5), 3),
    over_att_45p5 = round(mean(attempts >= 45.5), 3),
    over_att_46p5 = round(mean(attempts >= 46.5), 3),
    over_att_47p5 = round(mean(attempts >= 47.5), 3),
    over_att_48p5 = round(mean(attempts >= 48.5), 3),
    over_att_49p5 = round(mean(attempts >= 49.5), 3),
    
    .groups = "drop"
  ) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, q1_bucket)


# ══════════════════════════════════════════════════════════════════════════════
# ── Conditional Matrix: Q1 Pace → Full Game Rushing Yards ─────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Step 1: Q1 rushing yards per player per game ─────────────────────────────
q1_rushing <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(rusher_player_id), qtr == 1) %>%
  group_by(game_id, week, rusher_player_id, rusher_player_name) %>%
  summarise(
    q1_rush_yards = sum(ifelse(is.na(rushing_yards), 0, rushing_yards)),
    .groups = "drop"
  )

# ── Step 2: Join Q1 rushing yards onto full-game rushing data ────────────────
rushing_conditional <- rushing_weekly_total %>%
  select(game_id, week, rusher_player_id, rusher_player_name, rushing_yards) %>%
  left_join(
    q1_rushing %>% select(game_id, rusher_player_id, q1_rush_yards),
    by = c("game_id", "rusher_player_id")
  ) %>%
  mutate(
    q1_rush_yards = replace_na(q1_rush_yards, 0),
    q1_bucket = case_when(
      q1_rush_yards == 0  ~ "Q1 0 yds",
      q1_rush_yards <  10 ~ "Q1 1-9 yds",
      q1_rush_yards <  25 ~ "Q1 10-24 yds",
      q1_rush_yards <  50 ~ "Q1 25-49 yds",
      TRUE                ~ "Q1 50+ yds"
    ),
    q1_bucket = factor(q1_bucket, levels = c(
      "Q1 0 yds", "Q1 1-9 yds", "Q1 10-24 yds", "Q1 25-49 yds", "Q1 50+ yds"
    ))
  )

# ── Step 3: Build the conditional hit rate matrix ─────────────────────────────
rushing_yards_conditional_matrix <- rushing_conditional %>%
  group_by(player_id = rusher_player_id, player_name = rusher_player_name, q1_bucket) %>%
  summarise(
    n_games        = n(),
    avg_full_yards = round(mean(rushing_yards), 1),
    median_full    = round(median(rushing_yards), 1),
    
    # Full over hit rate range: seq(9.5, 149.5, by = 5) — matches season matrix
    over_ryd_9p5   = round(mean(rushing_yards >= 9.5),   3),
    over_ryd_14p5  = round(mean(rushing_yards >= 14.5),  3),
    over_ryd_19p5  = round(mean(rushing_yards >= 19.5),  3),
    over_ryd_24p5  = round(mean(rushing_yards >= 24.5),  3),
    over_ryd_29p5  = round(mean(rushing_yards >= 29.5),  3),
    over_ryd_34p5  = round(mean(rushing_yards >= 34.5),  3),
    over_ryd_39p5  = round(mean(rushing_yards >= 39.5),  3),
    over_ryd_44p5  = round(mean(rushing_yards >= 44.5),  3),
    over_ryd_49p5  = round(mean(rushing_yards >= 49.5),  3),
    over_ryd_54p5  = round(mean(rushing_yards >= 54.5),  3),
    over_ryd_59p5  = round(mean(rushing_yards >= 59.5),  3),
    over_ryd_64p5  = round(mean(rushing_yards >= 64.5),  3),
    over_ryd_69p5  = round(mean(rushing_yards >= 69.5),  3),
    over_ryd_74p5  = round(mean(rushing_yards >= 74.5),  3),
    over_ryd_79p5  = round(mean(rushing_yards >= 79.5),  3),
    over_ryd_84p5  = round(mean(rushing_yards >= 84.5),  3),
    over_ryd_89p5  = round(mean(rushing_yards >= 89.5),  3),
    over_ryd_94p5  = round(mean(rushing_yards >= 94.5),  3),
    over_ryd_99p5  = round(mean(rushing_yards >= 99.5),  3),
    over_ryd_104p5 = round(mean(rushing_yards >= 104.5), 3),
    over_ryd_109p5 = round(mean(rushing_yards >= 109.5), 3),
    over_ryd_114p5 = round(mean(rushing_yards >= 114.5), 3),
    over_ryd_119p5 = round(mean(rushing_yards >= 119.5), 3),
    over_ryd_124p5 = round(mean(rushing_yards >= 124.5), 3),
    over_ryd_129p5 = round(mean(rushing_yards >= 129.5), 3),
    over_ryd_134p5 = round(mean(rushing_yards >= 134.5), 3),
    over_ryd_139p5 = round(mean(rushing_yards >= 139.5), 3),
    over_ryd_144p5 = round(mean(rushing_yards >= 144.5), 3),
    over_ryd_149p5 = round(mean(rushing_yards >= 149.5), 3),
    
    .groups = "drop"
  ) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, q1_bucket)

# ══════════════════════════════════════════════════════════════════════════════
# ── Conditional Matrix: Q1 Pace → Full Game Receiving Yards ───────────────────
# ══════════════════════════════════════════════════════════════════════════════

q1_receiving <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(receiver_id), qtr == 1) %>%
  group_by(game_id, week, receiver_player_id, receiver_player_name) %>%
  summarise(
    q1_rec_yards = sum(ifelse(is.na(receiving_yards), 0, receiving_yards)),
    .groups = "drop"
  )

receiving_conditional <- receiving_weekly_total %>%
  select(game_id, week, receiver_player_id, receiver_player_name, rec_yrds) %>%
  left_join(
    q1_receiving %>% select(game_id, receiver_player_id, q1_rec_yards),
    by = c("game_id", "receiver_player_id")
  ) %>%
  mutate(
    q1_rec_yards = replace_na(q1_rec_yards, 0),
    q1_bucket = case_when(
      q1_rec_yards == 0  ~ "Q1 0 yds",
      q1_rec_yards <  25 ~ "Q1 1-24 yds",
      q1_rec_yards <  50 ~ "Q1 25-49 yds",
      TRUE               ~ "Q1 50+ yds"
    ),
    q1_bucket = factor(q1_bucket, levels = c(
      "Q1 0 yds", "Q1 1-24 yds", "Q1 25-49 yds", "Q1 50+ yds"
    ))
  )

receiving_yards_conditional_matrix <- receiving_conditional %>%
  group_by(player_id = receiver_player_id, player_name = receiver_player_name, q1_bucket) %>%
  summarise(
    n_games        = n(),
    avg_full_yards = round(mean(rec_yrds), 1),
    median_full    = round(median(rec_yrds), 1),
    
    over_ryd_9p5   = round(mean(rec_yrds >= 9.5),   3),
    over_ryd_14p5  = round(mean(rec_yrds >= 14.5),  3),
    over_ryd_19p5  = round(mean(rec_yrds >= 19.5),  3),
    over_ryd_24p5  = round(mean(rec_yrds >= 24.5),  3),
    over_ryd_29p5  = round(mean(rec_yrds >= 29.5),  3),
    over_ryd_34p5  = round(mean(rec_yrds >= 34.5),  3),
    over_ryd_39p5  = round(mean(rec_yrds >= 39.5),  3),
    over_ryd_44p5  = round(mean(rec_yrds >= 44.5),  3),
    over_ryd_49p5  = round(mean(rec_yrds >= 49.5),  3),
    over_ryd_54p5  = round(mean(rec_yrds >= 54.5),  3),
    over_ryd_59p5  = round(mean(rec_yrds >= 59.5),  3),
    over_ryd_64p5  = round(mean(rec_yrds >= 64.5),  3),
    over_ryd_69p5  = round(mean(rec_yrds >= 69.5),  3),
    over_ryd_74p5  = round(mean(rec_yrds >= 74.5),  3),
    over_ryd_79p5  = round(mean(rec_yrds >= 79.5),  3),
    over_ryd_84p5  = round(mean(rec_yrds >= 84.5),  3),
    over_ryd_89p5  = round(mean(rec_yrds >= 89.5),  3),
    over_ryd_94p5  = round(mean(rec_yrds >= 94.5),  3),
    over_ryd_99p5  = round(mean(rec_yrds >= 99.5),  3),
    over_ryd_104p5 = round(mean(rec_yrds >= 104.5), 3),
    over_ryd_109p5 = round(mean(rec_yrds >= 109.5), 3),
    over_ryd_114p5 = round(mean(rec_yrds >= 114.5), 3),
    over_ryd_119p5 = round(mean(rec_yrds >= 119.5), 3),
    over_ryd_124p5 = round(mean(rec_yrds >= 124.5), 3),
    over_ryd_129p5 = round(mean(rec_yrds >= 129.5), 3),
    over_ryd_134p5 = round(mean(rec_yrds >= 134.5), 3),
    over_ryd_139p5 = round(mean(rec_yrds >= 139.5), 3),
    over_ryd_144p5 = round(mean(rec_yrds >= 144.5), 3),
    over_ryd_149p5 = round(mean(rec_yrds >= 149.5), 3),
    
    .groups = "drop"
  ) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, q1_bucket)


# ══════════════════════════════════════════════════════════════════════════════
# ── Conditional Matrix: Q1 Pace → Full Game Receptions ────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

q1_receptions <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(receiver_id), qtr == 1) %>%
  group_by(game_id, week, receiver_player_id, receiver_player_name) %>%
  summarise(
    q1_receptions = sum(complete_pass, na.rm = TRUE),
    .groups = "drop"
  )

receptions_conditional <- receiving_weekly_total %>%
  select(game_id, week, receiver_player_id, receiver_player_name, receptions) %>%
  left_join(
    q1_receptions %>% select(game_id, receiver_player_id, q1_receptions),
    by = c("game_id", "receiver_player_id")
  ) %>%
  mutate(
    q1_receptions = replace_na(q1_receptions, 0),
    q1_bucket = case_when(
      q1_receptions == 0 ~ "Q1 0 rec",
      q1_receptions == 1 ~ "Q1 1 rec",
      q1_receptions == 2 ~ "Q1 2 rec",
      TRUE               ~ "Q1 3+ rec"
    ),
    q1_bucket = factor(q1_bucket, levels = c(
      "Q1 0 rec", "Q1 1 rec", "Q1 2 rec", "Q1 3+ rec"
    ))
  )

receptions_conditional_matrix <- receptions_conditional %>%
  group_by(player_id = receiver_player_id, player_name = receiver_player_name, q1_bucket) %>%
  summarise(
    n_games       = n(),
    avg_full_rec  = round(mean(receptions), 1),
    median_full   = round(median(receptions), 1),
    
    # Full over hit rate range: seq(0.5, 12.5, by = 1)
    over_rec_0p5  = round(mean(receptions >= 0.5),  3),
    over_rec_1p5  = round(mean(receptions >= 1.5),  3),
    over_rec_2p5  = round(mean(receptions >= 2.5),  3),
    over_rec_3p5  = round(mean(receptions >= 3.5),  3),
    over_rec_4p5  = round(mean(receptions >= 4.5),  3),
    over_rec_5p5  = round(mean(receptions >= 5.5),  3),
    over_rec_6p5  = round(mean(receptions >= 6.5),  3),
    over_rec_7p5  = round(mean(receptions >= 7.5),  3),
    over_rec_8p5  = round(mean(receptions >= 8.5),  3),
    over_rec_9p5  = round(mean(receptions >= 9.5),  3),
    over_rec_10p5 = round(mean(receptions >= 10.5), 3),
    over_rec_11p5 = round(mean(receptions >= 11.5), 3),
    over_rec_12p5 = round(mean(receptions >= 12.5), 3),
    
    .groups = "drop"
  ) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(player_name, q1_bucket)

# ══════════════════════════════════════════════════════════════════════════════
# ── H1 (Halftime) aggregation tables — mirrors Q1 structure ──────────────────
# ══════════════════════════════════════════════════════════════════════════════

# H1 passing yards per player per game
h1_passing <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(passer_player_id), qtr <= 2, sack == 0) %>%
  group_by(game_id, week, passer_player_id, passer_player_name) %>%
  summarise(
    h1_pass_yards = sum(ifelse(is.na(passing_yards), 0, passing_yards)),
    h1_completions = sum(complete_pass, na.rm = TRUE),
    .groups = "drop"
  )

# H1 rushing yards per player per game
h1_rushing <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(rusher_player_id), qtr <= 2) %>%
  group_by(game_id, week, rusher_player_id, rusher_player_name) %>%
  summarise(
    h1_rush_yards = sum(ifelse(is.na(rushing_yards), 0, rushing_yards)),
    .groups = "drop"
  )

# H1 receiving yards per player per game
h1_receiving <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(receiver_id), qtr <= 2) %>%
  group_by(game_id, week, receiver_player_id = receiver_id, receiver_player_name = receiver) %>%
  summarise(
    h1_rec_yards = sum(ifelse(is.na(receiving_yards), 0, receiving_yards)),
    .groups = "drop"
  )

# H1 receptions per player per game
h1_receptions <- pbp_nfl %>%
  filter(season_type == seasontype, !is.na(receiver_id), qtr <= 2) %>%
  group_by(game_id, week, receiver_player_id = receiver_id, receiver_player_name = receiver) %>%
  summarise(
    h1_receptions = sum(complete_pass, na.rm = TRUE),
    .groups = "drop"
  )


# ══════════════════════════════════════════════════════════════════════════════
# ── Cross-Cut Conditional: Q1 Pace × Pass Def Tier → Full Game Passing Yards ──
# ══════════════════════════════════════════════════════════════════════════════

# ── Join Q1 pace, pass def tier, and full game yards together ────────────────
passing_conditional_xcut <- passing_stats_weekly_total_qb %>%
  select(game_id, week, passer_player_id, passer_player_name, 
         pass_yards, pass_def_tier) %>%
  left_join(
    q1_passing %>% select(game_id, passer_player_id, q1_pass_yards),
    by = c("game_id", "passer_player_id")
  ) %>%
  mutate(
    q1_pass_yards = replace_na(q1_pass_yards, 0),
    q1_bucket = case_when(
      q1_pass_yards <  40 ~ "Q1 under 40 yds",
      q1_pass_yards <  70 ~ "Q1 40-69 yds",
      q1_pass_yards < 100 ~ "Q1 70-99 yds",
      TRUE                ~ "Q1 100+ yds"
    ),
    q1_bucket = factor(q1_bucket, levels = c(
      "Q1 under 40 yds", "Q1 40-69 yds", "Q1 70-99 yds", "Q1 100+ yds"
    ))
  ) %>%
  filter(!is.na(pass_def_tier))

# ── Build the cross-cut hit rate matrix ──────────────────────────────────────
passing_yards_xcut_matrix <- passing_conditional_xcut %>%
  group_by(player_id = passer_player_id, player_name = passer_player_name, 
           q1_bucket, pass_def_tier) %>%
  summarise(
    n_games        = n(),
    avg_full_yards = round(mean(pass_yards), 1),
    median_full    = round(median(pass_yards), 1),
    
    # Full over hit rate range
    over_yds_149p5 = round(mean(pass_yards >= 149.5), 3),
    over_yds_154p5 = round(mean(pass_yards >= 154.5), 3),
    over_yds_159p5 = round(mean(pass_yards >= 159.5), 3),
    over_yds_164p5 = round(mean(pass_yards >= 164.5), 3),
    over_yds_169p5 = round(mean(pass_yards >= 169.5), 3),
    over_yds_174p5 = round(mean(pass_yards >= 174.5), 3),
    over_yds_179p5 = round(mean(pass_yards >= 179.5), 3),
    over_yds_184p5 = round(mean(pass_yards >= 184.5), 3),
    over_yds_189p5 = round(mean(pass_yards >= 189.5), 3),
    over_yds_194p5 = round(mean(pass_yards >= 194.5), 3),
    over_yds_199p5 = round(mean(pass_yards >= 199.5), 3),
    over_yds_204p5 = round(mean(pass_yards >= 204.5), 3),
    over_yds_209p5 = round(mean(pass_yards >= 209.5), 3),
    over_yds_214p5 = round(mean(pass_yards >= 214.5), 3),
    over_yds_219p5 = round(mean(pass_yards >= 219.5), 3),
    over_yds_224p5 = round(mean(pass_yards >= 224.5), 3),
    over_yds_229p5 = round(mean(pass_yards >= 229.5), 3),
    over_yds_234p5 = round(mean(pass_yards >= 234.5), 3),
    over_yds_239p5 = round(mean(pass_yards >= 239.5), 3),
    over_yds_244p5 = round(mean(pass_yards >= 244.5), 3),
    over_yds_249p5 = round(mean(pass_yards >= 249.5), 3),
    over_yds_254p5 = round(mean(pass_yards >= 254.5), 3),
    over_yds_259p5 = round(mean(pass_yards >= 259.5), 3),
    over_yds_264p5 = round(mean(pass_yards >= 264.5), 3),
    over_yds_269p5 = round(mean(pass_yards >= 269.5), 3),
    over_yds_274p5 = round(mean(pass_yards >= 274.5), 3),
    over_yds_279p5 = round(mean(pass_yards >= 279.5), 3),
    over_yds_284p5 = round(mean(pass_yards >= 284.5), 3),
    over_yds_289p5 = round(mean(pass_yards >= 289.5), 3),
    over_yds_294p5 = round(mean(pass_yards >= 294.5), 3),
    over_yds_299p5 = round(mean(pass_yards >= 299.5), 3),
    over_yds_304p5 = round(mean(pass_yards >= 304.5), 3),
    over_yds_309p5 = round(mean(pass_yards >= 309.5), 3),
    over_yds_314p5 = round(mean(pass_yards >= 314.5), 3),
    over_yds_319p5 = round(mean(pass_yards >= 319.5), 3),
    over_yds_324p5 = round(mean(pass_yards >= 324.5), 3),
    over_yds_329p5 = round(mean(pass_yards >= 329.5), 3),
    over_yds_334p5 = round(mean(pass_yards >= 334.5), 3),
    over_yds_339p5 = round(mean(pass_yards >= 339.5), 3),
    over_yds_344p5 = round(mean(pass_yards >= 344.5), 3),
    over_yds_349p5 = round(mean(pass_yards >= 349.5), 3),
    over_yds_354p5 = round(mean(pass_yards >= 354.5), 3),
    over_yds_359p5 = round(mean(pass_yards >= 359.5), 3),
    over_yds_364p5 = round(mean(pass_yards >= 364.5), 3),
    over_yds_369p5 = round(mean(pass_yards >= 369.5), 3),
    over_yds_374p5 = round(mean(pass_yards >= 374.5), 3),
    
    .groups = "drop"
  ) %>%
  left_join(current_team, by = "player_id") %>%
  arrange(display_name, q1_bucket, pass_def_tier)


# ══════════════════════════════════════════════════════════════════════════════
# ── Recent Form Layer (Season-Aware) ──────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# Recent form is calculated ONLY from games in the current season.
# During offseason: shows last 4 games of the season that just ended.
# During in-season: shows last 4 games of the current season, resets at Week 1.
# The `last_n_games` column tells the dashboard how many games contributed
# so it can visually flag low samples.

# ── Passing recent form ───────────────────────────────────────────────────────



passing_recent_form <- passing_weekly_full %>%
  filter(season == !!recent_season) %>%
  group_by(player_id, player_name) %>%
  arrange(desc(week), .by_group = TRUE) %>%
  slice_head(n = 4) %>%
  summarise(
    last_n_games       = n(),
    last4_avg_yards    = round(mean(pass_yards), 2),
    last4_avg_cmp      = round(mean(completions), 2),
    last4_avg_att      = round(mean(attempts), 2),
    last4_avg_td       = round(mean(touchdowns), 2),
    last4_avg_int      = round(mean(interceptions), 2),
    last4_over_yds_224p5 = round(mean(pass_yards >= 224.5), 3),
    last4_over_yds_249p5 = round(mean(pass_yards >= 249.5), 3),
    last4_over_yds_274p5 = round(mean(pass_yards >= 274.5), 3),
    last4_over_yds_299p5 = round(mean(pass_yards >= 299.5), 3),
    last4_over_cmp_19p5  = round(mean(completions >= 19.5), 3),
    last4_over_cmp_22p5  = round(mean(completions >= 22.5), 3),
    last4_over_cmp_25p5  = round(mean(completions >= 25.5), 3),
    last4_over_td_1p5    = round(mean(touchdowns >= 1.5), 3),
    last4_over_td_2p5    = round(mean(touchdowns >= 2.5), 3),
    .groups = "drop"
  )

passing_hit_rates <- passing_hit_rates %>%
  left_join(passing_recent_form, by = "player_id") %>%
  mutate(
    trend_yards = case_when(
      is.na(last4_avg_yards)                       ~ "insufficient_data",
      last4_avg_yards > avg_pass_yards * 1.15      ~ "heating_up",
      last4_avg_yards < avg_pass_yards * 0.85      ~ "cooling_down",
      TRUE                                         ~ "stable"
    ),
    trend_completions = case_when(
      is.na(last4_avg_cmp)                     ~ "insufficient_data",
      last4_avg_cmp > avg_completions * 1.15   ~ "heating_up",
      last4_avg_cmp < avg_completions * 0.85   ~ "cooling_down",
      TRUE                                     ~ "stable"
  ),
  trend_attempts = case_when(
    is.na(last4_avg_att)                     ~ "insufficient_data",
    last4_avg_att > avg_attempts * 1.15      ~ "heating_up",
    last4_avg_att < avg_attempts * 0.85      ~ "cooling_down",
    TRUE                                     ~ "stable"
  )
  )



# ── Rushing recent form ───────────────────────────────────────────────────────
rushing_recent_form <- rushing_weekly_full %>%
  filter(season == !!recent_season) %>%
  group_by(player_id, player_name) %>%
  arrange(desc(week), .by_group = TRUE) %>%
  slice_head(n = 4) %>%
  summarise(
    last_n_games          = n(),
    last4_avg_yards       = round(mean(rushing_yards), 2),
    last4_avg_carries     = round(mean(carries), 2),
    last4_over_ryd_49p5   = round(mean(rushing_yards >= 49.5), 3),
    last4_over_ryd_74p5   = round(mean(rushing_yards >= 74.5), 3),
    last4_over_ryd_99p5   = round(mean(rushing_yards >= 99.5), 3),
    last4_over_car_14p5   = round(mean(carries >= 14.5), 3),
    last4_over_car_19p5   = round(mean(carries >= 19.5), 3),
    .groups = "drop"
  )

rushing_hit_rates <- rushing_hit_rates %>%
  left_join(rushing_recent_form, by = "player_id") %>%
  mutate(
    trend_yards = case_when(
      is.na(last4_avg_yards)                       ~ "insufficient_data",
      last4_avg_yards > avg_rush_yards * 1.25      ~ "heating_up",
      last4_avg_yards < avg_rush_yards * 0.75      ~ "cooling_down",
      TRUE                                         ~ "stable"
    )
  )

# ── Receiving recent form ─────────────────────────────────────────────────────
receiving_recent_form <- receiving_weekly_full %>%
  filter(season == !!recent_season) %>%
  group_by(player_id, player_name) %>%
  arrange(desc(week), .by_group = TRUE) %>%
  slice_head(n = 4) %>%
  summarise(
    last_n_games          = n(),
    last4_avg_yards       = round(mean(rec_yrds), 2),
    last4_avg_receptions  = round(mean(receptions), 2),
    last4_avg_targets     = round(mean(targets), 2),
    last4_over_ryd_49p5   = round(mean(rec_yrds >= 49.5), 3),
    last4_over_ryd_74p5   = round(mean(rec_yrds >= 74.5), 3),
    last4_over_ryd_99p5   = round(mean(rec_yrds >= 99.5), 3),
    last4_over_rec_3p5    = round(mean(receptions >= 3.5), 3),
    last4_over_rec_5p5    = round(mean(receptions >= 5.5), 3),
    last4_over_rec_7p5    = round(mean(receptions >= 7.5), 3),
    .groups = "drop"
  )

receiving_hit_rates <- receiving_hit_rates %>%
  left_join(receiving_recent_form, by = "player_id") %>%
  mutate(
    trend_yards = case_when(
      is.na(last4_avg_yards)                       ~ "insufficient_data",
      last4_avg_yards > avg_rec_yards * 1.25       ~ "heating_up",
      last4_avg_yards < avg_rec_yards * 0.75       ~ "cooling_down",
      TRUE                                         ~ "stable"
    )
  )

# ── Tackles recent form ───────────────────────────────────────────────────────
tackle_recent_form <- tackle_weekly_full %>%
  filter(season == !!recent_season) %>%
  group_by(player_id, player_name) %>%
  arrange(desc(week), .by_group = TRUE) %>%
  slice_head(n = 4) %>%
  summarise(
    last_n_games          = n(),
    last4_avg_tackles     = round(mean(total_tackles), 2),
    last4_avg_solo        = round(mean(solo_tackles), 2),
    last4_over_tkl_5p5    = round(mean(total_tackles >= 5.5), 3),
    last4_over_tkl_7p5    = round(mean(total_tackles >= 7.5), 3),
    last4_over_tkl_9p5    = round(mean(total_tackles >= 9.5), 3),
    last4_over_solo_3p5   = round(mean(solo_tackles >= 3.5), 3),
    last4_over_solo_5p5   = round(mean(solo_tackles >= 5.5), 3),
    .groups = "drop"
  )

tackle_hit_rates <- tackle_hit_rates %>%
  left_join(tackle_recent_form, by = "player_id") %>%
  mutate(
    trend_tackles = case_when(
      is.na(last4_avg_tackles)                     ~ "insufficient_data",
      last4_avg_tackles > avg_tackles * 1.25       ~ "heating_up",
      last4_avg_tackles < avg_tackles * 0.75       ~ "cooling_down",
      TRUE                                         ~ "stable"
    )
  )

# ── Interceptions recent form ─────────────────────────────────────────────────
interception_recent_form <- interception_weekly_full %>%
  filter(season == !!recent_season) %>%
  group_by(player_id, player_name) %>%
  arrange(desc(week), .by_group = TRUE) %>%
  slice_head(n = 4) %>%
  summarise(
    last_n_games          = n(),
    last4_avg_int         = round(mean(interceptions), 3),
    last4_over_0p5        = round(mean(interceptions >= 0.5), 3),
    last4_over_1p5        = round(mean(interceptions >= 1.5), 3),
    .groups = "drop"
  )

interception_hit_rates <- interception_hit_rates %>%
  left_join(interception_recent_form, by = "player_id") %>%
  mutate(
    trend_int = case_when(
      is.na(last4_avg_int)                              ~ "insufficient_data",
      last4_avg_int > interceptions_per_game * 1.25     ~ "heating_up",
      last4_avg_int < interceptions_per_game * 0.75     ~ "cooling_down",
      TRUE                                              ~ "stable"
    )
  )





# ══════════════════════════════════════════════════════════════════════════════
# ── JSON Export: Game Log Schema ──────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

library(jsonlite)

# ── Master player list ───────────────────────────────────────────────────────
all_players <- bind_rows(
  passing_hit_rates      %>% select(player_id, display_name, current_team, position),
  rushing_hit_rates      %>% select(player_id, display_name, current_team, position),
  receiving_hit_rates    %>% select(player_id, display_name, current_team, position),
  tackle_hit_rates       %>% select(player_id, display_name, current_team, position),
  sack_matrix            %>% select(player_id, display_name, current_team, position),
  interception_hit_rates %>% select(player_id, display_name, current_team, position)
) %>%
  distinct(player_id, .keep_all = TRUE) %>%
  filter(!is.na(player_id), !is.na(display_name)) %>%
  left_join(
    current_team %>% select(player_id, headshot_url, team_change_note, is_rookie),
    by = "player_id"
  )

# ── Passing game log ─────────────────────────────────────────────────────────
passing_log <- passing_weekly_full %>%
  left_join(
    passing_stats_weekly_total %>% select(passer_player_id, game_id, defteam),
    by = c("player_id" = "passer_player_id", "nflverse_game_id" = "game_id")
  ) %>%
  left_join(q1_passing %>% select(game_id, passer_player_id, q1_pass_yards, q1_completions),
            by = c("player_id" = "passer_player_id", "nflverse_game_id" = "game_id")) %>%
  left_join(h1_passing %>% select(game_id, passer_player_id, h1_pass_yards, h1_completions),
            by = c("player_id" = "passer_player_id", "nflverse_game_id" = "game_id")) %>%
  transmute(
    player_id,
    wk       = week,
    season   = season,
    yards    = pass_yards,
    cmp      = completions,
    att      = attempts,
    td       = touchdowns,
    int      = interceptions,
    loc      = location,
    wx       = weather_bucket,
    opp      = pass_def_tier,
    q1_yds   = replace_na(q1_pass_yards, 0),
    q1_cmp   = replace_na(q1_completions, 0),
    h1_yds   = replace_na(h1_pass_yards, 0),
    h1_cmp   = replace_na(h1_completions, 0)
  ) %>%
  filter(!is.na(loc))

# ── Rushing game log ─────────────────────────────────────────────────────────
rushing_log <- rushing_weekly_full %>%
  left_join(q1_rushing %>% select(game_id, rusher_player_id, q1_rush_yards),
            by = c("player_id" = "rusher_player_id", "nflverse_game_id" = "game_id")) %>%
  left_join(h1_rushing %>% select(game_id, rusher_player_id, h1_rush_yards),
            by = c("player_id" = "rusher_player_id", "nflverse_game_id" = "game_id")) %>%
  transmute(
    player_id,
    wk       = week,
    season   = season,
    yards    = rushing_yards,
    carries,
    loc      = location,
    wx       = weather_bucket,
    opp      = run_def_tier,
    q1_yds   = replace_na(q1_rush_yards, 0),
    h1_yds   = replace_na(h1_rush_yards, 0)
  ) %>%
  filter(!is.na(loc), carries > 0)

# ── Receiving game log ───────────────────────────────────────────────────────
receiving_log <- receiving_weekly_full %>%
  left_join(q1_receiving %>% select(game_id, receiver_player_id, q1_rec_yards),
            by = c("player_id" = "receiver_player_id", "nflverse_game_id" = "game_id")) %>%
  left_join(q1_receptions %>% select(game_id, receiver_player_id, q1_receptions),
            by = c("player_id" = "receiver_player_id", "nflverse_game_id" = "game_id")) %>%
  left_join(h1_receiving %>% select(game_id, receiver_player_id, h1_rec_yards),
            by = c("player_id" = "receiver_player_id", "nflverse_game_id" = "game_id")) %>%
  left_join(h1_receptions %>% select(game_id, receiver_player_id, h1_receptions),
            by = c("player_id" = "receiver_player_id", "nflverse_game_id" = "game_id")) %>%
  transmute(
    player_id,
    wk       = week,
    season   = season,
    yards    = rec_yrds,
    rec      = receptions,
    tgt      = targets,
    loc      = location,
    wx       = weather_bucket,
    opp      = pass_def_tier,
    q1_yds   = replace_na(q1_rec_yards, 0),
    q1_rec   = replace_na(q1_receptions, 0),
    h1_yds   = replace_na(h1_rec_yards, 0),
    h1_rec   = replace_na(h1_receptions, 0)
  ) %>%
  filter(!is.na(loc), tgt > 0)

# ── Sack game log ────────────────────────────────────────────────────────────
sack_log <- sack_weekly_full %>%
  filter(season_type == seasontype)  %>%
  transmute(
    player_id,
    wk    = week,
    season   = season,
    sacks,
    loc   = location,
    wx    = weather_bucket,
    opp   = off_volume_tier
  ) %>%
  filter(!is.na(loc))

# ── Tackle game log ──────────────────────────────────────────────────────────
tackle_log <- tackle_weekly_full  %>%
  transmute(
    player_id,
    wk     = week,
    season   = season,
    tkl    = total_tackles,
    solo   = solo_tackles,
    ast    = assist_tackles,
    loc    = location,
    wx     = weather_bucket,
    opp    = off_volume_tier
  ) %>%
  filter(!is.na(loc))

# ── Interception game log ────────────────────────────────────────────────────
int_log <- interception_weekly_full  %>%
  transmute(
    player_id,
    wk    = week,
    season   = season,
    ints  = interceptions,
    loc   = location,
    wx    = weather_bucket,
    opp   = off_volume_tier
  ) %>%
  filter(!is.na(loc))

# ── Build nested per-player game logs ────────────────────────────────────────
build_player <- function(pid) {
  meta <- all_players %>% filter(player_id == pid) %>% slice(1)
  out <- list(
    display_name    = meta$display_name,
    current_team    = meta$current_team,
    position        = meta$position,
    headshot_url    = meta$headshot_url,
    team_change_note = if (!is.na(meta$team_change_note)) meta$team_change_note else NULL,
    is_rookie        = if (!is.na(meta$is_rookie)) meta$is_rookie else FALSE
  )
  
  p <- passing_log   %>% filter(player_id == pid) %>% select(-player_id)
  if (nrow(p) > 0) out$passing <- p
  
  r <- rushing_log   %>% filter(player_id == pid) %>% select(-player_id)
  if (nrow(r) > 0) out$rushing <- r
  
  rc <- receiving_log %>% filter(player_id == pid) %>% select(-player_id)
  if (nrow(rc) > 0) out$receiving <- rc
  
  s <- sack_log      %>% filter(player_id == pid) %>% select(-player_id)
  if (nrow(s) > 0) out$sacks <- s
  
  t <- tackle_log    %>% filter(player_id == pid) %>% select(-player_id)
  if (nrow(t) > 0) out$tackles <- t
  
  i <- int_log       %>% filter(player_id == pid) %>% select(-player_id)
  if (nrow(i) > 0) out$interceptions <- i
  
  out
}

message("Building game logs for ", nrow(all_players), " players...")

master_json <- all_players$player_id %>%
  set_names() %>%
  map(build_player)

# ── Add metadata ─────────────────────────────────────────────────────────────
export_object <- list(
  meta = list(
    current_season = current_season,
    prior_season   = prior_season,
    season_type    = seasontype,
    generated_at   = as.character(Sys.time()),
    n_players      = length(master_json)
  ),
  players = master_json
)

# ── Write minified JSON ──────────────────────────────────────────────────────
output_path <- "NFL/propbetiq_nfl.json"

write_json(
  export_object,
  path       = output_path,
  auto_unbox = TRUE,
  na         = "null",
  null       = "null",
  pretty     = FALSE,
  dataframe  = "rows"  # important: exports each game log as array of objects
)

file_size_mb <- file.info(output_path)$size / 1024 / 1024
message("Wrote ", output_path, " (", round(file_size_mb, 2), " MB)")


# ══════════════════════════════════════════════════════════════════════════════
# PUSH NFL FILES TO GITHUB PAGES
# ══════════════════════════════════════════════════════════════════════════════

push_nfl_to_github <- function() {
  
  # GitHub Actions supplies GH_PUSH_TOKEN; local runs fall back to the token file
  github_token <- Sys.getenv("GH_PUSH_TOKEN")
  if (!nzchar(github_token)) {
    token_file <- "C:/Users/ccrai/OneDrive/Documents/.github_token.txt"
    if (!file.exists(token_file)) stop("GitHub token file not found at ", token_file)
    github_token <- readLines(token_file, warn = FALSE)[1]
  }
  github_user  <- "backironanalytics"
  github_repo  <- "DDS"
  
  # Get current per-tier slugs (rotates if needed)
  slugs <- get_current_slugs("nfl")
  
  # Rewrite HTML to point to slugged JSON — each tier gets its own temp file
  basic_html_tmp <- rewrite_html_json_ref(
    "NFL/index-basic.html", "propbetiq_nfl.json", slugs$json
  )
  edge_html_tmp <- rewrite_html_json_ref(
    "NFL/index-edge.html", "propbetiq_nfl.json", slugs$json
  )
  
  files_to_push <- list(
    list(local = basic_html_tmp,           remote = paste0("NFL/", slugs$html_basic)),
    list(local = edge_html_tmp,            remote = paste0("NFL/", slugs$html_edge)),
    list(local = "NFL/propbetiq_nfl.json", remote = paste0("NFL/", slugs$json))
  )
  
  base_url <- paste0("https://api.github.com/repos/", github_user, "/", github_repo, "/contents/")
  headers  <- httr::add_headers(
    Authorization = paste("token", github_token),
    Accept        = "application/vnd.github.v3+json"
  )
  
  for (f in files_to_push) {
    if (!file.exists(f$local)) {
      message("  Skipping missing file: ", f$local)
      next
    }
    message("  Pushing ", f$remote, "...")
    
    raw_bytes <- readBin(f$local, "raw", file.size(f$local))
    encoded   <- base64enc::base64encode(raw_bytes)
    
    existing <- httr::GET(paste0(base_url, f$remote), headers)
    sha      <- NULL
    if (httr::status_code(existing) == 200) sha <- httr::content(existing)$sha
    
    body <- list(
      message = paste0("PropBetIQ NFL update ", format(Sys.Date(), "%Y-%m-%d")),
      content = encoded
    )
    if (!is.null(sha)) body$sha <- sha
    
    result <- httr::PUT(
      url    = paste0(base_url, f$remote),
      config = headers,
      body   = jsonlite::toJSON(body, auto_unbox = TRUE),
      httr::content_type_json()
    )
    
    if (httr::status_code(result) %in% c(200, 201)) {
      message("    ✓ ", f$remote, " pushed successfully")
    } else {
      message("    ✗ Failed to push ", f$remote, " — status: ", httr::status_code(result))
    }
    
    Sys.sleep(0.5)
  }
  
  # Cleanup — pass ALL current slugs (basic, edge, json) so cleanup keeps all 3
  message("\n  Cleaning up old orphan files from GitHub...")
  cleanup_old_slugs(
    sport         = "nfl",
    github_user   = github_user,
    github_repo   = github_repo,
    github_token  = github_token,
    current_slugs = list(
      basic = slugs$basic_slug,
      edge  = slugs$edge_slug,
      json  = slugs$json_slug
    )
  )
}
tryCatch({
  message("\nPushing NFL files to GitHub Pages...")
  push_nfl_to_github()
  message("✓ NFL GitHub push complete")
  print_current_urls()  # Print URLs for Patreon updates
}, error = function(e) {
  message("NFL GitHub push failed (non-fatal): ", conditionMessage(e))
})
