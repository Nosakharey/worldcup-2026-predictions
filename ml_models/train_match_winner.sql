CREATE OR REPLACE MODEL `worldcup-2026-predictions.ml_models.match_winner_model_v3`
OPTIONS(
  model_type='LOGISTIC_REG',
  input_label_cols=['match_result'],
  max_iterations=50,
  learn_rate=0.1
) AS
SELECT
  m.match_result,
  ht.recent_win_rate_pct as home_recent_win_rate,
  ht.wc_win_rate_pct as home_wc_win_rate,
  ht.attacking_strength as home_attacking_strength,
  ht.defensive_strength as home_defensive_strength,
  ht.fifa_overall_rating as home_fifa_rating,
  ht.squad_avg_rating as home_squad_rating,
  ht.top_scorer_goals as home_top_scorer_goals,
  ht.num_players_in_form as home_players_in_form,
  at.recent_win_rate_pct as away_recent_win_rate,
  at.wc_win_rate_pct as away_wc_win_rate,
  at.attacking_strength as away_attacking_strength,
  at.defensive_strength as away_defensive_strength,
  at.fifa_overall_rating as away_fifa_rating,
  at.squad_avg_rating as away_squad_rating,
  at.top_scorer_goals as away_top_scorer_goals,
  at.num_players_in_form as away_players_in_form,
  m.is_neutral_venue,
  m.is_world_cup
FROM `worldcup-2026-predictions.dbt_nagbonze.fct_matches` m
JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` ht
  ON m.home_team = ht.team_name
JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` at
  ON m.away_team = at.team_name
WHERE m.match_year >= 2010
AND m.match_result IS NOT NULL