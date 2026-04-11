CREATE OR REPLACE TABLE `worldcup-2026-predictions.predictions.group_stage_predictions` AS
SELECT
  f.match_id,
  f.group_name,
  f.home_team,
  f.away_team,
  f.match_date,
  f.venue,
  f.city,
  ht.star_player_name as home_star_player,
  at.star_player_name as away_star_player,
  ROUND(ht.combined_strength_score, 2) as home_strength,
  ROUND(at.combined_strength_score, 2) as away_strength,
  predicted_match_result,
  ROUND(probs[OFFSET(0)].prob * 100, 1) as home_win_pct,
  ROUND(probs[OFFSET(1)].prob * 100, 1) as draw_pct,
  ROUND(probs[OFFSET(2)].prob * 100, 1) as away_win_pct
FROM ML.PREDICT(
  MODEL `worldcup-2026-predictions.ml_models.match_winner_model_v3`,
  (
    SELECT
      f.*,
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
      true as is_neutral_venue,
      true as is_world_cup
    FROM `worldcup-2026-predictions.raw_data.world_cup_2026_fixtures` f
    LEFT JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` ht
      ON LOWER(f.home_team) = ht.team_name
    LEFT JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` at
      ON LOWER(f.away_team) = at.team_name
  )
)
JOIN `worldcup-2026-predictions.raw_data.world_cup_2026_fixtures` f USING (match_id)
LEFT JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` ht
  ON LOWER(f.home_team) = ht.team_name
LEFT JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` at
  ON LOWER(f.away_team) = at.team_name
ORDER BY f.match_date, f.match_id