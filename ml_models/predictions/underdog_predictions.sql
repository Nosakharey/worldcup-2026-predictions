CREATE OR REPLACE TABLE `worldcup-2026-predictions.predictions.underdog_predictions` AS
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
  ht.confederation as home_confederation,
  at.confederation as away_confederation,
  ROUND(ht.combined_strength_score, 2) as home_combined_score,
  ROUND(at.combined_strength_score, 2) as away_combined_score,
  ROUND(ABS(ht.combined_strength_score - at.combined_strength_score), 2) as strength_gap,
  ROUND(ABS(ht.fifa_overall_rating - at.fifa_overall_rating), 0) as fifa_rating_gap,
  CASE
    WHEN ht.combined_strength_score < at.combined_strength_score
    THEN f.home_team ELSE f.away_team
  END as underdog_team,
  CASE
    WHEN ht.combined_strength_score >= at.combined_strength_score
    THEN f.home_team ELSE f.away_team
  END as favorite_team,
  ROUND(predicted_is_upset_probs[OFFSET(1)].prob * 100, 1) as upset_probability_pct,
  predicted_is_upset,
  CASE
    WHEN ROUND(predicted_is_upset_probs[OFFSET(1)].prob * 100, 1) >= 70
    THEN 'High Upset Potential'
    WHEN ROUND(predicted_is_upset_probs[OFFSET(1)].prob * 100, 1) >= 40
    THEN 'Medium Upset Potential'
    ELSE 'Low Upset Potential'
  END as upset_classification
FROM ML.PREDICT(
  MODEL `worldcup-2026-predictions.ml_models.underdog_model`,
  (
    SELECT
      f.*,
      ABS(ht.overall_strength_score - at.overall_strength_score) as strength_gap,
      ABS(ht.fifa_overall_rating - at.fifa_overall_rating) as fifa_rating_gap,
      ABS(ht.recent_win_rate_pct - at.recent_win_rate_pct) as recent_form_gap,
      ABS(ht.wc_win_rate_pct - at.wc_win_rate_pct) as wc_experience_gap,
      LEAST(ht.overall_strength_score, at.overall_strength_score) as weaker_team_strength,
      CASE
        WHEN ht.overall_strength_score < at.overall_strength_score
        THEN ht.defensive_strength
        ELSE at.defensive_strength
      END as weaker_team_defense,
      CASE
        WHEN ht.overall_strength_score < at.overall_strength_score
        THEN ht.recent_win_rate_pct
        ELSE at.recent_win_rate_pct
      END as weaker_team_recent_form,
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
ORDER BY upset_probability_pct DESC