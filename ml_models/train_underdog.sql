CREATE OR REPLACE MODEL `worldcup-2026-predictions.ml_models.underdog_model`
OPTIONS(
  model_type='LOGISTIC_REG',
  input_label_cols=['is_upset']
) AS
SELECT
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
  m.is_neutral_venue,
  m.is_world_cup,
  CASE
    WHEN (m.match_result = 'home_win'
      AND ht.overall_strength_score < at.overall_strength_score)
    OR (m.match_result = 'away_win'
      AND at.overall_strength_score < ht.overall_strength_score)
    THEN true
    ELSE false
  END as is_upset
FROM `worldcup-2026-predictions.dbt_nagbonze.fct_matches` m
JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` ht
  ON m.home_team = ht.team_name
JOIN `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` at
  ON m.away_team = at.team_name
WHERE m.match_year >= 2010