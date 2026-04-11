CREATE OR REPLACE MODEL `worldcup-2026-predictions.ml_models.golden_ball_model_v2`
OPTIONS(
  model_type='LINEAR_REG',
  input_label_cols=['tournament_impact_score']
) AS
SELECT
  p.current_rating,
  p.current_season_goals,
  p.current_season_assists,
  p.key_passes,
  p.dribbles_success,
  p.fifa_overall,
  p.fifa_shooting,
  p.fifa_passing,
  p.historical_goals,
  p.age,
  p.position,
  ROUND(
    (p.current_rating * 10 * 0.3) +
    (p.current_season_goals * 0.2) +
    (p.current_season_assists * 0.2) +
    (p.fifa_overall * 0.3)
  , 2) as tournament_impact_score
FROM `worldcup-2026-predictions.dbt_nagbonze.dim_players` p
WHERE p.current_rating IS NOT NULL
AND p.fifa_overall IS NOT NULL