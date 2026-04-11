CREATE OR REPLACE TABLE `worldcup-2026-predictions.predictions.golden_boot_predictions` AS
SELECT
  p.player_name,
  p.nationality,
  p.position,
  p.current_club,
  p.age,
  p.current_season_goals,
  p.current_rating,
  p.fifa_shooting,
  p.historical_goals,
  p.league_weight,
  ROUND(predicted_wc_goals, 2) as predicted_wc_goals,
  ROUND(
    (p.current_season_goals * 0.4) +
    (p.fifa_shooting * 0.3) +
    (p.fifa_overall * 0.2) +
    (p.historical_goals * 0.1)
  , 2) as best_golden_boot_score,
  ROW_NUMBER() OVER (ORDER BY predicted_wc_goals DESC) as golden_boot_rank
FROM ML.PREDICT(
  MODEL `worldcup-2026-predictions.ml_models.golden_boot_model_v2`,
  (
    SELECT * FROM `worldcup-2026-predictions.dbt_nagbonze.dim_players`
    WHERE current_season_goals IS NOT NULL
  )
) p
ORDER BY predicted_wc_goals DESC