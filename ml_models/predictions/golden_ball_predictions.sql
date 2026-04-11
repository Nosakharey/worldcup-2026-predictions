CREATE OR REPLACE TABLE `worldcup-2026-predictions.predictions.golden_ball_predictions` AS
SELECT
  p.player_name,
  p.nationality,
  p.position,
  p.current_club,
  p.age,
  p.current_season_goals,
  p.current_season_assists,
  p.current_rating,
  p.key_passes,
  p.dribbles_success,
  p.fifa_overall,
  p.fifa_passing,
  p.historical_goals,
  p.league_weight,
  ROUND(
    (p.current_rating * 10 * 0.3) +
    (p.current_season_goals * 0.2) +
    (p.current_season_assists * 0.2) +
    (p.fifa_overall * 0.3)
  , 2) as golden_ball_score,
  ROUND(predicted_tournament_impact_score, 2) as predicted_tournament_impact_score,
  ROW_NUMBER() OVER (ORDER BY predicted_tournament_impact_score DESC) as golden_ball_rank,
  ROW_NUMBER() OVER (
    PARTITION BY p.nationality
    ORDER BY predicted_tournament_impact_score DESC
  ) as rank_in_squad
FROM ML.PREDICT(
  MODEL `worldcup-2026-predictions.ml_models.golden_ball_model_v2`,
  (
    SELECT * FROM `worldcup-2026-predictions.dbt_nagbonze.dim_players`
    WHERE current_rating IS NOT NULL
    AND fifa_overall IS NOT NULL
  )
) p
ORDER BY predicted_tournament_impact_score DESC