CREATE OR REPLACE TABLE `worldcup-2026-predictions.predictions.best_young_player_predictions` AS
SELECT
  p.player_name,
  p.nationality,
  p.position,
  p.current_club,
  p.age,
  p.current_season_goals,
  p.current_season_assists,
  p.current_rating,
  p.fifa_overall,
  p.historical_goals,
  p.league_weight,
  (23 - p.age) as youth_bonus,
  ROUND(predicted_tournament_impact_score, 2) as predicted_tournament_impact_score,
  ROUND(
    predicted_tournament_impact_score +
    ((23 - p.age) * 0.5)
  , 2) as young_player_score,
  ROW_NUMBER() OVER (
    ORDER BY predicted_tournament_impact_score + ((23 - p.age) * 0.5) DESC
  ) as young_player_rank
FROM ML.PREDICT(
  MODEL `worldcup-2026-predictions.ml_models.young_player_model`,
  (
    SELECT * FROM `worldcup-2026-predictions.dbt_nagbonze.dim_players`
    WHERE age <= 23
    AND current_rating IS NOT NULL
    AND fifa_overall IS NOT NULL
  )
) p
ORDER BY young_player_score DESC