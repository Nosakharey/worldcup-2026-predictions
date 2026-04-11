CREATE OR REPLACE TABLE `worldcup-2026-predictions.predictions.tournament_winner_predictions` AS
SELECT
  ts.team_name,
  ts.wc_2026_group,
  ts.confederation,
  ts.overall_strength_score,
  ts.recent_win_rate_pct,
  ts.wc_win_rate_pct,
  ts.fifa_overall_rating,
  ts.squad_avg_rating,
  ts.star_player_name,
  ts.top_scorer_goals,
  ROUND(predicted_reached_final * 100, 1) as finals_probability_pct,
  ROUND((1 - predicted_reached_final) * 100, 1) as eliminated_probability_pct,
  ROW_NUMBER() OVER (ORDER BY predicted_reached_final DESC) as tournament_rank
FROM ML.PREDICT(
  MODEL `worldcup-2026-predictions.ml_models.tournament_winner_model`,
  (
    SELECT
      ts.*,
      CASE
        WHEN ts.confederation = 'UEFA' THEN 1
        WHEN ts.confederation = 'CONMEBOL' THEN 2
        ELSE 3
      END as confederation_tier
    FROM `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` ts
  )
)
ORDER BY finals_probability_pct DESC