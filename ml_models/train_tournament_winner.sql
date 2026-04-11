CREATE OR REPLACE MODEL `worldcup-2026-predictions.ml_models.tournament_winner_model`
OPTIONS(
  model_type='LOGISTIC_REG',
  input_label_cols=['reached_final']
) AS
SELECT
  ts.team_name,
  ts.overall_strength_score,
  ts.recent_win_rate_pct,
  ts.wc_win_rate_pct,
  ts.attacking_strength,
  ts.defensive_strength,
  ts.fifa_overall_rating,
  ts.squad_avg_rating,
  ts.num_players_in_form,
  ts.squad_total_goals,
  CASE
    WHEN ts.confederation = 'UEFA' THEN 1
    WHEN ts.confederation = 'CONMEBOL' THEN 2
    ELSE 3
  END as confederation_tier,
  CASE
    WHEN f.champion = ts.team_name
    OR f.runner_up = ts.team_name THEN true
    ELSE false
  END as reached_final
FROM `worldcup-2026-predictions.dbt_nagbonze.mart_squad_strength` ts
CROSS JOIN `worldcup-2026-predictions.dbt_nagbonze.stg_fifa_summary` f
WHERE f.tournament_year >= 1990