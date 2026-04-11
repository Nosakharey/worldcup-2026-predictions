CREATE OR REPLACE MODEL `worldcup-2026-predictions.ml_models.golden_boot_model_v2`
OPTIONS(
  model_type='LINEAR_REG',
  input_label_cols=['wc_goals']
) AS
SELECT
  p.current_season_goals,
  p.current_season_assists,
  p.current_rating,
  p.shots_total,
  p.shots_on,
  p.conversion_rate,
  p.penalty_goals,
  p.fifa_shooting,
  p.fifa_pace,
  p.fifa_dribbling,
  p.historical_goals,
  p.age,
  p.position,
  COUNT(g.goal_sk) as wc_goals
FROM `worldcup-2026-predictions.dbt_nagbonze.dim_players` p
JOIN `worldcup-2026-predictions.dbt_nagbonze.fct_goals` g
  ON p.player_sk = g.player_sk
JOIN `worldcup-2026-predictions.dbt_nagbonze.fct_matches` m
  ON g.match_sk = m.match_sk
WHERE m.is_world_cup = true
AND g.is_own_goal = false
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13