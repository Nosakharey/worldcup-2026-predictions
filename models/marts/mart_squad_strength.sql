{{ config(
    materialized='table',
    cluster_by=['team_name']
) }}

with team_strength as (
    select * from {{ ref('mart_team_strength') }}
),

players as (
    select * from {{ ref('dim_players') }}
),

wc_teams as (
    select * from {{ ref('world_cup_2026_groups') }}
),

-- squad level aggregations per nation
squad_stats as (
    select
        nationality as team_name,

        -- squad size in our data
        count(*) as num_players_tracked,

        -- average current form
        round(avg(current_rating), 3) as squad_avg_rating,
        round(avg(fifa_overall), 2) as squad_avg_fifa_overall,

        -- current season attacking output
        sum(current_season_goals) as squad_total_goals,
        sum(current_season_assists) as squad_total_assists,
        sum(shots_total) as squad_total_shots,
        sum(shots_on) as squad_total_shots_on,
        round(avg(conversion_rate), 2) as squad_avg_conversion_rate,

        -- in form players (rating > 7.5)
        countif(current_rating >= 7.5) as num_players_in_form,

        -- young players
        countif(is_young_player = true) as num_young_players,

        -- top scorer stats
        max(current_season_goals) as top_scorer_goals,
        max(current_season_assists) as top_assister_assists,
        max(golden_boot_score) as best_golden_boot_score,
        max(best_assist_score) as best_assist_score,
        max(golden_ball_score) as best_golden_ball_score,

        -- historical international pedigree
        sum(historical_goals) as squad_total_historical_goals,
        max(historical_goals) as top_scorer_historical_goals,

        -- FIFA ability
        round(avg(fifa_shooting), 2) as squad_avg_shooting,
        round(avg(fifa_pace), 2) as squad_avg_pace,
        round(avg(fifa_passing), 2) as squad_avg_passing,
        round(avg(fifa_dribbling), 2) as squad_avg_dribbling,
        round(avg(fifa_defending), 2) as squad_avg_defending,
        round(avg(fifa_physic), 2) as squad_avg_physic,

        -- composite squad score
        -- current form 50% + FIFA ability 30% + historical 20%
        round(
            (avg(current_rating) * 10 * 0.5) +
            (avg(fifa_overall) * 0.3) +
            (least(sum(historical_goals) / 10.0, 10) * 0.2)
        , 2) as squad_composite_score

    from players
    group by nationality
),

-- get star player per nation
star_players as (
    select distinct
        nationality as team_name,
        first_value(player_name) over (
            partition by nationality
            order by golden_ball_score desc
        ) as star_player_name,
        first_value(current_season_goals) over (
            partition by nationality
            order by golden_boot_score desc
        ) as star_player_goals,
        first_value(current_season_assists) over (
            partition by nationality
            order by best_assist_score desc
        ) as star_player_assists,
        first_value(current_rating) over (
            partition by nationality
            order by golden_ball_score desc
        ) as star_player_rating,
        first_value(fifa_overall) over (
            partition by nationality
            order by fifa_overall desc
        ) as star_player_fifa_overall,
        first_value(position) over (
            partition by nationality
            order by golden_ball_score desc
        ) as star_player_position
    from players
),

final as (
    select
        t.team_name,
        t.wc_2026_group,
        t.confederation,

        -- team historical performance
        t.total_games,
        t.win_rate_pct,
        t.recent_win_rate_pct,
        t.wc_win_rate_pct,
        t.weighted_win_rate_pct,
        t.overall_strength_score,
        t.attacking_strength,
        t.defensive_strength,
        t.neutral_win_rate_pct,

        -- goals stats
        t.avg_goals_scored,
        t.avg_goals_conceded,
        t.recent_avg_goals_scored,
        t.recent_avg_goals_conceded,
        t.wc_avg_goals_scored,
        t.wc_avg_goals_conceded,

        -- FIFA team ratings
        t.fifa_overall_rating,
        t.fifa_attack_rating,
        t.fifa_midfield_rating,
        t.fifa_defence_rating,

        -- squad current form
        coalesce(s.num_players_tracked, 0) as num_players_tracked,
        coalesce(s.squad_avg_rating, 6.5) as squad_avg_rating,
        coalesce(s.squad_avg_fifa_overall, 70) as squad_avg_fifa_overall,
        coalesce(s.squad_total_goals, 0) as squad_total_goals,
        coalesce(s.squad_total_assists, 0) as squad_total_assists,
        coalesce(s.squad_total_shots, 0) as squad_total_shots,
        coalesce(s.squad_total_shots_on, 0) as squad_total_shots_on,
        coalesce(s.squad_avg_conversion_rate, 0) as squad_avg_conversion_rate,
        coalesce(s.num_players_in_form, 0) as num_players_in_form,
        coalesce(s.num_young_players, 0) as num_young_players,
        coalesce(s.top_scorer_goals, 0) as top_scorer_goals,
        coalesce(s.top_assister_assists, 0) as top_assister_assists,
        coalesce(s.best_golden_boot_score, 0) as best_golden_boot_score,
        coalesce(s.best_assist_score, 0) as best_assist_score,
        coalesce(s.best_golden_ball_score, 0) as best_golden_ball_score,
        coalesce(s.squad_total_historical_goals, 0) as squad_total_historical_goals,
        coalesce(s.top_scorer_historical_goals, 0) as top_scorer_historical_goals,

        -- FIFA skill breakdown
        coalesce(s.squad_avg_shooting, 50) as squad_avg_shooting,
        coalesce(s.squad_avg_pace, 50) as squad_avg_pace,
        coalesce(s.squad_avg_passing, 50) as squad_avg_passing,
        coalesce(s.squad_avg_dribbling, 50) as squad_avg_dribbling,
        coalesce(s.squad_avg_defending, 50) as squad_avg_defending,
        coalesce(s.squad_avg_physic, 50) as squad_avg_physic,
        coalesce(s.squad_composite_score, 0) as squad_composite_score,

        -- star player
        coalesce(sp.star_player_name, 'unknown') as star_player_name,
        coalesce(sp.star_player_goals, 0) as star_player_goals,
        coalesce(sp.star_player_assists, 0) as star_player_assists,
        coalesce(sp.star_player_rating, 0) as star_player_rating,
        coalesce(sp.star_player_fifa_overall, 0) as star_player_fifa_overall,
        coalesce(sp.star_player_position, 'unknown') as star_player_position,

        -- FINAL COMBINED STRENGTH SCORE
        -- Team form 40% + Squad current form 35% + FIFA ratings 25%
        round(
            (t.overall_strength_score * 0.40) +
            (coalesce(s.squad_composite_score, 50) * 0.35) +
            (coalesce(t.fifa_overall_rating, 75) * 0.25)
        , 2) as combined_strength_score

    from team_strength t
    left join squad_stats s
        on t.team_name = s.team_name
    left join star_players sp
        on t.team_name = sp.team_name
)

select * from final