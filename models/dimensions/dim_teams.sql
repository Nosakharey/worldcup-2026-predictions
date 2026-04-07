{{ config(
    materialized='table',
    cluster_by=['team_name', 'confederation']
) }}

with national_teams as (
    select
        {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_sk,
        team_name,
        `group` as wc_2026_group,
        confederation
    from {{ ref('world_cup_2026_groups') }}
),

team_stats as (
    select
        {{ standardize_team_name('home_team') }} as team_name,
        count(*) as total_home_games,
        sum(case when home_score > away_score then 1 else 0 end) as home_wins,
        sum(case when home_score = away_score then 1 else 0 end) as home_draws,
        sum(case when home_score < away_score then 1 else 0 end) as home_losses,
        avg(home_score) as avg_home_goals_scored,
        avg(away_score) as avg_home_goals_conceded
    from {{ ref('stg_results') }}
    group by 1
),

away_stats as (
    select
        {{ standardize_team_name('away_team') }} as team_name,
        count(*) as total_away_games,
        sum(case when away_score > home_score then 1 else 0 end) as away_wins,
        sum(case when away_score = home_score then 1 else 0 end) as away_draws,
        sum(case when away_score < home_score then 1 else 0 end) as away_losses,
        avg(away_score) as avg_away_goals_scored,
        avg(home_score) as avg_away_goals_conceded
    from {{ ref('stg_results') }}
    group by 1
),

fifa_ratings as (
    select
        lower(trim(team_name)) as team_name,
        max(overall) as fifa_overall,
        max(attack) as fifa_attack,
        max(midfield) as fifa_midfield,
        max(defence) as fifa_defence
    from {{ ref('stg_teams_24') }}
    group by 1
),

combined as (
    select
        n.team_sk,
        n.team_name,
        n.wc_2026_group,
        n.confederation,
        -- historical stats
        coalesce(h.total_home_games, 0) + coalesce(a.total_away_games, 0) 
            as total_games,
        coalesce(h.home_wins, 0) + coalesce(a.away_wins, 0) 
            as total_wins,
        coalesce(h.home_draws, 0) + coalesce(a.away_draws, 0) 
            as total_draws,
        coalesce(h.home_losses, 0) + coalesce(a.away_losses, 0) 
            as total_losses,
        round(
            (coalesce(h.home_wins, 0) + coalesce(a.away_wins, 0)) * 100.0 /
            nullif(
                coalesce(h.total_home_games, 0) + coalesce(a.total_away_games, 0)
            , 0)
        , 2) as win_rate_pct,
        round(
            (coalesce(h.avg_home_goals_scored, 0) + 
             coalesce(a.avg_away_goals_scored, 0)) / 2
        , 2) as avg_goals_scored,
        round(
            (coalesce(h.avg_home_goals_conceded, 0) + 
             coalesce(a.avg_away_goals_conceded, 0)) / 2
        , 2) as avg_goals_conceded,
        -- fifa ratings
        coalesce(f.fifa_overall, 70) as fifa_overall,
        coalesce(f.fifa_attack, 70) as fifa_attack,
        coalesce(f.fifa_midfield, 70) as fifa_midfield,
        coalesce(f.fifa_defence, 70) as fifa_defence
    from national_teams n
    left join team_stats h on n.team_name = h.team_name
    left join away_stats a on n.team_name = a.team_name
    left join fifa_ratings f on n.team_name = f.team_name
)

select * from combined