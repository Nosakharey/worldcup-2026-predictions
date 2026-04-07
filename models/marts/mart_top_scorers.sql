{{ config(
    materialized='table',
    cluster_by=["nationality", "scorer_name"]
) }}

with goalscorers as (
    select * from {{ ref('stg_goalscorers') }}
),

wc_teams as (
    select * from {{ ref('world_cup_2026_groups') }}
),

scorer_stats as (
    select
        {{ standardize_team_name('g.scoring_team') }} as nationality,
        g.scorer_name,
        count(*) as total_goals,
        sum(case when g.is_penalty then 1 else 0 end) as penalty_goals,
        sum(case when g.is_own_goal then 1 else 0 end) as own_goals,
        count(*) - sum(case when g.is_penalty then 1 else 0 end) 
            - sum(case when g.is_own_goal then 1 else 0 end) as open_play_goals,
        min(g.match_date) as first_goal_date,
        max(g.match_date) as last_goal_date
    from goalscorers g
    group by 1, 2
),

wc_scorers as (
    select
        s.*,
        w.group as wc_2026_group
    from scorer_stats s
    inner join wc_teams w
        on s.nationality = w.team_name
)

select * from wc_scorers