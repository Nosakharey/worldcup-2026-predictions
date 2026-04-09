{{ config(
    materialized='table',
    cluster_by=["nationality", "overall_rating"]
) }}

with players as (
    select * from {{ ref('stg_players_24') }}
),

scorers as (
    select * from {{ ref('stg_goalscorers') }}
),

wc_groups as (
    select * from {{ ref('world_cup_2026_groups') }}
),

player_goals as (
    select
        scorer_name,
        scoring_team as nationality,
        count(*) as international_goals
    from scorers
    group by 1, 2
),

final as (
    select
        p.short_name,
        p.long_name,
        p.player_positions,
        {{ standardize_team_name('p.nationality_name') }} as nationality,
        p.club_name,
        p.overall as overall_rating,
        p.pace,
        p.shooting,
        p.passing,
        p.dribbling,
        p.defending,
        p.physic,
        coalesce(g.international_goals, 0) as international_goals,
        w.group as wc_2026_group
    from players p
    left join player_goals g
        on lower(trim(p.short_name)) = g.scorer_name
        and {{ standardize_team_name('p.nationality_name') }} = g.nationality
    inner join wc_groups w
        on {{ standardize_team_name('p.nationality_name') }} = w.team_name
)

select * from final