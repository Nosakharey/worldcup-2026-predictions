{{ config(
    materialized='table',
    partition_by={
        "field": "match_date",
        "data_type": "date",
        "granularity": "year"
    },
    cluster_by=["home_team", "away_team"]
) }}

with matches as (
    select * from {{ ref('stg_results') }}
),

shootouts as (
    select * from {{ ref('stg_shootouts') }}
),

final as (
    select
        m.match_date,
        {{ standardize_team_name('m.home_team') }} as home_team,
        {{ standardize_team_name('m.away_team') }} as away_team,
        m.home_score,
        m.away_score,
        m.tournament,
        m.is_neutral_venue,
        m.home_score - m.away_score as goal_difference,
        case
            when m.home_score > m.away_score then 'home_win'
            when m.home_score < m.away_score then 'away_win'
            else 'draw'
        end as match_result,
        case
            when s.winner is not null then true
            else false
        end as had_shootout,
        s.winner as shootout_winner
    from matches m
    left join shootouts s
        on m.match_date = s.match_date
        and lower(m.home_team) = lower(s.home_team)
        and lower(m.away_team) = lower(s.away_team)
)

select * from final