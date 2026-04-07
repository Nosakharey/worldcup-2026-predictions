{{ config(
    materialized='table',
    partition_by={
        'field': 'match_date',
        'data_type': 'date',
        'granularity': 'year'
    },
    cluster_by=['home_team', 'away_team']
) }}

with matches as (
    select * from {{ ref('stg_results') }}
),

teams as (
    select
        team_sk,
        team_name
    from {{ ref('dim_teams') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'match_date',
            'home_team',
            'away_team',
            'home_score',
            'away_score'
        ]) }} as match_sk,

        m.match_date,
        m.home_team,
        m.away_team,
        m.home_score,
        m.away_score,
        m.tournament,
        m.city,
        m.country,
        m.is_neutral_venue,

        case
            when m.home_score > m.away_score then 'home_win'
            when m.home_score < m.away_score then 'away_win'
            else 'draw'
        end as match_result,

        m.home_score + m.away_score as total_goals,

        case
            when lower(m.tournament) like '%world cup%'
                then true
            else false
        end as is_world_cup,

        case
            when lower(m.tournament) like '%world cup%'
            or lower(m.tournament) like '%continental%'
            or lower(m.tournament) like '%euro%'
            or lower(m.tournament) like '%copa america%'
            or lower(m.tournament) like '%africa cup%'
            or lower(m.tournament) like '%asian cup%'
            or lower(m.tournament) like '%gold cup%'
            or lower(m.tournament) like '%nations league%'
                then true
            else false
        end as is_major_tournament,

        case
            when lower(m.tournament) = 'friendly'
                then true
            else false
        end as is_friendly,

        extract(year from m.match_date) as match_year,

        case
            when extract(year from m.match_date) >= 2022 then 1.0
            when extract(year from m.match_date) >= 2018 then 0.75
            when extract(year from m.match_date) >= 2014 then 0.5
            when extract(year from m.match_date) >= 2010 then 0.25
            else 0.1
        end as recency_weight,

        ht.team_sk as home_team_sk,
        away_teams.team_sk as away_team_sk

    from matches m
    left join teams ht
        on {{ standardize_team_name('m.home_team') }} = ht.team_name
    left join teams away_teams
        on {{ standardize_team_name('m.away_team') }} = away_teams.team_name
)

select * from final