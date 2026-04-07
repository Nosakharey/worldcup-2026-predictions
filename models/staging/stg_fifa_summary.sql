with source as (
    select * from {{ source('raw_data', 'fifa_world_cup_summary') }}
),

renamed as (
    select
        cast(YEAR as integer) as tournament_year,
        lower(trim(HOST)) as host_country,
        lower(trim(CHAMPION)) as champion,
        lower(trim(`RUNNER UP`)) as runner_up,
        lower(trim(`THIRD PLACE`)) as third_place,
        cast(TEAMS as integer) as teams_count,
        cast(`MATCHES PLAYED` as integer) as matches_played,
        cast(`GOALS SCORED` as integer) as goals_scored,
        cast(`AVG GOALS PER GAME` as numeric) as avg_goals_per_game
    from source
    where YEAR is not null
)

select * from renamed