with source as (
    select * from {{ source('raw_data', 'players_24') }}
),

renamed as (
    select
        cast(player_id as integer) as player_id,
        short_name,
        long_name,
        player_positions,
        cast(overall as integer) as overall,
        cast(potential as integer) as potential,
        cast(value_eur as numeric) as value_eur,
        cast(wage_eur as numeric) as wage_eur,
        cast(age as integer) as age,
        nationality_name,
        club_name,
        league_name,
        cast(overall as integer) as overall_rating,
        cast(pace as integer) as pace,
        cast(shooting as integer) as shooting,
        cast(passing as integer) as passing,
        cast(dribbling as integer) as dribbling,
        cast(defending as integer) as defending,
        cast(physic as integer) as physic
    from source
    where player_id is not null
)

select * from renamed