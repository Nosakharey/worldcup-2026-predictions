with source as (
    select * from {{ source('raw_data', 'teams_24') }}
),

renamed as (
    select
        cast(team_id as integer) as team_id,
        lower(trim(team_name)) as team_name,
        lower(trim(nationality_name)) as nationality_name,
        lower(trim(league_name)) as league_name,
        cast(overall as integer) as overall,
        cast(attack as integer) as attack,
        cast(midfield as integer) as midfield,
        cast(defence as integer) as defence,
        cast(international_prestige as integer) as international_prestige,
        cast(domestic_prestige as integer) as domestic_prestige,
        cast(starting_xi_average_age as numeric) as starting_xi_average_age,
        cast(whole_team_average_age as numeric) as whole_team_average_age
    from source
    where team_id is not null
)

select * from renamed