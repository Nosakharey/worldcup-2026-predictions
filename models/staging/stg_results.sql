with source as (
    select * from {{ source('raw_data', 'results') }}
),

renamed as (
    select
        cast(date as date) as match_date,
        lower(trim(home_team)) as home_team,
        lower(trim(away_team)) as away_team,
        cast(home_score as integer) as home_score,
        cast(away_score as integer) as away_score,
        lower(trim(tournament)) as tournament,
        lower(trim(city)) as city,
        lower(trim(country)) as country,
        cast(neutral as boolean) as is_neutral_venue
    from source
    where home_score is not null
    and away_score is not null
)

select * from renamed