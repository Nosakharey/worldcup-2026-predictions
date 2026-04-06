with source as (
    select * from {{ source('raw_data', 'shootouts') }}
),

renamed as (
    select
        cast(date as date) as match_date,
        lower(trim(home_team)) as home_team,
        lower(trim(away_team)) as away_team,
        lower(trim(winner)) as winner,
        lower(trim(first_shooter)) as first_shooter
    from source
    where date is not null
    and winner is not null
)

select * from renamed