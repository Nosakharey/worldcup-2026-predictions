with source as (
    select * from {{ source('raw_data', 'goalscorers') }}
),

renamed as (
    select
        cast(date as date) as match_date,
        lower(trim(home_team)) as home_team,
        lower(trim(away_team)) as away_team,
        lower(trim(team)) as scoring_team,
        lower(trim(scorer)) as scorer_name,
        safe_cast(nullif(trim(cast(minute as string)), 'NA') as int64) as minute_scored,
        cast(own_goal as boolean) as is_own_goal,
        cast(penalty as boolean) as is_penalty
    from source
    where scorer is not null
    and date is not null
)

select * from renamed