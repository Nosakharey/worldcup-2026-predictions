with source as (
    select * from {{ source('raw_data', 'current_player_stats') }}
),

missing_players as (
    select * from {{ ref('missing_players') }}
),

final as (
    select
        player_id,
        lower(trim(player_name)) as player_name,
        lower(trim(firstname)) as firstname,
        lower(trim(lastname)) as lastname,
        lower(trim(nationality)) as nationality,
        age,
        cast(birth_date as date) as birth_date,
        lower(trim(league)) as league,
        lower(trim(club)) as club,
        season,
        lower(trim(position)) as position,
        coalesce(appearances, 0) as appearances,
        coalesce(minutes, 0) as minutes,
        cast(rating as float64) as rating,
        coalesce(goals, 0) as goals,
        coalesce(assists, 0) as assists,
        coalesce(shots_total, 0) as shots_total,
        coalesce(shots_on, 0) as shots_on,
        coalesce(penalty_goals, 0) as penalty_goals,
        coalesce(key_passes, 0) as key_passes,
        coalesce(yellow_cards, 0) as yellow_cards,
        coalesce(red_cards, 0) as red_cards,
        coalesce(dribbles_success, 0) as dribbles_success,
        case
            when shots_total > 0
            then round(cast(goals as float64) * 100.0 / shots_total, 1)
            else 0
        end as conversion_rate,
        case
            when age < 23 then true
            else false
        end as is_young_player
    from source
    where player_id is not null

    union all

    select
        player_id,
        lower(trim(player_name)) as player_name,
        lower(trim(firstname)) as firstname,
        lower(trim(lastname)) as lastname,
        lower(trim(nationality)) as nationality,
        age,
        cast(birth_date as date) as birth_date,
        lower(trim(league)) as league,
        lower(trim(club)) as club,
        season,
        lower(trim(position)) as position,
        coalesce(appearances, 0) as appearances,
        coalesce(minutes, 0) as minutes,
        cast(rating as float64) as rating,
        coalesce(goals, 0) as goals,
        coalesce(assists, 0) as assists,
        coalesce(shots_total, 0) as shots_total,
        coalesce(shots_on, 0) as shots_on,
        coalesce(penalty_goals, 0) as penalty_goals,
        coalesce(key_passes, 0) as key_passes,
        coalesce(yellow_cards, 0) as yellow_cards,
        coalesce(red_cards, 0) as red_cards,
        coalesce(dribbles_success, 0) as dribbles_success,
        case
            when shots_total > 0
            then round(cast(goals as float64) * 100.0 / shots_total, 1)
            else 0
        end as conversion_rate,
        case
            when age < 23 then true
            else false
        end as is_young_player
    from missing_players
)

select * from final