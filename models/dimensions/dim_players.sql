{{ config(
    materialized='table',
    cluster_by=['nationality', 'player_name']
) }}

with api_players as (
    select
        player_id as api_player_id,
        lower(trim(player_name)) as player_name,
        lower(trim(firstname)) as firstname,
        lower(trim(lastname)) as lastname,
        lower(trim(nationality)) as nationality,
        age,
        birth_date,
        lower(trim(position)) as position,
        lower(trim(club)) as current_club,
        appearances,
        minutes,
        rating as current_rating,
        goals as current_season_goals,
        assists as current_season_assists,
        shots_total,
        shots_on,
        penalty_goals as current_penalty_goals,
        key_passes,
        yellow_cards,
        red_cards,
        dribbles_success,
        conversion_rate,
        is_young_player
    from {{ ref('stg_current_player_stats') }}
),

fifa_players as (
    select
        player_id as fifa_player_id,
        lower(trim(short_name)) as short_name,
        lower(trim(long_name)) as long_name,
        lower(trim(split(long_name, ' ')[safe_offset(0)])) as fifa_firstname,
        lower(trim(split(long_name, ' ')[safe_offset(
            array_length(split(long_name, ' ')) - 1
        )])) as fifa_lastname,
        lower(trim(nationality_name)) as nationality,
        overall_rating,
        shooting,
        pace,
        passing,
        dribbling,
        defending,
        physic,
        lower(trim(club_name)) as club_name,
        lower(trim(player_positions)) as fifa_position,
        row_number() over (
            partition by lower(trim(short_name)),
                         lower(trim(nationality_name))
            order by overall_rating desc
        ) as rn
    from {{ ref('stg_players_24') }}
),

best_fifa as (
    select * from fifa_players
    where rn = 1
),

historical_scorers as (
    select
        lower(trim(scorer_name)) as player_name,
        {{ standardize_team_name('scoring_team') }} as nationality,
        count(*) as historical_goals,
        sum(case when is_penalty then 1 else 0 end) as historical_penalty_goals,
        sum(case when is_own_goal then 1 else 0 end) as historical_own_goals,
        count(*)
            - sum(case when is_penalty then 1 else 0 end)
            - sum(case when is_own_goal then 1 else 0 end)
            as historical_open_play_goals,
        min(match_date) as first_goal_date,
        max(match_date) as last_goal_date
    from {{ ref('stg_goalscorers') }}
    group by 1, 2
),

api_fifa_joined as (
    select
        a.api_player_id,
        f.fifa_player_id,
        a.player_name,
        a.firstname,
        a.lastname,
        a.nationality,
        a.age,
        a.birth_date,
        a.position,
        a.current_club,
        a.appearances,
        a.minutes,
        a.current_rating,
        a.current_season_goals,
        a.current_season_assists,
        a.shots_total,
        a.shots_on,
        a.current_penalty_goals,
        a.key_passes,
        a.yellow_cards,
        a.red_cards,
        a.dribbles_success,
        a.conversion_rate,
        a.is_young_player,
        coalesce(f.overall_rating, 70) as fifa_overall,
        coalesce(f.shooting, 50) as fifa_shooting,
        coalesce(f.pace, 50) as fifa_pace,
        coalesce(f.passing, 50) as fifa_passing,
        coalesce(f.dribbling, 50) as fifa_dribbling,
        coalesce(f.defending, 50) as fifa_defending,
        coalesce(f.physic, 50) as fifa_physic
    from api_players a
    left join best_fifa f
        on a.nationality = f.nationality
        and (
            a.lastname = f.fifa_lastname
            or lower(f.long_name) like concat('%', a.lastname, '%')
            or lower(split(f.short_name, '. ')[safe_offset(1)]) = a.lastname
        )
),

final_joined as (
    select
        a.*,
        coalesce(h.historical_goals, 0) as historical_goals,
        coalesce(h.historical_penalty_goals, 0) as historical_penalty_goals,
        coalesce(h.historical_own_goals, 0) as historical_own_goals,
        coalesce(h.historical_open_play_goals, 0) as historical_open_play_goals,
        h.first_goal_date,
        h.last_goal_date
    from api_fifa_joined a
    left join historical_scorers h
        on a.nationality = h.nationality
        and (
            -- Method 1: exact full name match
            lower(a.player_name) = h.player_name
            or
            -- Method 2: last word of lastname matches last word of scorer
            lower(regexp_extract(a.lastname, r'\S+$')) =
                lower(regexp_extract(h.player_name, r'\S+$'))
            or
            -- Method 3: firstname + last word of lastname
            lower(concat(
                split(a.firstname, ' ')[safe_offset(0)],
                ' ',
                regexp_extract(a.lastname, r'\S+$')
            )) = h.player_name
            or
            -- Method 4: first word of firstname + full lastname
            lower(concat(
                split(a.firstname, ' ')[safe_offset(0)],
                ' ',
                a.lastname
            )) = h.player_name
            or
            -- Method 5: firstname + first word of lastname
            lower(concat(
                split(a.firstname, ' ')[safe_offset(0)],
                ' ',
                split(a.lastname, ' ')[safe_offset(0)]
            )) = h.player_name
        )
        and extract(year from h.first_goal_date) >=
            extract(year from a.birth_date) + 15
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['api_player_id', 'nationality']
        ) }} as player_sk,
        api_player_id,
        max(fifa_player_id) as fifa_player_id,
        player_name,
        firstname,
        lastname,
        nationality,
        age,
        birth_date,
        position,
        max(current_club) as current_club,
        max(appearances) as appearances,
        max(minutes) as minutes,
        max(current_rating) as current_rating,
        max(current_season_goals) as current_season_goals,
        max(current_season_assists) as current_season_assists,
        max(shots_total) as shots_total,
        max(shots_on) as shots_on,
        max(current_penalty_goals) as current_penalty_goals,
        max(key_passes) as key_passes,
        max(yellow_cards) as yellow_cards,
        max(red_cards) as red_cards,
        max(dribbles_success) as dribbles_success,
        max(conversion_rate) as conversion_rate,
        is_young_player,
        max(fifa_overall) as fifa_overall,
        max(fifa_shooting) as fifa_shooting,
        max(fifa_pace) as fifa_pace,
        max(fifa_passing) as fifa_passing,
        max(fifa_dribbling) as fifa_dribbling,
        max(fifa_defending) as fifa_defending,
        max(fifa_physic) as fifa_physic,
        max(historical_goals) as historical_goals,
        max(historical_penalty_goals) as historical_penalty_goals,
        max(historical_own_goals) as historical_own_goals,
        max(historical_open_play_goals) as historical_open_play_goals,
        min(first_goal_date) as first_goal_date,
        max(last_goal_date) as last_goal_date,
        round(
            (max(current_season_goals) * 0.4) +
            (max(fifa_shooting) * 0.3) +
            (max(fifa_overall) * 0.2) +
            (max(historical_goals) * 0.1)
        , 2) as golden_boot_score,
        round(
            (max(current_season_assists) * 0.4) +
            (max(key_passes) * 0.3) +
            (max(fifa_passing) * 0.2) +
            (max(fifa_dribbling) * 0.1)
        , 2) as best_assist_score,
        round(
            (max(fifa_overall) * 0.3) +
            (max(current_rating) * 10 * 0.3) +
            (max(current_season_goals) * 0.2) +
            (max(current_season_assists) * 0.2)
        , 2) as golden_ball_score
    from final_joined
    group by
        api_player_id,
        player_name,
        firstname,
        lastname,
        nationality,
        age,
        birth_date,
        position,
        is_young_player
)

select * from final