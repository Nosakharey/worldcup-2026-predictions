{{ config(
    materialized='table',
    cluster_by=['scoring_team', 'scorer_name']
) }}

with goalscorers as (
    select * from {{ ref('stg_goalscorers') }}
),

matches as (
    select
        match_sk,
        match_date,
        home_team,
        away_team,
        tournament,
        is_world_cup,
        is_major_tournament,
        is_friendly,
        match_year,
        recency_weight
    from {{ ref('fct_matches') }}
),

players as (
    select
        player_sk,
        player_name,
        firstname,
        lastname,
        nationality,
        birth_date
    from {{ ref('dim_players') }}
),

goals_with_match as (
    select
        g.match_date,
        g.home_team,
        g.away_team,
        g.scoring_team,
        g.scorer_name,
        g.minute_scored,
        g.is_own_goal,
        g.is_penalty,
        m.match_sk,
        m.tournament,
        m.is_world_cup,
        m.is_major_tournament,
        m.is_friendly,
        m.match_year,
        m.recency_weight
    from goalscorers g
    left join matches m
        on g.match_date = m.match_date
        and {{ standardize_team_name('g.home_team') }} = m.home_team
        and {{ standardize_team_name('g.away_team') }} = m.away_team
),

goals_with_player as (
    select
        g.*,
        p.player_sk,
        p.birth_date
    from goals_with_match g
    left join players p
        on {{ standardize_team_name('g.scoring_team') }} = p.nationality
        and (
            -- Method 1: exact name match
            lower(g.scorer_name) = p.player_name
            or
            -- Method 2: last word of scorer matches last word of lastname
            lower(regexp_extract(g.scorer_name, r'\S+$')) =
                lower(regexp_extract(p.lastname, r'\S+$'))
            or
            -- Method 3: firstname + last word of lastname
            lower(concat(
                split(p.firstname, ' ')[safe_offset(0)],
                ' ',
                regexp_extract(p.lastname, r'\S+$')
            )) = lower(g.scorer_name)
            or
            -- Method 4: firstname + first word of lastname
            lower(concat(
                split(p.firstname, ' ')[safe_offset(0)],
                ' ',
                split(p.lastname, ' ')[safe_offset(0)]
            )) = lower(g.scorer_name)
        )
        -- player must have been at least 15 years old when they scored
        and (
            p.birth_date is null
            or extract(year from g.match_date) >=
                safe_cast(left(cast(p.birth_date as string), 4) as int64) + 15
        )
),

numbered as (
    select
        *,
        row_number() over (
            partition by
                match_date,
                home_team,
                away_team,
                scoring_team,
                scorer_name,
                minute_scored
            order by minute_scored
        ) as goal_row_number
    from goals_with_player
),

final as (
    select
        -- surrogate key including row number to handle duplicates
        {{ dbt_utils.generate_surrogate_key([
            'match_date',
            'home_team',
            'away_team',
            'scoring_team',
            'scorer_name',
            'cast(minute_scored as string)',
            'cast(goal_row_number as string)'
        ]) }} as goal_sk,

        -- match reference
        match_sk,

        -- player reference
        player_sk,

        -- goal details
        match_date,
        home_team,
        away_team,
        {{ standardize_team_name('scoring_team') }} as scoring_team,
        case
            when lower(trim(scorer_name)) = 'na' then null
            else lower(trim(scorer_name))
        end as scorer_name,
        minute_scored,
        is_own_goal,
        is_penalty,

        -- open play flag
        case
            when is_own_goal = false and is_penalty = false
                then true
            else false
        end as is_open_play,

        -- tournament context
        tournament,
        coalesce(is_world_cup, false) as is_world_cup,
        coalesce(is_major_tournament, false) as is_major_tournament,
        coalesce(is_friendly, false) as is_friendly,
        match_year,
        coalesce(recency_weight, 0.1) as recency_weight,

        -- goal value weight
        case
            when coalesce(is_world_cup, false) = true then 3.0
            when coalesce(is_major_tournament, false) = true then 2.0
            when coalesce(is_friendly, false) = true then 0.5
            else 1.0
        end as goal_importance_weight

    from numbered
)

select * from final