{{ config(
    materialized='table',
    cluster_by=["team_name"]
) }}

with matches as (
    select * from {{ ref('fct_matches') }}
),

teams as (
    select
        lower(trim(team_name)) as team_name,
        max(overall) as overall,
        max(attack) as attack,
        max(midfield) as midfield,
        max(defence) as defence
    from {{ ref('stg_teams_24') }}
    group by 1
),

wc_groups as (
    select * from {{ ref('world_cup_2026_groups') }}
),

-- ALL TIME home record
home_record as (
    select
        home_team as team_name,
        count(*) as home_games,
        sum(case when match_result = 'home_win' then 1 else 0 end) as home_wins,
        sum(case when match_result = 'draw' then 1 else 0 end) as home_draws,
        sum(case when match_result = 'away_win' then 1 else 0 end) as home_losses,
        avg(home_score) as avg_home_goals_scored,
        avg(away_score) as avg_home_goals_conceded
    from matches
    group by 1
),

-- ALL TIME away record
away_record as (
    select
        away_team as team_name,
        count(*) as away_games,
        sum(case when match_result = 'away_win' then 1 else 0 end) as away_wins,
        sum(case when match_result = 'draw' then 1 else 0 end) as away_draws,
        sum(case when match_result = 'home_win' then 1 else 0 end) as away_losses,
        avg(away_score) as avg_away_goals_scored,
        avg(home_score) as avg_away_goals_conceded
    from matches
    group by 1
),

-- RECENT FORM: last 5 years (2020-2025)
recent_home as (
    select
        home_team as team_name,
        count(*) as recent_home_games,
        sum(case when match_result = 'home_win' then 1 else 0 end) as recent_home_wins,
        avg(home_score) as recent_avg_home_scored,
        avg(away_score) as recent_avg_home_conceded
    from matches
    where match_year >= 2020
    group by 1
),

recent_away as (
    select
        away_team as team_name,
        count(*) as recent_away_games,
        sum(case when match_result = 'away_win' then 1 else 0 end) as recent_away_wins,
        avg(away_score) as recent_avg_away_scored,
        avg(home_score) as recent_avg_away_conceded
    from matches
    where match_year >= 2020
    group by 1
),

-- WORLD CUP specific record
wc_home as (
    select
        home_team as team_name,
        count(*) as wc_home_games,
        sum(case when match_result = 'home_win' then 1 else 0 end) as wc_home_wins,
        avg(home_score) as wc_avg_home_scored,
        avg(away_score) as wc_avg_home_conceded
    from matches
    where is_world_cup = true
    group by 1
),

wc_away as (
    select
        away_team as team_name,
        count(*) as wc_away_games,
        sum(case when match_result = 'away_win' then 1 else 0 end) as wc_away_wins,
        avg(away_score) as wc_avg_away_scored,
        avg(home_score) as wc_avg_away_conceded
    from matches
    where is_world_cup = true
    group by 1
),

-- NEUTRAL VENUE record (most relevant for WC 2026)
neutral_record as (
    select
        home_team as team_name,
        count(*) as neutral_games,
        sum(case when match_result = 'home_win' then 1 else 0 end) as neutral_wins,
        sum(case when match_result = 'draw' then 1 else 0 end) as neutral_draws,
        avg(home_score) as neutral_avg_scored,
        avg(away_score) as neutral_avg_conceded
    from matches
    where is_neutral_venue = true
    group by 1
),

-- WEIGHTED WIN RATE using recency_weight
weighted_record as (
    select
        home_team as team_name,
        sum(recency_weight) as total_weight,
        sum(case when match_result = 'home_win'
            then recency_weight else 0 end) as weighted_wins,
        sum(case when match_result = 'draw'
            then recency_weight * 0.5 else 0 end) as weighted_draws
    from matches
    group by 1
),

combined as (
    select
        h.team_name,

        -- all time record
        h.home_games + a.away_games as total_games,
        h.home_wins + a.away_wins as total_wins,
        h.home_draws + a.away_draws as total_draws,
        h.home_losses + a.away_losses as total_losses,
        round((h.home_wins + a.away_wins) * 100.0 /
            nullif(h.home_games + a.away_games, 0), 2) as win_rate_pct,
        round((h.avg_home_goals_scored + a.avg_away_goals_scored) / 2, 2)
            as avg_goals_scored,
        round((h.avg_home_goals_conceded + a.avg_away_goals_conceded) / 2, 2)
            as avg_goals_conceded,

        -- recent form (2020-2025)
        coalesce(rh.recent_home_games, 0) +
            coalesce(ra.recent_away_games, 0) as recent_total_games,
        coalesce(rh.recent_home_wins, 0) +
            coalesce(ra.recent_away_wins, 0) as recent_total_wins,
        round(
            (coalesce(rh.recent_home_wins, 0) + coalesce(ra.recent_away_wins, 0))
            * 100.0 /
            nullif(coalesce(rh.recent_home_games, 0) +
                coalesce(ra.recent_away_games, 0), 0)
        , 2) as recent_win_rate_pct,
        round(
            (coalesce(rh.recent_avg_home_scored, 0) +
                coalesce(ra.recent_avg_away_scored, 0)) / 2
        , 2) as recent_avg_goals_scored,
        round(
            (coalesce(rh.recent_avg_home_conceded, 0) +
                coalesce(ra.recent_avg_away_conceded, 0)) / 2
        , 2) as recent_avg_goals_conceded,

        -- world cup record
        coalesce(wh.wc_home_games, 0) +
            coalesce(wa.wc_away_games, 0) as wc_total_games,
        coalesce(wh.wc_home_wins, 0) +
            coalesce(wa.wc_away_wins, 0) as wc_total_wins,
        round(
            (coalesce(wh.wc_home_wins, 0) + coalesce(wa.wc_away_wins, 0))
            * 100.0 /
            nullif(coalesce(wh.wc_home_games, 0) +
                coalesce(wa.wc_away_games, 0), 0)
        , 2) as wc_win_rate_pct,
        round(
            (coalesce(wh.wc_avg_home_scored, 0) +
                coalesce(wa.wc_avg_away_scored, 0)) / 2
        , 2) as wc_avg_goals_scored,
        round(
            (coalesce(wh.wc_avg_home_conceded, 0) +
                coalesce(wa.wc_avg_away_conceded, 0)) / 2
        , 2) as wc_avg_goals_conceded,

        -- neutral venue record
        coalesce(n.neutral_games, 0) as neutral_games,
        coalesce(n.neutral_wins, 0) as neutral_wins,
        round(
            coalesce(n.neutral_wins, 0) * 100.0 /
            nullif(coalesce(n.neutral_games, 0), 0)
        , 2) as neutral_win_rate_pct,
        coalesce(n.neutral_avg_scored, 0) as neutral_avg_scored,
        coalesce(n.neutral_avg_conceded, 0) as neutral_avg_conceded,

        -- weighted win rate
        round(
            (coalesce(w.weighted_wins, 0) + coalesce(w.weighted_draws, 0))
            * 100.0 /
            nullif(coalesce(w.total_weight, 0), 0)
        , 2) as weighted_win_rate_pct

    from home_record h
    join away_record a on h.team_name = a.team_name
    left join recent_home rh on h.team_name = rh.team_name
    left join recent_away ra on h.team_name = ra.team_name
    left join wc_home wh on h.team_name = wh.team_name
    left join wc_away wa on h.team_name = wa.team_name
    left join neutral_record n on h.team_name = n.team_name
    left join weighted_record w on h.team_name = w.team_name
),

final as (
    select
        c.*,

        -- composite strength score
        round(
            (coalesce(c.recent_win_rate_pct, c.win_rate_pct) * 0.4) +
            (coalesce(c.wc_win_rate_pct, c.win_rate_pct) * 0.3) +
            (c.win_rate_pct * 0.2) +
            (coalesce(c.neutral_win_rate_pct, c.win_rate_pct) * 0.1)
        , 2) as overall_strength_score,

        -- attacking strength
        round(
            (coalesce(c.recent_avg_goals_scored, c.avg_goals_scored) * 0.6) +
            (coalesce(c.wc_avg_goals_scored, c.avg_goals_scored) * 0.4)
        , 2) as attacking_strength,

        -- defensive strength (lower = better)
        round(
            (coalesce(c.recent_avg_goals_conceded, c.avg_goals_conceded) * 0.6) +
            (coalesce(c.wc_avg_goals_conceded, c.avg_goals_conceded) * 0.4)
        , 2) as defensive_strength,

        -- FIFA ratings
        t.overall as fifa_overall_rating,
        t.attack as fifa_attack_rating,
        t.midfield as fifa_midfield_rating,
        t.defence as fifa_defence_rating,

        -- WC 2026 group
        w.group as wc_2026_group,
        w.confederation

    from combined c
    left join teams t
        on c.team_name = t.team_name
    inner join wc_groups w
        on c.team_name = w.team_name
)

select * from final