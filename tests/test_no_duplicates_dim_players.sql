-- This test FAILS if duplicates exist
-- Returns rows that appear more than once
SELECT
    api_player_id,
    nationality,
    COUNT(*) as duplicate_count
FROM {{ ref('dim_players') }}
GROUP BY api_player_id, nationality
HAVING COUNT(*) > 1