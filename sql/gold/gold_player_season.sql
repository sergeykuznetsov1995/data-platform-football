-- Example materialization query for gold_player_season
-- Replace with real fields once silver tables are ready
CREATE TABLE IF NOT EXISTS gold_player_season AS
SELECT player_id, season,
       COUNT(*) AS matches
FROM silver_matches
GROUP BY 1,2;
