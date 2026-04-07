-- Production-Grade Gold Layer for Cricket Data Analytics
-- Advanced materialized tables with Dynamic Table technology
-- Real-time analytics mart architecture
-- Author: Expert Snowflake Data Engineer

USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE cricket;
CREATE SCHEMA IF NOT EXISTS cricket.gold;
USE SCHEMA cricket.gold;

BEGIN;

-- ============================================================================
-- 1. ANALYTICS LOGGING & MONITORING TABLE
-- ============================================================================

CREATE OR REPLACE TABLE cricket.gold.analytics_log (
    task_name VARCHAR COMMENT 'Task name',
    execution_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Execution timestamp',
    status VARCHAR COMMENT 'Status (SUCCESS/FAILED)',
    rows_processed INT COMMENT 'Rows processed',
    rows_affected INT COMMENT 'Rows inserted/updated',
    dynamic_table_lag_minutes INT COMMENT 'Dynamic table lag in minutes',
    error_message VARCHAR COMMENT 'Error message if any',
    duration_seconds INT COMMENT 'Task duration'
)
COMMENT 'Gold layer analytics pipeline monitoring log';

-- ============================================================================
-- 2. DYNAMIC TABLES (Real-time, Auto-refreshing)
-- ============================================================================

-- Player Dimension (Dynamic Table - 5 min lag)
CREATE OR REPLACE DYNAMIC TABLE cricket.gold.player_dim
TARGET_LAG = '5 minutes'
WAREHOUSE = compute_wh
LAG_POLICY = AUTO
AS
SELECT
    COALESCE(p.match_type_number, -1) AS match_type_number,
    p.country COMMENT 'Player country',
    p.player_name COMMENT 'Player name',
    COUNT(DISTINCT p.match_type_number) AS matches_participated,
    ROW_NUMBER() OVER (PARTITION BY p.country ORDER BY p.player_name) AS player_rank_in_country,
    p.ingestion_ts,
    CURRENT_TIMESTAMP() AS gold_load_ts,
    p.data_quality_status,
    DATEDIFF(MINUTE, p.silver_load_ts, CURRENT_TIMESTAMP()) AS query_refresh_lag_minutes,
    'player' AS record_source,
    TRUE AS is_latest
FROM cricket.silver.player_clean p
WHERE p.is_current_record = TRUE
    AND p.data_quality_status = 'VALID'
GROUP BY p.match_type_number, p.country, p.player_name, p.ingestion_ts, p.data_quality_status, p.silver_load_ts
COMMENT 'Gold layer player dimension with dynamic refresh'
CLUSTER BY (country, match_type_number);

-- Delivery Fact Table (Dynamic Table - 5 min lag, Real-time analytics)
CREATE OR REPLACE DYNAMIC TABLE cricket.gold.delivery_fact
TARGET_LAG = '5 minutes'
WAREHOUSE = compute_wh
LAG_POLICY = AUTO
AS
SELECT
    d.match_type_number COMMENT 'Match type identifier',
    d.team_name COMMENT 'Team name',
    d.over COMMENT 'Over number',
    CASE WHEN d.over > 0 THEN (d.over - 1) / 6 + 1 ELSE 0 END AS session_id COMMENT 'Session/Phase (6-over blocks)',
    d.bowler COMMENT 'Bowler name',
    d.batter COMMENT 'Batter name',
    d.non_striker COMMENT 'Non-striker name',
    d.runs COMMENT 'Runs scored',
    d.extras COMMENT 'Extra runs',
    CASE WHEN d.runs = 4 THEN 1 ELSE 0 END AS boundary_4_count COMMENT 'Boundary 4 flag',
    CASE WHEN d.runs = 6 THEN 1 ELSE 0 END AS boundary_6_count COMMENT 'Boundary 6 flag',
    CASE WHEN d.runs = 0 AND d.extras = 0 THEN 1 ELSE 0 END AS dot_ball_count COMMENT 'Dot ball flag',
    d.total COMMENT 'Total runs for delivery',
    d.extra_type COMMENT 'Type of extra',
    CASE WHEN d.player_out != 'None' THEN 1 ELSE 0 END AS wicket_flag COMMENT 'Wicket flag',
    d.player_out COMMENT 'Player out name',
    d.player_out_kind COMMENT 'Kind of dismissal',
    d.player_out_fielder COMMENT 'Fielder involved',
    d.review_by_team COMMENT 'Team requesting review',
    d.review_decision COMMENT 'Review decision',
    d.review_type COMMENT 'Review type',
    d.stg_file_hashkey COMMENT 'Source hash key',
    d.stg_file_row_number COMMENT 'Source row number',
    d.ingestion_ts,
    CURRENT_TIMESTAMP() AS gold_load_ts,
    d.data_quality_status,
    DATEDIFF(MINUTE, d.silver_load_ts, CURRENT_TIMESTAMP()) AS query_refresh_lag_minutes,
    'delivery' AS record_source,
    TRUE AS is_latest
FROM cricket.silver.delivery_clean d
WHERE d.is_current_record = TRUE
    AND d.data_quality_status = 'VALID'
COMMENT 'Gold layer delivery fact table with real-time enrichment'
CLUSTER BY (match_type_number, team_name, over);

-- Match Dimension (Dynamic Table - 10 min lag)
CREATE OR REPLACE DYNAMIC TABLE cricket.gold.match_dim
TARGET_LAG = '10 minutes'
WAREHOUSE = compute_wh
LAG_POLICY = AUTO
AS
SELECT
    m.match_type_number COMMENT 'Match type identifier',
    m.event_name COMMENT 'Event name',
    m.match_number COMMENT 'Match number',
    m.event_date COMMENT 'Event date',
    m.event_year COMMENT 'Event year',
    m.event_month COMMENT 'Event month',
    m.event_day COMMENT 'Event day',
    m.match_type COMMENT 'Match type (T20/ODI/Test)',
    m.season COMMENT 'Season',
    m.team_type COMMENT 'Team type',
    m.overs COMMENT 'Overs per innings',
    m.city COMMENT 'City',
    m.venue COMMENT 'Venue',
    m.gender COMMENT 'Gender (M/F)',
    m.first_team COMMENT 'First team',
    m.second_team COMMENT 'Second team',
    m.winner COMMENT 'Winner team',
    m.won_by_runs COMMENT 'Runs margin',
    m.won_by_wickets COMMENT 'Wickets margin',
    CASE 
        WHEN m.won_by_runs > 0 THEN 'runs'
        WHEN m.won_by_wickets > 0 THEN 'wickets'
        ELSE 'tie/super_over'
    END AS margin_type COMMENT 'Type of winning margin',
    m.player_of_match COMMENT 'Player of the match',
    m.match_referee COMMENT 'Match referee',
    m.toss_winner COMMENT 'Toss winner',
    m.toss_decision COMMENT 'Toss decision (bat/field)',
    m.ingestion_ts,
    CURRENT_TIMESTAMP() AS gold_load_ts,
    m.data_quality_status,
    DATEDIFF(MINUTE, m.silver_load_ts, CURRENT_TIMESTAMP()) AS query_refresh_lag_minutes,
    'match' AS record_source,
    TRUE AS is_latest
FROM cricket.silver.match_clean m
WHERE m.is_current_record = TRUE
    AND m.data_quality_status = 'VALID'
COMMENT 'Gold layer match dimension with analytics enrichment'
CLUSTER BY (event_date, match_type_number);

-- ============================================================================
-- 3. MATERIALIZED AGGREGATE TABLES (Task-driven, Complex logic)
-- ============================================================================

-- Player Performance Analytics Table
CREATE OR REPLACE TABLE cricket.gold.player_performance (
    player_name TEXT COMMENT 'Player name',
    country TEXT COMMENT 'Player country',
    match_type_number INT COMMENT 'Match type',
    team_name TEXT COMMENT 'Batting/Bowling team',
    matches_count INT COMMENT 'Total matches',
    balls_faced INT COMMENT 'Balls faced (batting)',
    runs_scored INT COMMENT 'Runs scored',
    batting_avg FLOAT COMMENT 'Batting average',
    strike_rate FLOAT COMMENT 'Strike rate',
    boundaries_4 INT COMMENT 'Fours scored',
    boundaries_6 INT COMMENT 'Sixes scored',
    wickets_taken INT COMMENT 'Wickets taken (bowling)',
    balls_bowled INT COMMENT 'Balls bowled',
    runs_conceded INT COMMENT 'Runs conceded (bowling)',
    bowling_econ FLOAT COMMENT 'Economy rate',
    dot_balls_bowled INT COMMENT 'Dot balls bowled',
    role_type VARCHAR COMMENT 'Role (Batter/Bowler/All-rounder)',
    rolling_5_match_avg FLOAT COMMENT 'Rolling 5-match average',
    form_status VARCHAR COMMENT 'Form (Hot/In-form/Struggling)',
    last_match_date DATE COMMENT 'Last match participation date',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    gold_load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    data_quality_status VARCHAR DEFAULT 'VALID',
    query_refresh_lag_minutes INT,
    record_source VARCHAR DEFAULT 'player_performance',
    is_latest BOOLEAN DEFAULT TRUE
)
CLUSTER BY (player_name, country)
COMMENT 'Gold layer: player performance analytics with kpis';

-- Insert initial data (will be updated by task)
INSERT INTO cricket.gold.player_performance
SELECT 'Not Started' AS player_name, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'VALID', 0, 'player_performance', TRUE
WHERE FALSE;

-- Match Summary Executive Mart
CREATE OR REPLACE TABLE cricket.gold.match_summary (
    match_summary_id BIGINT COMMENT 'Unique identifier',
    event_date DATE COMMENT 'Event date',
    event_year INT COMMENT 'Event year',
    event_month INT COMMENT 'Event month',
    match_type VARCHAR COMMENT 'Match type',
    venue VARCHAR COMMENT 'Venue',
    first_team VARCHAR COMMENT 'First team',
    second_team VARCHAR COMMENT 'Second team',
    toss_winner VARCHAR COMMENT 'Toss winner',
    toss_decision VARCHAR COMMENT 'Toss decision',
    winner VARCHAR COMMENT 'Match winner',
    margin_type VARCHAR COMMENT 'Winning margin type',
    margin_value INT COMMENT 'Winning margin value',
    player_of_match VARCHAR COMMENT 'Player of the match',
    first_team_runs INT COMMENT 'First team total runs',
    second_team_runs INT COMMENT 'Second team total runs',
    first_team_wickets INT COMMENT 'First team wickets lost',
    second_team_wickets INT COMMENT 'Second team wickets lost',
    highest_wicket_taker VARCHAR COMMENT 'Highest wicket taker',
    wickets_taken INT COMMENT 'Wickets by highest taker',
    highest_run_scorer VARCHAR COMMENT 'Highest run scorer',
    runs_scored INT COMMENT 'Runs by highest scorer',
    total_deliveries INT COMMENT 'Total deliveries in match',
    total_runs_scored INT COMMENT 'Total runs in match',
    total_boundaries_4 INT COMMENT 'Total fours',
    total_boundaries_6 INT COMMENT 'Total sixes',
    total_dot_balls INT COMMENT 'Total dot balls',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    gold_load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    data_quality_status VARCHAR DEFAULT 'VALID',
    query_refresh_lag_minutes INT,
    record_source VARCHAR DEFAULT 'match_summary',
    is_latest BOOLEAN DEFAULT TRUE
)
CLUSTER BY (event_date)
COMMENT 'Gold layer: executive match summary dashboard';

-- Insert initial data (will be updated by task)
INSERT INTO cricket.gold.match_summary
SELECT ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP()), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'VALID', 0, 'match_summary', TRUE
WHERE FALSE;

-- ============================================================================
-- 4. TASK CHAIN - GOLD ANALYTICS PIPELINE
-- ============================================================================

-- Player Performance Population Task (Hourly)
CREATE OR REPLACE TASK cricket.gold.gold_player_performance_task
WAREHOUSE = compute_wh
SCHEDULE = '1 HOUR'
ON_ERROR = 'CONTINUE'
AS
DECLARE
    p_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    p_rows_affected INT := 0;
BEGIN
    TRUNCATE TABLE cricket.gold.player_performance;
    
    INSERT INTO cricket.gold.player_performance
    WITH batting_stats AS (
        SELECT
            p.player_name,
            p.country,
            d.match_type_number,
            d.team_name,
            COUNT(DISTINCT d.match_type_number) AS matches_count,
            COUNT(*) AS balls_faced,
            SUM(d.runs) AS runs_scored,
            ROUND(SUM(d.runs) / NULLIF(COUNT(*), 0), 2) AS batting_avg,
            ROUND((SUM(d.runs) / NULLIF(COUNT(*), 0)) * 100, 2) AS strike_rate,
            SUM(d.boundary_4_count) AS boundaries_4,
            SUM(d.boundary_6_count) AS boundaries_6,
            ROW_NUMBER() OVER (PARTITION BY p.player_name ORDER BY d.ingestion_ts DESC LIMIT 5) AS match_rank
        FROM cricket.gold.player_dim p
        LEFT JOIN cricket.gold.delivery_fact d ON p.player_name = d.batter
        WHERE p.is_latest = TRUE
        GROUP BY p.player_name, p.country, d.match_type_number, d.team_name
    ),
    bowling_stats AS (
        SELECT
            p.player_name,
            p.country,
            d.match_type_number,
            d.team_name,
            COUNT(DISTINCT d.match_type_number) AS matches_count,
            COUNT(*) AS balls_bowled,
            SUM(d.runs + d.extras) AS runs_conceded,
            ROUND((SUM(d.runs + d.extras) / NULLIF(COUNT(*), 0)) * 6, 2) AS bowling_econ,
            SUM(d.wicket_flag) AS wickets_taken,
            SUM(d.dot_ball_count) AS dot_balls_bowled
        FROM cricket.gold.player_dim p
        LEFT JOIN cricket.gold.delivery_fact d ON p.player_name = d.bowler
        WHERE p.is_latest = TRUE
        GROUP BY p.player_name, p.country, d.match_type_number, d.team_name
    )
    SELECT
        bs.player_name,
        bs.country,
        bs.match_type_number,
        bs.team_name,
        COALESCE(bs.matches_count, 0) AS matches_count,
        COALESCE(bs.balls_faced, 0) AS balls_faced,
        COALESCE(bs.runs_scored, 0) AS runs_scored,
        COALESCE(bs.batting_avg, 0) AS batting_avg,
        COALESCE(bs.strike_rate, 0) AS strike_rate,
        COALESCE(bs.boundaries_4, 0) AS boundaries_4,
        COALESCE(bs.boundaries_6, 0) AS boundaries_6,
        COALESCE(bw.wickets_taken, 0) AS wickets_taken,
        COALESCE(bw.balls_bowled, 0) AS balls_bowled,
        COALESCE(bw.runs_conceded, 0) AS runs_conceded,
        COALESCE(bw.bowling_econ, 0) AS bowling_econ,
        COALESCE(bw.dot_balls_bowled, 0) AS dot_balls_bowled,
        CASE
            WHEN COALESCE(bs.runs_scored, 0) > 0 AND COALESCE(bw.wickets_taken, 0) > 0 THEN 'All-rounder'
            WHEN COALESCE(bs.runs_scored, 0) > 0 THEN 'Batter'
            WHEN COALESCE(bw.wickets_taken, 0) > 0 THEN 'Bowler'
            ELSE 'None'
        END AS role_type,
        ROUND(AVG(bs.runs_scored) OVER (PARTITION BY bs.player_name ORDER BY bs.ingestion_ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) AS rolling_5_match_avg,
        CASE
            WHEN COALESCE(bs.batting_avg, 0) > 40 OR COALESCE(bw.bowling_econ, 0) < 7 THEN 'Hot'
            WHEN COALESCE(bs.batting_avg, 0) > 30 OR COALESCE(bw.bowling_econ, 0) < 8 THEN 'In-form'
            ELSE 'Struggling'
        END AS form_status,
        CURRENT_DATE() AS last_match_date,
        CURRENT_TIMESTAMP() AS ingestion_ts,
        CURRENT_TIMESTAMP() AS gold_load_ts,
        'VALID' AS data_quality_status,
        DATEDIFF(MINUTE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()) AS query_refresh_lag_minutes,
        'player_performance' AS record_source,
        TRUE AS is_latest
    FROM batting_stats bs
    FULL OUTER JOIN bowling_stats bw ON bs.player_name = bw.player_name;
    
    p_rows_affected := @@rowcount;
    INSERT INTO cricket.gold.analytics_log (task_name, status, rows_processed, duration_seconds)
    VALUES ('gold_player_performance_task', 'SUCCESS', p_rows_affected, DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO cricket.gold.analytics_log (task_name, status, error_message, duration_seconds)
        VALUES ('gold_player_performance_task', 'FAILED', SQLERRM(), DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
        RAISE;
END;

-- Match Summary Population Task (30 minutes)
CREATE OR REPLACE TASK cricket.gold.gold_match_summary_task
WAREHOUSE = compute_wh
SCHEDULE = '30 MINUTES'
ON_ERROR = 'CONTINUE'
AS
DECLARE
    p_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    p_rows_affected INT := 0;
BEGIN
    TRUNCATE TABLE cricket.gold.match_summary;
    
    INSERT INTO cricket.gold.match_summary
    WITH team_stats AS (
        SELECT
            m.match_type_number,
            m.event_date,
            m.event_year,
            m.event_month,
            m.match_type,
            m.venue,
            m.first_team,
            m.second_team,
            m.toss_winner,
            m.toss_decision,
            m.winner,
            m.margin_type,
            COALESCE(m.won_by_runs, m.won_by_wickets) AS margin_value,
            m.player_of_match,
            SUM(CASE WHEN d.team_name = m.first_team THEN d.runs ELSE 0 END) AS first_team_runs,
            SUM(CASE WHEN d.team_name = m.second_team THEN d.runs ELSE 0 END) AS second_team_runs,
            SUM(CASE WHEN d.team_name = m.first_team AND d.wicket_flag = 1 THEN 1 ELSE 0 END) AS first_team_wickets,
            SUM(CASE WHEN d.team_name = m.second_team AND d.wicket_flag = 1 THEN 1 ELSE 0 END) AS second_team_wickets,
            COUNT(*) AS total_deliveries,
            SUM(d.runs + d.extras) AS total_runs_scored,
            SUM(d.boundary_4_count) AS total_boundaries_4,
            SUM(d.boundary_6_count) AS total_boundaries_6,
            SUM(d.dot_ball_count) AS total_dot_balls
        FROM cricket.gold.match_dim m
        LEFT JOIN cricket.gold.delivery_fact d ON m.match_type_number = d.match_type_number
        WHERE m.is_latest = TRUE
        GROUP BY m.match_type_number, m.event_date, m.event_year, m.event_month, m.match_type, m.venue,
                 m.first_team, m.second_team, m.toss_winner, m.toss_decision, m.winner, m.margin_type,
                 m.won_by_runs, m.won_by_wickets, m.player_of_match
    ),
    player_contributions AS (
        SELECT
            d.match_type_number,
            d.bowler AS highest_wicket_taker,
            SUM(d.wicket_flag) AS wickets_taken,
            d.batter AS highest_run_scorer,
            SUM(d.runs) AS runs_scored,
            ROW_NUMBER() OVER (PARTITION BY d.match_type_number ORDER BY SUM(d.wicket_flag) DESC) AS wicket_rank,
            ROW_NUMBER() OVER (PARTITION BY d.match_type_number ORDER BY SUM(d.runs) DESC) AS run_rank
        FROM cricket.gold.delivery_fact d
        WHERE d.is_latest = TRUE
        GROUP BY d.match_type_number, d.bowler, d.batter
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY ts.event_date DESC, ts.match_type_number DESC) AS match_summary_id,
        ts.event_date,
        ts.event_year,
        ts.event_month,
        ts.match_type,
        ts.venue,
        ts.first_team,
        ts.second_team,
        ts.toss_winner,
        ts.toss_decision,
        ts.winner,
        ts.margin_type,
        ts.margin_value,
        ts.player_of_match,
        ts.first_team_runs,
        ts.second_team_runs,
        ts.first_team_wickets,
        ts.second_team_wickets,
        MAX(CASE WHEN pw.wicket_rank = 1 THEN pw.highest_wicket_taker END) AS highest_wicket_taker,
        MAX(CASE WHEN pw.wicket_rank = 1 THEN pw.wickets_taken END) AS wickets_taken,
        MAX(CASE WHEN pr.run_rank = 1 THEN pr.highest_run_scorer END) AS highest_run_scorer,
        MAX(CASE WHEN pr.run_rank = 1 THEN pr.runs_scored END) AS runs_scored,
        ts.total_deliveries,
        ts.total_runs_scored,
        ts.total_boundaries_4,
        ts.total_boundaries_6,
        ts.total_dot_balls,
        CURRENT_TIMESTAMP() AS ingestion_ts,
        CURRENT_TIMESTAMP() AS gold_load_ts,
        'VALID' AS data_quality_status,
        DATEDIFF(MINUTE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()) AS query_refresh_lag_minutes,
        'match_summary' AS record_source,
        TRUE AS is_latest
    FROM team_stats ts
    LEFT JOIN player_contributions pw ON ts.match_type_number = pw.match_type_number
    LEFT JOIN player_contributions pr ON ts.match_type_number = pr.match_type_number
    GROUP BY ts.event_date, ts.event_year, ts.event_month, ts.match_type, ts.venue, ts.first_team, ts.second_team,
             ts.toss_winner, ts.toss_decision, ts.winner, ts.margin_type, ts.margin_value, ts.player_of_match,
             ts.first_team_runs, ts.second_team_runs, ts.first_team_wickets, ts.second_team_wickets,
             ts.total_deliveries, ts.total_runs_scored, ts.total_boundaries_4, ts.total_boundaries_6, ts.total_dot_balls;
    
    p_rows_affected := @@rowcount;
    INSERT INTO cricket.gold.analytics_log (task_name, status, rows_processed, duration_seconds)
    VALUES ('gold_match_summary_task', 'SUCCESS', p_rows_affected, DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO cricket.gold.analytics_log (task_name, status, error_message, duration_seconds)
        VALUES ('gold_match_summary_task', 'FAILED', SQLERRM(), DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
        RAISE;
END;

-- Gold Validation & Quality Task
CREATE OR REPLACE TASK cricket.gold.gold_validation_task
WAREHOUSE = compute_wh
SCHEDULE = '30 MINUTES'
ON_ERROR = 'CONTINUE'
AS
DECLARE
    p_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    p_player_dim_count INT;
    p_delivery_fact_count INT;
    p_match_dim_count INT;
    p_player_perf_count INT;
    p_match_summary_count INT;
BEGIN
    -- Validate record counts
    p_player_dim_count := (SELECT COUNT(*) FROM cricket.gold.player_dim);
    p_delivery_fact_count := (SELECT COUNT(*) FROM cricket.gold.delivery_fact);
    p_match_dim_count := (SELECT COUNT(*) FROM cricket.gold.match_dim);
    p_player_perf_count := (SELECT COUNT(*) FROM cricket.gold.player_performance);
    p_match_summary_count := (SELECT COUNT(*) FROM cricket.gold.match_summary);
    
    -- Log validation
    INSERT INTO cricket.gold.analytics_log (task_name, status, rows_processed, duration_seconds)
    VALUES (
        'gold_validation_task',
        CASE WHEN p_player_dim_count > 0 AND p_delivery_fact_count > 0 AND p_match_dim_count > 0 THEN 'SUCCESS' ELSE 'WARNING' END,
        p_player_dim_count + p_delivery_fact_count + p_match_dim_count + p_player_perf_count + p_match_summary_count,
        DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP())
    );
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO cricket.gold.analytics_log (task_name, status, error_message, duration_seconds)
        VALUES ('gold_validation_task', 'FAILED', SQLERRM(), DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
        RAISE;
END;

-- ============================================================================
-- 5. RESUME TASKS
-- ============================================================================

ALTER TASK cricket.gold.gold_player_performance_task RESUME;
ALTER TASK cricket.gold.gold_match_summary_task RESUME;
ALTER TASK cricket.gold.gold_validation_task RESUME;

COMMIT;

-- ============================================================================
-- 6. VERIFICATION & ANALYTICS QUERIES
-- ============================================================================

-- Task Status Dashboard
SHOW TASKS IN SCHEMA cricket.gold;

-- Analytics Pipeline Execution History
SELECT 
    task_name,
    execution_ts,
    status,
    rows_processed,
    duration_seconds,
    DATEDIFF(MINUTE, execution_ts, CURRENT_TIMESTAMP()) AS minutes_ago
FROM cricket.gold.analytics_log
ORDER BY execution_ts DESC
LIMIT 50;

-- Data Freshness Dashboard
SELECT 
    'player_dim' AS layer,
    COUNT(*) AS record_count,
    MAX(gold_load_ts) AS last_refresh,
    DATEDIFF(MINUTE, MAX(gold_load_ts), CURRENT_TIMESTAMP()) AS minutes_since_refresh,
    ROUND(AVG(query_refresh_lag_minutes), 1) AS avg_lag_minutes
FROM cricket.gold.player_dim
UNION ALL
SELECT 
    'delivery_fact',
    COUNT(*),
    MAX(gold_load_ts),
    DATEDIFF(MINUTE, MAX(gold_load_ts), CURRENT_TIMESTAMP()),
    ROUND(AVG(query_refresh_lag_minutes), 1)
FROM cricket.gold.delivery_fact
UNION ALL
SELECT 
    'match_dim',
    COUNT(*),
    MAX(gold_load_ts),
    DATEDIFF(MINUTE, MAX(gold_load_ts), CURRENT_TIMESTAMP()),
    ROUND(AVG(query_refresh_lag_minutes), 1)
FROM cricket.gold.match_dim
UNION ALL
SELECT 
    'player_performance',
    COUNT(*),
    MAX(gold_load_ts),
    DATEDIFF(MINUTE, MAX(gold_load_ts), CURRENT_TIMESTAMP()),
    ROUND(AVG(query_refresh_lag_minutes), 1)
FROM cricket.gold.player_performance
UNION ALL
SELECT 
    'match_summary',
    COUNT(*),
    MAX(gold_load_ts),
    DATEDIFF(MINUTE, MAX(gold_load_ts), CURRENT_TIMESTAMP()),
    ROUND(AVG(query_refresh_lag_minutes), 1)
FROM cricket.gold.match_summary;

-- Data Quality Report
SELECT 
    'player_dim' AS table_name,
    data_quality_status,
    COUNT(*) AS record_count
FROM cricket.gold.player_dim
GROUP BY data_quality_status
UNION ALL
SELECT 
    'delivery_fact',
    data_quality_status,
    COUNT(*)
FROM cricket.gold.delivery_fact
GROUP BY data_quality_status
UNION ALL
SELECT 
    'match_dim',
    data_quality_status,
    COUNT(*)
FROM cricket.gold.match_dim
GROUP BY data_quality_status;

-- ============================================================================
-- 7. ANALYTICAL SAMPLE QUERIES
-- ============================================================================

-- Top 10 Batters by Strike Rate (Minimum 20 balls)
SELECT TOP 10
    player_name,
    country,
    balls_faced,
    runs_scored,
    strike_rate,
    boundaries_4,
    boundaries_6,
    form_status
FROM cricket.gold.player_performance
WHERE balls_faced >= 20
    AND strike_rate > 0
ORDER BY strike_rate DESC;

-- Top 10 Bowlers by Economy Rate (Minimum 10 balls bowled)
SELECT TOP 10
    player_name,
    country,
    balls_bowled,
    runs_conceded,
    wickets_taken,
    bowling_econ,
    dot_balls_bowled,
    form_status
FROM cricket.gold.player_performance
WHERE balls_bowled >= 10
    AND bowling_econ > 0
ORDER BY bowling_econ ASC;

-- Team-wise Win Analysis
SELECT
    winner,
    COUNT(*) AS matches_won,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cricket.gold.match_dim), 2) AS win_percentage,
    AVG(margin_value) AS avg_winning_margin,
    MIN(event_date) AS first_win_date,
    MAX(event_date) AS last_win_date
FROM cricket.gold.match_dim
WHERE winner != 'None'
GROUP BY winner
ORDER BY matches_won DESC;

-- Player Performance by Match Type
SELECT
    player_name,
    match_type_number,
    role_type,
    matches_count,
    batting_avg,
    strike_rate,
    bowling_econ,
    wickets_taken
FROM cricket.gold.player_performance
WHERE role_type IN ('Batter', 'Bowler', 'All-rounder')
ORDER BY matches_count DESC, batting_avg DESC
LIMIT 30;

-- Daily Match Summary for Dashboard
SELECT
    event_date,
    COUNT(*) AS matches_played,
    SUM(total_deliveries) AS total_balls_played,
    ROUND(SUM(total_runs_scored) / NULLIF(SUM(total_deliveries), 0) * 6, 2) AS run_rate,
    SUM(total_boundaries_4) AS total_fours,
    SUM(total_boundaries_6) AS total_sixes,
    COUNT(DISTINCT first_team) AS teams_participated
FROM cricket.gold.match_summary
WHERE event_date >= CURRENT_DATE() - INTERVAL '30 days'
GROUP BY event_date
ORDER BY event_date DESC;

-- Delivery-level Analytics by Team
SELECT
    team_name,
    COUNT(*) AS deliveries,
    SUM(runs) AS total_runs,
    ROUND(SUM(runs) * 100.0 / COUNT(*), 2) AS avg_runs_per_delivery,
    SUM(boundary_4_count) AS fours,
    SUM(boundary_6_count) AS sixes,
    SUM(dot_ball_count) AS dot_balls,
    SUM(wicket_flag) AS wickets_lost
FROM cricket.gold.delivery_fact
GROUP BY team_name
ORDER BY total_runs DESC;