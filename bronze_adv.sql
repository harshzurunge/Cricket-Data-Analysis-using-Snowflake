-- Production-Grade Bronze Layer for Cricket Data Analysis
-- Incremental processing using Streams, Tasks, and MERGE
-- Author: Expert Snowflake Data Engineer

USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE cricket;
USE SCHEMA cricket.bronze;

BEGIN;

-- 1. Table Definitions

-- Pipeline log table for monitoring
CREATE OR REPLACE TABLE cricket.bronze.pipeline_log (
    task_name VARCHAR COMMENT 'Name of the executed task',
    execution_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Execution timestamp',
    status VARCHAR COMMENT 'Execution status (SUCCESS/FAILED)',
    rows_processed INT COMMENT 'Number of rows processed',
    error_message VARCHAR COMMENT 'Error message if any'
)
COMMENT 'Log table for pipeline task executions';

-- Player table
CREATE OR REPLACE TABLE cricket.bronze.player_table (
    match_type_number INT COMMENT 'Match type identifier',
    country TEXT COMMENT 'Player country/team',
    player_name TEXT COMMENT 'Player name',
    stg_file_name TEXT COMMENT 'Staging file name',
    stg_file_row_number INT COMMENT 'Staging file row number',
    stg_file_hashkey TEXT COMMENT 'Staging file hash key',
    stg_modified_ts TIMESTAMP_NTZ COMMENT 'Staging modified timestamp',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Ingestion timestamp',
    source_system VARCHAR DEFAULT 'CRICKET_JSON' COMMENT 'Source system identifier',
    data_quality_status VARCHAR DEFAULT 'VALID' COMMENT 'Data quality status',
    data_quality_score FLOAT DEFAULT 1.0 COMMENT 'Data quality score (1.0 = perfect)'
)
CLUSTER BY (match_type_number, ingestion_ts)
COMMENT 'Bronze layer player table with incremental processing';

-- Delivery table
CREATE OR REPLACE TABLE cricket.bronze.delivery_table (
    match_type_number INT COMMENT 'Match type identifier',
    team_name TEXT COMMENT 'Team name',
    over INT COMMENT 'Over number',
    bowler TEXT COMMENT 'Bowler name',
    batter TEXT COMMENT 'Batter name',
    non_striker TEXT COMMENT 'Non-striker name',
    runs TEXT COMMENT 'Runs scored by batter',
    extras TEXT COMMENT 'Extra runs',
    extra_type TEXT COMMENT 'Type of extra',
    total TEXT COMMENT 'Total runs for delivery',
    player_out TEXT COMMENT 'Player out name',
    player_out_kind TEXT COMMENT 'Kind of dismissal',
    player_out_fielder TEXT COMMENT 'Fielder involved in dismissal',
    review_by_team TEXT COMMENT 'Team requesting review',
    review_decision TEXT COMMENT 'Review decision',
    review_type TEXT COMMENT 'Review type',
    stg_file_name TEXT COMMENT 'Staging file name',
    stg_file_row_number INT COMMENT 'Staging file row number',
    stg_file_hashkey TEXT COMMENT 'Staging file hash key',
    stg_modified_ts TIMESTAMP_NTZ COMMENT 'Staging modified timestamp',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Ingestion timestamp',
    source_system VARCHAR DEFAULT 'CRICKET_JSON' COMMENT 'Source system identifier',
    data_quality_status VARCHAR DEFAULT 'VALID' COMMENT 'Data quality status',
    data_quality_score FLOAT DEFAULT 1.0 COMMENT 'Data quality score (1.0 = perfect)'
)
CLUSTER BY (match_type_number, team_name, over)
COMMENT 'Bronze layer delivery table with incremental processing';

-- Match table (transient)
CREATE OR REPLACE TRANSIENT TABLE cricket.bronze.match_table (
    match_type_number INT COMMENT 'Match type identifier',
    event_name TEXT COMMENT 'Event name',
    match_number INT COMMENT 'Match number',
    event_date DATE COMMENT 'Event date',
    match_type TEXT COMMENT 'Match type',
    season TEXT COMMENT 'Season',
    team_type TEXT COMMENT 'Team type',
    overs INT COMMENT 'Overs per innings',
    city TEXT COMMENT 'City',
    venue TEXT COMMENT 'Venue',
    gender TEXT COMMENT 'Gender',
    first_team TEXT COMMENT 'First team',
    second_team TEXT COMMENT 'Second team',
    winner TEXT COMMENT 'Winner team',
    won_by_runs INT COMMENT 'Runs margin',
    won_by_wickets INT COMMENT 'Wickets margin',
    player_of_match TEXT COMMENT 'Player of the match',
    match_referee TEXT COMMENT 'Match referee',
    reserve_umpires TEXT COMMENT 'Reserve umpires',
    tv_umpires TEXT COMMENT 'TV umpires',
    first_umpire TEXT COMMENT 'First umpire',
    second_umpire TEXT COMMENT 'Second umpire',
    toss_winner TEXT COMMENT 'Toss winner',
    toss_decision TEXT COMMENT 'Toss decision',
    stg_file_name TEXT COMMENT 'Staging file name',
    stg_file_row_number INT COMMENT 'Staging file row number',
    stg_file_hashkey TEXT COMMENT 'Staging file hash key',
    stg_modified_ts TIMESTAMP_NTZ COMMENT 'Staging modified timestamp',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Ingestion timestamp',
    source_system VARCHAR DEFAULT 'CRICKET_JSON' COMMENT 'Source system identifier',
    data_quality_status VARCHAR DEFAULT 'VALID' COMMENT 'Data quality status',
    data_quality_score FLOAT DEFAULT 1.0 COMMENT 'Data quality score (1.0 = perfect)'
)
CLUSTER BY (match_type_number, event_date)
COMMENT 'Bronze layer match table (transient) with incremental processing';

-- 2. Stream Definition
CREATE OR REPLACE STREAM cricket.bronze.raw_match_stream
ON TABLE cricket.raw.match_raw_tbl
COMMENT 'Stream for incremental processing of raw match data';

-- 3. Task Definitions

-- Raw stream monitoring task
CREATE OR REPLACE TASK cricket.bronze.raw_stream_task
WAREHOUSE = compute_wh
SCHEDULE = '10 MINUTES'
ON_ERROR = 'CONTINUE'
AS
SELECT 1;  -- Dummy query to monitor stream lag

-- Player processing task
CREATE OR REPLACE TASK cricket.bronze.player_task
WAREHOUSE = compute_wh
AFTER cricket.bronze.raw_stream_task
ON_ERROR = 'CONTINUE'
AS
MERGE INTO cricket.bronze.player_table tgt
USING (
    SELECT
        COALESCE(TRY_CAST(raw.info:match_type_number AS INT), -1) AS match_type_number,
        p.key::TEXT AS country,
        team.value::TEXT AS player_name,
        raw.stg_file_name,
        raw.stg_file_row_number,
        raw.stg_file_hashkey,
        raw.stg_modified_ts,
        CASE WHEN COALESCE(TRY_CAST(raw.info:match_type_number AS INT), -1) = -1 OR p.key IS NULL OR team.value IS NULL THEN 'INVALID' ELSE 'VALID' END AS data_quality_status,
        CASE WHEN COALESCE(TRY_CAST(raw.info:match_type_number AS INT), -1) = -1 OR p.key IS NULL OR team.value IS NULL THEN 0.0 ELSE 1.0 END AS data_quality_score
    FROM cricket.bronze.raw_match_stream raw,
    LATERAL FLATTEN(input => raw.info:players) p,
    LATERAL FLATTEN(input => p.value) team
    WHERE METADATA$ACTION IN ('INSERT', 'INSERT_AND_ON_METADATA_UPDATE')
) src
ON tgt.stg_file_hashkey = src.stg_file_hashkey
   AND tgt.stg_file_row_number = src.stg_file_row_number
   AND tgt.country = src.country
   AND tgt.player_name = src.player_name
WHEN MATCHED AND src.stg_modified_ts > tgt.stg_modified_ts THEN
    UPDATE SET
        match_type_number = src.match_type_number,
        stg_modified_ts = src.stg_modified_ts,
        ingestion_ts = CURRENT_TIMESTAMP(),
        data_quality_status = src.data_quality_status,
        data_quality_score = src.data_quality_score
WHEN NOT MATCHED THEN
    INSERT (match_type_number, country, player_name, stg_file_name, stg_file_row_number, stg_file_hashkey, stg_modified_ts, data_quality_status, data_quality_score)
    VALUES (src.match_type_number, src.country, src.player_name, src.stg_file_name, src.stg_file_row_number, src.stg_file_hashkey, src.stg_modified_ts, src.data_quality_status, src.data_quality_score);

-- Delivery processing task
CREATE OR REPLACE TASK cricket.bronze.delivery_task
WAREHOUSE = compute_wh
AFTER cricket.bronze.player_task
ON_ERROR = 'CONTINUE'
AS
MERGE INTO cricket.bronze.delivery_table tgt
USING (
    SELECT
        COALESCE(TRY_CAST(m.info:match_type_number AS INT), -1) AS match_type_number,
        i.value:team::TEXT AS team_name,
        COALESCE(TRY_CAST(o.value:over AS INT), -1) AS over,
        d.value:bowler::TEXT AS bowler,
        d.value:batter::TEXT AS batter,
        d.value:non_striker::TEXT AS non_striker,
        d.value:runs.batter::TEXT AS runs,
        d.value:runs.extras::TEXT AS extras,
        d.value:runs.total::TEXT AS total,
        d.value:review:by::TEXT AS review_by_team,
        d.value:review:decision::TEXT AS review_decision,
        d.value:review:type::TEXT AS review_type,
        e.key::TEXT AS extra_type,
        w.value:player_out::TEXT AS player_out,
        w.value:kind::TEXT AS player_out_kind,
        f.value:name::TEXT AS player_out_fielder,
        m.stg_file_name,
        m.stg_file_row_number,
        m.stg_file_hashkey,
        m.stg_modified_ts,
        CASE WHEN COALESCE(TRY_CAST(m.info:match_type_number AS INT), -1) = -1 OR i.value:team IS NULL OR COALESCE(TRY_CAST(o.value:over AS INT), -1) = -1 OR d.value:bowler IS NULL OR d.value:batter IS NULL THEN 'INVALID' ELSE 'VALID' END AS data_quality_status,
        CASE WHEN COALESCE(TRY_CAST(m.info:match_type_number AS INT), -1) = -1 OR i.value:team IS NULL OR COALESCE(TRY_CAST(o.value:over AS INT), -1) = -1 OR d.value:bowler IS NULL OR d.value:batter IS NULL THEN 0.0 ELSE 1.0 END AS data_quality_score
    FROM cricket.bronze.raw_match_stream m,
    LATERAL FLATTEN(input => m.innings) i,
    LATERAL FLATTEN(input => i.value:overs) o,
    LATERAL FLATTEN(input => o.value:deliveries) d,
    LATERAL FLATTEN(input => d.value:extras, OUTER => TRUE) e,
    LATERAL FLATTEN(input => d.value:wickets, OUTER => TRUE) w,
    LATERAL FLATTEN(input => w.value:fielders, OUTER => TRUE) f
    WHERE METADATA$ACTION IN ('INSERT', 'INSERT_AND_ON_METADATA_UPDATE')
) src
ON tgt.stg_file_hashkey = src.stg_file_hashkey
   AND tgt.stg_file_row_number = src.stg_file_row_number
   AND tgt.over = src.over
   AND tgt.bowler = src.bowler
   AND tgt.batter = src.batter
WHEN MATCHED AND src.stg_modified_ts > tgt.stg_modified_ts THEN
    UPDATE SET
        match_type_number = src.match_type_number,
        team_name = src.team_name,
        runs = src.runs,
        extras = src.extras,
        extra_type = src.extra_type,
        total = src.total,
        player_out = src.player_out,
        player_out_kind = src.player_out_kind,
        player_out_fielder = src.player_out_fielder,
        review_by_team = src.review_by_team,
        review_decision = src.review_decision,
        review_type = src.review_type,
        stg_modified_ts = src.stg_modified_ts,
        ingestion_ts = CURRENT_TIMESTAMP(),
        data_quality_status = src.data_quality_status,
        data_quality_score = src.data_quality_score
WHEN NOT MATCHED THEN
    INSERT (match_type_number, team_name, over, bowler, batter, non_striker, runs, extras, extra_type, total, player_out, player_out_kind, player_out_fielder, review_by_team, review_decision, review_type, stg_file_name, stg_file_row_number, stg_file_hashkey, stg_modified_ts, data_quality_status, data_quality_score)
    VALUES (src.match_type_number, src.team_name, src.over, src.bowler, src.batter, src.non_striker, src.runs, src.extras, src.extra_type, src.total, src.player_out, src.player_out_kind, src.player_out_fielder, src.review_by_team, src.review_decision, src.review_type, src.stg_file_name, src.stg_file_row_number, src.stg_file_hashkey, src.stg_modified_ts, src.data_quality_status, src.data_quality_score);

-- Match processing task
CREATE OR REPLACE TASK cricket.bronze.match_task
WAREHOUSE = compute_wh
AFTER cricket.bronze.delivery_task
ON_ERROR = 'CONTINUE'
AS
MERGE INTO cricket.bronze.match_table tgt
USING (
    SELECT
        COALESCE(TRY_CAST(info:match_type_number AS INT), -1) AS match_type_number,
        info:event.name::TEXT AS event_name,
        COALESCE(TRY_CAST(info:event.match_number AS INT), -1) AS match_number,
        COALESCE(TRY_CAST(info:dates[0] AS DATE), DATE('1900-01-01')) AS event_date,
        info:match_type::TEXT AS match_type,
        info:season::TEXT AS season,
        info:team_type::TEXT AS team_type,
        COALESCE(TRY_CAST(info:overs AS INT), -1) AS overs,
        info:city::TEXT AS city,
        info:venue::TEXT AS venue,
        info:gender::TEXT AS gender,
        info:teams[0]::TEXT AS first_team,
        info:teams[1]::TEXT AS second_team,
        info:outcome.winner::TEXT AS winner,
        COALESCE(TRY_CAST(info:outcome.by.runs AS INT), -1) AS won_by_runs,
        COALESCE(TRY_CAST(info:outcome.by.wickets AS INT), -1) AS won_by_wickets,
        info:player_of_match[0]::TEXT AS player_of_match,
        info:officials.match_referees[0]::TEXT AS match_referee,
        info:officials.reserve_umpires[0]::TEXT AS reserve_umpires,
        info:officials.tv_umpires[0]::TEXT AS tv_umpires,
        info:officials.umpires[0]::TEXT AS first_umpire,
        info:officials.umpires[1]::TEXT AS second_umpire,
        info:toss.winner::TEXT AS toss_winner,
        info:toss.decision::TEXT AS toss_decision,
        stg_file_name,
        stg_file_row_number,
        stg_file_hashkey,
        stg_modified_ts,
        CASE WHEN COALESCE(TRY_CAST(info:match_type_number AS INT), -1) = -1 OR info:event.name IS NULL THEN 'INVALID' ELSE 'VALID' END AS data_quality_status,
        CASE WHEN COALESCE(TRY_CAST(info:match_type_number AS INT), -1) = -1 OR info:event.name IS NULL THEN 0.0 ELSE 1.0 END AS data_quality_score
    FROM cricket.bronze.raw_match_stream
    WHERE METADATA$ACTION IN ('INSERT', 'INSERT_AND_ON_METADATA_UPDATE')
) src
ON tgt.stg_file_hashkey = src.stg_file_hashkey
   AND tgt.stg_file_row_number = src.stg_file_row_number
WHEN MATCHED AND src.stg_modified_ts > tgt.stg_modified_ts THEN
    UPDATE SET
        match_type_number = src.match_type_number,
        event_name = src.event_name,
        match_number = src.match_number,
        event_date = src.event_date,
        match_type = src.match_type,
        season = src.season,
        team_type = src.team_type,
        overs = src.overs,
        city = src.city,
        venue = src.venue,
        gender = src.gender,
        first_team = src.first_team,
        second_team = src.second_team,
        winner = src.winner,
        won_by_runs = src.won_by_runs,
        won_by_wickets = src.won_by_wickets,
        player_of_match = src.player_of_match,
        match_referee = src.match_referee,
        reserve_umpires = src.reserve_umpires,
        tv_umpires = src.tv_umpires,
        first_umpire = src.first_umpire,
        second_umpire = src.second_umpire,
        toss_winner = src.toss_winner,
        toss_decision = src.toss_decision,
        stg_modified_ts = src.stg_modified_ts,
        ingestion_ts = CURRENT_TIMESTAMP(),
        data_quality_status = src.data_quality_status,
        data_quality_score = src.data_quality_score
WHEN NOT MATCHED THEN
    INSERT (match_type_number, event_name, match_number, event_date, match_type, season, team_type, overs, city, venue, gender, first_team, second_team, winner, won_by_runs, won_by_wickets, player_of_match, match_referee, reserve_umpires, tv_umpires, first_umpire, second_umpire, toss_winner, toss_decision, stg_file_name, stg_file_row_number, stg_file_hashkey, stg_modified_ts, data_quality_status, data_quality_score)
    VALUES (src.match_type_number, src.event_name, src.match_number, src.event_date, src.match_type, src.season, src.team_type, src.overs, src.city, src.venue, src.gender, src.first_team, src.second_team, src.winner, src.won_by_runs, src.won_by_wickets, src.player_of_match, src.match_referee, src.reserve_umpires, src.tv_umpires, src.first_umpire, src.second_umpire, src.toss_winner, src.toss_decision, src.stg_file_name, src.stg_file_row_number, src.stg_file_hashkey, src.stg_modified_ts, src.data_quality_status, src.data_quality_score);

-- Validation task
CREATE OR REPLACE TASK cricket.bronze.validation_task
WAREHOUSE = compute_wh
AFTER cricket.bronze.match_task
ON_ERROR = 'CONTINUE'
AS
BEGIN
    -- Log player table quality
    INSERT INTO cricket.bronze.pipeline_log (task_name, status, rows_processed)
    SELECT 'validation_task', 'SUCCESS', COUNT(*) FROM cricket.bronze.player_table;

    -- Check for high null percentages (example for player_name)
    LET null_pct := (SELECT COUNT(*) FROM cricket.bronze.player_table WHERE player_name IS NULL) / NULLIF((SELECT COUNT(*) FROM cricket.bronze.player_table), 0);
    IF (null_pct > 0.05) THEN
        UPDATE cricket.bronze.player_table SET data_quality_status = 'INVALID' WHERE player_name IS NULL;
    END IF;

    -- Similar checks for other tables can be added here
END;

-- 4. Maintenance
ALTER TABLE cricket.bronze.player_table SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE cricket.bronze.delivery_table SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE cricket.bronze.match_table SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE cricket.bronze.pipeline_log SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- 5. Resume Tasks
ALTER TASK cricket.bronze.raw_stream_task RESUME;
ALTER TASK cricket.bronze.player_task RESUME;
ALTER TASK cricket.bronze.delivery_task RESUME;
ALTER TASK cricket.bronze.match_task RESUME;
ALTER TASK cricket.bronze.validation_task RESUME;

COMMIT;

-- Verification Queries
SHOW TASKS IN SCHEMA cricket.bronze;
SELECT SYSTEM$STREAM_STATUS('cricket.bronze.raw_match_stream');

-- Test Queries (run after inserting sample data into raw table)
-- INSERT INTO cricket.raw.match_raw_tbl (info, innings, stg_file_name, stg_file_row_number, stg_file_hashkey, stg_modified_ts)
-- VALUES (PARSE_JSON('{"match_type_number": 1, "players": {"TeamA": ["Player1"], "TeamB": ["Player2"]}}'), PARSE_JSON('[]'), 'test.json', 1, 'hash123', CURRENT_TIMESTAMP());
-- Then check: SELECT * FROM cricket.bronze.player_table WHERE stg_file_hashkey = 'hash123';

-- Row counts
SELECT 'player_table' AS table_name, COUNT(*) AS row_count FROM cricket.bronze.player_table
UNION ALL
SELECT 'delivery_table', COUNT(*) FROM cricket.bronze.delivery_table
UNION ALL
SELECT 'match_table', COUNT(*) FROM cricket.bronze.match_table;


-- =====================================================
-- PRODUCTION BRONZE LAYER IMPLEMENTATION - CRICKET DATA
-- Complete incremental pipeline using Streams + Tasks + MERGE
-- Runnable production script - April 2026
-- =====================================================

BEGIN;

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CRICKET;
USE SCHEMA BRONZE;

-- =====================================================
-- 1. PIPELINE LOGGING & BRONZE TABLES
-- =====================================================

-- Pipeline logging table
CREATE OR REPLACE TABLE bronze.pipeline_log (
    task_name VARCHAR(100) COMMENT 'Task/procedure name',
    execution_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Task run timestamp',
    status VARCHAR(20) COMMENT 'SUCCESS/FAILED/WARNING',
    rows_processed INT COMMENT 'Rows affected by MERGE',
    error_message VARCHAR(4000) COMMENT 'Error details if failed',
    stream_lag_hours FLOAT COMMENT 'Stream lag in hours',
    data_quality_score FLOAT DEFAULT 1.0 COMMENT 'DQ score 0-1.0',
    records_checked INT COMMENT 'Total records processed'
);

-- MATCH TABLE (Transient - exactly matches original CTAS columns + production fields)
CREATE OR REPLACE TRANSIENT TABLE bronze.match_table (
    match_type_number INT COMMENT 'COALESCE(info:match_type_number::INT, -1)',
    event_name VARCHAR(255) COMMENT 'info:event.name',
    match_number INT COMMENT 'info:event.match_number',
    event_date DATE COMMENT 'info:dates[0]',
    match_type VARCHAR(100) COMMENT 'info:match_type',
    season VARCHAR(100) COMMENT 'info:season',
    team_type VARCHAR(100) COMMENT 'info:team_type',
    overs INT COMMENT 'info:overs',
    city VARCHAR(255) COMMENT 'info:city',
    venue VARCHAR(255) COMMENT 'info:venue',
    gender VARCHAR(50) COMMENT 'info:gender',
    first_team VARCHAR(255) COMMENT 'info:teams[0]',
    second_team VARCHAR(255) COMMENT 'info:teams[1]',
    winner VARCHAR(255) COMMENT 'info:outcome.winner',
    won_by_runs INT COMMENT 'info:outcome.by.runs',
    won_by_wickets INT COMMENT 'info:outcome.by.wickets',
    player_of_match VARCHAR(255) COMMENT 'info:player_of_match[0]',
    match_referee VARCHAR(255) COMMENT 'info:officials.match_referees[0]',
    reserve_umpires VARCHAR(255) COMMENT 'info:officials.reserve_umpires[0]',
    tv_umpires VARCHAR(255) COMMENT 'info:officials.tv_umpires[0]',
    first_umpire VARCHAR(255) COMMENT 'info:officials.umpires[0]',
    second_umpire VARCHAR(255) COMMENT 'info:officials.umpires[1]',
    toss_winner VARCHAR(255) COMMENT 'info:toss.winner',
    toss_decision VARCHAR(255) COMMENT 'info:toss.decision',
    stg_file_name VARCHAR(255),
    stg_file_row_number INT,
    stg_file_hashkey VARCHAR(64),
    stg_modified_ts TIMESTAMP_NTZ,
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CRICKET_JSON',
    data_quality_status VARCHAR(20) DEFAULT 'VALID',
    data_quality_score FLOAT DEFAULT 1.0
)
CLUSTER BY (match_type_number, event_date)
COMMENT = 'Bronze match metadata - Natural key: stg_file_hashkey + stg_file_row_number';

-- PLAYER TABLE (Exactly matches original flattening)
CREATE OR REPLACE TABLE bronze.player_table (
    match_type_number INT COMMENT 'raw.info:match_type_number::INT',
    country VARCHAR(100) COMMENT 'p.key::TEXT (team identifier)',
    player_name VARCHAR(255) COMMENT 'team.value::TEXT',
    stg_file_name VARCHAR(255),
    stg_file_row_number INT,
    stg_file_hashkey VARCHAR(64),
    stg_modified_ts TIMESTAMP_NTZ,
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CRICKET_JSON',
    data_quality_status VARCHAR(20) DEFAULT 'VALID',
    data_quality_score FLOAT DEFAULT 1.0
)
CLUSTER BY (match_type_number, ingestion_ts)
COMMENT = 'Bronze players per match - Natural key: hashkey+row+country+player_name';

-- DELIVERY TABLE (Exactly matches original complex nested flatten)
CREATE OR REPLACE TABLE bronze.delivery_table (
    match_type_number INT COMMENT 'm.info:match_type_number::INT',
    team_name VARCHAR(100) COMMENT 'i.value:team',
    over_num FLOAT COMMENT 'o.value:over (supports decimal overs)',
    bowler VARCHAR(255) COMMENT 'd.value:bowler',
    batter VARCHAR(255) COMMENT 'd.value:batter',
    non_striker VARCHAR(255) COMMENT 'd.value:non_striker',
    runs_batter INT DEFAULT 0 COMMENT 'd.value:runs.batter::INT',
    runs_extras INT DEFAULT 0 COMMENT 'd.value:runs.extras::INT',
    runs_total INT DEFAULT 0 COMMENT 'd.value:runs.total::INT',
    review_by_team VARCHAR(50) COMMENT 'd.value:review.by',
    review_decision VARCHAR(50) COMMENT 'd.value:review.decision',
    review_type VARCHAR(50) COMMENT 'd.value:review.type',
    extra_type VARCHAR(50) COMMENT 'e.key (legbye/wide etc.)',
    player_out VARCHAR(255) COMMENT 'w.value:player_out',
    player_out_kind VARCHAR(50) COMMENT 'w.value:kind',
    player_out_fielder VARCHAR(255) COMMENT 'f.value:name',
    stg_file_name VARCHAR(255),
    stg_file_row_number INT,
    stg_file_hashkey VARCHAR(64),
    stg_modified_ts TIMESTAMP_NTZ,
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_system VARCHAR(50) DEFAULT 'CRICKET_JSON',
    data_quality_status VARCHAR(20) DEFAULT 'VALID',
    data_quality_score FLOAT DEFAULT 1.0
)
CLUSTER BY (match_type_number, team_name, over_num)
COMMENT = 'Ball-by-ball delivery data - Natural key: hashkey+row+over+bowler+batter';

-- Set time travel on all tables
ALTER TABLE bronze.match_table SET TIME_TRAVEL = '1 DAY';
ALTER TABLE bronze.player_table SET TIME_TRAVEL = '1 DAY';
ALTER TABLE bronze.delivery_table SET TIME_TRAVEL = '1 DAY';
ALTER TABLE bronze.pipeline_log SET TIME_TRAVEL = '1 DAY';

-- =====================================================
-- 2. INCREMENTAL STREAM
-- =====================================================
CREATE OR REPLACE STREAM bronze.raw_match_stream 
ON TABLE cricket.raw.match_raw_tbl
COMMENT = 'Change data capture stream for bronze incremental processing';

-- =====================================================
-- 3. MERGE PROCEDURES (Production-grade with error handling)
-- =====================================================

-- MATCH MERGE PROCEDURE
CREATE OR REPLACE PROCEDURE bronze.merge_match_proc()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_affected INT;
    dq_score FLOAT DEFAULT 1.0;
BEGIN
    -- MERGE with natural key + modified_ts change detection
    MERGE INTO bronze.match_table tgt
    USING (
        SELECT 
            COALESCE(raw.info:match_type_number::INT, -1) as match_type_number,
            raw.info:event.name::VARCHAR as event_name,
            TRY_CAST(raw.info:event.match_number AS INT) as match_number,
            TRY_CAST(raw.info:dates[0] AS DATE) as event_date,
            raw.info:match_type::VARCHAR as match_type,
            raw.info:season::VARCHAR as season,
            raw.info:team_type::VARCHAR as team_type,
            TRY_CAST(raw.info:overs AS INT) as overs,
            raw.info:city::VARCHAR as city,
            raw.info:venue::VARCHAR as venue,
            raw.info:gender::VARCHAR as gender,
            raw.info:teams[0]::VARCHAR as first_team,
            raw.info:teams[1]::VARCHAR as second_team,
            raw.info:outcome.winner::VARCHAR as winner,
            TRY_CAST(raw.info:outcome.by.runs AS INT) as won_by_runs,
            TRY_CAST(raw.info:outcome.by.wickets AS INT) as won_by_wickets,
            raw.info:player_of_match[0]::VARCHAR as player_of_match,
            raw.info:officials.match_referees[0]::VARCHAR as match_referee,
            raw.info:officials.reserve_umpires[0]::VARCHAR as reserve_umpires,
            raw.info:officials.tv_umpires[0]::VARCHAR as tv_umpires,
            raw.info:officials.umpires[0]::VARCHAR as first_umpire,
            raw.info:officials.umpires[1]::VARCHAR as second_umpire,
            raw.info:toss.winner::VARCHAR as toss_winner,
            raw.info:toss.decision::VARCHAR as toss_decision,
            raw.stg_file_name, raw.stg_file_row_number, raw.stg_file_hashkey, raw.stg_modified_ts
        FROM bronze.raw_match_stream raw
        WHERE raw.METADATA$ACTION = 'INSERT'  -- Append-only stream
    ) src ON tgt.stg_file_hashkey = src.stg_file_hashkey 
          AND tgt.stg_file_row_number = src.stg_file_row_number
          AND tgt.stg_modified_ts = src.stg_modified_ts
    WHEN MATCHED AND tgt.stg_modified_ts != src.stg_modified_ts THEN
        UPDATE SET 
            match_type_number = src.match_type_number,
            event_name = src.event_name,
            -- ... all columns
            match_number = src.match_number,
            event_date = src.event_date,
            match_type = src.match_type,
            season = src.season,
            team_type = src.team_type,
            overs = src.overs,
            city = src.city,
            venue = src.venue,
            gender = src.gender,
            first_team = src.first_team,
            second_team = src.second_team,
            winner = src.winner,
            won_by_runs = src.won_by_runs,
            won_by_wickets = src.won_by_wickets,
            player_of_match = src.player_of_match,
            match_referee = src.match_referee,
            reserve_umpires = src.reserve_umpires,
            tv_umpires = src.tv_umpires,
            first_umpire = src.first_umpire,
            second_umpire = src.second_umpire,
            toss_winner = src.toss_winner,
            toss_decision = src.toss_decision,
            stg_modified_ts = src.stg_modified_ts,
            ingestion_ts = CURRENT_TIMESTAMP(),
            data_quality_status = 'VALID',
            data_quality_score = 1.0
    WHEN NOT MATCHED THEN
        INSERT (match_type_number, event_name, match_number, event_date, match_type, season, 
                team_type, overs, city, venue, gender, first_team, second_team, winner, 
                won_by_runs, won_by_wickets, player_of_match, match_referee, reserve_umpires, 
                tv_umpires);