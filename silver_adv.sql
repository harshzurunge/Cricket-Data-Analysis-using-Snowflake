-- Production-Grade Silver Layer for Cricket Data Analysis
-- Incremental processing using Streams, Tasks, and MERGE
-- Preserves exact cleaning logic from original code
-- Author: Expert Snowflake Data Engineer

USE DATABASE cricket;
USE ROLE sysadmin;
USE WAREHOUSE compute_wh;

CREATE OR REPLACE SCHEMA cricket.silver;
USE SCHEMA cricket.silver;

BEGIN;

-- ============================================================================
-- 1. PIPELINE LOGGING TABLE
-- ============================================================================

CREATE OR REPLACE TABLE cricket.silver.pipeline_log (
    task_name VARCHAR COMMENT 'Name of the executed task',
    execution_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Execution timestamp',
    status VARCHAR COMMENT 'Execution status (SUCCESS/FAILED)',
    rows_processed INT COMMENT 'Number of rows processed',
    rows_affected INT COMMENT 'Number of rows inserted/updated',
    error_message VARCHAR COMMENT 'Error message if any',
    duration_seconds INT COMMENT 'Task duration in seconds'
)
COMMENT 'Silver layer pipeline task execution log';

-- ============================================================================
-- 2. STREAM DEFINITIONS ON BRONZE TABLES
-- ============================================================================

CREATE OR REPLACE STREAM cricket.silver.bronze_player_stream
ON TABLE cricket.bronze.player_table
COMMENT 'Stream for incremental player data changes from bronze layer';

CREATE OR REPLACE STREAM cricket.silver.bronze_delivery_stream
ON TABLE cricket.bronze.delivery_table
COMMENT 'Stream for incremental delivery data changes from bronze layer';

CREATE OR REPLACE STREAM cricket.silver.bronze_match_stream
ON TABLE cricket.bronze.match_table
COMMENT 'Stream for incremental match data changes from bronze layer';

-- ============================================================================
-- 3. SILVER TABLE DEFINITIONS
-- ============================================================================

-- Player Clean Table
CREATE OR REPLACE TABLE cricket.silver.player_clean (
    match_type_number INT COMMENT 'Match type identifier',
    country TEXT COMMENT 'Player country (cleaned)',
    player_name TEXT COMMENT 'Player name (cleaned)',
    
    stg_file_name TEXT COMMENT 'Staging file name',
    stg_file_hashkey TEXT COMMENT 'Staging file hash key',
    stg_modified_ts TIMESTAMP_NTZ COMMENT 'Staging modified timestamp',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Bronze ingestion timestamp (passthrough)',
    silver_load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Silver load timestamp',
    source_system VARCHAR DEFAULT 'CRICKET_SILVER' COMMENT 'Source system identifier',
    data_quality_status VARCHAR DEFAULT 'VALID' COMMENT 'Data quality status (VALID/INVALID)',
    data_quality_score FLOAT DEFAULT 1.0 COMMENT 'Data quality score (0.0-1.0)',
    is_current_record BOOLEAN DEFAULT TRUE COMMENT 'SCD Type 2: Current record flag'
)
CLUSTER BY (match_type_number, country)
COMMENT 'Silver layer player clean table with incremental processing';

-- Delivery Clean Table
CREATE OR REPLACE TABLE cricket.silver.delivery_clean (
    match_type_number INT COMMENT 'Match type identifier',
    team_name TEXT COMMENT 'Team name (cleaned)',
    over INT COMMENT 'Over number',
    bowler TEXT COMMENT 'Bowler name (cleaned)',
    batter TEXT COMMENT 'Batter name (cleaned)',
    non_striker TEXT COMMENT 'Non-striker name (cleaned)',
    runs INT COMMENT 'Runs scored by batter (numeric)',
    extras INT COMMENT 'Extra runs (numeric)',
    extra_type TEXT COMMENT 'Type of extra (cleaned)',
    total INT COMMENT 'Total runs for delivery (numeric)',
    player_out TEXT COMMENT 'Player out name (cleaned)',
    player_out_kind TEXT COMMENT 'Kind of dismissal (cleaned)',
    player_out_fielder TEXT COMMENT 'Fielder involved in dismissal (cleaned)',
    review_by_team TEXT COMMENT 'Team requesting review (cleaned)',
    review_decision TEXT COMMENT 'Review decision (cleaned)',
    review_type TEXT COMMENT 'Review type (cleaned)',
    
    stg_file_name TEXT COMMENT 'Staging file name',
    stg_file_row_number INT COMMENT 'Staging file row number',
    stg_file_hashkey TEXT COMMENT 'Staging file hash key',
    stg_modified_ts TIMESTAMP_NTZ COMMENT 'Staging modified timestamp',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Bronze ingestion timestamp (passthrough)',
    silver_load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Silver load timestamp',
    source_system VARCHAR DEFAULT 'CRICKET_SILVER' COMMENT 'Source system identifier',
    data_quality_status VARCHAR DEFAULT 'VALID' COMMENT 'Data quality status (VALID/INVALID)',
    data_quality_score FLOAT DEFAULT 1.0 COMMENT 'Data quality score (0.0-1.0)',
    is_current_record BOOLEAN DEFAULT TRUE COMMENT 'SCD Type 2: Current record flag'
)
CLUSTER BY (match_type_number, team_name, over)
COMMENT 'Silver layer delivery clean table with incremental processing';

-- Match Clean Table
CREATE OR REPLACE TABLE cricket.silver.match_clean (
    match_type_number INT COMMENT 'Match type identifier',
    event_name TEXT COMMENT 'Event name (cleaned)',
    match_number INT COMMENT 'Match number',
    event_date DATE COMMENT 'Event date (cleaned)',
    event_year INT COMMENT 'Derived: Year from event_date',
    event_month INT COMMENT 'Derived: Month from event_date',
    event_day INT COMMENT 'Derived: Day from event_date',
    match_type TEXT COMMENT 'Match type (cleaned)',
    season TEXT COMMENT 'Season (cleaned)',
    team_type TEXT COMMENT 'Team type (cleaned)',
    overs INT COMMENT 'Overs per innings',
    city TEXT COMMENT 'City (cleaned)',
    venue TEXT COMMENT 'Venue (cleaned)',
    gender TEXT COMMENT 'Gender (cleaned)',
    first_team TEXT COMMENT 'First team (cleaned)',
    second_team TEXT COMMENT 'Second team (cleaned)',
    winner TEXT COMMENT 'Winner team (cleaned)',
    won_by_runs INT COMMENT 'Runs margin',
    won_by_wickets INT COMMENT 'Wickets margin',
    player_of_match TEXT COMMENT 'Player of the match (cleaned)',
    match_referee TEXT COMMENT 'Match referee (cleaned)',
    reserve_umpires TEXT COMMENT 'Reserve umpires (cleaned)',
    tv_umpires TEXT COMMENT 'TV umpires (cleaned)',
    first_umpire TEXT COMMENT 'First umpire (cleaned)',
    second_umpire TEXT COMMENT 'Second umpire (cleaned)',
    toss_winner TEXT COMMENT 'Toss winner (cleaned)',
    toss_decision TEXT COMMENT 'Toss decision (cleaned)',
    
    stg_file_name TEXT COMMENT 'Staging file name',
    stg_file_row_number INT COMMENT 'Staging file row number',
    stg_file_hashkey TEXT COMMENT 'Staging file hash key',
    stg_modified_ts TIMESTAMP_NTZ COMMENT 'Staging modified timestamp',
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Bronze ingestion timestamp (passthrough)',
    silver_load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Silver load timestamp',
    source_system VARCHAR DEFAULT 'CRICKET_SILVER' COMMENT 'Source system identifier',
    data_quality_status VARCHAR DEFAULT 'VALID' COMMENT 'Data quality status (VALID/INVALID)',
    data_quality_score FLOAT DEFAULT 1.0 COMMENT 'Data quality score (0.0-1.0)',
    is_current_record BOOLEAN DEFAULT TRUE COMMENT 'SCD Type 2: Current record flag'
)
CLUSTER BY (match_type_number, event_date)
COMMENT 'Silver layer match clean table with incremental processing';

-- ============================================================================
-- 4. TASK DEFINITIONS - SILVER PIPELINE CHAIN
-- ============================================================================

-- Player Processing Task
CREATE OR REPLACE TASK cricket.silver.silver_player_task
WAREHOUSE = compute_wh
SCHEDULE = '15 MINUTES'
ON_ERROR = 'CONTINUE'
AS
DECLARE
    p_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    p_rows_affected INT := 0;
BEGIN
    MERGE INTO cricket.silver.player_clean tgt
    USING (
        SELECT
            p.match_type_number::INT AS match_type_number,
            CASE
                WHEN TRIM(COALESCE(REGEXP_REPLACE(p.country, '\\s+', ' '), '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(p.country, '\\s+', ' ')))
            END AS country,
            CASE
                WHEN TRIM(COALESCE(REGEXP_REPLACE(p.player_name, '\\s+', ' '), '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(p.player_name, '\\s+', ' ')))
            END AS player_name,
            
            p.stg_file_name,
            p.stg_file_hashkey,
            p.stg_modified_ts,
            p.ingestion_ts,
            CASE
                WHEN p.match_type_number IS NULL 
                     OR TRIM(COALESCE(REGEXP_REPLACE(p.country, '\\s+', ' '), '')) = ''
                     OR TRIM(COALESCE(REGEXP_REPLACE(p.player_name, '\\s+', ' '), '')) = ''
                THEN 'INVALID'
                ELSE 'VALID'
            END AS data_quality_status,
            CASE
                WHEN p.match_type_number IS NULL 
                     OR TRIM(COALESCE(REGEXP_REPLACE(p.country, '\\s+', ' '), '')) = ''
                     OR TRIM(COALESCE(REGEXP_REPLACE(p.player_name, '\\s+', ' '), '')) = ''
                THEN 0.5
                ELSE 1.0
            END AS data_quality_score
        FROM cricket.silver.bronze_player_stream p
        WHERE p.match_type_number IS NOT NULL
          AND METADATA$ACTION IN ('INSERT', 'INSERT_AND_ON_METADATA_UPDATE')
    ) src
    ON tgt.stg_file_hashkey = src.stg_file_hashkey
       AND tgt.stg_file_row_number = src.stg_file_row_number
       AND tgt.country = src.country
       AND tgt.player_name = src.player_name
    WHEN MATCHED AND src.stg_modified_ts > tgt.stg_modified_ts THEN
        UPDATE SET
            match_type_number = src.match_type_number,
            stg_modified_ts = src.stg_modified_ts,
            silver_load_ts = CURRENT_TIMESTAMP(),
            data_quality_status = src.data_quality_status,
            data_quality_score = src.data_quality_score,
            is_current_record = TRUE
    WHEN NOT MATCHED THEN
        INSERT (
            match_type_number, country, player_name, stg_file_name,
            stg_file_hashkey, stg_modified_ts, ingestion_ts, data_quality_status,
            data_quality_score, is_current_record
        )
        VALUES (
            src.match_type_number, src.country, src.player_name, src.stg_file_name,
            src.stg_file_hashkey, src.stg_modified_ts, src.ingestion_ts,
            src.data_quality_status, src.data_quality_score, TRUE
        );
    
    p_rows_affected := @@rowcount;
    INSERT INTO cricket.silver.pipeline_log (task_name, status, rows_processed, duration_seconds)
    VALUES ('silver_player_task', 'SUCCESS', p_rows_affected, DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO cricket.silver.pipeline_log (task_name, status, error_message, duration_seconds)
        VALUES ('silver_player_task', 'FAILED', SQLERRM(), DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
        RAISE;
END;

-- Delivery Processing Task
CREATE OR REPLACE TASK cricket.silver.silver_delivery_task
WAREHOUSE = compute_wh
AFTER cricket.silver.silver_player_task
ON_ERROR = 'CONTINUE'
AS
DECLARE
    p_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    p_rows_affected INT := 0;
BEGIN
    MERGE INTO cricket.silver.delivery_clean tgt
    USING (
        SELECT
            d.match_type_number::INT AS match_type_number,
            CASE
                WHEN TRIM(COALESCE(d.team_name::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.team_name::TEXT, '\\s+', ' ')))
            END AS team_name,
            COALESCE(TRY_TO_NUMBER(d.over::TEXT), 0) AS over,
            CASE
                WHEN TRIM(COALESCE(d.bowler::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.bowler::TEXT, '\\s+', ' ')))
            END AS bowler,
            CASE
                WHEN TRIM(COALESCE(d.batter::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.batter::TEXT, '\\s+', ' ')))
            END AS batter,
            CASE
                WHEN TRIM(COALESCE(d.non_striker::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.non_striker::TEXT, '\\s+', ' ')))
            END AS non_striker,
            COALESCE(TRY_TO_NUMBER(d.runs::TEXT), 0) AS runs,
            COALESCE(TRY_TO_NUMBER(d.extras::TEXT), 0) AS extras,
            CASE
                WHEN TRIM(COALESCE(d.extra_type::TEXT, '')) = '' THEN 'None'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.extra_type::TEXT, '\\s+', ' ')))
            END AS extra_type,
            COALESCE(TRY_TO_NUMBER(d.total::TEXT), 0) AS total,
            CASE
                WHEN TRIM(COALESCE(d.player_out::TEXT, '')) = '' THEN 'None'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.player_out::TEXT, '\\s+', ' ')))
            END AS player_out,
            CASE
                WHEN TRIM(COALESCE(d.player_out_kind::TEXT, '')) = '' THEN 'None'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.player_out_kind::TEXT, '\\s+', ' ')))
            END AS player_out_kind,
            CASE
                WHEN TRIM(COALESCE(d.player_out_fielder::TEXT, '')) = '' THEN 'None'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.player_out_fielder::TEXT, '\\s+', ' ')))
            END AS player_out_fielder,
            CASE
                WHEN TRIM(COALESCE(d.review_by_team::TEXT, '')) = '' THEN 'None'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.review_by_team::TEXT, '\\s+', ' ')))
            END AS review_by_team,
            CASE
                WHEN TRIM(COALESCE(d.review_decision::TEXT, '')) = '' THEN 'None'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.review_decision::TEXT, '\\s+', ' ')))
            END AS review_decision,
            CASE
                WHEN TRIM(COALESCE(d.review_type::TEXT, '')) = '' THEN 'None'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(d.review_type::TEXT, '\\s+', ' ')))
            END AS review_type,
            d.stg_file_name,
            d.stg_file_row_number,
            d.stg_file_hashkey,
            d.stg_modified_ts,
            d.ingestion_ts,
            CASE
                WHEN d.match_type_number IS NULL
                     OR TRIM(COALESCE(d.team_name::TEXT, '')) = ''
                     OR d.bowler IS NULL OR d.batter IS NULL
                THEN 'INVALID'
                ELSE 'VALID'
            END AS data_quality_status,
            CASE
                WHEN d.match_type_number IS NULL
                     OR TRIM(COALESCE(d.team_name::TEXT, '')) = ''
                     OR d.bowler IS NULL OR d.batter IS NULL
                THEN 0.5
                ELSE 1.0
            END AS data_quality_score
        FROM cricket.silver.bronze_delivery_stream d
        WHERE d.match_type_number IS NOT NULL
          AND METADATA$ACTION IN ('INSERT', 'INSERT_AND_ON_METADATA_UPDATE')
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
            non_striker = src.non_striker,
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
            silver_load_ts = CURRENT_TIMESTAMP(),
            data_quality_status = src.data_quality_status,
            data_quality_score = src.data_quality_score,
            is_current_record = TRUE
    WHEN NOT MATCHED THEN
        INSERT (
            match_type_number, team_name, over, bowler, batter, non_striker,
            runs, extras, extra_type, total, player_out, player_out_kind,
            player_out_fielder, review_by_team, review_decision, review_type,
            stg_file_name, stg_file_row_number, stg_file_hashkey, stg_modified_ts,
            ingestion_ts, data_quality_status, data_quality_score, is_current_record
        )
        VALUES (
            src.match_type_number, src.team_name, src.over, src.bowler, src.batter,
            src.non_striker, src.runs, src.extras, src.extra_type, src.total,
            src.player_out, src.player_out_kind, src.player_out_fielder,
            src.review_by_team, src.review_decision, src.review_type,
            src.stg_file_name, src.stg_file_row_number, src.stg_file_hashkey,
            src.stg_modified_ts, src.ingestion_ts, src.data_quality_status,
            src.data_quality_score, TRUE
        );
    
    p_rows_affected := @@rowcount;
    INSERT INTO cricket.silver.pipeline_log (task_name, status, rows_processed, duration_seconds)
    VALUES ('silver_delivery_task', 'SUCCESS', p_rows_affected, DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO cricket.silver.pipeline_log (task_name, status, error_message, duration_seconds)
        VALUES ('silver_delivery_task', 'FAILED', SQLERRM(), DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
        RAISE;
END;

-- Match Processing Task
CREATE OR REPLACE TASK cricket.silver.silver_match_task
WAREHOUSE = compute_wh
AFTER cricket.silver.silver_delivery_task
ON_ERROR = 'CONTINUE'
AS
DECLARE
    p_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    p_rows_affected INT := 0;
BEGIN
    MERGE INTO cricket.silver.match_clean tgt
    USING (
        SELECT
            m.match_type_number::INT AS match_type_number,
            CASE
                WHEN TRIM(COALESCE(m.event_name::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.event_name::TEXT, '\\s+', ' ')))
            END AS event_name,
            COALESCE(m.match_number::INT, 0) AS match_number,
            COALESCE(m.event_date::DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD')) AS event_date,
            DATE_PART('year', COALESCE(m.event_date::DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD'))) AS event_year,
            DATE_PART('month', COALESCE(m.event_date::DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD'))) AS event_month,
            DATE_PART('day', COALESCE(m.event_date::DATE, TO_DATE('1900-01-01', 'YYYY-MM-DD'))) AS event_day,
            CASE
                WHEN TRIM(COALESCE(m.match_type::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.match_type::TEXT, '\\s+', ' ')))
            END AS match_type,
            CASE
                WHEN TRIM(COALESCE(m.season::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.season::TEXT, '\\s+', ' ')))
            END AS season,
            CASE
                WHEN TRIM(COALESCE(m.team_type::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.team_type::TEXT, '\\s+', ' ')))
            END AS team_type,
            COALESCE(m.overs::INT, 0) AS overs,
            CASE
                WHEN TRIM(COALESCE(m.city::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.city::TEXT, '\\s+', ' ')))
            END AS city,
            CASE
                WHEN TRIM(COALESCE(m.venue::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.venue::TEXT, '\\s+', ' ')))
            END AS venue,
            CASE
                WHEN TRIM(COALESCE(m.gender::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.gender::TEXT, '\\s+', ' ')))
            END AS gender,
            CASE
                WHEN TRIM(COALESCE(m.first_team::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.first_team::TEXT, '\\s+', ' ')))
            END AS first_team,
            CASE
                WHEN TRIM(COALESCE(m.second_team::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.second_team::TEXT, '\\s+', ' ')))
            END AS second_team,
            CASE
                WHEN TRIM(COALESCE(m.winner::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.winner::TEXT, '\\s+', ' ')))
            END AS winner,
            COALESCE(m.won_by_runs::INT, 0) AS won_by_runs,
            COALESCE(m.won_by_wickets::INT, 0) AS won_by_wickets,
            CASE
                WHEN TRIM(COALESCE(m.player_of_match::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.player_of_match::TEXT, '\\s+', ' ')))
            END AS player_of_match,
            CASE
                WHEN TRIM(COALESCE(m.match_referee::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.match_referee::TEXT, '\\s+', ' ')))
            END AS match_referee,
            CASE
                WHEN TRIM(COALESCE(m.reserve_umpires::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.reserve_umpires::TEXT, '\\s+', ' ')))
            END AS reserve_umpires,
            CASE
                WHEN TRIM(COALESCE(m.tv_umpires::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.tv_umpires::TEXT, '\\s+', ' ')))
            END AS tv_umpires,
            CASE
                WHEN TRIM(COALESCE(m.first_umpire::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.first_umpire::TEXT, '\\s+', ' ')))
            END AS first_umpire,
            CASE
                WHEN TRIM(COALESCE(m.second_umpire::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.second_umpire::TEXT, '\\s+', ' ')))
            END AS second_umpire,
            CASE
                WHEN TRIM(COALESCE(m.toss_winner::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.toss_winner::TEXT, '\\s+', ' ')))
            END AS toss_winner,
            CASE
                WHEN TRIM(COALESCE(m.toss_decision::TEXT, '')) = '' THEN 'Unknown'
                ELSE INITCAP(TRIM(REGEXP_REPLACE(m.toss_decision::TEXT, '\\s+', ' ')))
            END AS toss_decision,
            m.stg_file_name,
            m.stg_file_row_number,
            m.stg_file_hashkey,
            m.stg_modified_ts,
            m.ingestion_ts,
            CASE
                WHEN m.match_type_number::INT IS NULL OR m.event_name IS NULL
                THEN 'INVALID'
                ELSE 'VALID'
            END AS data_quality_status,
            CASE
                WHEN m.match_type_number::INT IS NULL OR m.event_name IS NULL
                THEN 0.5
                ELSE 1.0
            END AS data_quality_score
        FROM cricket.silver.bronze_match_stream m
        WHERE m.match_type_number::INT IS NOT NULL
          AND METADATA$ACTION IN ('INSERT', 'INSERT_AND_ON_METADATA_UPDATE')
    ) src
    ON tgt.stg_file_hashkey = src.stg_file_hashkey
       AND tgt.stg_file_row_number = src.stg_file_row_number
    WHEN MATCHED AND src.stg_modified_ts > tgt.stg_modified_ts THEN
        UPDATE SET
            match_type_number = src.match_type_number,
            event_name = src.event_name,
            match_number = src.match_number,
            event_date = src.event_date,
            event_year = src.event_year,
            event_month = src.event_month,
            event_day = src.event_day,
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
            silver_load_ts = CURRENT_TIMESTAMP(),
            data_quality_status = src.data_quality_status,
            data_quality_score = src.data_quality_score,
            is_current_record = TRUE
    WHEN NOT MATCHED THEN
        INSERT (
            match_type_number, event_name, match_number, event_date, event_year,
            event_month, event_day, match_type, season, team_type, overs, city,
            venue, gender, first_team, second_team, winner, won_by_runs, won_by_wickets,
            player_of_match, match_referee, reserve_umpires, tv_umpires, first_umpire,
            second_umpire, toss_winner, toss_decision, stg_file_name, stg_file_row_number,
            stg_file_hashkey, stg_modified_ts, ingestion_ts, data_quality_status,
            data_quality_score, is_current_record
        )
        VALUES (
            src.match_type_number, src.event_name, src.match_number, src.event_date,
            src.event_year, src.event_month, src.event_day, src.match_type, src.season,
            src.team_type, src.overs, src.city, src.venue, src.gender, src.first_team,
            src.second_team, src.winner, src.won_by_runs, src.won_by_wickets,
            src.player_of_match, src.match_referee, src.reserve_umpires, src.tv_umpires,
            src.first_umpire, src.second_umpire, src.toss_winner, src.toss_decision,
            src.stg_file_name, src.stg_file_row_number, src.stg_file_hashkey,
            src.stg_modified_ts, src.ingestion_ts, src.data_quality_status,
            src.data_quality_score, TRUE
        );
    
    p_rows_affected := @@rowcount;
    INSERT INTO cricket.silver.pipeline_log (task_name, status, rows_processed, duration_seconds)
    VALUES ('silver_match_task', 'SUCCESS', p_rows_affected, DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO cricket.silver.pipeline_log (task_name, status, error_message, duration_seconds)
        VALUES ('silver_match_task', 'FAILED', SQLERRM(), DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
        RAISE;
END;

-- Validation & Monitoring Task
CREATE OR REPLACE TASK cricket.silver.silver_validation_task
WAREHOUSE = compute_wh
AFTER cricket.silver.silver_match_task
ON_ERROR = 'CONTINUE'
AS
DECLARE
    p_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    p_player_null_pct FLOAT;
    p_delivery_null_pct FLOAT;
    p_match_null_pct FLOAT;
BEGIN
    -- Check player table nulls in critical columns
    p_player_null_pct := (
        SELECT COALESCE(COUNT(*), 0) 
        FROM cricket.silver.player_clean 
        WHERE player_name IS NULL OR country IS NULL
    ) / NULLIF((SELECT COUNT(*) FROM cricket.silver.player_clean), 0);
    
    IF (COALESCE(p_player_null_pct, 0) > 0.01) THEN
        UPDATE cricket.silver.player_clean 
        SET data_quality_status = 'INVALID', data_quality_score = 0.3
        WHERE player_name IS NULL OR country IS NULL;
    END IF;
    
    -- Check delivery table nulls in critical columns
    p_delivery_null_pct := (
        SELECT COALESCE(COUNT(*), 0)
        FROM cricket.silver.delivery_clean
        WHERE team_name IS NULL OR bowler IS NULL OR batter IS NULL
    ) / NULLIF((SELECT COUNT(*) FROM cricket.silver.delivery_clean), 0);
    
    IF (COALESCE(p_delivery_null_pct, 0) > 0.01) THEN
        UPDATE cricket.silver.delivery_clean
        SET data_quality_status = 'INVALID', data_quality_score = 0.3
        WHERE team_name IS NULL OR bowler IS NULL OR batter IS NULL;
    END IF;
    
    -- Check match table nulls in critical columns
    p_match_null_pct := (
        SELECT COALESCE(COUNT(*), 0)
        FROM cricket.silver.match_clean
        WHERE event_name IS NULL
    ) / NULLIF((SELECT COUNT(*) FROM cricket.silver.match_clean), 0);
    
    IF (COALESCE(p_match_null_pct, 0) > 0.01) THEN
        UPDATE cricket.silver.match_clean
        SET data_quality_status = 'INVALID', data_quality_score = 0.3
        WHERE event_name IS NULL;
    END IF;
    
    INSERT INTO cricket.silver.pipeline_log (task_name, status, rows_processed, duration_seconds)
    VALUES ('silver_validation_task', 'SUCCESS', 0, DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO cricket.silver.pipeline_log (task_name, status, error_message, duration_seconds)
        VALUES ('silver_validation_task', 'FAILED', SQLERRM(), DATEDIFF(SECOND, p_start_ts, CURRENT_TIMESTAMP()));
        RAISE;
END;

-- ============================================================================
-- 5. MAINTENANCE - TIME TRAVEL & SETTINGS
-- ============================================================================

ALTER TABLE cricket.silver.player_clean SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER TABLE cricket.silver.delivery_clean SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER TABLE cricket.silver.match_clean SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER TABLE cricket.silver.pipeline_log SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- ============================================================================
-- 6. RESUME TASKS
-- ============================================================================

ALTER TASK cricket.silver.silver_player_task RESUME;
ALTER TASK cricket.silver.silver_delivery_task RESUME;
ALTER TASK cricket.silver.silver_match_task RESUME;
ALTER TASK cricket.silver.silver_validation_task RESUME;

COMMIT;

-- ============================================================================
-- 7. VERIFICATION QUERIES
-- ============================================================================

-- Show all tasks in silver schema
SHOW TASKS IN SCHEMA cricket.silver;

-- Check stream status
SELECT SYSTEM$STREAM_STATUS('cricket.silver.bronze_player_stream');
SELECT SYSTEM$STREAM_STATUS('cricket.silver.bronze_delivery_stream');
SELECT SYSTEM$STREAM_STATUS('cricket.silver.bronze_match_stream');

-- Data quality dashboard
SELECT 'player_clean' AS table_name, data_quality_status, COUNT(*) AS row_count
FROM cricket.silver.player_clean
GROUP BY data_quality_status
UNION ALL
SELECT 'delivery_clean', data_quality_status, COUNT(*)
FROM cricket.silver.delivery_clean
GROUP BY data_quality_status
UNION ALL
SELECT 'match_clean', data_quality_status, COUNT(*)
FROM cricket.silver.match_clean
GROUP BY data_quality_status;

-- Pipeline execution history
SELECT task_name, execution_ts, status, rows_processed, duration_seconds
FROM cricket.silver.pipeline_log
ORDER BY execution_ts DESC
LIMIT 20;

-- Row counts by table
SELECT 'player_clean' AS table_name, COUNT(*) AS row_count FROM cricket.silver.player_clean
UNION ALL
SELECT 'delivery_clean', COUNT(*) FROM cricket.silver.delivery_clean
UNION ALL
SELECT 'match_clean', COUNT(*) FROM cricket.silver.match_clean;

-- Data freshness check
SELECT 'player_clean' AS table_name, MAX(silver_load_ts) AS last_load
FROM cricket.silver.player_clean
UNION ALL
SELECT 'delivery_clean', MAX(silver_load_ts)
FROM cricket.silver.delivery_clean
UNION ALL
SELECT 'match_clean', MAX(silver_load_ts)
FROM cricket.silver.match_clean;