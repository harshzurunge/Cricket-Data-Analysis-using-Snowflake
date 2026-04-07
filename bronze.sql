/* 
==========================================================
📌 BRONZE LAYER - OPTIMIZED JSON PARSING VERSION
==========================================================
*/

----------------------------------------------------------
-- 🔹 STEP 0: SET CONTEXT
----------------------------------------------------------
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CRICKET;

CREATE SCHEMA IF NOT EXISTS CRICKET.BRONZE;

----------------------------------------------------------
-- 🔹 STEP 1: STREAM
----------------------------------------------------------
USE SCHEMA CRICKET.RAW;

CREATE OR REPLACE STREAM MATCH_RAW_STREAM
ON TABLE MATCH_RAW_TBL
APPEND_ONLY = TRUE;

----------------------------------------------------------
-- 🔹 STEP 2: TABLES (same as before)
----------------------------------------------------------
USE SCHEMA CRICKET.BRONZE;

-- (Tables remain same — no change)

----------------------------------------------------------
-- 🔹 STEP 3: PLAYER TABLE (NO CHANGE)
----------------------------------------------------------
MERGE INTO CRICKET.BRONZE.PLAYER_TABLE tgt
USING (

    WITH BASE_RAW AS (
        SELECT * FROM CRICKET.RAW.MATCH_RAW_STREAM
    )

    SELECT
        TRY_TO_NUMBER(raw.info:match_type_number) AS MATCH_TYPE_NUMBER,
        p.key::STRING AS COUNTRY,
        team.value::STRING AS PLAYER_NAME,

        raw.STG_FILE_NAME,
        raw.STG_FILE_ROW_NUMBER,
        raw.STG_FILE_HASHKEY,
        raw.STG_MODIFIED_TS

    FROM BASE_RAW raw,
    LATERAL FLATTEN(input => raw.info:players) p,
    LATERAL FLATTEN(input => p.value) team

) src

ON tgt.STG_FILE_HASHKEY = src.STG_FILE_HASHKEY
AND tgt.PLAYER_NAME = src.PLAYER_NAME

WHEN NOT MATCHED THEN INSERT VALUES (
    src.MATCH_TYPE_NUMBER,
    src.COUNTRY,
    src.PLAYER_NAME,
    src.STG_FILE_NAME,
    src.STG_FILE_ROW_NUMBER,
    src.STG_FILE_HASHKEY,
    src.STG_MODIFIED_TS
);

----------------------------------------------------------
-- 🔹 STEP 4: DELIVERY TABLE (OPTIMIZED JSON PARSING)
----------------------------------------------------------
/*
📌 KEY IMPROVEMENT:
Instead of one big flatten chain,
we break parsing into logical layers:

RAW → INNINGS → OVERS → DELIVERIES
*/

MERGE INTO CRICKET.BRONZE.DELIVERY_TABLE tgt
USING (

    WITH BASE_RAW AS (
        SELECT * FROM CRICKET.RAW.MATCH_RAW_STREAM
    ),

    -- 🔹 LEVEL 1: INNINGS
    INNINGS_LEVEL AS (
        SELECT
            m.*,
            TRY_TO_NUMBER(m.info:match_type_number) AS MATCH_TYPE_NUMBER,
            i.value AS INNINGS_DATA
        FROM BASE_RAW m,
        LATERAL FLATTEN(input => m.innings) i
    ),

    -- 🔹 LEVEL 2: OVERS
    OVERS_LEVEL AS (
        SELECT
            MATCH_TYPE_NUMBER,
            INNINGS_DATA:value:team::STRING AS TEAM_NAME,
            o.value AS OVER_DATA,
            STG_FILE_NAME,
            STG_FILE_ROW_NUMBER,
            STG_FILE_HASHKEY,
            STG_MODIFIED_TS
        FROM INNINGS_LEVEL,
        LATERAL FLATTEN(input => INNINGS_DATA:value:overs) o
    ),

    -- 🔹 LEVEL 3: DELIVERIES
    DELIVERY_LEVEL AS (
        SELECT
            MATCH_TYPE_NUMBER,
            TEAM_NAME,
            TRY_TO_NUMBER(OVER_DATA:over) AS OVER,
            d.value AS DELIVERY_DATA,

            STG_FILE_NAME,
            STG_FILE_ROW_NUMBER,
            STG_FILE_HASHKEY,
            STG_MODIFIED_TS

        FROM OVERS_LEVEL,
        LATERAL FLATTEN(input => OVER_DATA:deliveries) d
    )

    -- 🔹 FINAL SELECT
    SELECT
        MATCH_TYPE_NUMBER,
        TEAM_NAME,
        OVER,

        DELIVERY_DATA:bowler::STRING AS BOWLER,
        DELIVERY_DATA:batter::STRING AS BATTER,
        DELIVERY_DATA:non_striker::STRING AS NON_STRIKER,

        TRY_TO_NUMBER(DELIVERY_DATA:runs.batter) AS RUNS,
        TRY_TO_NUMBER(DELIVERY_DATA:runs.extras) AS EXTRAS,
        TRY_TO_NUMBER(DELIVERY_DATA:runs.total) AS TOTAL,

        DELIVERY_DATA:review:by::STRING AS REVIEW_BY_TEAM,
        DELIVERY_DATA:review:decision::STRING AS REVIEW_DECISION,
        DELIVERY_DATA:review:type::STRING AS REVIEW_TYPE,

        e.key::STRING AS EXTRA_TYPE,
        w.value:player_out::STRING AS PLAYER_OUT,
        w.value:kind::STRING AS PLAYER_OUT_KIND,
        f.value:name::STRING AS PLAYER_OUT_FIELDER,

        STG_FILE_NAME,
        STG_FILE_ROW_NUMBER,
        STG_FILE_HASHKEY,
        STG_MODIFIED_TS

    FROM DELIVERY_LEVEL
    LEFT JOIN LATERAL FLATTEN(input => DELIVERY_DATA:extras, OUTER => TRUE) e
    LEFT JOIN LATERAL FLATTEN(input => DELIVERY_DATA:wickets, OUTER => TRUE) w
    LEFT JOIN LATERAL FLATTEN(input => w.value:fielders, OUTER => TRUE) f

) src

ON tgt.STG_FILE_HASHKEY = src.STG_FILE_HASHKEY
AND tgt.OVER = src.OVER
AND tgt.BATTER = src.BATTER

WHEN NOT MATCHED THEN INSERT VALUES (
    src.MATCH_TYPE_NUMBER,
    src.TEAM_NAME,
    src.OVER,
    src.BOWLER,
    src.BATTER,
    src.NON_STRIKER,
    src.RUNS,
    src.EXTRAS,
    src.TOTAL,
    src.REVIEW_BY_TEAM,
    src.REVIEW_DECISION,
    src.REVIEW_TYPE,
    src.EXTRA_TYPE,
    src.PLAYER_OUT,
    src.PLAYER_OUT_KIND,
    src.PLAYER_OUT_FIELDER,
    src.STG_FILE_NAME,
    src.STG_FILE_ROW_NUMBER,
    src.STG_FILE_HASHKEY,
    src.STG_MODIFIED_TS
);

----------------------------------------------------------
-- 🔹 STEP 5: MATCH TABLE (NO CHANGE)
----------------------------------------------------------
MERGE INTO CRICKET.BRONZE.MATCH_TABLE tgt
USING (

    WITH BASE_RAW AS (
        SELECT * FROM CRICKET.RAW.MATCH_RAW_STREAM
    )

    SELECT
        TRY_TO_NUMBER(info:match_type_number) AS MATCH_TYPE_NUMBER,
        info:event.name::STRING AS EVENT_NAME,
        TRY_TO_NUMBER(info:event.match_number) AS MATCH_NUMBER,
        TRY_TO_DATE(info:dates[0]) AS EVENT_DATE,
        info:match_type::STRING AS MATCH_TYPE,
        info:season::STRING AS SEASON,
        info:team_type::STRING AS TEAM_TYPE,
        TRY_TO_NUMBER(info:overs) AS OVERS,
        info:city::STRING AS CITY,
        info:venue::STRING AS VENUE,
        info:gender::STRING AS GENDER,
        info:teams[0]::STRING AS FIRST_TEAM,
        info:teams[1]::STRING AS SECOND_TEAM,

        info:outcome.winner::STRING AS WINNER,
        TRY_TO_NUMBER(info:outcome.by.runs) AS WON_BY_RUNS,
        TRY_TO_NUMBER(info:outcome.by.wickets) AS WON_BY_WICKETS,
        info:player_of_match[0]::STRING AS PLAYER_OF_MATCH,

        info:officials.match_referees[0]::STRING AS MATCH_REFEREE,
        info:officials.reserve_umpires[0]::STRING AS RESERVE_UMPIRES,
        info:officials.tv_umpires[0]::STRING AS TV_UMPIRES,
        info:officials.umpires[0]::STRING AS FIRST_UMPIRE,
        info:officials.umpires[1]::STRING AS SECOND_UMPIRE,
        info:toss.winner::STRING AS TOSS_WINNER,
        info:toss.decision::STRING AS TOSS_DECISION,

        STG_FILE_NAME,
        STG_FILE_ROW_NUMBER,
        STG_FILE_HASHKEY,
        STG_MODIFIED_TS

    FROM BASE_RAW

) src

ON tgt.STG_FILE_HASHKEY = src.STG_FILE_HASHKEY

WHEN NOT MATCHED THEN INSERT VALUES (
    src.MATCH_TYPE_NUMBER,
    src.EVENT_NAME,
    src.MATCH_NUMBER,
    src.EVENT_DATE,
    src.MATCH_TYPE,
    src.SEASON,
    src.TEAM_TYPE,
    src.OVERS,
    src.CITY,
    src.VENUE,
    src.GENDER,
    src.FIRST_TEAM,
    src.SECOND_TEAM,
    src.WINNER,
    src.WON_BY_RUNS,
    src.WON_BY_WICKETS,
    src.PLAYER_OF_MATCH,
    src.MATCH_REFEREE,
    src.RESERVE_UMPIRES,
    src.TV_UMPIRES,
    src.FIRST_UMPIRE,
    src.SECOND_UMPIRE,
    src.TOSS_WINNER,
    src.TOSS_DECISION,
    src.STG_FILE_NAME,
    src.STG_FILE_ROW_NUMBER,
    src.STG_FILE_HASHKEY,
    src.STG_MODIFIED_TS
);

----------------------------------------------------------
-- 🔹 STEP 6: PERFORMANCE
----------------------------------------------------------
ALTER TABLE CRICKET.BRONZE.DELIVERY_TABLE 
CLUSTER BY (MATCH_TYPE_NUMBER, OVER);

----------------------------------------------------------
-- ✅ END
----------------------------------------------------------

-- use role sysadmin;
-- use warehouse compute_wh;
-- use database cricket;
-- use schema cricket.bronze;

-- -- Player_table
-- create or replace table cricket.bronze.player_table as 
-- select
--     raw.info:match_type_number::int as match_type_number,
--     p.key::text as country,
--     team.value::text as player_name,

--     raw.stg_file_name ,
--     raw.stg_file_row_number,
--     raw.stg_file_hashkey,
--     raw.stg_modified_ts
-- from cricket.raw.match_raw_tbl raw,
-- lateral flatten (input => raw.info:players) p,
-- lateral flatten (input => p.value) team;

-- select * from cricket.bronze.player_table;

-- -------------------------------------------------------------------------
-- -- delivery_table

-- create or replace table cricket.bronze.delivery_table as
-- select
--     m.info:match_type_number::int as match_type_number,
--     i.value:team::text as team_name,
--     o.value:over::int as over,
--     d.value:bowler::text as bowler,
--     d.value:batter::text as batter,
--     d.value:non_striker::text as non_striker,
--     d.value:runs.batter::text as runs,
--     d.value:runs.extras::text as extras,
--     d.value:runs.total::text as total,

--     d.value:review:by::text as review_by_team,
--     d.value:review:decision::text as review_decision,
--     d.value:review:type::text as review_type,

--     e.key::text as extra_type,
--     w.value:player_out::text as player_out,
--     w.value:kind::text as player_out_kind,
--     f.value:name::text as player_out_fielder,

--     m.stg_file_name ,
--     m.stg_file_row_number,
--     m.stg_file_hashkey,
--     m.stg_modified_ts
-- from cricket.raw.match_raw_tbl m,
-- lateral flatten (input => m.innings) i,
-- lateral flatten (input => i.value:overs) o,
-- lateral flatten(input => o.value:deliveries) d,
-- lateral flatten (input => d.value:extras, outer => True) e,
-- lateral flatten (input => d.value:wickets, outer => True) w,
-- lateral flatten (input => w.value:fielders, outer => True) f;

-- -------------------------------------------------------------------------
-- -- match_table
-- create or replace transient table cricket.bronze.match_table as
-- select
--     info:match_type_number::int as match_type_number, 
--     info:event.name::text as event_name,

--     info:event.match_number :: int as match_number, 
--     info:dates[0]::date as event_date,
--     info:match_type::text as match_type,
--     info:season::text as season,
--     info:team_type::text as team_type,
--     info:overs::int as overs,
--     info:city::text as city,
--     info:venue::text as venue, 
--     info:gender::text as gender,
--     info:teams[0]::text as first_team,
--     info:teams[1]::text as second_team,

--     info:outcome.winner::text as winner,
--     info:outcome.by.runs::int as won_by_runs,
--     info:outcome.by.wickets::int as won_by_wickets,
--     info:player_of_match[0]::text as player_of_match,

--     info:officials.match_referees[0]::text as match_referee,
--     info:officials.reserve_umpires[0]::text as reserve_umpires,
--     info:officials.tv_umpires[0]::text as tv_umpires,
--     info:officials.umpires[0]::text as first_umpire,
--     info:officials.umpires[1]::text as second_umpire,
--     info:toss.winner::text as toss_winner,
--     info:toss.decision::text as toss_decision,

--     stg_file_name ,
--     stg_file_row_number,
--     stg_file_hashkey,
--     stg_modified_ts
-- from 
--     cricket.raw.match_raw_tbl;
