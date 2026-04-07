/*
==========================================================
📌 SILVER LAYER - PRODUCTION GRADE IMPLEMENTATION
📌 PROJECT: ICC Men's Cricket World Cup 2023 Analysis
📌 PURPOSE:
    - Clean and standardize Bronze data
    - Handle nulls, formatting issues
    - Prepare data for analytics (Gold layer)
==========================================================

✅ Key Features:
- Reusable UDF (eliminates repetitive cleaning logic)
- Incremental loading using MERGE
- Standardized null handling (Unknown / None / 0)
- Derived columns for analytics
==========================================================
*/

----------------------------------------------------------
-- 🔹 STEP 1: SET CONTEXT
----------------------------------------------------------
USE DATABASE CRICKET;
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS CRICKET.SILVER;
USE SCHEMA CRICKET.SILVER;

----------------------------------------------------------
-- 🔹 STEP 2: CREATE COMMON CLEANING FUNCTION
----------------------------------------------------------
/*
This function:
- Removes extra spaces
- Handles NULL / empty values
- Applies proper casing
- Replaces blanks with default values
*/

CREATE OR REPLACE FUNCTION CLEAN_TEXT(input STRING, default_val STRING)
RETURNS STRING
AS
$$
    CASE 
        WHEN TRIM(COALESCE(REGEXP_REPLACE(input, '\\s+', ' '), '')) = '' 
        THEN default_val
        ELSE INITCAP(TRIM(REGEXP_REPLACE(input, '\\s+', ' ')))
    END
$$;

----------------------------------------------------------
-- 🔹 STEP 3: CREATE TARGET TABLES (SAFE)
----------------------------------------------------------

-- PLAYER CLEAN TABLE
CREATE TABLE IF NOT EXISTS PLAYER_CLEAN (
    MATCH_TYPE_NUMBER INT,
    COUNTRY STRING,
    PLAYER_NAME STRING,
    STG_FILE_NAME STRING,
    STG_FILE_HASHKEY STRING,
    STG_MODIFIED_TS TIMESTAMP
);

-- DELIVERY CLEAN TABLE
CREATE TABLE IF NOT EXISTS DELIVERY_CLEAN (
    MATCH_TYPE_NUMBER INT,
    TEAM_NAME STRING,
    OVER INT,
    BOWLER STRING,
    BATTER STRING,
    NON_STRIKER STRING,
    RUNS INT,
    EXTRAS INT,
    TOTAL INT,
    EXTRA_TYPE STRING,
    PLAYER_OUT STRING,
    PLAYER_OUT_KIND STRING,
    PLAYER_OUT_FIELDER STRING,
    REVIEW_BY_TEAM STRING,
    REVIEW_DECISION STRING,
    REVIEW_TYPE STRING,
    STG_FILE_NAME STRING,
    STG_FILE_ROW_NUMBER INT,
    STG_FILE_HASHKEY STRING,
    STG_MODIFIED_TS TIMESTAMP
);

-- MATCH CLEAN TABLE
CREATE TABLE IF NOT EXISTS MATCH_CLEAN (
    MATCH_TYPE_NUMBER INT,
    EVENT_NAME STRING,
    MATCH_NUMBER INT,
    EVENT_DATE DATE,
    EVENT_YEAR INT,
    EVENT_MONTH INT,
    EVENT_DAY INT,
    MATCH_TYPE STRING,
    SEASON STRING,
    TEAM_TYPE STRING,
    OVERS INT,
    CITY STRING,
    VENUE STRING,
    GENDER STRING,
    FIRST_TEAM STRING,
    SECOND_TEAM STRING,
    WINNER STRING,
    WON_BY_RUNS INT,
    WON_BY_WICKETS INT,
    PLAYER_OF_MATCH STRING,
    MATCH_REFEREE STRING,
    RESERVE_UMPIRES STRING,
    TV_UMPIRES STRING,
    FIRST_UMPIRE STRING,
    SECOND_UMPIRE STRING,
    TOSS_WINNER STRING,
    TOSS_DECISION STRING,
    STG_FILE_NAME STRING,
    STG_FILE_ROW_NUMBER INT,
    STG_FILE_HASHKEY STRING,
    STG_MODIFIED_TS TIMESTAMP
);

----------------------------------------------------------
-- 🔹 STEP 4: PLAYER CLEAN (INCREMENTAL LOAD)
----------------------------------------------------------
/*
- Cleans player names & country
- Uses UDF to avoid repetition
- Inserts only new records
*/

MERGE INTO PLAYER_CLEAN tgt
USING (
    SELECT
        p.match_type_number,
        CLEAN_TEXT(p.country, 'Unknown') AS country,
        CLEAN_TEXT(p.player_name, 'Unknown') AS player_name,
        p.stg_file_name,
        p.stg_file_hashkey,
        p.stg_modified_ts
    FROM CRICKET.BRONZE.PLAYER_TABLE p
    WHERE p.match_type_number IS NOT NULL
) src

ON tgt.stg_file_hashkey = src.stg_file_hashkey
AND tgt.player_name = src.player_name

WHEN NOT MATCHED THEN INSERT VALUES (
    src.match_type_number,
    src.country,
    src.player_name,
    src.stg_file_name,
    src.stg_file_hashkey,
    src.stg_modified_ts
);

----------------------------------------------------------
-- 🔹 STEP 5: DELIVERY CLEAN (INCREMENTAL LOAD)
----------------------------------------------------------
/*
- Cleans team, player names
- Handles null numeric values
- Standardizes extra & wicket info
*/

MERGE INTO DELIVERY_CLEAN tgt
USING (
    SELECT
        d.match_type_number,
        CLEAN_TEXT(d.team_name, 'Unknown') AS team_name,
        COALESCE(TRY_TO_NUMBER(d.over), 0) AS over,
        CLEAN_TEXT(d.bowler, 'Unknown') AS bowler,
        CLEAN_TEXT(d.batter, 'Unknown') AS batter,
        CLEAN_TEXT(d.non_striker, 'Unknown') AS non_striker,
        COALESCE(TRY_TO_NUMBER(d.runs), 0) AS runs,
        COALESCE(TRY_TO_NUMBER(d.extras), 0) AS extras,
        COALESCE(TRY_TO_NUMBER(d.total), 0) AS total,
        CLEAN_TEXT(d.extra_type, 'None') AS extra_type,
        CLEAN_TEXT(d.player_out, 'None') AS player_out,
        CLEAN_TEXT(d.player_out_kind, 'None') AS player_out_kind,
        CLEAN_TEXT(d.player_out_fielder, 'None') AS player_out_fielder,
        CLEAN_TEXT(d.review_by_team, 'None') AS review_by_team,
        CLEAN_TEXT(d.review_decision, 'None') AS review_decision,
        CLEAN_TEXT(d.review_type, 'None') AS review_type,
        d.stg_file_name,
        d.stg_file_row_number,
        d.stg_file_hashkey,
        d.stg_modified_ts
    FROM CRICKET.BRONZE.DELIVERY_TABLE d
    WHERE d.match_type_number IS NOT NULL
) src

ON tgt.stg_file_hashkey = src.stg_file_hashkey
AND tgt.over = src.over
AND tgt.batter = src.batter

WHEN NOT MATCHED THEN INSERT VALUES (
    src.match_type_number,
    src.team_name,
    src.over,
    src.bowler,
    src.batter,
    src.non_striker,
    src.runs,
    src.extras,
    src.total,
    src.extra_type,
    src.player_out,
    src.player_out_kind,
    src.player_out_fielder,
    src.review_by_team,
    src.review_decision,
    src.review_type,
    src.stg_file_name,
    src.stg_file_row_number,
    src.stg_file_hashkey,
    src.stg_modified_ts
);

----------------------------------------------------------
-- 🔹 STEP 6: MATCH CLEAN (INCREMENTAL LOAD)
----------------------------------------------------------
/*
- Cleans match-level data
- Creates derived date columns (year, month, day)
- Standardizes categorical values
*/

MERGE INTO MATCH_CLEAN tgt
USING (
    SELECT
        m.match_type_number,
        CLEAN_TEXT(m.event_name, 'Unknown') AS event_name,
        COALESCE(m.match_number, 0) AS match_number,

        COALESCE(m.event_date, TO_DATE('1900-01-01')) AS event_date,
        DATE_PART(YEAR, COALESCE(m.event_date, TO_DATE('1900-01-01'))) AS event_year,
        DATE_PART(MONTH, COALESCE(m.event_date, TO_DATE('1900-01-01'))) AS event_month,
        DATE_PART(DAY, COALESCE(m.event_date, TO_DATE('1900-01-01'))) AS event_day,

        CLEAN_TEXT(m.match_type, 'Unknown') AS match_type,
        CLEAN_TEXT(m.season, 'Unknown') AS season,
        CLEAN_TEXT(m.team_type, 'Unknown') AS team_type,
        COALESCE(m.overs, 0) AS overs,
        CLEAN_TEXT(m.city, 'Unknown') AS city,
        CLEAN_TEXT(m.venue, 'Unknown') AS venue,
        CLEAN_TEXT(m.gender, 'Unknown') AS gender,
        CLEAN_TEXT(m.first_team, 'Unknown') AS first_team,
        CLEAN_TEXT(m.second_team, 'Unknown') AS second_team,
        CLEAN_TEXT(m.winner, 'Unknown') AS winner,
        COALESCE(m.won_by_runs, 0) AS won_by_runs,
        COALESCE(m.won_by_wickets, 0) AS won_by_wickets,
        CLEAN_TEXT(m.player_of_match, 'Unknown') AS player_of_match,
        CLEAN_TEXT(m.match_referee, 'Unknown') AS match_referee,
        CLEAN_TEXT(m.reserve_umpires, 'Unknown') AS reserve_umpires,
        CLEAN_TEXT(m.tv_umpires, 'Unknown') AS tv_umpires,
        CLEAN_TEXT(m.first_umpire, 'Unknown') AS first_umpire,
        CLEAN_TEXT(m.second_umpire, 'Unknown') AS second_umpire,
        CLEAN_TEXT(m.toss_winner, 'Unknown') AS toss_winner,
        CLEAN_TEXT(m.toss_decision, 'Unknown') AS toss_decision,

        m.stg_file_name,
        m.stg_file_row_number,
        m.stg_file_hashkey,
        m.stg_modified_ts

    FROM CRICKET.BRONZE.MATCH_TABLE m
    WHERE m.match_type_number IS NOT NULL
) src

ON tgt.stg_file_hashkey = src.stg_file_hashkey

WHEN NOT MATCHED THEN INSERT VALUES (
    src.match_type_number,
    src.event_name,
    src.match_number,
    src.event_date,
    src.event_year,
    src.event_month,
    src.event_day,
    src.match_type,
    src.season,
    src.team_type,
    src.overs,
    src.city,
    src.venue,
    src.gender,
    src.first_team,
    src.second_team,
    src.winner,
    src.won_by_runs,
    src.won_by_wickets,
    src.player_of_match,
    src.match_referee,
    src.reserve_umpires,
    src.tv_umpires,
    src.first_umpire,
    src.second_umpire,
    src.toss_winner,
    src.toss_decision,
    src.stg_file_name,
    src.stg_file_row_number,
    src.stg_file_hashkey,
    src.stg_modified_ts
);

----------------------------------------------------------
-- ✅ END OF SILVER LAYER
----------------------------------------------------------
------------------------------------------------------------------------------------
-- use database cricket;
-- use role sysadmin;
-- use warehouse compute_wh;  

-- create or replace schema cricket.silver;
-- use schema cricket.silver;

-- --player_clean
-- create or replace table cricket.silver.player_clean as
-- select
--     p.match_type_number::int as match_type_number,
--     case
--         when trim(coalesce(regexp_replace(p.country, '\\s+', ' '), '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(p.country, '\\s+', ' ')))
--     end as country,
--     case
--         when trim(coalesce(regexp_replace(p.player_name, '\\s+', ' '), '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(p.player_name, '\\s+', ' ')))
--     end as player_name,

--     p.stg_file_name,
--     p.stg_file_hashkey,
--     p.stg_modified_ts
-- from cricket.bronze.player_table p
-- where p.match_type_number is not null;


-- -----------------------------------------------------------------------
-- -- delivery_clean

-- create or replace table cricket.silver.delivery_clean as
-- select
--     d.match_type_number::int as match_type_number,
--     case
--         when trim(coalesce(d.team_name::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(d.team_name::text, '\\s+', ' ')))
--     end as team_name,
--     coalesce(try_to_number(d.over::text), 0000) as over,
--     case
--         when trim(coalesce(d.bowler::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(d.bowler::text, '\\s+', ' ')))
--     end as bowler,
--     case
--         when trim(coalesce(d.batter::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(d.batter::text, '\\s+', ' ')))
--     end as batter,
--     case
--         when trim(coalesce(d.non_striker::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(d.non_striker::text, '\\s+', ' ')))
--     end as non_striker,
--     coalesce(try_to_number(d.runs::text), 0000) as runs,
--     coalesce(try_to_number(d.extras::text), 0000) as extras,
--     case
--         when trim(coalesce(d.extra_type::text, '')) = '' then 'None'
--         else initcap(trim(regexp_replace(d.extra_type::text, '\\s+', ' ')))
--     end as extra_type,
--     coalesce(try_to_number(d.total::text), 0000) as total,
--     case
--         when trim(coalesce(d.player_out::text, '')) = '' then 'None'
--         else initcap(trim(regexp_replace(d.player_out::text, '\\s+', ' ')))
--     end as player_out,
--     case
--         when trim(coalesce(d.player_out_kind::text, '')) = '' then 'None'
--         else initcap(trim(regexp_replace(d.player_out_kind::text, '\\s+', ' ')))
--     end as player_out_kind,
--     case
--         when trim(coalesce(d.player_out_fielder::text, '')) = '' then 'None'
--         else initcap(trim(regexp_replace(d.player_out_fielder::text, '\\s+', ' ')))
--     end as player_out_fielder,

--     case
--         when trim(coalesce(d.review_by_team::text, '')) = '' then 'None'
--         else initcap(trim(regexp_replace(d.review_by_team::text, '\\s+', ' ')))
--     end as review_by_team,
--     case
--         when trim(coalesce(d.review_decision::text, '')) = '' then 'None'
--         else initcap(trim(regexp_replace(d.review_decision::text, '\\s+', ' ')))
--     end as review_decision,
--     case
--         when trim(coalesce(d.review_type::text, '')) = '' then 'None'
--         else initcap(trim(regexp_replace(d.review_type::text, '\\s+', ' ')))
--     end as review_type,

--     d.stg_file_name ,
--     d.stg_file_row_number,
--     d.stg_file_hashkey,
--     d.stg_modified_ts
-- from cricket.bronze.delivery_table d
-- where d.match_type_number is not null;

-- --------------------------------------------------------------------------------
-- -- match_clean

-- create or replace table cricket.silver.match_clean as
-- select
--     m.match_type_number::int as match_type_number,
--     case
--         when trim(coalesce(m.event_name::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.event_name::text, '\\s+', ' ')))
--     end as event_name,
--     coalesce(m.match_number::int, 0000) as match_number,
--     coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD')) as event_date,
--     date_part('year', coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD'))) as event_year,
--     date_part('month', coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD'))) as event_month,
--     date_part('day', coalesce(m.event_date::date, to_date('1900-01-01', 'YYYY-MM-DD'))) as event_day,
--     case
--         when trim(coalesce(m.match_type::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.match_type::text, '\\s+', ' ')))
--     end as match_type,
--     case
--         when trim(coalesce(m.season::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.season::text, '\\s+', ' ')))
--     end as season,
--     case
--         when trim(coalesce(m.team_type::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.team_type::text, '\\s+', ' ')))
--     end as team_type,
--     coalesce(m.overs::int, 0000) as overs,
--     case
--         when trim(coalesce(m.city::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.city::text, '\\s+', ' ')))
--     end as city,
--     case
--         when trim(coalesce(m.venue::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.venue::text, '\\s+', ' ')))
--     end as venue,
--     case
--         when trim(coalesce(m.gender::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.gender::text, '\\s+', ' ')))
--     end as gender,
--     case
--         when trim(coalesce(m.first_team::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.first_team::text, '\\s+', ' ')))
--     end as first_team,
--     case
--         when trim(coalesce(m.second_team::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.second_team::text, '\\s+', ' ')))
--     end as second_team,
--     case
--         when trim(coalesce(m.winner::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.winner::text, '\\s+', ' ')))
--     end as winner,
--     coalesce(m.won_by_runs::int, 0000) as won_by_runs,
--     coalesce(m.won_by_wickets::int, 0000) as won_by_wickets,
--     case
--         when trim(coalesce(m.player_of_match::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.player_of_match::text, '\\s+', ' ')))
--     end as player_of_match,
--     case
--         when trim(coalesce(m.match_referee::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.match_referee::text, '\\s+', ' ')))
--     end as match_referee,
--     case
--         when trim(coalesce(m.reserve_umpires::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.reserve_umpires::text, '\\s+', ' ')))
--     end as reserve_umpires,
--     case
--         when trim(coalesce(m.tv_umpires::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.tv_umpires::text, '\\s+', ' ')))
--     end as tv_umpires,
--     case
--         when trim(coalesce(m.first_umpire::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.first_umpire::text, '\\s+', ' ')))
--     end as first_umpire,
--     case
--         when trim(coalesce(m.second_umpire::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.second_umpire::text, '\\s+', ' ')))
--     end as second_umpire,
--     case
--         when trim(coalesce(m.toss_winner::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.toss_winner::text, '\\s+', ' ')))
--     end as toss_winner,
--     case
--         when trim(coalesce(m.toss_decision::text, '')) = '' then 'Unknown'
--         else initcap(trim(regexp_replace(m.toss_decision::text, '\\s+', ' ')))
--     end as toss_decision,

--     m.stg_file_name,
--     m.stg_file_row_number,
--     m.stg_file_hashkey,
--     m.stg_modified_ts
-- from 
--     cricket.bronze.match_table m
-- where m.match_type_number::int is not null;
