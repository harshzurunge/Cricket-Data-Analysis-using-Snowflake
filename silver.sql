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
-- 🔹 CLEANING FUNCTION (GOOD - NO CHANGE)
----------------------------------------------------------
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
-- 🔹 PLAYER CLEAN
----------------------------------------------------------
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
-- 🔹 DELIVERY CLEAN (FIXED)
----------------------------------------------------------
MERGE INTO DELIVERY_CLEAN tgt
USING (
    SELECT
        d.match_type_number,

        CLEAN_TEXT(d.team_name, 'Unknown') AS team_name,

        -- ✅ TRUST BRONZE BUT KEEP SAFE
        COALESCE(d.over, 0) AS over,

        CLEAN_TEXT(d.bowler, 'Unknown') AS bowler,
        CLEAN_TEXT(d.batter, 'Unknown') AS batter,
        CLEAN_TEXT(d.non_striker, 'Unknown') AS non_striker,

        COALESCE(d.runs, 0) AS runs,
        COALESCE(d.extras, 0) AS extras,
        COALESCE(d.total, 0) AS total,

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

-- ✅ STRONGER UNIQUE KEY
ON tgt.stg_file_hashkey = src.stg_file_hashkey
AND tgt.over = src.over
AND tgt.batter = src.batter
AND tgt.bowler = src.bowler

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
-- 🔹 MATCH CLEAN (FIXED)
----------------------------------------------------------
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
