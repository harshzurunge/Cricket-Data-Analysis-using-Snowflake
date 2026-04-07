/*
==========================================================
📌 GOLD LAYER - OPTIMIZED (VIEW-BASED ARCHITECTURE)
📌 PROJECT: ICC Men's Cricket World Cup 2023 Analysis
==========================================================

📌 PURPOSE:
- Provide clean, business-ready data
- Expose curated datasets for reporting (Power BI, dashboards)
- Keep structure simple (no star schema as per use case)

✅ FEATURES:
- Lightweight views (no data duplication)
- Standardized column selection
- Consistent naming conventions
- Derived columns for analytics
==========================================================
*/

----------------------------------------------------------
-- 🔹 STEP 1: SET CONTEXT
----------------------------------------------------------
USE DATABASE CRICKET;
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE SCHEMA IF NOT EXISTS CRICKET.GOLD;
USE SCHEMA CRICKET.GOLD;

----------------------------------------------------------
-- 🔹 STEP 2: PLAYER VIEW
----------------------------------------------------------
/*
Purpose:
- Provides cleaned player-level data
- Directly sourced from Silver layer
- Used for player analysis and filtering
*/

CREATE OR REPLACE VIEW PLAYER AS
SELECT
    MATCH_TYPE_NUMBER,
    COUNTRY,
    PLAYER_NAME
FROM CRICKET.SILVER.PLAYER_CLEAN;

----------------------------------------------------------
-- 🔹 STEP 3: DELIVERY VIEW
----------------------------------------------------------
/*
Purpose:
- Provides ball-by-ball level data
- Core dataset for most analytics
- Includes derived flags for easier reporting
*/

CREATE OR REPLACE VIEW DELIVERY AS
SELECT
    MATCH_TYPE_NUMBER,
    TEAM_NAME,
    OVER,
    BOWLER,
    BATTER,
    NON_STRIKER,

    RUNS,
    EXTRAS,
    TOTAL,
    EXTRA_TYPE,

    PLAYER_OUT,
    PLAYER_OUT_KIND,
    PLAYER_OUT_FIELDER,

    REVIEW_BY_TEAM,
    REVIEW_DECISION,
    REVIEW_TYPE,

FROM CRICKET.SILVER.DELIVERY_CLEAN;

----------------------------------------------------------
-- 🔹 STEP 4: MATCH VIEW
----------------------------------------------------------
/*
Purpose:
- Provides match-level metadata
- Includes derived date attributes for filtering
*/

CREATE OR REPLACE VIEW MATCH AS
SELECT
    MATCH_TYPE_NUMBER,
    EVENT_NAME,
    MATCH_NUMBER,
    EVENT_DATE,
    EVENT_YEAR,
    EVENT_MONTH,
    EVENT_DAY,

    MATCH_TYPE,
    SEASON,
    TEAM_TYPE,
    OVERS,

    CITY,
    VENUE,
    GENDER,

    FIRST_TEAM,
    SECOND_TEAM,
    WINNER,

    WON_BY_RUNS,
    WON_BY_WICKETS,

    PLAYER_OF_MATCH,

    MATCH_REFEREE,
    RESERVE_UMPIRES,
    TV_UMPIRES,
    FIRST_UMPIRE,
    SECOND_UMPIRE,

    TOSS_WINNER,
    TOSS_DECISION

FROM CRICKET.SILVER.MATCH_CLEAN;

----------------------------------------------------------
-- 🔹 STEP 6: PERFORMANCE OPTIMIZATION (OPTIONAL)
----------------------------------------------------------

-- If queries are heavy, you can convert views to materialized views
-- Example:

CREATE MATERIALIZED VIEW DELIVERY_MV AS
SELECT * FROM DELIVERY;

----------------------------------------------------------
-- ✅ END OF GOLD LAYER
----------------------------------------------------------




---------------------------------------------------------------------------
-- create or replace view cricket.gold.player as
-- select
--     match_type_number,
--     country,
--     player_name
-- from cricket.silver.player_clean p;

-- create or replace view cricket.gold.delivery as
-- select
--     match_type_number,
--     team_name,
--     over,
--     bowler,
--     batter,
--     non_striker,
--     runs,
--     extras,
--     extra_type,
--     total,
--     player_out,
--     player_out_kind,
--     player_out_fielder,
--     review_by_team,
--     review_decision,
--     review_type
-- from cricket.silver.delivery_clean d;


-- create or replace view cricket.gold.match as
-- select
--     match_type_number,
--     event_name,
--     match_number,
--     event_date,
--     event_year,
--     event_month,
--     event_day,
--     match_type,
--     season,
--     team_type,
--     overs,
--     city,
--     venue,
--     gender,
--     first_team,
--     second_team,
--     winner,
--     won_by_runs,
--     won_by_wickets,
--     player_of_match,
--     match_referee,
--     reserve_umpires,
--     tv_umpires,
--     first_umpire,
--     second_umpire,
--     toss_winner,
--     toss_decision
-- from cricket.silver.match_clean m;