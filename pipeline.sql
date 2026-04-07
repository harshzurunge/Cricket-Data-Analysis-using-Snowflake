/* 
==========================================================
📌 PRODUCTION PIPELINE - SNOWFLAKE (MEDALLION ARCHITECTURE)
RAW → BRONZE → SILVER → GOLD
==========================================================
*/

----------------------------------------------------------
-- 🔹 STEP 0: CONTEXT
----------------------------------------------------------
USE DATABASE CRICKET;
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

----------------------------------------------------------
-- 🔹 STEP 1: STREAMS
----------------------------------------------------------

-- RAW → BRONZE
CREATE OR REPLACE STREAM RAW_MATCH_STREAM 
ON TABLE CRICKET.RAW.MATCH_RAW_TBL 
APPEND_ONLY = TRUE;

-- BRONZE → SILVER
CREATE OR REPLACE STREAM BRONZE_PLAYER_STREAM 
ON TABLE CRICKET.BRONZE.PLAYER_TABLE;

CREATE OR REPLACE STREAM BRONZE_MATCH_STREAM 
ON TABLE CRICKET.BRONZE.MATCH_TABLE;

CREATE OR REPLACE STREAM BRONZE_DELIVERY_STREAM 
ON TABLE CRICKET.BRONZE.DELIVERY_TABLE;

----------------------------------------------------------
-- 🔹 STEP 2: LOAD RAW
----------------------------------------------------------
CREATE OR REPLACE TASK LOAD_RAW
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS
COPY INTO CRICKET.RAW.MATCH_RAW_TBL
FROM (
    SELECT 
        t.$1:meta::OBJECT,
        t.$1:info::VARIANT,
        t.$1:innings::ARRAY,

        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,
        METADATA$FILE_LAST_MODIFIED
    FROM @CRICKET.LAND.MY_STG/CRICKET/JSON
    (FILE_FORMAT => 'CRICKET.LAND.MY_JSON_FORMAT') t
)
ON_ERROR = 'CONTINUE'
FORCE = FALSE;

----------------------------------------------------------
-- 🔹 STEP 3: LOAD BRONZE
----------------------------------------------------------
CREATE OR REPLACE TASK LOAD_BRONZE
WAREHOUSE = COMPUTE_WH
AFTER LOAD_RAW
WHEN SYSTEM$STREAM_HAS_DATA('CRICKET.RAW.RAW_MATCH_STREAM')
AS

BEGIN

---------------------------
-- PLAYER TABLE
---------------------------
MERGE INTO CRICKET.BRONZE.PLAYER_TABLE tgt
USING (
    SELECT * FROM CRICKET.RAW.RAW_MATCH_STREAM
) raw,
LATERAL FLATTEN(input => raw.info:players) p,
LATERAL FLATTEN(input => p.value) team

ON tgt.stg_file_hashkey = raw.stg_file_hashkey
AND tgt.player_name = team.value::STRING

WHEN NOT MATCHED THEN INSERT VALUES (
    TRY_TO_NUMBER(raw.info:match_type_number::STRING),
    p.key::STRING,
    team.value::STRING,
    raw.stg_file_name,
    raw.stg_file_row_number,
    raw.stg_file_hashkey,
    raw.stg_modified_ts
);

---------------------------
-- MATCH TABLE
---------------------------
MERGE INTO CRICKET.BRONZE.MATCH_TABLE tgt
USING CRICKET.RAW.RAW_MATCH_STREAM src

ON tgt.stg_file_hashkey = src.stg_file_hashkey

WHEN NOT MATCHED THEN INSERT VALUES (
    TRY_TO_NUMBER(src.info:match_type_number::STRING),
    src.info:event.name::STRING,
    TRY_TO_NUMBER(src.info:event.match_number::STRING),
    TRY_TO_DATE(src.info:dates[0]::STRING),
    src.info:match_type::STRING,
    src.info:season::STRING,
    src.info:team_type::STRING,
    TRY_TO_NUMBER(src.info:overs::STRING),
    src.info:city::STRING,
    src.info:venue::STRING,
    src.info:gender::STRING,
    src.info:teams[0]::STRING,
    src.info:teams[1]::STRING,
    src.info:outcome.winner::STRING,
    TRY_TO_NUMBER(src.info:outcome.by.runs::STRING),
    TRY_TO_NUMBER(src.info:outcome.by.wickets::STRING),
    src.info:player_of_match[0]::STRING,
    src.info:officials.match_referees[0]::STRING,
    src.info:officials.reserve_umpires[0]::STRING,
    src.info:officials.tv_umpires[0]::STRING,
    src.info:officials.umpires[0]::STRING,
    src.info:officials.umpires[1]::STRING,
    src.info:toss.winner::STRING,
    src.info:toss.decision::STRING,
    src.stg_file_name,
    src.stg_file_row_number,
    src.stg_file_hashkey,
    src.stg_modified_ts
);

---------------------------
-- DELIVERY TABLE
---------------------------
MERGE INTO CRICKET.BRONZE.DELIVERY_TABLE tgt
USING (

    WITH BASE_RAW AS (
        SELECT * FROM CRICKET.RAW.RAW_MATCH_STREAM
    ),

    INNINGS_LEVEL AS (
        SELECT
            m.*,
            TRY_TO_NUMBER(m.info:match_type_number::STRING) AS MATCH_TYPE_NUMBER,
            i.value AS INNINGS_DATA
        FROM BASE_RAW m,
        LATERAL FLATTEN(input => m.innings) i
    ),

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

    DELIVERY_LEVEL AS (
        SELECT
            MATCH_TYPE_NUMBER,
            TEAM_NAME,
            TRY_TO_NUMBER(OVER_DATA:over::STRING) AS OVER,
            d.value AS DELIVERY_DATA,
            STG_FILE_NAME,
            STG_FILE_ROW_NUMBER,
            STG_FILE_HASHKEY,
            STG_MODIFIED_TS
        FROM OVERS_LEVEL,
        LATERAL FLATTEN(input => OVER_DATA:deliveries) d
    )

    SELECT
        MATCH_TYPE_NUMBER,
        TEAM_NAME,
        OVER,

        DELIVERY_DATA:bowler::STRING,
        DELIVERY_DATA:batter::STRING,
        DELIVERY_DATA:non_striker::STRING,

        TRY_TO_NUMBER(DELIVERY_DATA:runs.batter::STRING),
        TRY_TO_NUMBER(DELIVERY_DATA:runs.extras::STRING),
        TRY_TO_NUMBER(DELIVERY_DATA:runs.total::STRING),

        DELIVERY_DATA:review:by::STRING,
        DELIVERY_DATA:review:decision::STRING,
        DELIVERY_DATA:review:type::STRING,

        e.key::STRING,
        w.value:player_out::STRING,
        w.value:kind::STRING,
        f.value:name::STRING,

        STG_FILE_NAME,
        STG_FILE_ROW_NUMBER,
        STG_FILE_HASHKEY,
        STG_MODIFIED_TS

    FROM DELIVERY_LEVEL
    LEFT JOIN LATERAL FLATTEN(input => DELIVERY_DATA:extras, OUTER => TRUE) e
    LEFT JOIN LATERAL FLATTEN(input => DELIVERY_DATA:wickets, OUTER => TRUE) w
    LEFT JOIN LATERAL FLATTEN(input => w.value:fielders, OUTER => TRUE) f

) src

ON tgt.stg_file_hashkey = src.stg_file_hashkey
AND tgt.over = src.over
AND tgt.batter = src.batter

WHEN NOT MATCHED THEN INSERT VALUES (
    src.*
);

END;

----------------------------------------------------------
-- 🔹 STEP 4: SILVER TASKS
----------------------------------------------------------

CREATE OR REPLACE TASK LOAD_SILVER_PLAYER
WAREHOUSE = COMPUTE_WH
AFTER LOAD_BRONZE
WHEN SYSTEM$STREAM_HAS_DATA('CRICKET.BRONZE.BRONZE_PLAYER_STREAM')
AS
MERGE INTO CRICKET.SILVER.PLAYER_CLEAN tgt
USING CRICKET.BRONZE.BRONZE_PLAYER_STREAM src
ON tgt.stg_file_hashkey = src.stg_file_hashkey
AND tgt.player_name = src.player_name
WHEN NOT MATCHED THEN INSERT VALUES (
    src.match_type_number,
    CLEAN_TEXT(src.country,'Unknown'),
    CLEAN_TEXT(src.player_name,'Unknown'),
    src.stg_file_name,
    src.stg_file_hashkey,
    src.stg_modified_ts
);

----------------------------------------------------------

CREATE OR REPLACE TASK LOAD_SILVER_MATCH
WAREHOUSE = COMPUTE_WH
AFTER LOAD_BRONZE
WHEN SYSTEM$STREAM_HAS_DATA('CRICKET.BRONZE.BRONZE_MATCH_STREAM')
AS
MERGE INTO CRICKET.SILVER.MATCH_CLEAN tgt
USING CRICKET.BRONZE.BRONZE_MATCH_STREAM src
ON tgt.stg_file_hashkey = src.stg_file_hashkey
WHEN NOT MATCHED THEN INSERT VALUES (
    src.match_type_number,
    CLEAN_TEXT(src.event_name,'Unknown'),
    src.match_number,
    src.event_date,
    YEAR(src.event_date),
    MONTH(src.event_date),
    DAY(src.event_date),
    CLEAN_TEXT(src.match_type,'Unknown'),
    CLEAN_TEXT(src.season,'Unknown'),
    CLEAN_TEXT(src.team_type,'Unknown'),
    src.overs,
    CLEAN_TEXT(src.city,'Unknown'),
    CLEAN_TEXT(src.venue,'Unknown'),
    CLEAN_TEXT(src.gender,'Unknown'),
    CLEAN_TEXT(src.first_team,'Unknown'),
    CLEAN_TEXT(src.second_team,'Unknown'),
    CLEAN_TEXT(src.winner,'Unknown'),
    src.won_by_runs,
    src.won_by_wickets,
    CLEAN_TEXT(src.player_of_match,'Unknown'),
    CLEAN_TEXT(src.match_referee,'Unknown'),
    CLEAN_TEXT(src.reserve_umpires,'Unknown'),
    CLEAN_TEXT(src.tv_umpires,'Unknown'),
    CLEAN_TEXT(src.first_umpire,'Unknown'),
    CLEAN_TEXT(src.second_umpire,'Unknown'),
    CLEAN_TEXT(src.toss_winner,'Unknown'),
    CLEAN_TEXT(src.toss_decision,'Unknown'),
    src.stg_file_name,
    src.stg_file_row_number,
    src.stg_file_hashkey,
    src.stg_modified_ts
);

----------------------------------------------------------

CREATE OR REPLACE TASK LOAD_SILVER_DELIVERY
WAREHOUSE = COMPUTE_WH
AFTER LOAD_BRONZE
WHEN SYSTEM$STREAM_HAS_DATA('CRICKET.BRONZE.BRONZE_DELIVERY_STREAM')
AS
MERGE INTO CRICKET.SILVER.DELIVERY_CLEAN tgt
USING CRICKET.BRONZE.BRONZE_DELIVERY_STREAM src
ON tgt.stg_file_hashkey = src.stg_file_hashkey
AND tgt.over = src.over
AND tgt.batter = src.batter
WHEN NOT MATCHED THEN INSERT VALUES (
    src.match_type_number,
    CLEAN_TEXT(src.team_name,'Unknown'),
    src.over,
    CLEAN_TEXT(src.bowler,'Unknown'),
    CLEAN_TEXT(src.batter,'Unknown'),
    CLEAN_TEXT(src.non_striker,'Unknown'),
    src.runs,
    src.extras,
    src.total,
    CLEAN_TEXT(src.extra_type,'None'),
    CLEAN_TEXT(src.player_out,'None'),
    CLEAN_TEXT(src.player_out_kind,'None'),
    CLEAN_TEXT(src.player_out_fielder,'None'),
    CLEAN_TEXT(src.review_by_team,'None'),
    CLEAN_TEXT(src.review_decision,'None'),
    CLEAN_TEXT(src.review_type,'None'),
    src.stg_file_name,
    src.stg_file_row_number,
    src.stg_file_hashkey,
    src.stg_modified_ts
);

----------------------------------------------------------
-- 🔹 STEP 5: GOLD TASK
----------------------------------------------------------
CREATE OR REPLACE TASK LOAD_GOLD
WAREHOUSE = COMPUTE_WH
AFTER LOAD_SILVER_PLAYER, LOAD_SILVER_MATCH, LOAD_SILVER_DELIVERY
AS
ALTER MATERIALIZED VIEW CRICKET.GOLD.DELIVERY_SUMMARY_MV REFRESH;

----------------------------------------------------------
-- 🔹 STEP 6: ACTIVATE PIPELINE
----------------------------------------------------------
ALTER TASK LOAD_RAW RESUME;
ALTER TASK LOAD_BRONZE RESUME;
ALTER TASK LOAD_SILVER_PLAYER RESUME;
ALTER TASK LOAD_SILVER_MATCH RESUME;
ALTER TASK LOAD_SILVER_DELIVERY RESUME;
ALTER TASK LOAD_GOLD RESUME;