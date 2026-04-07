/*
==========================================================
📌 RAW LAYER - PRODUCTION GRADE
==========================================================
Purpose:
- Store raw JSON data as-is
- Maintain ingestion metadata
- Enable incremental & traceable loads
==========================================================
*/

----------------------------------------------------------
-- 🔹 STEP 1: SET CONTEXT
----------------------------------------------------------
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CRICKET;
USE SCHEMA CRICKET.RAW;

----------------------------------------------------------
-- 🔹 STEP 2: CREATE RAW TABLE (SAFE)
----------------------------------------------------------
CREATE TABLE IF NOT EXISTS MATCH_RAW_TBL (
    META OBJECT NOT NULL,
    INFO VARIANT NOT NULL,
    INNINGS ARRAY NOT NULL,

    -- File Metadata (Important for lineage)
    STG_FILE_NAME STRING NOT NULL,
    STG_FILE_ROW_NUMBER INT NOT NULL,
    STG_FILE_HASHKEY STRING NOT NULL,
    STG_MODIFIED_TS TIMESTAMP NOT NULL,

    -- Audit Columns
    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw table storing JSON cricket data with metadata for lineage and tracking';

----------------------------------------------------------
-- 🔹 STEP 3: COPY INTO (INCREMENTAL LOAD)
----------------------------------------------------------
/*
Key Improvements:
- Avoid duplicate loads using METADATA$FILE_CONTENT_KEY
- Load only new/unprocessed files
*/

COPY INTO MATCH_RAW_TBL
FROM (
    SELECT 
        t.$1:meta::OBJECT,
        t.$1:info::VARIANT,
        t.$1:innings::ARRAY,

        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$FILE_CONTENT_KEY,   -- unique hash
        METADATA$FILE_LAST_MODIFIED

    FROM @CRICKET.LAND.MY_STG/CRICKET/JSON
    (FILE_FORMAT => 'CRICKET.LAND.MY_JSON_FORMAT') t
)
ON_ERROR = 'CONTINUE'
FORCE = FALSE;   -- prevents reloading same files

----------------------------------------------------------
-- 🔹 STEP 4: OPTIONAL DEDUP CHECK (GOOD PRACTICE)
----------------------------------------------------------
/*
You can periodically check duplicates
*/
-- SELECT STG_FILE_HASHKEY, COUNT(*)
-- FROM MATCH_RAW_TBL
-- GROUP BY 1
-- HAVING COUNT(*) > 1;

----------------------------------------------------------
-- 🔹 STEP 5: VALIDATION QUERY
----------------------------------------------------------
SELECT COUNT(*) AS TOTAL_RECORDS FROM MATCH_RAW_TBL;

----------------------------------------------------------
-- ✅ END OF RAW LAYER
----------------------------------------------------------