USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CRICKET;
USE SCHEMA CRICKET.RAW;

----------------------------------------------------------
-- 🔹 STEP 2: CREATE STREAM ON RAW TABLE
----------------------------------------------------------
/*
Stream captures only NEW records from RAW table.
This enables incremental loading instead of full reload.
*/
USE SCHEMA CRICKET.RAW;

CREATE OR REPLACE STREAM MATCH_RAW_STREAM
ON TABLE MATCH_RAW_TBL
APPEND_ONLY = TRUE;