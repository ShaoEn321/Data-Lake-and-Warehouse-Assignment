-- =========================================================
-- (BADGER) CONTEXT & SESSION SETUP
-- =========================================================
USE ROLE training_role;
ALTER SESSION SET QUERY_TAG= '(BADGER)- Assignment: Working with External Table in Data Lake';

USE WAREHOUSE BADGER_WH;
ALTER WAREHOUSE BADGER_WH SET WAREHOUSE_SIZE = 'XSMALL';

USE DATABASE BADGER_DB;
USE SCHEMA RAW;

-- =========================================================
-- (BADGER) FILE FORMAT (use a UNIQUE name to avoid "in use" lock)
-- We UNLOAD with HEADER=TRUE, so we READ with SKIP_HEADER=1
-- =========================================================
CREATE OR REPLACE FILE FORMAT RAW.ONTIME_REPORTING_CSV_BADGER_V3
TYPE = CSV
SKIP_HEADER = 1
COMPRESSION = AUTO
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('\\N', 'NULL', '')
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- =========================================================
-- (BADGER) STORED PROCEDURE
-- Unloads filtered ONTIME_REPORTING data into:
--   /year=YYYY/quarter=Qn/month=m/
-- =========================================================
CREATE OR REPLACE PROCEDURE RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH
(
  FOLDER  VARCHAR,
  YEAR    VARCHAR,
  QUARTER VARCHAR,  -- pass "1".."4"
  MONTH   VARCHAR   -- pass "1".."12"
)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  var stage_path =
    "@RAW.DATALAKE_STAGE/" + FOLDER +
    "/year=" + YEAR +
    "/quarter=Q" + QUARTER +
    "/month=" + MONTH + "/";

  var file_prefix = YEAR + "Q" + QUARTER + "_M" + MONTH + "_OTP_";

  var copy_sql =
    "COPY INTO " + stage_path + file_prefix + " FROM ( " +
    "  SELECT * " +
    "  FROM RAW.ONTIME_REPORTING " +
    "  WHERE YEAR = " + YEAR + " " +
    "    AND QUARTER = " + QUARTER + " " +
    "    AND MONTH = " + MONTH + " " +
    ") " +
    "OVERWRITE=TRUE " +
    "HEADER=TRUE " +
    "FILE_FORMAT=(TYPE=CSV COMPRESSION=NONE FIELD_DELIMITER=',' " +
    "            SKIP_HEADER=0 NULL_IF=() FIELD_OPTIONALLY_ENCLOSED_BY='\"')";

  snowflake.execute({ sqlText: copy_sql });
  return 1;
$$;

-- =========================================================
-- (BADGER) DATA UNLOAD EXECUTION
-- Example: Q1 (Jan–Mar) across multiple years
-- =========================================================

-- ===== 2018 Q1 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2018','1','1');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2018','1','2');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2018','1','3');

-- ===== 2019 Q1 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2019','1','1');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2019','1','2');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2019','1','3');

-- ===== 2020 Q1 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2020','1','1');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2020','1','2');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2020','1','3');

-- ===== 2021 Q1 =====
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2021','1','1');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2021','1','2');
CALL RAW.ASG_UNLOAD_ONTIME_REPORTING_CSV_BY_MONTH('BADGER_ASG','2021','1','3');

-- Confirm files exist (folder pattern year=/quarter=Q/ month=)
LIST @RAW.DATALAKE_STAGE/BADGER_ASG;

-- =========================================================
-- (BADGER) EXTERNAL TABLES
-- Put typed tables in CONFORMED (cleaner for marking)
-- =========================================================
USE SCHEMA CONFORMED;

-- ---------------------------------------------------------
-- 1) NO PARTITION (baseline full scan)
-- ---------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE CONFORMED.EXTERNAL_ONTIME_BADGER_NOPART (
  year          INT    AS (TRY_CAST(value:c1::string AS INT)),
  quarter       INT    AS (TRY_CAST(value:c2::string AS INT)),
  month         INT    AS (TRY_CAST(value:c3::string AS INT)),
  day_of_month  INT    AS (TRY_CAST(value:c4::string AS INT)),
  day_of_week   INT    AS (TRY_CAST(value:c5::string AS INT)),
  fl_date       DATE   AS (TRY_CAST(value:c6::string AS DATE)),
  op_carrier    STRING AS (value:c9::string),
  origin        STRING AS (value:c15::string),
  dest          STRING AS (value:c24::string),
  arr_delay     INT    AS (TRY_CAST(value:c43::string AS INT)),
  cancelled     INT    AS (TRY_CAST(value:c48::string AS INT))
)
LOCATION = @RAW.DATALAKE_STAGE/BADGER_ASG
FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_BADGER_V3');

ALTER EXTERNAL TABLE CONFORMED.EXTERNAL_ONTIME_BADGER_NOPART REFRESH;

-- ---------------------------------------------------------
-- 2) PARTITIONED (YEAR + QUARTER) from PATH (real pruning)
-- ---------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE CONFORMED.EXTERNAL_ONTIME_BADGER_PART (
    year         INT    AS (SPLIT_PART(SPLIT_PART(metadata$filename,'year=',2), '/', 1)::int),
    quarter      STRING AS (SPLIT_PART(SPLIT_PART(metadata$filename,'quarter=',2), '/', 1)),  -- Keep as STRING (e.g., "Q1")
    month        INT    AS (TRY_CAST(value:c3::string AS INT)),
    day_of_month INT    AS (TRY_CAST(value:c4::string AS INT)),
    day_of_week  INT    AS (TRY_CAST(value:c5::string AS INT)),
    fl_date      DATE   AS (TRY_CAST(value:c6::string AS DATE)),
    op_carrier   STRING AS (value:c9::string),
    origin       STRING AS (value:c15::string),
    dest         STRING AS (value:c24::string),
    arr_delay    INT    AS (TRY_CAST(value:c43::string AS INT)),
    cancelled    INT    AS (TRY_CAST(value:c48::string AS INT))
)
PARTITION BY (year, quarter)
LOCATION = @RAW.DATALAKE_STAGE/BADGER_ASG
FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_BADGER_V3');

ALTER EXTERNAL TABLE CONFORMED.EXTERNAL_ONTIME_BADGER_PART REFRESH;

-- ---------------------------------------------------------
-- 3) GRANULAR PARTITIONED (YEAR + QUARTER + MONTH) from PATH
-- This is the "real" bonus pruning
-- ---------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE CONFORMED.EXTERNAL_ONTIME_BADGER_GRAN_PART (
    year         INT    AS (SPLIT_PART(SPLIT_PART(metadata$filename, 'year=', 2), '/', 1)::int),
    quarter      STRING AS (SPLIT_PART(SPLIT_PART(metadata$filename, 'quarter=', 2), '/', 1)),  -- Keep as STRING (e.g., "Q1")
    month        INT    AS (SPLIT_PART(SPLIT_PART(metadata$filename, 'month=', 2), '/', 1)::int),  -- Extract from folder path
    day_of_month INT    AS (TRY_CAST(value:c4::string as int)),
    day_of_week  INT    AS (TRY_CAST(value:c5::string as int)),
    fl_date      DATE   AS (TRY_CAST(value:c6::string as date)),
    op_carrier   STRING AS (value:c9::string),
    origin       STRING AS (value:c15::string),
    dest         STRING AS (value:c24::string),
    arr_delay    INT    AS (TRY_CAST(value:c43::string as int)),
    cancelled    INT    AS (TRY_CAST(value:c48::string as int))
)
PARTITION BY (year, quarter, month)
LOCATION = @RAW.DATALAKE_STAGE/BADGER_ASG
FILE_FORMAT = (FORMAT_NAME = 'RAW.ONTIME_REPORTING_CSV_BADGER_V3');

ALTER EXTERNAL TABLE CONFORMED.EXTERNAL_ONTIME_BADGER_GRAN_PART REFRESH;

-- Quick validation
SELECT COUNT(*) FROM CONFORMED.EXTERNAL_ONTIME_BADGER_NOPART;
SELECT COUNT(*) FROM CONFORMED.EXTERNAL_ONTIME_BADGER_PART;
SELECT COUNT(*) FROM CONFORMED.EXTERNAL_ONTIME_BADGER_GRAN_PART;



-- =========================================================
-- (BADGER) PERFORMANCE COMPARISON QUERIES (Partition Evidence)
-- IMPORTANT: semicolons + cached results off
-- =========================================================
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Query A: No partition (scans all 12 months)
SELECT year, quarter, month, origin, dest, op_carrier, arr_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_NOPART
WHERE year = 2019 AND month = 2 AND cancelled = 0;

-- Query B: Year+Quarter partition (scans 3 months of Q1 2019)
SELECT year, quarter, month, origin, dest, op_carrier, arr_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_PART
WHERE year = 2019 AND quarter = 'Q1' AND month = 2 AND cancelled = 0;

-- Query C: Year+Quarter+Month partition (scans ONLY February 2019) 
SELECT year, quarter, month, origin, dest, op_carrier, arr_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_GRAN_PART
WHERE year = 2019 AND quarter = 'Q1' AND month = 2 AND cancelled = 0;

-- Simple comparison queries for screenshots
SELECT COUNT(*), AVG(arr_delay) AS avg_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_NOPART
WHERE year = 2019 AND month = 1;

SELECT COUNT(*), AVG(arr_delay) AS avg_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_PART
WHERE year = 2019 AND quarter = 'Q1' AND month = 1;

SELECT COUNT(*), AVG(arr_delay) AS avg_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_GRAN_PART
WHERE year = 2019 AND quarter = 'Q1' AND month = 1;


-- =========================================================
-- (BADGER) MODELED LAYER — VIEW + MV (safe pattern)
-- MV often cannot be built directly on External Tables,
-- so we stage filtered data into an internal table first.
-- =========================================================
USE SCHEMA MODELED;

CREATE OR REPLACE VIEW MODELED.OTP_FY2019_SEA_MARKETS_BADGER_ASG
(year, quarter, op_carrier, dest, origin, arr_delay) AS
SELECT year, quarter, op_carrier, dest, origin, arr_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_PART
WHERE origin = 'SEA'
  AND dest IN ('SFO','LAX','ORD','JFK','SLC','HNL','DEN','BOS','IAH','ATL')
  AND cancelled = 0
  AND year = 2019
  AND quarter = 'Q1'  
  AND arr_delay IS NOT NULL;

-- Internal table staging (so MV is valid)
CREATE OR REPLACE TABLE MODELED.OTP_FY2019_SEA_MARKETS_BADGER_ASG_TBL AS
SELECT year, quarter, op_carrier, dest, origin, arr_delay
FROM CONFORMED.EXTERNAL_ONTIME_BADGER_PART
WHERE origin = 'SEA'
  AND dest IN ('SFO','LAX','ORD','JFK','SLC','HNL','DEN','BOS','IAH','ATL')
  AND cancelled = 0
  AND year = 2019
  AND quarter = 'Q1'  
  AND arr_delay IS NOT NULL;

CREATE OR REPLACE MATERIALIZED VIEW MODELED.AVG_OTP_FY2019_SEA_MARKETS_BADGER_ASG_MV AS
SELECT dest, op_carrier, AVG(arr_delay) AS avg_arr_delay
FROM MODELED.OTP_FY2019_SEA_MARKETS_BADGER_ASG_TBL
GROUP BY 1,2;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Compare (view)
SELECT dest, op_carrier, AVG(arr_delay) AS avg_arr_delay
FROM MODELED.OTP_FY2019_SEA_MARKETS_BADGER_ASG
GROUP BY 1,2
ORDER BY 1,2;

-- Compare (MV)
SELECT dest, op_carrier, avg_arr_delay
FROM MODELED.AVG_OTP_FY2019_SEA_MARKETS_BADGER_ASG_MV
ORDER BY 1,2;

-- =========================================================
-- (BADGER) CLEANUP
-- =========================================================
ALTER SESSION UNSET QUERY_TAG;
ALTER WAREHOUSE BADGER_WH SET WAREHOUSE_SIZE = 'XSMALL';
ALTER WAREHOUSE BADGER_WH SUSPEND;
