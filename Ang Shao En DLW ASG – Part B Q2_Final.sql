-- ============================================================
-- DLW Part B - Q2 (BADGER)  Practical-aligned pipeline
-- DIRECTORY TABLE + STAGE STREAM + PYTHON UDF + TASK DAG (t1->t2->t3)
-- ============================================================
USE ROLE TRAINING_ROLE;
USE DATABASE BADGER_DB;
USE SCHEMA RAW;
USE WAREHOUSE BADGER_WH;

-- Suspend tasks if they exist
ALTER TASK IF EXISTS RAW.BADGER_T3_BUILD_FINAL_VIEW SUSPEND;
ALTER TASK IF EXISTS RAW.BADGER_T3 SUSPEND;

ALTER TASK IF EXISTS RAW.BADGER_T2_PARSE_PDFS SUSPEND;
ALTER TASK IF EXISTS RAW.BADGER_T2 SUSPEND;

ALTER TASK IF EXISTS RAW.BADGER_T1_LOAD_PDF_METADATA SUSPEND;
ALTER TASK IF EXISTS RAW.BADGER_T1_REFRESH_PDF_STAGE SUSPEND;
ALTER TASK IF EXISTS RAW.BADGER_T1 SUSPEND;

DROP TASK IF EXISTS RAW.BADGER_T3_BUILD_FINAL_VIEW;
DROP TASK IF EXISTS RAW.BADGER_T3;

DROP TASK IF EXISTS RAW.BADGER_T2_PARSE_PDFS;
DROP TASK IF EXISTS RAW.BADGER_T2;

DROP TASK IF EXISTS RAW.BADGER_T1_LOAD_PDF_METADATA;
DROP TASK IF EXISTS RAW.BADGER_T1_REFRESH_PDF_STAGE;
DROP TASK IF EXISTS RAW.BADGER_T1;

DROP VIEW IF EXISTS RAW.BADGER_PDF_RAW_FILE_CATALOG_VW;
DROP VIEW IF EXISTS MODELED.BADGER_FINAL_INTEGRATED_VW;

USE SCHEMA MODELED;
DROP VIEW IF EXISTS MODELED.BADGER_FINAL_INTEGRATED_VW;
USE SCHEMA RAW;

DROP STREAM IF EXISTS RAW.BADGER_PDF_RAW_CATALOG_STREAM;
DROP STREAM IF EXISTS RAW.BADGER_PDF_STAGE_STREAM;

DROP TABLE IF EXISTS RAW.BADGER_PDF_PROCESSED_FILE_CATALOG;
DROP TABLE IF EXISTS RAW.BADGER_PDF_RAW_FILE_CATALOG;

DROP FUNCTION IF EXISTS RAW.BADGER_GET_PDF_TEXT(STRING);
DROP FUNCTION IF EXISTS RAW.BADGER_GET_PDF_PAYLOAD(STRING);

DROP STAGE IF EXISTS RAW.BADGER_PDF_INT_STAGE;
-- ============================================================
-- DLW Part B - Q2 (BADGER) Practical-aligned pipeline
-- DIRECTORY TABLE + STAGE STREAM + PYTHON UDF + TASK DAG (T1->T2->T3)
-- PLUS: Query Profile (WITH vs WITHOUT partitions) on final views
-- ============================================================

-- ----------------------------
-- 0) CONTEXT / SESSION SETUP
-- ----------------------------
USE ROLE TRAINING_ROLE;
USE DATABASE BADGER_DB;
USE SCHEMA RAW;
USE WAREHOUSE BADGER_WH;

ALTER SESSION SET QUERY_TAG =
'(BADGER)- Part B Q2: UDF + Stream + Tasks + Partition Performance Test';

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- ----------------------------
-- 1) STAGE + DIRECTORY + STREAM (ORDER MUST MATCH PRACTICAL)
-- ----------------------------
CREATE STAGE IF NOT EXISTS RAW.BADGER_PDF_INT_STAGE;

ALTER STAGE RAW.BADGER_PDF_INT_STAGE SET DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE STREAM RAW.BADGER_PDF_STAGE_STREAM
  ON STAGE RAW.BADGER_PDF_INT_STAGE;

ALTER STAGE RAW.BADGER_PDF_INT_STAGE REFRESH;

-- Checks (screenshots)
SELECT COUNT(*) AS pdfs
FROM DIRECTORY(@RAW.BADGER_PDF_INT_STAGE)
WHERE RELATIVE_PATH ILIKE '%.pdf';

SELECT *
FROM RAW.BADGER_PDF_STAGE_STREAM
WHERE METADATA$ACTION='INSERT'
ORDER BY RELATIVE_PATH;

-- ----------------------------
-- 2) RAW + PROCESSED TABLES
-- ----------------------------
CREATE OR REPLACE TRANSIENT TABLE RAW.BADGER_PDF_RAW_FILE_CATALOG (
  file_name     STRING,
  relative_path STRING,
  file_url      STRING,
  scoped_url    STRING,
  size          NUMBER,
  last_modified TIMESTAMP_LTZ,
  extracted     VARIANT,
  insert_ts     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TRANSIENT TABLE RAW.BADGER_PDF_PROCESSED_FILE_CATALOG (
  pdf_id         STRING,
  file_name      STRING,
  relative_path  STRING,
  file_url       STRING,
  last_modified  TIMESTAMP_LTZ,
  pdf_metadata   VARIANT,
  page_count     NUMBER,
  extracted_text STRING,
  insert_ts      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------
-- 3) PYTHON UDF (metadata + full text)
-- ----------------------------
CREATE OR REPLACE FUNCTION RAW.BADGER_GET_PDF_PAYLOAD(FILE_URL STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python','PyPDF2')
HANDLER = 'main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from PyPDF2 import PdfReader

def main(file_url: str):
    with SnowflakeFile.open(file_url, 'rb') as f:
        reader = PdfReader(f)

        meta_obj = {}
        try:
            m = reader.metadata
            if m:
                for k, v in m.items():
                    meta_obj[str(k)] = None if v is None else str(v)
        except Exception:
            meta_obj = {}

        pages = len(reader.pages)
        text_parts = []
        for i in range(pages):
            try:
                text_parts.append(reader.pages[i].extract_text() or "")
            except Exception:
                text_parts.append("")
        full_text = "\n".join(text_parts)

    return {
        "metadata": meta_obj,
        "page_count": pages,
        "chars": len(full_text),
        "text": full_text
    }
$$;

-- UDF test (screenshot)
SELECT
  RELATIVE_PATH,
  RAW.BADGER_GET_PDF_PAYLOAD(
    BUILD_SCOPED_FILE_URL(@RAW.BADGER_PDF_INT_STAGE, RELATIVE_PATH)
  ) AS parsed
FROM DIRECTORY(@RAW.BADGER_PDF_INT_STAGE)
WHERE RELATIVE_PATH ILIKE '%.pdf'
LIMIT 2;

-- ----------------------------
-- 4) VIEW for processed load
-- ----------------------------
CREATE OR REPLACE VIEW RAW.BADGER_PDF_RAW_FILE_CATALOG_VW AS
SELECT
  COALESCE(
    REGEXP_SUBSTR(relative_path, '^([^_]+)_', 1, 1, 'e', 1),
    REGEXP_REPLACE(relative_path, '\\.pdf$', '', 1, 1, 'i')
  ) AS pdf_id,
  file_name,
  relative_path,
  file_url,
  last_modified,
  extracted:metadata        AS pdf_metadata,
  extracted:page_count::INT AS page_count,
  extracted:text::STRING    AS extracted_text,
  insert_ts
FROM RAW.BADGER_PDF_RAW_FILE_CATALOG;

-- ----------------------------
-- 5) TASK DAG (T1->T2->T3)
-- ----------------------------

-- T1 (serverless): refresh stage
CREATE OR REPLACE TASK RAW.BADGER_T1
SCHEDULE = '1 minute'
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
ALTER STAGE RAW.BADGER_PDF_INT_STAGE REFRESH;

-- T2 (warehouse): load RAW using stream + directory + Python U=DF
CREATE OR REPLACE TASK RAW.BADGER_T2
WAREHOUSE = BADGER_WH
AFTER RAW.BADGER_T1
WHEN SYSTEM$STREAM_HAS_DATA('RAW.BADGER_PDF_STAGE_STREAM')
AS
INSERT INTO RAW.BADGER_PDF_RAW_FILE_CATALOG
(file_name, relative_path, file_url, scoped_url, size, last_modified, extracted)
SELECT
  id.relative_path AS file_name,
  id.relative_path,
  id.file_url,
  BUILD_SCOPED_FILE_URL(@RAW.BADGER_PDF_INT_STAGE, id.relative_path) AS scoped_url,
  id.size,
  id.last_modified::TIMESTAMP_LTZ,
  RAW.BADGER_GET_PDF_PAYLOAD(
    BUILD_SCOPED_FILE_URL(@RAW.BADGER_PDF_INT_STAGE, id.relative_path)
  ) AS extracted
FROM DIRECTORY(@RAW.BADGER_PDF_INT_STAGE) id
JOIN RAW.BADGER_PDF_STAGE_STREAM st
  ON id.relative_path = st.relative_path
WHERE st.METADATA$ACTION='INSERT'
  AND id.relative_path ILIKE '%.pdf';

-- T3 (serverless): load PROCESSED + build BOTH final views (PART vs NOPART)
CREATE OR REPLACE TASK RAW.BADGER_T3
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AFTER RAW.BADGER_T2
AS
BEGIN
  -- (A) Load processed
  INSERT INTO RAW.BADGER_PDF_PROCESSED_FILE_CATALOG
  (pdf_id, file_name, relative_path, file_url, last_modified, pdf_metadata, page_count, extracted_text)
  SELECT
    pdf_id, file_name, relative_path, file_url, last_modified,
    pdf_metadata, page_count, extracted_text
  FROM RAW.BADGER_PDF_RAW_FILE_CATALOG_VW;

  -- (B) Build schema
  CREATE SCHEMA IF NOT EXISTS MODELED;

  -- (C1) FINAL VIEW WITH partitions (good pruning)
  CREATE OR REPLACE VIEW MODELED.BADGER_FINAL_INTEGRATED_VW_PART AS
  WITH partb_stats AS (
    SELECT
      YEAR::INT AS year,
      QUARTER::STRING AS quarter,
      AVG(ARR_DELAY) AS avg_arr_delay,
      AVG(CANCELLED)::FLOAT AS cancel_rate
    FROM CONFORMED.EXTERNAL_ONTIME_REPORTING_BADGER_ASG_PART
    WHERE YEAR = 2019
      AND QUARTER = 'Q1'
    GROUP BY 1,2
  )
  SELECT
    c.PRODUCT_ID,
    c.BRAND,
    c.MODEL_NAME,
    c.CATEGORY,
    c.REVIEW_DATE,
    YEAR(c.REVIEW_DATE)  AS review_year,
    MONTH(c.REVIEW_DATE) AS review_month,
    DAY(c.REVIEW_DATE)   AS review_day,
    c.RATING,
    c.REVIEW_TEXT,
    c.REVIEWER_LOCATION_NAME,
    c.REVIEWER_GEO_POINT,

    p.pdf_id,
    p.file_name,
    p.relative_path,
    p.file_url,
    p.last_modified,
    p.pdf_metadata,
    p.page_count,
    p.extracted_text,

    s.avg_arr_delay,
    s.cancel_rate
  FROM RAW.BADGER_PDF_PROCESSED_FILE_CATALOG p
  LEFT JOIN DLW_GROUP1_DB.GROUP1_ASG.CURATED_PRODUCT_FEEDBACK c
    ON c.PRODUCT_ID = p.pdf_id
  LEFT JOIN partb_stats s
    ON 1=1;

  -- (C2) FINAL VIEW WITHOUT partitions (worse pruning)
  CREATE OR REPLACE VIEW MODELED.BADGER_FINAL_INTEGRATED_VW_NOPART AS
  WITH partb_stats AS (
    SELECT
      YEAR::INT AS year,
      QUARTER::STRING AS quarter,
      AVG(ARR_DELAY) AS avg_arr_delay,
      AVG(CANCELLED)::FLOAT AS cancel_rate
    FROM CONFORMED.EXTERNAL_ONTIME_REPORTING_BADGER_ASG_PART
    WHERE TO_NUMBER(VALUE:"c1") = 2019
      AND ('Q' || TO_VARCHAR(QUARTER(TO_DATE(VALUE:"c6")))) = 'Q1'
    GROUP BY 1,2
  )
  SELECT
    c.PRODUCT_ID,
    c.BRAND,
    c.MODEL_NAME,
    c.CATEGORY,
    c.REVIEW_DATE,
    YEAR(c.REVIEW_DATE)  AS review_year,
    MONTH(c.REVIEW_DATE) AS review_month,
    DAY(c.REVIEW_DATE)   AS review_day,
    c.RATING,
    c.REVIEW_TEXT,
    c.REVIEWER_LOCATION_NAME,
    c.REVIEWER_GEO_POINT,

    p.pdf_id,
    p.file_name,
    p.relative_path,
    p.file_url,
    p.last_modified,
    p.pdf_metadata,
    p.page_count,
    p.extracted_text,

    s.avg_arr_delay,
    s.cancel_rate
  FROM RAW.BADGER_PDF_PROCESSED_FILE_CATALOG p
  LEFT JOIN DLW_GROUP1_DB.GROUP1_ASG.CURATED_PRODUCT_FEEDBACK c
    ON c.PRODUCT_ID = p.pdf_id
  LEFT JOIN partb_stats s
    ON 1=1;

END;

-- ----------------------------
-- 6) RUN / TEST
-- ----------------------------
ALTER TASK RAW.BADGER_T2 RESUME;
ALTER TASK RAW.BADGER_T3 RESUME;

EXECUTE TASK RAW.BADGER_T1;

-- Task history (screenshot)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
  RESULT_LIMIT => 50
))
WHERE NAME ILIKE '%BADGER_T%'
ORDER BY COMPLETED_TIME DESC;

-- Task graph visualization
SELECT *
FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
WHERE ROOT_TASK_NAME ILIKE '%BADGER%';

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS())
WHERE ROOT_TASK_NAME ILIKE '%BADGER%'
ORDER BY COMPLETED_TIME DESC;

-- Validate loads
SELECT COUNT(*) AS raw_rows FROM RAW.BADGER_PDF_RAW_FILE_CATALOG;
SELECT COUNT(*) AS processed_rows FROM RAW.BADGER_PDF_PROCESSED_FILE_CATALOG;

-- ----------------------------
-- 7) PARTITION PERFORMANCE TEST (Query Profile screenshots)
-- Run these two queries separately and screenshot Query Profile for each
-- ----------------------------

-- Query A: WITH partitions
SELECT COUNT(*) AS rows_cnt, MIN(avg_arr_delay) AS min_delay, MAX(avg_arr_delay) AS max_delay
FROM MODELED.BADGER_FINAL_INTEGRATED_VW_PART;

-- Query B: WITHOUT partitions
SELECT COUNT(*) AS rows_cnt, MIN(avg_arr_delay) AS min_delay, MAX(avg_arr_delay) AS max_delay
FROM MODELED.BADGER_FINAL_INTEGRATED_VW_NOPART;

-- (Optional) show Part A filtering using your split columns
SELECT *
FROM MODELED.BADGER_FINAL_INTEGRATED_VW_PART
WHERE review_year IN (2024, 2025)
ORDER BY REVIEW_DATE DESC
LIMIT 50;

-- ----------------------------
-- 8) CLEANUP (avoid credits)
-- ----------------------------
ALTER TASK RAW.BADGER_T3 SUSPEND;
ALTER TASK RAW.BADGER_T2 SUSPEND;
ALTER TASK RAW.BADGER_T1 SUSPEND;

ALTER SESSION UNSET QUERY_TAG;
ALTER WAREHOUSE BADGER_WH SUSPEND;