# Data Lakehouse & Warehousing (DLW) Assignment — Part A + Part B (Snowflake)

This repository contains my **DLW assignment deliverables** implemented on **Snowflake**, covering:
- **Part A:** Semi-structured ingestion + curation pipeline (RAW → CONFORMED → MODELED)
- **Part B Q1:** External table + partition pruning performance
- **Part B Q2:** Unstructured PDF processing automation (UDFs / Streams / Tasks)

> Note: This repo focuses on **submission artifacts (SQL + report docs)** rather than a full runnable app.

---

## 📌 Files in this repo

### Reports (documentation)
- `T01_ Ang Shao En_DLW Assignment_Part A.docx`
- `Ang Shao En DLW Assignment Part B.docx`

### SQL Scripts (implementation)
- `Ang Shao En DLW ASG - Part B Q1_Final.sql`
- `Ang Shao En DLW ASG – Part B Q2_Final Resubmission (1).sql`

---

## 🧱 Architecture Overview (High Level)

### Part A — Curated Dataset Pipeline
Typical flow:
1. **RAW**: land source files (JSON/CSV) into staging tables
2. **CONFORMED**: cleaned + standardized datasets (types, keys, null handling)
3. **MODELED**: analytics-ready views/tables for reporting (KPIs / dimensions)

Key concepts demonstrated:
- File formats + stages
- COPY / ingestion patterns
- Standardization & transformation logic
- Final modeled views for analytics queries

---

### Part B Q1 — External Table & Partition Pruning
Implemented and evaluated **external tables** (partitioned vs non-partitioned) to demonstrate:
- Query performance improvements using **partition pruning**
- Evidence via query history / query profile comparisons

Key concepts demonstrated:
- External stage & external tables
- Partitioning strategy (e.g., year/quarter/month)
- Performance comparison methodology

---

### Part B Q2 — Unstructured PDF Automation (Pipelines)
Built an automated pipeline for processing **PDF documents** into structured outputs, typically using:
- **Directory tables / stage listing**
- Metadata capture (file name, path, last modified)
- Python UDF for text extraction (e.g., PDF → extracted text)
- Streams + Tasks orchestration (DAG: ingest → transform → publish)

Key concepts demonstrated:
- Unstructured data processing (PDF)
- Automation with **Streams** and **Tasks**
- Final modeled views combining unstructured + structured data sources

---

## ▶️ How to Use / Review

### Option 1 (Recommended): Review as portfolio artifacts
1. Read the report docs for methodology, design decisions, and screenshots evidence.
2. Open the SQL scripts to see the complete implementation.

### Option 2: Run the SQL scripts in Snowflake (if you have the same environment)
> You may need to adjust **database/schema/warehouse/stage names** to match your account.

Suggested execution order:
1. Part A script(s) and setup (refer to `...Part A.docx`)
2. Run **Part B Q1** script:
   - `Ang Shao En DLW ASG - Part B Q1_Final.sql`
3. Run **Part B Q2** script:
   - `Ang Shao En DLW ASG – Part B Q2_Final.sql`

---

## 🧪 What to Look For (Good Screenshot/Evidence Checklist)

### Part A
- RAW tables populated
- CONFORMED transformations (types/keys)
- MODELED views working + example analytic queries

### Part B Q1
- External table definitions (partitioned + non-partitioned)
- Same query executed against both
- Query Profile / timing evidence showing pruning impact

### Part B Q2
- Stream capturing stage changes / file metadata
- Task graph running in order (T1 → T2 → T3 style)
- Extracted PDF text stored and used in modeled outputs

---

## 🛠 Tools & Skills
- Snowflake (Warehouses, Stages, File Formats, External Tables)
- SQL (DDL/DML, transformations, view modeling)
- Automation (Streams, Tasks, Stored Procedures where applicable)
- Unstructured data handling (PDF text extraction via Python UDF)

---
