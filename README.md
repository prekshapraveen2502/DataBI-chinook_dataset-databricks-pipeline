# DAMG 7370 — Chinook Data Engineering Pipeline
**Northeastern University | Spring 2026 | Team Project**

---

## Overview

This repository contains the implementation of an end-to-end data engineering pipeline for the Chinook music store dataset, built on Databricks. The pipeline follows a Medallion Architecture (Raw to Bronze to Silver to Gold) and is fully parameterized, metadata-driven, and orchestrated using Databricks Jobs.

---

## Architecture
```
Azure SQL Server (chinook_az_catalog)
        |
        v
workspace.staging        -- Delta, handoff layer
        |
        v
Raw Zone (Parquet)       -- /Volumes/workspace/raw_zone/chinook/<table>/<date>/
        |
        v
workspace.bronze         -- Delta, daily snapshot, no transformations
        |
        v
workspace.silver         -- Delta, DQX-validated and cleaned
        |
        v
workspace.gold           -- Delta, dimensional model (facts + dimensions)
```

---

## Repository Structure
```
DataBI/
├── setup/
    ├── 00_metadata_setup           -- One-time setup: creates metadata tables   
├── notebooks/
│   ├── 00_validation_screenshots   -- Validation queries for documentation
│   ├── 01_A_Get_Tables_List        -- Reads pipeline_control, passes table list to Job
│   ├── 01_B_Extract_Data_From_Source  -- Extracts one table from Azure SQL to staging
│   ├── 02_load_raw                 -- Writes Parquet snapshots to volume, logs to execution_log
│   ├── 03_raw_to_bronze            -- Loads Raw Parquet into Bronze Delta tables
│   ├── 04_bronze_to_silver         -- DQX validation + cleaning, writes to Silver
│   └── 05_silver_to_gold           -- Builds dimensional model in Gold
└── docs/
    └── Mapping_Document.xlsx       -- Source-to-target column mapping for Gold layer
```

---

## Environment Setup

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Azure SQL Server connection configured via Databricks Connection Manager
- GitHub repository linked to Databricks workspace

### Step 1 — Create Schemas and Volume (Manual, Databricks UI)

Under the `workspace` catalog, create the following schemas:

| Schema | Purpose |
|---|---|
| raw_zone | Contains the chinook volume for Parquet files |
| staging | Temporary Delta handoff layer |
| bronze | Daily Delta snapshot |
| silver | Cleaned and validated Delta tables |
| gold | Dimensional model (facts and dimensions) |
| metadata | Pipeline control and audit tables |

Inside `raw_zone`, create a Volume named `chinook`.

### Step 2 — Azure SQL Connection (Manual, Databricks UI)

1. Go to Catalog, click the gear icon, select Connections
2. Create a new SQL Server connection
3. Name it `azuresqlserverdb`
4. Reference source tables using: `chinook_az_catalog.chinook.<table_name>`

### Step 3 — Whitelist IP Address

Each team member must whitelist their Databricks serverless IP in the Azure SQL Server firewall. To find your IP, run the following in any notebook:
```python
import subprocess
result = subprocess.run(['curl', '-s', 'ifconfig.me'], capture_output=True, text=True)
print(result.stdout)
```

Then add this IP to Azure Portal under SQL Server, Networking, Firewall Rules.

### Step 4 — Run Metadata Setup

Open `notebooks/00_metadata_setup` and run all cells. This creates:

- `workspace.metadata.pipeline_control` -- 11 rows, one per Chinook table
- `workspace.metadata.execution_log` -- audit trail, populated at runtime
- `workspace.metadata.dqx_execution_log` -- DQX validation metrics per run

---

## Notebooks

### 01_A_Get_Tables_List
Reads `pipeline_control` filtered by `active_flag = 'Y'` and passes the table list to the Databricks Job via `dbutils.jobs.taskValues`. This drives all downstream For Each tasks.

### 01_B_Extract_Data_From_Source
Called once per table by the For Each task. Reads from the Azure SQL source via the Connection Manager and writes to a staging Delta table. No credentials are stored in the notebook.

### 02_load_raw
Reads from staging and writes an immutable Parquet snapshot to the volume using a dynamic dated path:
```
/Volumes/workspace/raw_zone/chinook/<table>/<YYYY-MM-DD>/<table>.parquet
```
Validates that `source_row_count == target_row_count` and logs every run to `execution_log`.

### 03_raw_to_bronze
Reads the latest successful Parquet file path from `execution_log` and writes to Bronze in overwrite mode. No transformations applied -- Bronze is an exact Delta copy of Raw.

### 04_bronze_to_silver
Installs `databricks-labs-dqx` at runtime. Applies per-table data quality rules (not null, range checks), splits records into passed and failed sets, writes failed records to a quarantine table, logs DQX metrics, applies string cleaning (trim, empty to null), and writes cleaned data to Silver.

### 05_silver_to_gold
Builds the Gold dimensional model from Silver tables:

| Table | Type | Notes |
|---|---|---|
| dim_artist | SCD Type 1 | Surrogate key via monotonically_increasing_id() |
| dim_genre | SCD Type 1 | Surrogate key via monotonically_increasing_id() |
| dim_mediatype | SCD Type 1 | Surrogate key via monotonically_increasing_id() |
| dim_employee | SCD Type 1 | Surrogate key via monotonically_increasing_id() |
| dim_customer | SCD Type 2 | Tracks historical changes across pipeline runs |
| fact_sales | Fact | Grain: one row per invoice line item |
| fact_sales_customer_agg | Aggregated Fact | Built from fact_sales, not directly from Silver |

---

## Job Orchestration

The full pipeline runs as a single Databricks Job named `chinook_pipeline`.

### Task Order

| Task | Type | Depends On |
|---|---|---|
| Get_Tables_List | Notebook | -- |
| For_Each_Extract | For Each | Get_Tables_List |
| For_Each_Load_Raw | For Each | For_Each_Extract |
| For_Each_Raw_To_Bronze | For Each | For_Each_Load_Raw |
| For_Each_Bronze_To_Silver | For Each | For_Each_Raw_To_Bronze |
| Silver_To_Gold | Notebook | For_Each_Bronze_To_Silver |

### Job Parameters

All parameters are passed via the Job configuration panel. Nothing is hardcoded in any notebook.

| Parameter | Value |
|---|---|
| catalog_name | workspace |
| source_catalog | chinook_az_catalog |
| source_schema | chinook |
| staging_schema | staging |
| bronze_schema | bronze |
| silver_schema | silver |
| gold_schema | gold |
| metadata_schema | metadata |
| volume_name | chinook |

---

## Parameterization

Every notebook uses `dbutils.widgets` for all configurable values. No table names, catalog names, or schema names are hardcoded anywhere. To add a new source table to the pipeline, insert a row into `pipeline_control` with `active_flag = 'Y'` -- no code changes required.

---

## Data Quality

DQX validation runs in `04_bronze_to_silver` before any data reaches Silver.

- Records that fail validation are written to `workspace.metadata.quarantine_<table>`
- Pass and fail counts are logged to `workspace.metadata.dqx_execution_log`
- If more than 5% of records fail for any table, the Job task fails and the pipeline stops before Silver is loaded

---

## Team Collaboration

Each team member works on their own branch and raises a pull request into `main`.

| Branch | Member |
|---|---|
| main | Protected -- final merged code |
| member-1-branch | Preksha |
| member-2-branch | Riyanshi |
| member-3-branch | Pradyumna |

The Databricks workspace is linked directly to this repository. Notebooks can be committed and pushed from within Databricks without leaving the platform.

---

## Validation

After a successful pipeline run, open `notebooks/00_validation_screenshots` and run all cells to generate outputs for each documentation figure. The summary cell at the end confirms row counts across all zones.

---

## Dataset

The Chinook music store dataset contains 11 tables:

Album, Artist, Customer, Employee, Genre, Invoice, InvoiceLine, MediaType, Playlist, PlaylistTrack, Track

Source: Azure SQL Server hosted on `sqlserver-damg7370.database.windows.net`

---

## Course Information

| Field | Detail |
|---|---|
| Course | DAMG 7370 - Designing Advanced Data Architectures for Business Intelligence |
| Semester | Spring 2026 |
| Institution | Northeastern University, College Of Engineering |
| Project | Midterm Team Project |
