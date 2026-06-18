# Workforce Analytics Lakehouse

This project builds a Workforce Analytics Lakehouse using Databricks Free Edition, Unity Catalog, Databricks-managed storage, Python, and Delta Lake.

The current stage of the project focuses on the **Bronze layer**, where raw source data is ingested and stored without cleaning or transformation.

## Project Goal

The goal of this project is to collect workforce analytics data from public labor market sources and prepare it for later transformation into Silver and Gold Delta tables.

The project uses:

* Bureau of Labor Statistics (BLS)
* O*NET downloadable database
* Databricks Free Edition
* Unity Catalog
* Managed Unity Catalog Volumes
* Python ingestion scripts
* GitHub version control

This project does **not** use AWS S3 directly. Databricks-managed storage is used through Unity Catalog.

---

# Architecture

The project follows a medallion architecture:

```text
Bronze  → Raw source files
Silver  → Cleaned, validated, structured tables
Gold    → Analytics-ready dashboard tables
```

At this stage, only the **Bronze layer** has been implemented.

The Bronze layer stores:

* Raw BLS API JSON files
* Raw O*NET text database files
* Raw O*NET ZIP archive
* Manifest files documenting what was downloaded

No cleaning, validation, deduplication, casting, or transformation is performed in the Bronze layer.

---

# Databricks Storage Setup

This project uses a Unity Catalog managed volume for raw Bronze files.

## Create the Catalog, Schemas, and Volume

Run this SQL in Databricks:

```sql
CREATE CATALOG IF NOT EXISTS workforce_analytics;

CREATE SCHEMA IF NOT EXISTS workforce_analytics.bronze;
CREATE SCHEMA IF NOT EXISTS workforce_analytics.silver;
CREATE SCHEMA IF NOT EXISTS workforce_analytics.gold;

CREATE VOLUME IF NOT EXISTS workforce_analytics.bronze.raw_files;
```

This creates the managed Bronze volume:

```text
/Volumes/workforce_analytics/bronze/raw_files/
```

This volume is used to store raw JSON, ZIP, and tab-delimited text files.

---

# Repository Structure

Recommended repo structure:

```text
workforce-analytics-lakehouse/
│
├── src/
│   └── bronze/
│       ├── ingest_bls_api.py
│       └── ingest_onet_text.py
│
├── sql/
│   └── setup_unity_catalog.sql
│
├── README.md
└── requirements.txt
```

---

# Bronze Layer Design

The Bronze layer is responsible for raw ingestion only.

## Bronze Rules

The Bronze layer should:

* Store source files exactly as collected
* Preserve raw JSON and text file formats
* Keep the original O*NET ZIP archive
* Store files in meaningful folders
* Create manifest files for traceability
* Avoid cleaning, validation, casting, or transformation

The Bronze layer should **not**:

* Rename columns
* Cast data types
* Remove duplicates
* Filter bad records
* Flatten nested JSON
* Convert raw files into Delta tables
* Apply business logic

Those steps belong in the Silver layer.

---

# Bronze Storage Paths

The raw files are stored in this Unity Catalog managed volume:

```text
/Volumes/workforce_analytics/bronze/raw_files/
```

The expected folder structure is:

```text
/Volumes/workforce_analytics/bronze/raw_files/
│
├── bls/
│   └── api/
│       ├── jolts_national/
│       │   ├── job_openings_level_latest.json
│       │   ├── job_openings_rate_latest.json
│       │   ├── hires_level_latest.json
│       │   ├── hires_rate_latest.json
│       │   ├── quits_level_latest.json
│       │   ├── quits_rate_latest.json
│       │   ├── layoffs_discharges_level_latest.json
│       │   ├── layoffs_discharges_rate_latest.json
│       │   ├── total_separations_level_latest.json
│       │   └── total_separations_rate_latest.json
│       │
│       ├── cps_context/
│       │   ├── unemployment_rate_latest.json
│       │   ├── labor_force_participation_rate_latest.json
│       │   ├── employment_population_ratio_latest.json
│       │   ├── civilian_labor_force_level_latest.json
│       │   ├── civilian_employment_level_latest.json
│       │   └── civilian_unemployment_level_latest.json
│       │
│       └── _manifest.json
│
└── onet/
    ├── raw_zip/
    │   └── onet_database_30_3_text.zip
    │
    ├── text_files/
    │   ├── Occupation Data.txt
    │   ├── Education Categories.txt
    │   ├── Knowledge.txt
    │   ├── Abilities.txt
    │   ├── Technology Skills.txt
    │   └── ...
    │
    └── _manifest.json
```

---

# Data Sources

## 1. Bureau of Labor Statistics API

The BLS API is used to collect labor market time-series data.

Base API URL:

```text
https://api.bls.gov/publicAPI/v2/timeseries/data/
```

The current Bronze ingestion pulls the latest available value for selected BLS series.

The BLS ingestion script saves one raw JSON file per series.

## 2. O*NET Database Text Download

The O*NET downloadable database is used for occupation, skill, education, and work requirement data.

Source ZIP file:

```text
https://www.onetcenter.org/dl_files/database/db_30_3_text.zip
```

The O*NET text download contains tab-delimited `.txt` files, not comma-separated CSV files.

The Bronze ingestion script downloads the ZIP file, stores the raw ZIP archive, extracts the `.txt` files, and saves them into the Bronze managed volume.

---

# BLS Data Collected

The BLS Bronze ingestion collects 16 total series.

## JOLTS National Labor Demand and Turnover

These series come from JOLTS, the Job Openings and Labor Turnover Survey.

| File Name                              | Series ID               | Description                           |
| -------------------------------------- | ----------------------- | ------------------------------------- |
| `job_openings_level_latest.json`       | `JTS000000000000000JOL` | National job openings level           |
| `job_openings_rate_latest.json`        | `JTS000000000000000JOR` | National job openings rate            |
| `hires_level_latest.json`              | `JTS000000000000000HIL` | National hires level                  |
| `hires_rate_latest.json`               | `JTS000000000000000HIR` | National hires rate                   |
| `quits_level_latest.json`              | `JTS000000000000000QUL` | National quits level                  |
| `quits_rate_latest.json`               | `JTS000000000000000QUR` | National quits rate                   |
| `layoffs_discharges_level_latest.json` | `JTS000000000000000LDL` | National layoffs and discharges level |
| `layoffs_discharges_rate_latest.json`  | `JTS000000000000000LDR` | National layoffs and discharges rate  |
| `total_separations_level_latest.json`  | `JTS000000000000000TSL` | National total separations level      |
| `total_separations_rate_latest.json`   | `JTS000000000000000TSR` | National total separations rate       |

These files support analysis of:

* Job openings
* Hiring activity
* Quit behavior
* Layoffs and discharges
* Total separations
* Labor demand trends

## CPS National Labor Market Context

These series come from CPS, the Current Population Survey.

| File Name                                    | Series ID     | Description                             |
| -------------------------------------------- | ------------- | --------------------------------------- |
| `unemployment_rate_latest.json`              | `LNS14000000` | National unemployment rate              |
| `labor_force_participation_rate_latest.json` | `LNS11300000` | National labor force participation rate |
| `employment_population_ratio_latest.json`    | `LNS12300000` | National employment-population ratio    |
| `civilian_labor_force_level_latest.json`     | `LNS11000000` | National civilian labor force level     |
| `civilian_employment_level_latest.json`      | `LNS12000000` | National civilian employment level      |
| `civilian_unemployment_level_latest.json`    | `LNS13000000` | National civilian unemployment level    |

These files provide macroeconomic labor market context for later dashboards and analysis.

---

# O*NET Data Collected

The O*NET Bronze ingestion downloads the full O*NET database text ZIP file.

The raw ZIP file is saved here:

```text
/Volumes/workforce_analytics/bronze/raw_files/onet/raw_zip/onet_database_30_3_text.zip
```

The extracted text files are saved here:

```text
/Volumes/workforce_analytics/bronze/raw_files/onet/text_files/
```

Important O*NET files for later Silver processing include:

| File                                      | Purpose                                           |
| ----------------------------------------- | ------------------------------------------------- |
| `Occupation Data.txt`                     | Occupation titles and SOC codes                   |
| `Education Categories.txt`                | Education level category mappings                 |
| `Education, Training, and Experience.txt` | Education, training, and experience requirements  |
| `Knowledge.txt`                           | Knowledge areas by occupation                     |
| `Abilities.txt`                           | Worker abilities by occupation                    |
| `Skills.txt` or related skills files      | Skills by occupation, depending on O*NET version  |
| `Technology Skills.txt`                   | Tools and technologies linked to occupations      |
| `Task Statements.txt`                     | Job task descriptions                             |
| `Work Activities.txt`                     | Work activity data                                |
| `Job Zones.txt`                           | Job preparation level and experience requirements |

O*NET files are tab-delimited text files. In Silver, they should be read with a tab separator:

```python
.option("sep", "\t")
```

---

# Bronze Ingestion Scripts

## 1. BLS API Ingestion

File:

```text
src/bronze/ingest_bls_api.py
```

Purpose:

* Calls the BLS API
* Pulls selected JOLTS and CPS series
* Saves raw JSON responses
* Uses meaningful file names
* Creates a manifest file
* Does not clean or validate the data

Expected output path:

```text
/Volumes/workforce_analytics/bronze/raw_files/bls/api/
```

The script saves files into:

```text
jolts_national/
cps_context/
```

It also writes:

```text
_manifest.json
```

The manifest records:

* Source name
* BLS series ID
* Description
* File name
* Raw file path
* Fetch timestamp

## 2. O*NET Text Database Ingestion

File:

```text
src/bronze/ingest_onet_text.py
```

Purpose:

* Downloads the O*NET text database ZIP
* Stores the raw ZIP archive
* Extracts the `.txt` files
* Saves the extracted files into Bronze
* Creates a manifest file
* Does not clean or validate the data

Expected output path:

```text
/Volumes/workforce_analytics/bronze/raw_files/onet/
```

The script saves:

```text
raw_zip/onet_database_30_3_text.zip
text_files/*.txt
_manifest.json
```

The manifest records:

* Source name
* Source URL
* Database version
* Raw ZIP path
* Extracted text file path
* File count
* Fetch timestamp
* Extracted file names

---

# Dependencies

The Bronze ingestion scripts use only standard Python libraries.

No PySpark is required for Bronze ingestion.

Required Python libraries:

```text
os
json
time
zipfile
pathlib
datetime
urllib
```

Because these are built into Python, no additional packages are required for the Bronze ingestion scripts.

A `requirements.txt` file is optional at this stage.

---

# Running the Bronze Ingestion

The ingestion scripts should be run inside Databricks so they can write to the Unity Catalog managed volume.

## Recommended Workflow

1. Open the Databricks workspace.
2. Open the connected Git folder for this repository.
3. Confirm the Unity Catalog objects exist.
4. Run the setup SQL if needed.
5. Run the Bronze ingestion scripts.

## Step 1: Create Unity Catalog Objects

Run:

```sql
CREATE CATALOG IF NOT EXISTS workforce_analytics;

CREATE SCHEMA IF NOT EXISTS workforce_analytics.bronze;
CREATE SCHEMA IF NOT EXISTS workforce_analytics.silver;
CREATE SCHEMA IF NOT EXISTS workforce_analytics.gold;

CREATE VOLUME IF NOT EXISTS workforce_analytics.bronze.raw_files;
```

## Step 2: Run BLS Ingestion

Run:

```text
src/bronze/ingest_bls_api.py
```

Expected result:

```text
BLS raw JSON files saved to Bronze volume.
Manifest saved to: /Volumes/workforce_analytics/bronze/raw_files/bls/api/_manifest.json
```

## Step 3: Run O*NET Ingestion

Run:

```text
src/bronze/ingest_onet_text.py
```

Expected result:

```text
O*NET raw files saved to Bronze volume.
Manifest saved to: /Volumes/workforce_analytics/bronze/raw_files/onet/_manifest.json
```

---

# How to Verify Bronze Files

After running the scripts, check the files in Databricks Catalog Explorer.

Navigate to:

```text
Catalog → workforce_analytics → bronze → Volumes → raw_files
```

Expected folders:

```text
bls/
onet/
```

You can also check files with Python:

```python
import os

base_path = "/Volumes/workforce_analytics/bronze/raw_files"

for root, dirs, files in os.walk(base_path):
    for file in files:
        print(os.path.join(root, file))
```

Expected Bronze outputs:

* 16 BLS raw JSON files
* 1 BLS manifest file
* 1 O*NET raw ZIP file
* Multiple O*NET tab-delimited text files
* 1 O*NET manifest file

---

# Important Notes

## Bronze Does Not Use PySpark

The Bronze layer only downloads and stores raw files.

PySpark is not needed at this stage because no distributed transformations are performed.

PySpark will be introduced in the Silver layer to:

* Read raw JSON and text files
* Flatten nested BLS JSON
* Parse O*NET tab-delimited files
* Validate records
* Cast data types
* Write Delta tables

## Bronze Does Not Clean Data

The Bronze layer intentionally preserves the raw source data.

Cleaning and validation are delayed until the Silver layer so that the original source files remain available for auditing and reprocessing.

## BLS API Warnings

Some BLS API responses may include warning messages about missing catalog metadata. These warnings do not necessarily mean the API request failed.

If the response contains data and the status is `REQUEST_SUCCEEDED`, the raw response is still saved in Bronze.

Detailed validation of these responses will be handled in the Silver layer.

---

# Next Stage: Silver Layer

The next stage will create Silver processing scripts that:

* Read raw Bronze files
* Validate required fields
* Check response status
* Parse BLS JSON into structured rows
* Read O*NET tab-delimited text files
* Standardize column names
* Cast data types
* Build cleaned Delta tables
* Create validation error tables
* Prepare data for Gold analytics

Silver tables will be stored under:

```text
workforce_analytics.silver
```
