# Referral ETL Pipeline — Dockerized PySpark Project

## Project Overview

This project implements a containerized ETL pipeline using **PySpark** to process referral data and generate analytical reports. It includes:

* A **data profiling script** to assess input data quality
* A **data transformation pipeline** that cleans, joins, and validates data
* A **Dockerized runtime** for reproducible execution
* Output reports stored **outside the container** using Docker volumes

The project simulates a real-world data engineering workflow with modular code, clear documentation, and reproducible execution.

---

## Project Structure

```
referral-project/
│
├── data/                  # Input CSV files
├── output/                # Generated reports (host machine)
│
├── pipeline.py            # Main ETL pipeline
├── profiling.py           # Data profiling script
├── requirements.txt       # Python dependencies
├── Dockerfile             # Docker configuration
└── README.md              # Project documentation
```

---

## Architecture Summary

The pipeline processes raw CSV data through Spark transformation stages to produce a final referral analytics report.

High-level workflow:

* Raw CSV files are read from the `data/` folder
* Data is cleaned and standardized
* Tables are joined and enriched
* Business validation rules are applied
* Final output is exported to the `output/` folder

The entire workflow runs inside Docker for consistent execution across environments.

---

## Pipeline Flow Diagram

```
Raw CSV Files (data/)
        │
        ▼
Data Loading (Spark)
        │
        ▼
Cleaning & Casting
(type fixes, timestamps, normalization)
        │
        ▼
Transformations & Joins
(window functions + table joins)
        │
        ▼
Business Logic Validation
(rule checks + derived metrics)
        │
        ▼
Final Output Report
(output/final_output)
```

---

## Script Documentation

This project contains two executable scripts: **profiling.py** and **pipeline.py**. Both are modular and organized into reusable functions.

---

### profiling.py — Data Profiling Script

The profiling script evaluates the structure and quality of the raw datasets.

#### Key Functions

**create_spark_session()**

* Initializes Spark with a fixed timezone
* Reduces log verbosity
* Ensures consistent timestamp handling

**load_tables()**

* Loads CSV files from `/app/data`
* Creates Spark DataFrames

**clean_and_cast()**

* Casts columns to correct data types
* Normalizes timestamps
* Ensures schema consistency

**profile_table() / profile_all_tables()**

* Computes column statistics:

  * Data types
  * Null counts
  * Distinct counts
  * Min/max values

**save_output()**

* Converts profiling results to Pandas
* Writes report to `/app/output`
* Ensures output persists outside Docker

**Purpose:**
Validate data quality before running the ETL pipeline.

---

### pipeline.py — ETL Pipeline Script

The pipeline script produces the final referral analytics dataset.

#### Key Functions

**create_spark_session()**

* Creates Spark session with explicit timezone configuration

**load_tables()**

* Loads raw CSV datasets into Spark

**clean_and_cast()**

* Casts data types
* Applies timezone-aware timestamp conversions
* Standardizes schema

**transform_and_join()**

* Uses window functions to select latest records
* Joins referral, user, reward, and transaction tables
* Derives categorical fields

**apply_business_logic()**

* Applies rule-based validation checks
* Flags valid/invalid records
* Computes derived metrics

**save_output()**

* Writes final dataset using Spark overwrite mode
* Saves output to mounted `/app/output`

**Purpose:**
Transform raw referral data into a clean analytical dataset.

---

### Execution Flow

Both scripts follow this structure:

```
Spark Session Creation
        ↓
Data Loading
        ↓
Cleaning & Casting
        ↓
Processing Logic
        ↓
Output Generation
```

---

### Best Practices Implemented

* Modular function-based design
* Separation of profiling and production pipeline
* Explicit timezone handling
* Docker-compatible output storage
* No hardcoded credentials
* Clear console logging

---

## Data Dictionary (Final Output Report)

| Column Name              | Description                      | Data Type | Notes                 |
| ------------------------ | -------------------------------- | --------- | --------------------- |
| referral_details_id      | Unique referral log identifier   | Integer   | Primary key           |
| referral_id              | Referral transaction ID          | String    | Links referral events |
| referral_source          | Source of referral               | String    | Original category     |
| referral_source_category | Grouped referral type            | String    | Derived field         |
| referral_at              | Referral timestamp               | Timestamp | Timezone normalized   |
| referrer_id              | Referring user ID                | String    | Foreign key           |
| referrer_name            | Referrer name                    | String    | Cleaned text          |
| referrer_phone_number    | Referrer phone number            | String    | Contact info          |
| referrer_homeclub        | Referrer home location           | String    | Business location     |
| referee_id               | Referred user/lead ID            | String    | Linked entity         |
| referee_name             | Referee name                     | String    | Cleaned text          |
| referee_phone            | Referee phone                    | String    | Contact info          |
| referral_status          | Referral status description      | String    | From status table     |
| num_reward_days          | Days between referral and reward | Integer   | Derived metric        |
| transaction_id           | Payment transaction ID           | String    | Nullable              |
| transaction_status       | Transaction status               | String    | Standardized          |
| transaction_at           | Transaction timestamp            | Timestamp | Timezone normalized   |
| transaction_location     | Transaction location             | String    | Cleaned text          |
| transaction_type         | Transaction type                 | String    | New/renewal           |
| updated_at               | Referral update timestamp        | Timestamp | System update         |
| reward_granted_at        | Reward grant timestamp           | Timestamp | Nullable              |
| is_business_logic_valid  | Validation result flag           | Boolean   | Quality check         |

---

## Requirements

* Docker Desktop installed and running
* Recommended: 4 GB RAM available

No Python installation is required on the host machine.

---

## Build Docker Image

From the project root directory:

```
docker build -t referral-project .
```

---

## Running Instructions

### Run Data Profiling

#### PowerShell

```
docker run `
  -v ${PWD}/data:/app/data `
  -v ${PWD}/output:/app/output `
  referral-project python profiling.py
```

#### Windows Command Prompt

```
docker run -v C:\path\to\referral-project\data:/app/data -v C:\path\to\referral-project\output:/app/output referral-project python profiling.py
```

---

### Run ETL Pipeline

#### PowerShell

```
docker run `
  -v ${PWD}/data:/app/data `
  -v ${PWD}/output:/app/output `
  referral-project python pipeline.py
```

#### Windows Command Prompt

```
docker run -v C:\path\to\referral-project\data:/app/data -v C:\path\to\referral-project\output:/app/output referral-project python pipeline.py
```

---

## Output Files

All generated reports are saved in the local:

```
output/
```

folder on the host machine via Docker volume mounting.

This ensures:

* Reports persist after container shutdown
* Files are accessible outside Docker
* Results can be reviewed directly

---

## How to Verify Successful Execution

After running the scripts, check the `output/` folder.

You should see:

* Profiling report CSV
* Final pipeline output folder with CSV files

Shows successful pipeline execution.

---

## Design Principles

* Spark-native distributed processing
* Docker-based reproducibility
* Externalized outputs via mounted volumes
* Clear separation of concerns
* Production-style modular architecture

---

## Author

This project demonstrates a reproducible, containerized PySpark ETL pipeline suitable for real-world data engineering workflows.
