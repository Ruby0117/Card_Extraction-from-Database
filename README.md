# Survey Card Extraction Pipeline

A PySpark / Databricks pipeline that extracts, decodes, and exports **National Housing Survey** card data from a Dynamics 365 xxx Hive backend to per-year Excel files on Azure Data Lake Storage (ADLS).

---

## Overview

The pipeline reads raw survey card records from three Hive tables, translates all Dynamics 365 numeric option-set codes to human-readable labels, converts boolean flags to Yes/No, enriches each card with structure and province metadata, then writes one `.xlsx` file per survey year.

```
xxx.survey_card
xxx.survey_structure          ─► Transform ─► Join ─► Export (ADLS, one file/year)
xxx_shared.province_or_state
```

Survey years present in the data (as of last run):

| Year | Records (approx.) |
|------|-------------------|
| 2018 | ~49K              |
| 2020 | ~60K              |
| 2022 | ~58K              |
| 2023 | ~57K              |
| 2024 | ~62K              |
| 2025 | ~60K              |

Total dataset: **345,000+ records across 191 columns, 2018–2025.**

---

## Project Structure

```
survey-card-extraction/
├── src/
│   ├── __init__.py
│   ├── extract.py          # Main pipeline entry point & column selection
│   ├── decode.py           # Numeric option-set → human-readable label decoders
│   ├── bool_columns.py     # Boolean flag → Yes/No converter
│   ├── lookups.py          # Structure & province lookup joins
│   └── export.py           # ADLS Excel export (per survey year)
├── tests/
│   ├── test_decode.py
│   └── test_bool_columns.py
├── config/
│   └── settings.py         # Environment-level configuration
├── docs/
│   └── column_mapping.md   # Source column → alias mapping reference
├── .github/
│   └── workflows/
│       └── ci.yml          # GitHub Actions CI (lint + unit tests)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Quick Start

### Prerequisites

- Databricks Runtime 12+ (includes PySpark 3.3+)
- Azure Data Lake Storage Gen2 account
- The `com.crealytics:spark-excel` package available on your cluster

### Running on Databricks

1. Clone this repo or upload the `src/` folder to your Databricks workspace.
2. Attach the notebook or run via `dbutils.notebook.run(...)`.
3. Call the pipeline entry point:

```python
from src.extract import run_extraction

run_extraction(spark, output_username="your_username")
```

The output path will be:
```
abfss://sandbox@your-storage-account.dfs.core.windows.net/users/<your_username>/Extract/<year>.xlsx
```

### Running Unit Tests Locally

Tests use a local SparkSession and do **not** require a Databricks cluster.

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Module Reference

### `src/extract.py`

The main entry point. Orchestrates all pipeline steps:

1. Load raw Hive tables
2. Select and alias ~190 columns across 27 survey questions
3. Decode numeric codes
4. Convert boolean flags
5. Join lookups
6. Encode province IDs as Statistics Canada numeric codes
7. Export to ADLS

### `src/decode.py`

Contains one decoder function per survey question (or group of questions with identical encoding). All decoders accept and return a Spark DataFrame.

Key decoder groups:

| Function | Columns handled |
|---|---|
| `decode_dweltype` | `dweltype` |
| `decode_q1_ownermanager` | `q1_ownermanager` |
| `decode_select_all_no_options` | `q5_options`, `q6_options`, `q12_options`, `q13_options` |
| `decode_dk_refuse_options` | `q18_options`, `q22_options` |
| `decode_no_dk_refuse_options` | `q19_options`, `q20_options`, `q21_options`, `q23_options` |
| `decode_status_fields` | `auto_status`, `manual_status` |
| `apply_all_decodings` | Orchestrates all of the above |

### `src/bool_columns.py`

Converts all ~65 boolean flag columns (stored as native booleans or `'true'`/`'false'` strings by Dynamics 365) to `'Yes'` / `'No'`. The full list of affected columns is defined in `BOOLEAN_COLUMNS`.

### `src/lookups.py`

Builds and joins two lookup DataFrames:

- **structure_lookup** — maps `structidlookup` → `structid`, `ownerlookup`
- **province_lookup** — maps `provinceid` (lowercase UUID) → `province` (English name)

### `src/export.py`

Partitions the DataFrame by `surveyyear` and writes each partition to its own `.xlsx` file using the `com.crealytics.spark.excel` Spark library. Each file's sheet is named after the survey year.

---

## Column Mapping

See [`docs/column_mapping.md`](docs/column_mapping.md) for a full reference of every source column alias used in this pipeline.

---

## Configuration

Edit `config/settings.py` to point to your environment:

```python
ADLS_BASE_PATH       = "abfss://sandbox@your-storage-account.dfs.core.windows.net/users/{username}/Extract/"
HIVE_CARD_TABLE      = "xxx.survey_card"
HIVE_STRUCTURE_TABLE = "xxx.survey_structure"
HIVE_PROVINCE_TABLE  = "xxx_shared.province_or_state"
```

---

## CI

GitHub Actions runs linting (flake8) and unit tests on every push and pull request. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

Badge: ![CI](https://github.com/your-username/survey-card-extraction/actions/workflows/ci.yml/badge.svg)

---

## Contributing

1. Branch from `main`.
2. Add or update decoder functions in `src/decode.py`.
3. Add corresponding unit tests in `tests/test_decode.py`.
4. Open a pull request — CI must pass before merging.
