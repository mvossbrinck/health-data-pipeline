# Healthcare Data Pipeline Project

A hands-on data pipeline built with Python, dbt, and DuckDB using CMS (Centers for Medicare & Medicaid) hospital data. This project demonstrates the core concepts of modern data engineering: Extract, Load, Transform (ELT), data quality testing, and analytics delivery.

## What Is a Data Pipeline?

A data pipeline moves data from a **source** to a **destination**, transforming it along the way so it's useful for analysis. Every pipeline answers three questions:

1. **Where does the data come from?** (Extract)
2. **What do we do to it?** (Transform)
3. **Where does it go?** (Load / Deliver)

### ELT vs ETL

- **ETL** (Extract → Transform → Load): Transform data *before* loading it into the warehouse. Older approach.
- **ELT** (Extract → Load → Transform): Load raw data first, then transform it *inside* the warehouse. Modern approach — this is what dbt enables, and what most companies (including Pathpoint and Zivian) use today.

This project uses **ELT**: we load raw CMS data into DuckDB first, then use dbt to transform it in-place.

## Project Structure

```
healthcare_pipeline/
│
├── 01_extract_load.py          # EXTRACT & LOAD: Pull CMS data → DuckDB
├── 03_visualize.py             # VISUALIZE: Query marts → charts
├── run_pipeline.sh             # Run the full pipeline end-to-end
├── healthcare.duckdb           # The database (created at runtime)
├── dashboard.png               # Output visualization
│
└── dbt_transform/              # TRANSFORM layer (dbt project)
    ├── dbt_project.yml         # dbt config
    └── models/
        ├── staging/
        │   ├── sources.yml         # Defines raw data sources
        │   ├── schema.yml          # Data quality tests
        │   └── stg_hospitals.sql   # Staging: clean & standardize
        └── marts/
            ├── mart_state_hospital_summary.sql   # State-level rollup
            └── mart_hospital_quality.sql         # Hospital-level enrichment
```

## Pipeline Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   EXTRACT   │    │    LOAD     │    │  TRANSFORM  │    │   DELIVER   │
│             │───▶│             │───▶│             │───▶│             │
│  CMS API/   │    │  DuckDB     │    │  dbt models │    │  Dashboard  │
│  CSV file   │    │  raw schema │    │  staging →  │    │  Charts     │
│             │    │             │    │  marts      │    │  Reports    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
  01_extract_       01_extract_        dbt run             03_visualize.py
  load.py           load.py            dbt test
```

## Key Concepts Explained

### Schemas (raw vs analytics)
- **`raw` schema**: Where unprocessed data lands. Never modify this directly.
- **`analytics` schema**: Where dbt puts transformed data. This is what people query.

### dbt Model Layers
- **Sources** (`sources.yml`): Tell dbt where raw data lives
- **Staging models** (`stg_`): Clean column names, cast types, filter bad data. Materialized as **views** (lightweight).
- **Mart models** (`mart_`): Business logic, aggregations, metrics. Materialized as **tables** (fast to query).

### Why This Layering Matters
If a column name changes in the source data, you only fix it in ONE place (the staging model). All downstream marts automatically get the fix. This is the **DRY principle** applied to data.

### Data Quality Tests
dbt tests catch problems before bad data reaches dashboards:
- `unique`: No duplicate facility IDs
- `not_null`: No missing values in critical columns
- `accepted_values`: Ratings must be 1-5 (or null)

In production, failed tests can trigger alerts or block the pipeline from completing.

## How to Run

### Prerequisites
```bash
pip install pandas duckdb dbt-duckdb matplotlib
```

### Full pipeline
```bash
bash run_pipeline.sh
```

### Individual steps
```bash
# 1. Extract & Load
python3 01_extract_load.py

# 2. Transform
cd dbt_transform
dbt run

# 3. Test
dbt test

# 4. Visualize
cd ..
python3 03_visualize.py
```

## Adapting This to a Production Environment

| This Project | Production Equivalent |
|---|---|
| DuckDB (local file) | Postgres, Redshift, Snowflake, BigQuery |
| Python script pulling CSV | Airflow/Dagster DAG, Fivetran, Polytomic |
| `dbt run` locally | dbt Cloud, CI/CD triggered dbt runs |
| matplotlib charts | Looker, Tableau, Metabase, Hex |
| Manual `bash run_pipeline.sh` | Orchestrator (Airflow, Dagster, Prefect) on a schedule |

## Talking Points for Interviews

When discussing this project, you can speak to:

1. **Why ELT over ETL?** — Loading raw data first preserves the source of truth. Transformations are versioned in dbt (SQL in Git), making them auditable and reproducible.

2. **Why dbt?** — It brings software engineering practices (version control, testing, documentation, modularity) to SQL transformations. Both Pathpoint and many modern data teams use it.

3. **Why staging + marts?** — Separation of concerns. Staging handles the "plumbing" (renaming, casting, filtering). Marts handle the "thinking" (business logic, aggregations). This makes pipelines easier to debug and maintain.

4. **Why test data?** — Bad data in → bad decisions out. Tests are the safety net that prevents a broken upstream source from silently corrupting dashboards and reports.

5. **How would you orchestrate this?** — In production, you'd use an orchestrator like Airflow or Dagster to run extract → dbt run → dbt test → alert on failure, typically on a daily or hourly schedule.
