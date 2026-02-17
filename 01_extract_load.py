"""
STEP 1: EXTRACT & LOAD
=======================
This script pulls public CMS (Centers for Medicare & Medicaid) hospital data
and loads it into a DuckDB database. This is the "EL" in ELT.

In a production environment, this might pull from an API, SFTP server, 
or streaming source. Here we use a public CMS CSV for simplicity.

Data source: CMS Hospital General Information
https://data.cms.gov/provider-data/dataset/xubh-q36u
"""

import pandas as pd
import duckdb
import os

# ── CONFIG ──────────────────────────────────────────────────────────────
DB_PATH = "healthcare.duckdb"
RAW_SCHEMA = "raw"

# CMS Hospital General Information dataset
CMS_URL = "https://data.cms.gov/provider-data/sites/default/files/resources/092256becd267d9eeccf73bf7d16c46b/Hospital_General_Information.csv"

# ── EXTRACT ─────────────────────────────────────────────────────────────
print("=" * 60)
print("STEP 1: EXTRACTING DATA FROM CMS")
print("=" * 60)

print(f"\nDownloading hospital data from CMS...")
try:
    df_hospitals = pd.read_csv(CMS_URL)
    print(f"  ✓ Downloaded {len(df_hospitals):,} hospital records")
    print(f"  ✓ Columns: {len(df_hospitals.columns)}")
    print(f"  ✓ Sample columns: {list(df_hospitals.columns[:5])}")
except Exception as e:
    print(f"  ✗ Could not download from CMS. Using sample data instead.")
    print(f"    Error: {e}")
    
    # Fallback: create realistic sample data if CMS is unreachable
    import numpy as np
    np.random.seed(42)
    
    states = ['NY', 'CA', 'TX', 'FL', 'IL', 'PA', 'OH', 'CT', 'MA', 'NJ',
              'GA', 'NC', 'MI', 'VA', 'WA', 'AZ', 'TN', 'MO', 'MD', 'WI']
    
    hospital_types = ['Acute Care Hospitals', 'Critical Access Hospitals', 
                      'Childrens', 'Psychiatric']
    
    ownership_types = ['Voluntary non-profit - Private', 'Proprietary', 
                       'Government - Local', 'Government - State',
                       'Voluntary non-profit - Church', 'Government - Federal']
    
    ratings = [1, 2, 3, 4, 5, 'Not Available']
    
    n = 4000
    df_hospitals = pd.DataFrame({
        'Facility ID': [f'{10000 + i}' for i in range(n)],
        'Facility Name': [f'Hospital_{i}' for i in range(n)],
        'Address': [f'{np.random.randint(100, 9999)} Main St' for _ in range(n)],
        'City': [f'City_{np.random.randint(1, 500)}' for _ in range(n)],
        'State': np.random.choice(states, n),
        'ZIP Code': [f'{np.random.randint(10000, 99999)}' for _ in range(n)],
        'County Name': [f'County_{np.random.randint(1, 200)}' for _ in range(n)],
        'Phone Number': [f'({np.random.randint(200,999)}) {np.random.randint(200,999)}-{np.random.randint(1000,9999)}' for _ in range(n)],
        'Hospital Type': np.random.choice(hospital_types, n, p=[0.65, 0.20, 0.05, 0.10]),
        'Hospital Ownership': np.random.choice(ownership_types, n, p=[0.35, 0.25, 0.15, 0.10, 0.10, 0.05]),
        'Hospital overall rating': np.random.choice(ratings, n, p=[0.05, 0.15, 0.35, 0.30, 0.10, 0.05]),
        'Emergency Services': np.random.choice(['Yes', 'No'], n, p=[0.85, 0.15]),
        'Meets criteria for promoting interoperability of EHRs': np.random.choice(['Y', 'N', 'Not Available'], n, p=[0.70, 0.15, 0.15]),
    })
    print(f"  ✓ Created {len(df_hospitals):,} sample hospital records")

# ── LOAD ────────────────────────────────────────────────────────────────
print(f"\n{'=' * 60}")
print("STEP 2: LOADING DATA INTO DUCKDB")
print("=" * 60)

# Remove old database if it exists (clean run)
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = duckdb.connect(DB_PATH)

# Create raw schema (like a staging area for unprocessed data)
conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

# Load raw data as-is (no transformations yet — that's dbt's job)
conn.execute(f"CREATE TABLE {RAW_SCHEMA}.hospitals AS SELECT * FROM df_hospitals")

# Verify the load
count = conn.execute(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.hospitals").fetchone()[0]
columns = conn.execute(f"SELECT * FROM {RAW_SCHEMA}.hospitals LIMIT 0").description

print(f"  ✓ Created table: {RAW_SCHEMA}.hospitals")
print(f"  ✓ Rows loaded: {count:,}")
print(f"  ✓ Columns: {len(columns)}")

# Show a preview
print(f"\n  Preview of raw data:")
preview = conn.execute(f"SELECT * FROM {RAW_SCHEMA}.hospitals LIMIT 3").fetchdf()
print(preview.to_string(index=False, max_colwidth=30))

conn.close()

print(f"\n{'=' * 60}")
print(f"✓ EXTRACT & LOAD COMPLETE")
print(f"  Database: {DB_PATH}")
print(f"  Table: {RAW_SCHEMA}.hospitals ({count:,} rows)")
print(f"{'=' * 60}")
