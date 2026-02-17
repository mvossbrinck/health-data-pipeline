"""
STEP 1: EXTRACT & LOAD
=======================
This script extracts hospital data from the CMS (Centers for Medicare & Medicaid)
public API and loads it into a DuckDB database. This is the "EL" in ELT.

KEY CONCEPT: This demonstrates how a real data pipeline extracts from an API.
An API is just a way for two systems to talk to each other:
  - You send a REQUEST (like ordering at a restaurant)
  - The API sends back a RESPONSE with structured data (your order arrives)

The CMS API returns JSON — a structured format that looks like this:
  [
    {"facility_id": "10001", "hospital_name": "General Hospital", "state": "NY", ...},
    {"facility_id": "10002", "hospital_name": "City Medical", "state": "CA", ...},
  ]

Data source: CMS Hospital General Information
https://data.cms.gov/provider-data/dataset/xubh-q36u
API docs: https://data.cms.gov/provider-data/api
"""

import pandas as pd
import duckdb
import requests  # <-- The library used to call APIs
import json
import os
import time

# ── CONFIG ──────────────────────────────────────────────────────────────
DB_PATH = "healthcare.duckdb"
RAW_SCHEMA = "raw"

# CMS Provider Data API endpoint for Hospital General Information
# The dataset ID for hospitals is: xubh-q36u
API_BASE_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u"

# ── EXTRACT (via API) ──────────────────────────────────────────────────
print("=" * 60)
print("STEP 1: EXTRACTING DATA FROM CMS API")
print("=" * 60)


def extract_from_api(base_url, batch_size=500, max_records=None):
    """
    Extract data from the CMS API using PAGINATION.
    
    WHY PAGINATION MATTERS:
    APIs typically won't return millions of rows in one call.
    Instead, you request data in "pages" (batches):
      - First call:  "Give me rows 0-499"    (offset=0, limit=500)
      - Second call: "Give me rows 500-999"   (offset=500, limit=500)
      - Third call:  "Give me rows 1000-1499" (offset=1000, limit=500)
      - ...until the API returns fewer rows than requested (you've hit the end)
    
    This is one of the most common patterns in pipeline development.
    """
    all_records = []
    offset = 0
    page = 1
    
    print(f"\n  Calling API: {base_url}")
    print(f"  Batch size: {batch_size} records per request")
    print()
    
    while True:
        # ── BUILD THE API REQUEST ──
        # These are "query parameters" — they tell the API what we want
        params = {
            "limit": batch_size,    # How many records per page
            "offset": offset,       # Where to start (skip this many records)
        }
        
        print(f"  Page {page}: Requesting records {offset} to {offset + batch_size - 1}...", end=" ")
        
        try:
            # ── MAKE THE API CALL ──
            # requests.get() sends an HTTP GET request to the API
            # This is the equivalent of typing a URL into your browser
            response = requests.get(
                base_url,
                params=params,
                timeout=30  # Don't wait forever if the API is slow
            )
            
            # ── CHECK IF THE API CALL SUCCEEDED ──
            # HTTP status codes: 200 = success, 400s = your fault, 500s = their fault
            response.raise_for_status()  # Raises an error if status != 200
            
            # ── PARSE THE RESPONSE ──
            # The API returns JSON (structured text). We convert it to Python dicts.
            data = response.json()
            
            # CMS API wraps results in a "results" key
            records = data.get("results", [])
            
            print(f"Got {len(records)} records")
            all_records.extend(records)
            
            # ── CHECK IF WE'RE DONE ──
            # If we got fewer records than we asked for, we've reached the end
            if len(records) < batch_size:
                print(f"\n  Reached end of data (last page had {len(records)} records)")
                break
            
            # Check if we've hit our max
            if max_records and len(all_records) >= max_records:
                print(f"\n  Reached max_records limit ({max_records})")
                all_records = all_records[:max_records]
                break
            
            # ── MOVE TO NEXT PAGE ──
            offset += batch_size
            page += 1
            
            # Be polite to the API — don't hammer it with rapid requests
            time.sleep(0.5)
            
        except requests.exceptions.Timeout:
            print("TIMEOUT — API took too long. Retrying...")
            time.sleep(2)
            continue  # Retry the same page
            
        except requests.exceptions.HTTPError as e:
            print(f"API ERROR: {e}")
            break
            
        except requests.exceptions.ConnectionError as e:
            print(f"CONNECTION ERROR: {e}")
            print("  Could not reach the API. Check your internet connection.")
            break
    
    return all_records


# ── TRY THE REAL API, FALL BACK TO SAMPLE DATA ─────────────────────────
print("\nAttempting to extract from CMS API...")
try:
    # Pull first 2000 records (use max_records=None for all ~5000 hospitals)
    records = extract_from_api(API_BASE_URL, batch_size=500, max_records=2000)
    
    if len(records) > 0:
        df_hospitals = pd.DataFrame(records)
        print(f"\n  ✓ Extracted {len(df_hospitals):,} hospital records from API")
        print(f"  ✓ Columns: {list(df_hospitals.columns[:5])} ... ({len(df_hospitals.columns)} total)")
    else:
        raise ValueError("API returned no records")
        
except Exception as e:
    print(f"\n  ✗ API extraction failed: {e}")
    print(f"  → Falling back to sample data for demonstration\n")
    
    # ── FALLBACK: Generate realistic sample data ──
    # In production, you might load from a cached file instead
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


# ── LOAD INTO DATABASE ──────────────────────────────────────────────────
print(f"\n{'=' * 60}")
print("STEP 2: LOADING RAW DATA INTO DUCKDB")
print("=" * 60)

# Remove old database if it exists (clean run — idempotent!)
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = duckdb.connect(DB_PATH)

# Create raw schema (staging area for unprocessed data)
conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")

# Load raw data as-is — no transformations yet (that's dbt's job)
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
