#!/bin/bash
# ============================================================
# FULL PIPELINE RUNNER
# ============================================================
# This script runs the entire pipeline end-to-end:
#   1. Extract & Load (Python → DuckDB)
#   2. Transform (dbt)  
#   3. Test (dbt)
#   4. Visualize (Python + matplotlib)
#
# Usage: bash run_pipeline.sh
# ============================================================

set -e  # Exit on any error

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║       HEALTHCARE DATA PIPELINE — FULL RUN                ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Step 1: Extract & Load
echo "▶ PHASE 1: Extract & Load"
echo "─────────────────────────"
python3 01_extract_load.py
echo ""

# Step 2: Transform with dbt
echo "▶ PHASE 2: Transform (dbt run)"
echo "──────────────────────────────"
cd dbt_transform
dbt run
echo ""

# Step 3: Test data quality
echo "▶ PHASE 3: Data Quality Tests (dbt test)"
echo "─────────────────────────────────────────"
dbt test
echo ""

# Step 4: Generate dbt docs (optional but useful)
echo "▶ PHASE 4: Generate Documentation"
echo "──────────────────────────────────"
dbt docs generate
echo "  ✓ Docs generated (run 'dbt docs serve' to view)"
echo ""

# Step 5: Visualize
echo "▶ PHASE 5: Visualize & Deliver"
echo "──────────────────────────────"
cd ..
python3 03_visualize.py
echo ""

echo "╔════════════════════════════════════════════════════════════╗"
echo "║                 ✓ PIPELINE RUN COMPLETE                   ║"
echo "╚════════════════════════════════════════════════════════════╝"
