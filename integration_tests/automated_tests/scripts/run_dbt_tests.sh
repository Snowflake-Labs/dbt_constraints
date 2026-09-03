#!/bin/bash
# Complete dbt test workflow.
# Runs on the host using the dbt client from this cell's venv, which the test
# harness places first on PATH.

set -e  # Exit on any error

echo "=========================================="
echo "dbt_constraints Test Suite"
echo "Target: ${DBT_TARGET:-postgres}"
echo "Project: ${DBT_PROJECT_DIR:-.}"
echo "=========================================="

# Ensure we're in the project directory
cd "${DBT_PROJECT_DIR:-.}"

# Note: the run_dbt fixture runs dbt clean and dbt deps one time per session.
# A run of those commands here removes dbt_packages/ for the later tests.

# Step 1: Seed data (full refresh)
echo ""
echo "🌱 Seeding test data (full refresh)..."
dbt seed --full-refresh --target "${DBT_TARGET:-postgres}"

# Step 2: Build all models (full refresh)
echo ""
echo "🏗️  Building all models (full refresh)..."
dbt build --full-refresh --target "${DBT_TARGET:-postgres}"

# Step 3: Seed again (full refresh: dbt-oracle cannot re-load an existing seed
# without it, ORA-00955; harmless on other adapters since seeds are static)
echo ""
echo "🌱 Seeding test data (re-seed)..."
dbt seed --full-refresh --target "${DBT_TARGET:-postgres}"

# Step 4: Build again (incremental)
echo ""
echo "🏗️  Building all models (incremental)..."
dbt build --target "${DBT_TARGET:-postgres}"

echo ""
echo "=========================================="
echo "✅ All tests passed!"
echo "=========================================="
