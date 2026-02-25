#!/bin/bash
# Deploy script for PySpark migration
# Replaces Informatica PowerCenter deployment workflow
#
# Usage:
#   ./deploy.sh [environment] [job_name]
#   ./deploy.sh Prod job1_pay_calendar
#   ./deploy.sh Test                    # Deploy all jobs to Test

set -euo pipefail

ENVIRONMENT="${1:-Prod}"
JOB_NAME="${2:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== PySpark Migration Deployment ==="
echo "Environment: $ENVIRONMENT"
echo "Job: ${JOB_NAME:-all}"
echo "Project: $PROJECT_DIR"
echo ""

# Validate environment
case "$ENVIRONMENT" in
    Prod|Test|Dev)
        echo "Environment validated: $ENVIRONMENT"
        ;;
    *)
        echo "ERROR: Invalid environment '$ENVIRONMENT'. Must be Prod, Test, or Dev."
        exit 1
        ;;
esac

# Build Docker image
echo ""
echo "=== Building Docker Image ==="
cd "$PROJECT_DIR"
docker build -t biis-pyspark-migration:latest .
echo "Docker image built successfully."

# Run tests before deployment
echo ""
echo "=== Running Tests ==="
docker run --rm biis-pyspark-migration:latest \
    python -m pytest /app/pyspark_migration/tests/ -v --tb=short || {
    echo "ERROR: Tests failed. Aborting deployment."
    exit 1
}
echo "All tests passed."

# Deploy
echo ""
echo "=== Deploying ==="
if [ -n "$JOB_NAME" ]; then
    echo "Running job: $JOB_NAME"
    docker-compose run -e ENVIRONMENT="$ENVIRONMENT" -e JOB_NAME="$JOB_NAME" pyspark-job
else
    echo "Ready for job execution. Use:"
    echo "  docker-compose run -e JOB_NAME=<job_name> pyspark-job"
    echo ""
    echo "Available jobs:"
    echo "  job1_pay_calendar"
    echo "  job2_comptime"
    echo "  job3_pseudossn"
    echo "  job4_cpm_extract"
    echo "  job5_fda_leave"
    echo "  job6_ehrp2biis_update"
fi

echo ""
echo "=== Deployment Complete ==="
