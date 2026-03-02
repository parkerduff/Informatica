"""
Execute all PySpark migration jobs end-to-end in migration order.

Usage:
    python -m pyspark_migration.scripts.run_all_jobs
    python -m pyspark_migration.scripts.run_all_jobs --pp-end-year 2025 --pp-num 13

Replaces Informatica PowerCenter workflow scheduler.
Executes jobs in complexity order (simplest first):
  1. Pay_Calendar
  2. COMPTIME
  3. Pseudossn
  4. FDA_Leave
  5. CPM_NIH
  6. CPM_CDC
  7. EHRP2BIIS_UPDATE
"""

import argparse
import json
import logging
import sys
from datetime import datetime

from pyspark_migration.scripts.run_job import JOB_NAMES, run_job

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run all PySpark migration jobs in order"
    )
    parser.add_argument("--pp-end-year", help="Pay period end year")
    parser.add_argument("--pp-num", help="Pay period number")
    parser.add_argument("--source-file", help="Source file path (for file-based jobs)")
    parser.add_argument(
        "--output-metrics",
        default="/tmp/all_jobs_metrics.json",
        help="Path to write metrics JSON",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop execution if any job fails",
    )
    args = parser.parse_args()

    start_time = datetime.now()
    all_metrics = []
    failed_jobs = []

    logger.info("=" * 70)
    logger.info("STARTING ALL PYSPARK MIGRATION JOBS")
    logger.info("=" * 70)

    for job_name in JOB_NAMES:
        logger.info("-" * 60)
        logger.info("Running job %d/%d: %s", JOB_NAMES.index(job_name) + 1, len(JOB_NAMES), job_name)
        logger.info("-" * 60)

        try:
            metrics = run_job(job_name, args)
            all_metrics.append(metrics.to_dict())
            logger.info("Job %s completed: %s", job_name, metrics.status)
        except Exception as exc:
            logger.error("Job %s FAILED: %s", job_name, str(exc))
            failed_jobs.append(job_name)
            if args.stop_on_failure:
                logger.error("Stopping due to --stop-on-failure flag")
                break

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Write metrics
    output = {
        "execution_start": start_time.isoformat(),
        "execution_end": end_time.isoformat(),
        "total_duration_seconds": duration,
        "total_jobs": len(JOB_NAMES),
        "successful_jobs": len(JOB_NAMES) - len(failed_jobs),
        "failed_jobs": failed_jobs,
        "metrics": all_metrics,
    }

    with open(args.output_metrics, "w") as f:
        json.dump(output, f, indent=2, default=str)

    logger.info("=" * 70)
    logger.info(
        "COMPLETED: %d/%d jobs succeeded in %.1f seconds",
        len(JOB_NAMES) - len(failed_jobs),
        len(JOB_NAMES),
        duration,
    )
    if failed_jobs:
        logger.error("FAILED JOBS: %s", ", ".join(failed_jobs))
    logger.info("Metrics: %s", args.output_metrics)
    logger.info("=" * 70)

    sys.exit(1 if failed_jobs else 0)


if __name__ == "__main__":
    main()
