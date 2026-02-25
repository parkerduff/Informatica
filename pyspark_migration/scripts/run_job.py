"""
Job runner script — entry point for PySpark migration jobs.

Replaces Informatica Workflow Manager scheduling.
Usage:
    python -m pyspark_migration.scripts.run_job --job job1_pay_calendar
    JOB_NAME=job2_comptime python -m pyspark_migration.scripts.run_job

Environment Variables:
    JOB_NAME: Name of job to run (job1_pay_calendar, job2_comptime, etc.)
    ENVIRONMENT: Prod, Test, or Dev (replaces $PMRepositoryServiceName)
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark_migration.common.config import load_config_from_env
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.counter_error import CounterManager, ErrorManager
from pyspark_migration.common.logging_utils import setup_logging
from pyspark_migration.common.spark_session import create_spark_session

logger = logging.getLogger(__name__)

# Job registry (maps job names to their classes)
JOB_REGISTRY = {
    "job1_pay_calendar": "pyspark_migration.jobs.job1_pay_calendar.PayCalendarJob",
    "job2_comptime": "pyspark_migration.jobs.job2_comptime.CompTimeJob",
    "job3_pseudossn": "pyspark_migration.jobs.job3_pseudossn.PseudossnJob",
    "job4_cpm_extract": "pyspark_migration.jobs.job4_cpm_extract.CPMExtractJob",
    "job5_fda_leave": "pyspark_migration.jobs.job5_fda_leave.FDALeaveJob",
    "job6_ehrp2biis_update": "pyspark_migration.jobs.job6_ehrp2biis_update.EHRP2BIISUpdateJob",
}


def import_class(class_path: str):
    """Dynamically import a class from a dotted path."""
    module_path, class_name = class_path.rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def main():
    parser = argparse.ArgumentParser(
        description="Run PySpark migration jobs (replaces Informatica Workflow Manager)"
    )
    parser.add_argument(
        "--job", "-j",
        type=str,
        default=os.environ.get("JOB_NAME", ""),
        help="Job name to run (e.g., job1_pay_calendar)"
    )
    parser.add_argument(
        "--run-forever",
        action="store_true",
        default=False,
        help="Enable RUNFOREVER mode (for job6_ehrp2biis_update)"
    )
    args = parser.parse_args()

    job_name = args.job
    if not job_name:
        print("Error: No job specified. Use --job or set JOB_NAME env var.")
        print(f"Available jobs: {', '.join(JOB_REGISTRY.keys())}")
        sys.exit(1)

    if job_name not in JOB_REGISTRY:
        print(f"Error: Unknown job '{job_name}'.")
        print(f"Available jobs: {', '.join(JOB_REGISTRY.keys())}")
        sys.exit(1)

    # Setup
    config = load_config_from_env()
    setup_logging(job_name, config.paths.log_dir)
    logger.info(f"Starting job: {job_name} at {datetime.now()}")

    # Create Spark session
    spark = create_spark_session(config.spark, job_name)

    # Create shared services
    db_manager = DatabaseManager(config, spark)
    email_service = EmailService(config.email)
    counter_manager = CounterManager(db_manager)
    error_manager = ErrorManager(db_manager)

    # Import and instantiate job class
    job_class = import_class(JOB_REGISTRY[job_name])

    # Build constructor args based on job type
    if job_name in ("job1_pay_calendar",):
        job = job_class(spark, config, db_manager, email_service)
    elif job_name in ("job2_comptime", "job3_pseudossn", "job4_cpm_extract"):
        job = job_class(spark, config, db_manager, email_service, counter_manager)
    elif job_name in ("job5_fda_leave", "job6_ehrp2biis_update"):
        job = job_class(
            spark, config, db_manager, email_service,
            counter_manager, error_manager
        )
    else:
        job = job_class(spark, config, db_manager, email_service)

    # Run the job
    try:
        if job_name == "job6_ehrp2biis_update" and args.run_forever:
            metrics = job.run(run_forever=True)
        else:
            metrics = job.run()

        logger.info(f"Job completed: {metrics.status}")
        logger.info(f"Summary:\n{metrics.to_report()}")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
