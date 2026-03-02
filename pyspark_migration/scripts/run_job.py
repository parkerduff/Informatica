"""
CLI entry point for running individual PySpark migration jobs.

Usage:
    python -m pyspark_migration.scripts.run_job --job pay_calendar
    python -m pyspark_migration.scripts.run_job --job comptime --source-file /path/to/U0287D01
    python -m pyspark_migration.scripts.run_job --job cpm_nih --pp-end-year 2025 --pp-num 12
    python -m pyspark_migration.scripts.run_job --job all

Replaces Informatica PowerCenter workflow execution via pmcmd/pmrep commands.
"""

import argparse
import json
import logging
import sys
from datetime import datetime

from pyspark_migration.common.config import MigrationConfig
from pyspark_migration.common.counter_error import CounterErrorManager
from pyspark_migration.common.db_manager import DatabaseManager
from pyspark_migration.common.email_service import EmailService
from pyspark_migration.common.logging_utils import JobMetrics
from pyspark_migration.common.spark_session import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

JOB_NAMES = [
    "pay_calendar",
    "comptime",
    "pseudossn",
    "fda_leave",
    "cpm_nih",
    "cpm_cdc",
    "ehrp2biis",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PySpark migration jobs"
    )
    parser.add_argument(
        "--job",
        required=True,
        choices=JOB_NAMES + ["all"],
        help="Job to run (or 'all' for all jobs in order)",
    )
    parser.add_argument("--pp-end-year", help="Pay period end year parameter")
    parser.add_argument("--pp-num", help="Pay period number parameter")
    parser.add_argument("--source-file", help="Source file path (for file-based jobs)")
    parser.add_argument(
        "--output-metrics",
        default="/tmp/job_metrics.json",
        help="Path to write metrics JSON",
    )
    return parser.parse_args()


def run_job(job_name: str, args: argparse.Namespace) -> JobMetrics:
    """Run a single job by name."""
    config = MigrationConfig()
    spark = create_spark_session(config.spark, app_name_suffix=job_name)
    db_manager = DatabaseManager(spark, config.oracle, config.spark)
    email_service = EmailService(config.email)
    counter_manager = CounterErrorManager(spark, db_manager)

    try:
        if job_name == "pay_calendar":
            from pyspark_migration.jobs.job1_pay_calendar import PayCalendarJob
            job = PayCalendarJob(spark, config, db_manager, email_service)
            return job.run(pp_end_year=args.pp_end_year, pp_num=args.pp_num)

        elif job_name == "comptime":
            from pyspark_migration.jobs.job2_comptime import CompTimeJob
            job = CompTimeJob(spark, config, db_manager, email_service, counter_manager)
            return job.run(source_file_path=args.source_file)

        elif job_name == "pseudossn":
            from pyspark_migration.jobs.job3_pseudossn import PseudossnJob
            job = PseudossnJob(spark, config, db_manager, email_service, counter_manager)
            return job.run()

        elif job_name == "fda_leave":
            from pyspark_migration.jobs.job4_fda_leave import FDALeaveJob
            job = FDALeaveJob(spark, config, db_manager, email_service, counter_manager)
            return job.run(
                pp_end_year=args.pp_end_year,
                pp_num=args.pp_num,
                source_file_path=args.source_file,
            )

        elif job_name == "cpm_nih":
            from pyspark_migration.jobs.job5_cpm import CPMNIHJob
            job = CPMNIHJob(spark, config, db_manager, email_service, counter_manager)
            return job.run(pp_end_year=args.pp_end_year, pp_num=args.pp_num)

        elif job_name == "cpm_cdc":
            from pyspark_migration.jobs.job5_cpm import CPMCDCJob
            job = CPMCDCJob(spark, config, db_manager, email_service, counter_manager)
            return job.run(pp_end_year=args.pp_end_year, pp_num=args.pp_num)

        elif job_name == "ehrp2biis":
            from pyspark_migration.jobs.job6_ehrp2biis import EHRP2BIISUpdateJob
            job = EHRP2BIISUpdateJob(spark, config, db_manager, email_service)
            return job.run()

        else:
            raise ValueError(f"Unknown job: {job_name}")

    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    start_time = datetime.now()

    if args.job == "all":
        jobs_to_run = JOB_NAMES
    else:
        jobs_to_run = [args.job]

    all_metrics = []
    failed_jobs = []

    for job_name in jobs_to_run:
        logger.info("=" * 60)
        logger.info("Starting job: %s", job_name)
        logger.info("=" * 60)

        try:
            metrics = run_job(job_name, args)
            all_metrics.append(metrics.to_dict())
            logger.info("Job %s: %s", job_name, metrics.status)
        except Exception as exc:
            logger.error("Job %s FAILED: %s", job_name, str(exc))
            failed_jobs.append(job_name)

    # Write metrics output
    output = {
        "execution_start": start_time.isoformat(),
        "execution_end": datetime.now().isoformat(),
        "total_jobs": len(jobs_to_run),
        "failed_jobs": failed_jobs,
        "metrics": all_metrics,
    }

    with open(args.output_metrics, "w") as f:
        json.dump(output, f, indent=2, default=str)

    logger.info("Metrics written to %s", args.output_metrics)

    if failed_jobs:
        logger.error("FAILED JOBS: %s", ", ".join(failed_jobs))
        sys.exit(1)
    else:
        logger.info("ALL JOBS COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
