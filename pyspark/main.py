"""
BIISINT PySpark ETL Pipeline - Main Entry Point.

This module provides a unified CLI to run any of the converted
Informatica PowerCenter workflows as PySpark jobs.

Usage:
    python -m pyspark.main <pipeline> [options]

Pipelines:
    pay_calendar   - Set/verify current pay period
    comptime       - Load compensatory time daily records
    ehrp2biis      - EHRP to BIIS personnel action pipeline
    cpm            - Core CPM payroll staging
    cpm_nih        - NIH-specific CPM processing
    cpm_oig        - OIG-specific CPM processing
    cpm_cdc        - CDC-specific CPM processing
    cpm_afps       - AFPS-specific CPM processing
    les            - Leave and Earnings Statement processing
    pseudossn      - PseudoSSN management
    fda_leave      - FDA leave validation
"""

import argparse
import logging

from pyspark.utils.config import AppConfig


def main():
    parser = argparse.ArgumentParser(
        description="BIISINT PySpark ETL Pipeline Runner",
    )
    parser.add_argument(
        "pipeline",
        choices=[
            "pay_calendar", "comptime", "ehrp2biis",
            "cpm", "cpm_nih", "cpm_oig", "cpm_cdc", "cpm_afps",
            "les", "pseudossn", "fda_leave",
        ],
        help="Pipeline to execute",
    )
    parser.add_argument(
        "--input-file",
        help="Override input file path (for pipelines that read flat files)",
    )
    parser.add_argument(
        "--input-dir",
        help="Override input directory path",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level))
    config = AppConfig()

    # Build keyword arguments for the pipeline runner
    kwargs = {"config": config}
    if args.input_file:
        kwargs["input_file"] = args.input_file
    if args.input_dir:
        kwargs["input_dir"] = args.input_dir

    pipeline_map = {
        "pay_calendar": "pyspark.pipelines.pay_calendar",
        "comptime": "pyspark.pipelines.comptime",
        "ehrp2biis": "pyspark.pipelines.ehrp2biis",
        "cpm": "pyspark.pipelines.cpm",
        "cpm_nih": "pyspark.pipelines.cpm_nih",
        "cpm_oig": "pyspark.pipelines.cpm_oig",
        "cpm_cdc": "pyspark.pipelines.cpm_cdc",
        "cpm_afps": "pyspark.pipelines.cpm_afps",
        "les": "pyspark.pipelines.les",
        "pseudossn": "pyspark.pipelines.pseudossn",
        "fda_leave": "pyspark.pipelines.fda_leave",
    }

    module_name = pipeline_map[args.pipeline]

    # Import and run the pipeline
    import importlib
    module = importlib.import_module(module_name)

    # Filter kwargs to only those accepted by the run() function
    import inspect
    sig = inspect.signature(module.run)
    valid_kwargs = {
        k: v for k, v in kwargs.items() if k in sig.parameters
    }

    module.run(**valid_kwargs)


if __name__ == "__main__":
    main()
