"""CLI orchestrator for the BIIS ETL PySpark jobs.

Each subcommand maps to a job module under ``jobs/`` (or ``transfers/``). Those
job modules are implemented by the per-job migration sessions; the imports below
are deferred into each branch so this entrypoint stays importable before the job
modules exist.
"""
import argparse

from pyspark.sql import SparkSession


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BIIS ETL PySpark Jobs")
    subparsers = parser.add_subparsers(dest="job", required=True)

    parser_pseudo = subparsers.add_parser("pseudossn-load")
    parser_pseudo.add_argument("--input-file", required=True)

    subparsers.add_parser("ehrp2biis-full")

    parser_cpm = subparsers.add_parser("cpm-nih")
    parser_cpm.add_argument("--pay-period", required=True)
    parser_cpm.add_argument("--year", required=True)

    parser_transfer = subparsers.add_parser("transfer")
    parser_transfer.add_argument(
        "--agency", choices=["nih", "cdc", "oig", "fda", "afps"], required=True
    )
    parser_transfer.add_argument("--filename", required=True)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    spark = SparkSession.builder.appName("BIIS-ETL-%s" % args.job).getOrCreate()

    if args.job == "pseudossn-load":
        from jobs.pseudossn.load_from_sda import run

        run(spark, args.input_file)
    elif args.job == "ehrp2biis-full":
        from jobs.ehrp2biis.preload import run as preload
        from jobs.ehrp2biis.update import run as update
        from jobs.ehrp2biis.afterload import run as afterload

        preload(spark)
        update(spark)
        afterload(spark)
    elif args.job == "cpm-nih":
        from jobs.cpm.cpm_nih import run

        run(spark, pay_period=args.pay_period, year=args.year)
    elif args.job == "transfer":
        from transfers.transfer import run

        run(spark, agency=args.agency, filename=args.filename)
    else:  # pragma: no cover - argparse enforces a valid subcommand
        parser.error("Unknown job: %s" % args.job)


if __name__ == "__main__":
    main()
