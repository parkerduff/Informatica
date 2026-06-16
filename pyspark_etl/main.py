"""CLI orchestrator for the BIIS / EHRP PySpark ETL jobs.

Examples::

    python -m pyspark_etl.main pseudossn-load --input-file /data/.../sda_file
    python -m pyspark_etl.main ehrp2biis-full
    python -m pyspark_etl.main cpm-nih --pay-period 12 --year 2024
    python -m pyspark_etl.main comptime-load --input-file /data/.../U0287D01
    python -m pyspark_etl.main pay-calendar-set --pay-period 12 --year 2024
    python -m pyspark_etl.main transfer --agency NIH_CPM --file output.txt
    python -m pyspark_etl.main archive --input-dir IN --dest-dir OUT --pay-period 12
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional


def _spark(app: str):
    from pyspark_etl.utils.spark import get_spark

    return get_spark(app)


def _cmd_pseudossn_load(args) -> int:
    from pyspark_etl.jobs.pseudossn import load_from_sda

    load_from_sda.run(_spark("pseudossn"), args.input_file, write=not args.no_write)
    return 0


def _cmd_ehrp2biis_full(args) -> int:
    from pyspark_etl.jobs.ehrp2biis import afterload, preload, update

    if not preload.run(log_dir=args.log_dir, dry_run_email=args.dry_run_email):
        logging.error("Preload failed; aborting ehrp2biis-full")
        return 1
    update.run(_spark("ehrp2biis"), write=not args.no_write)
    ok = afterload.run(log_dir=args.log_dir, dry_run_email=args.dry_run_email)
    return 0 if ok else 1


def _cmd_ehrp2biis_preload(args) -> int:
    from pyspark_etl.jobs.ehrp2biis import preload

    return 0 if preload.run(log_dir=args.log_dir, dry_run_email=args.dry_run_email) else 1


def _cmd_ehrp2biis_update(args) -> int:
    from pyspark_etl.jobs.ehrp2biis import update

    update.run(_spark("ehrp2biis"), write=not args.no_write)
    return 0


def _cmd_ehrp2biis_afterload(args) -> int:
    from pyspark_etl.jobs.ehrp2biis import afterload

    return 0 if afterload.run(log_dir=args.log_dir, dry_run_email=args.dry_run_email) else 1


_CPM_RUNNERS = {
    "cpm-nih": "nih_payroll",
    "cpm-cdc": "cdc_payroll",
    "cpm-oig": "oig_payroll",
    "cpm-afps": "afps_payroll",
}


def _cmd_cpm(args) -> int:
    import importlib

    module = importlib.import_module(f"pyspark_etl.jobs.cpm.{_CPM_RUNNERS[args.command]}")
    module.run(_spark(args.command), args.pay_period, args.year,
               output_dir=args.output_dir, write=not args.no_write)
    return 0


def _cmd_fda_leave(args) -> int:
    from pyspark_etl.jobs.cpm import fda_leave

    fda_leave.run(_spark("fda_leave"), args.input_file, args.output_file,
                  args.batch_id, write=not args.no_write)
    return 0


def _cmd_comptime_load(args) -> int:
    from pyspark_etl.jobs.comptime import load

    load.run(_spark("comptime"), args.input_file, write=not args.no_write)
    return 0


def _cmd_pay_calendar_set(args) -> int:
    from pyspark_etl.jobs.pay_calendar import update

    update.set_pay_calendar(args.pay_period, args.year)
    update.verify_pay_calendar()
    return 0


def _cmd_pay_calendar_reset(args) -> int:
    from pyspark_etl.jobs.pay_calendar import update

    update.reset_pay_calendar()
    return 0


def _cmd_transfer(args) -> int:
    from pyspark_etl.transfers.configs import CONFIGS
    from pyspark_etl.transfers.sftp_client import run_transfer

    cfg = CONFIGS[args.agency]
    ok = run_transfer(args.file, cfg, dry_run_email=args.dry_run_email)
    return 0 if ok else 1


def _cmd_archive(args) -> int:
    from pyspark_etl.maintenance.archive_files import archive_files

    archive_files(args.input_dir, args.dest_dir, args.pay_period)
    return 0


def _cmd_remove(args) -> int:
    from pyspark_etl.maintenance.remove_file import remove_file

    remove_file(args.input_dir, args.filename)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pyspark_etl", description=__doc__)
    parser.add_argument("--log-dir", default=None, help="Directory for run logs")
    parser.add_argument("--dry-run-email", action="store_true",
                        help="Log notifications instead of sending them")
    parser.add_argument("--no-write", action="store_true",
                        help="Run transformations without writing to targets")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("pseudossn-load")
    p.add_argument("--input-file", required=True)
    p.set_defaults(func=_cmd_pseudossn_load)

    sub.add_parser("ehrp2biis-full").set_defaults(func=_cmd_ehrp2biis_full)
    sub.add_parser("ehrp2biis-preload").set_defaults(func=_cmd_ehrp2biis_preload)
    sub.add_parser("ehrp2biis-update").set_defaults(func=_cmd_ehrp2biis_update)
    sub.add_parser("ehrp2biis-afterload").set_defaults(func=_cmd_ehrp2biis_afterload)

    for name in _CPM_RUNNERS:
        p = sub.add_parser(name)
        p.add_argument("--pay-period", type=int, required=True)
        p.add_argument("--year", type=int, required=True)
        p.add_argument("--output-dir", default=None)
        p.set_defaults(func=_cmd_cpm)

    p = sub.add_parser("fda-leave")
    p.add_argument("--input-file", required=True)
    p.add_argument("--output-file", required=True)
    p.add_argument("--batch-id", type=int, required=True)
    p.set_defaults(func=_cmd_fda_leave)

    p = sub.add_parser("comptime-load")
    p.add_argument("--input-file", required=True)
    p.set_defaults(func=_cmd_comptime_load)

    p = sub.add_parser("pay-calendar-set")
    p.add_argument("--pay-period", type=int, default=None)
    p.add_argument("--year", type=int, default=None)
    p.set_defaults(func=_cmd_pay_calendar_set)

    sub.add_parser("pay-calendar-reset").set_defaults(func=_cmd_pay_calendar_reset)

    p = sub.add_parser("transfer")
    p.add_argument("--agency", required=True,
                   choices=["NIH_CPM", "NIH_LES", "CDC", "OIG", "FDA", "AFPS"])
    p.add_argument("--file", required=True)
    p.set_defaults(func=_cmd_transfer)

    p = sub.add_parser("archive")
    p.add_argument("--input-dir", required=True)
    p.add_argument("--dest-dir", required=True)
    p.add_argument("--pay-period", required=True)
    p.set_defaults(func=_cmd_archive)

    p = sub.add_parser("remove")
    p.add_argument("--input-dir", required=True)
    p.add_argument("--filename", required=True)
    p.set_defaults(func=_cmd_remove)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
