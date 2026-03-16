"""
File Transfers

Replaces all ksh SFTP transfer scripts with Python functions using
common/transfer.py:

Transfer Scripts replaced:
- Transfer Scripts/nih_cpm_transfer
- Transfer Scripts/nih_les_transfer
- Transfer Scripts/oig_transfer
- Transfer Scripts/fda_transfer
- Transfer Scripts/cdc_transfer (CDC-specific directory)
- Transfer Scripts/afps_transfer (AFPS-specific directory)

Also implements archive_files() and remove_old_files() for maintenance.
"""

import argparse
import glob
import logging
import os
import shutil
from datetime import datetime, timedelta

from pyspark_migration.common.notification import send_failure_email, send_success_email
from pyspark_migration.common.transfer import validate_and_transfer
from pyspark_migration.config.settings import FILE_PATHS, TRANSFER_DESTINATIONS

logger = logging.getLogger(__name__)


def transfer_nih_les():
    """
    Transfer NIH LES files.

    Source: /data/BIISINT/data/int/out/LES/
    Dest:   /opt/app/jail/sa-nihbiisu/outbound via m1csv301.hhs.gov
    """
    logger.info("Transferring NIH LES files")
    les_dir = FILE_PATHS["les_output_dir"]
    files = glob.glob(os.path.join(les_dir, "*.dat"))

    success = True
    for f in files:
        if not validate_and_transfer(f, "nih"):
            success = False

    if not files:
        logger.warning("No LES files found in %s", les_dir)

    return success


def transfer_nih_cpm():
    """
    Transfer NIH CPM files.

    Source: /data/BIISINT/data/int/out/CPM/
    Dest:   /opt/app/jail/sa-nihbiisu/outbound via m1csv301.hhs.gov
    """
    logger.info("Transferring NIH CPM files")
    cpm_dir = FILE_PATHS["cpm_output_dir"]
    files = glob.glob(os.path.join(cpm_dir, "nih_*"))

    success = True
    for f in files:
        if not validate_and_transfer(f, "nih"):
            success = False

    if not files:
        logger.warning("No NIH CPM files found in %s", cpm_dir)

    return success


def transfer_oig():
    """
    Transfer OIG files.

    Source: /data/BIISINT/data/int/out/CPM/
    Dest:   /opt/app/jail/sa-oig/outbound via m1csv301.hhs.gov
    """
    logger.info("Transferring OIG files")
    cpm_dir = FILE_PATHS["cpm_output_dir"]
    files = glob.glob(os.path.join(cpm_dir, "oig*"))

    success = True
    for f in files:
        if not validate_and_transfer(f, "oig"):
            success = False

    if not files:
        logger.warning("No OIG files found in %s", cpm_dir)

    return success


def transfer_fda():
    """
    Transfer FDA files.

    Source: /data/BIISINT/data/int/out/CPM/
    Dest:   /opt/app/jail/sa-fdausr2/outbound via m1csv301.hhs.gov
    """
    logger.info("Transferring FDA files")
    cpm_dir = FILE_PATHS["cpm_output_dir"]
    files = glob.glob(os.path.join(cpm_dir, "fda*"))

    success = True
    for f in files:
        if not validate_and_transfer(f, "fda"):
            success = False

    if not files:
        logger.warning("No FDA files found in %s", cpm_dir)

    return success


def transfer_cdc():
    """
    Transfer CDC files.

    Source: /data/BIISINT/data/int/out/CPM/
    Dest:   CDC-specific jail directory via m1csv301.hhs.gov
    """
    logger.info("Transferring CDC files")
    cpm_dir = FILE_PATHS["cpm_output_dir"]
    files = glob.glob(os.path.join(cpm_dir, "cdc*"))

    success = True
    for f in files:
        if not validate_and_transfer(f, "cdc"):
            success = False

    if not files:
        logger.warning("No CDC files found in %s", cpm_dir)

    return success


def transfer_afps():
    """
    Transfer AFPS files.

    Source: /data/BIISINT/data/int/out/CPM/
    Dest:   AFPS-specific jail directory via m1csv301.hhs.gov
    """
    logger.info("Transferring AFPS files")
    cpm_dir = FILE_PATHS["cpm_output_dir"]
    files = glob.glob(os.path.join(cpm_dir, "afps*"))

    success = True
    for f in files:
        if not validate_and_transfer(f, "afps"):
            success = False

    if not files:
        logger.warning("No AFPS files found in %s", cpm_dir)

    return success


def archive_files():
    """
    Archive processed output files.

    Moves files from output directories to timestamped archive subdirectories
    for audit and recovery purposes.
    """
    logger.info("Archiving processed files")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dirs = [FILE_PATHS["cpm_output_dir"], FILE_PATHS["les_output_dir"]]

    for output_dir in output_dirs:
        if not os.path.exists(output_dir):
            continue

        archive_dir = os.path.join(output_dir, "archive", timestamp)
        os.makedirs(archive_dir, exist_ok=True)

        for f in glob.glob(os.path.join(output_dir, "*")):
            if os.path.isfile(f):
                dest = os.path.join(archive_dir, os.path.basename(f))
                shutil.move(f, dest)
                logger.info("Archived: %s -> %s", f, dest)

    logger.info("File archival complete")


def remove_old_files(days_to_keep=30):
    """
    Remove old archived files beyond the retention period.

    Parameters
    ----------
    days_to_keep : int
        Number of days to retain archived files.
    """
    logger.info("Removing files older than %d days", days_to_keep)

    cutoff = datetime.now() - timedelta(days=days_to_keep)
    output_dirs = [FILE_PATHS["cpm_output_dir"], FILE_PATHS["les_output_dir"]]

    removed = 0
    for output_dir in output_dirs:
        archive_base = os.path.join(output_dir, "archive")
        if not os.path.exists(archive_base):
            continue

        for archive_subdir in os.listdir(archive_base):
            archive_path = os.path.join(archive_base, archive_subdir)
            if not os.path.isdir(archive_path):
                continue

            try:
                dir_time = datetime.strptime(archive_subdir, "%Y%m%d_%H%M%S")
                if dir_time < cutoff:
                    shutil.rmtree(archive_path)
                    logger.info("Removed old archive: %s", archive_path)
                    removed += 1
            except ValueError:
                continue

    logger.info("Removed %d old archive directories", removed)


def run_all_transfers():
    """Execute all file transfers sequentially."""
    logger.info("=" * 60)
    logger.info("Starting All File Transfers")
    logger.info("=" * 60)

    results = {
        "nih_les": transfer_nih_les(),
        "nih_cpm": transfer_nih_cpm(),
        "oig": transfer_oig(),
        "fda": transfer_fda(),
        "cdc": transfer_cdc(),
        "afps": transfer_afps(),
    }

    failures = [k for k, v in results.items() if not v]
    if failures:
        logger.error("Transfer failures: %s", failures)
        send_failure_email(
            "File Transfers",
            f"The following transfers failed: {', '.join(failures)}",
        )
    else:
        logger.info("All file transfers completed successfully")
        send_success_email(
            "File Transfers",
            "All file transfers completed successfully.",
        )

    return results


def run_maintenance():
    """Execute file maintenance (archive and cleanup)."""
    logger.info("=" * 60)
    logger.info("Starting File Maintenance")
    logger.info("=" * 60)

    archive_files()
    remove_old_files()

    logger.info("File maintenance complete")


def main():
    """CLI entry point for file transfers."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="BIIS File Transfers")
    parser.add_argument(
        "--action",
        type=str,
        choices=["all", "nih_les", "nih_cpm", "oig", "fda", "cdc", "afps",
                 "archive", "cleanup", "maintenance"],
        default="all",
        help="Transfer action to execute",
    )
    parser.add_argument("--days-to-keep", type=int, default=30,
                        help="Days to keep archived files (for cleanup)")
    args = parser.parse_args()

    if args.action == "all":
        run_all_transfers()
    elif args.action == "nih_les":
        transfer_nih_les()
    elif args.action == "nih_cpm":
        transfer_nih_cpm()
    elif args.action == "oig":
        transfer_oig()
    elif args.action == "fda":
        transfer_fda()
    elif args.action == "cdc":
        transfer_cdc()
    elif args.action == "afps":
        transfer_afps()
    elif args.action == "archive":
        archive_files()
    elif args.action == "cleanup":
        remove_old_files(days_to_keep=args.days_to_keep)
    elif args.action == "maintenance":
        run_maintenance()


if __name__ == "__main__":
    main()
