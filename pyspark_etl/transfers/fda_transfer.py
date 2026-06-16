"""Replaces ``Transfer Scripts/fda_transfer`` (-> sa-fdausr2/outbound)."""
from __future__ import annotations

from pyspark_etl.transfers.configs import FDA
from pyspark_etl.transfers.sftp_client import run_transfer


def run(local_path: str, dry_run_email: bool = False) -> bool:
    return run_transfer(local_path, FDA, dry_run_email=dry_run_email)
