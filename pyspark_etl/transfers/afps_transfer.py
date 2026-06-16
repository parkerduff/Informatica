"""Replaces ``Transfer Scripts/afps_transfer`` (-> sa-afps/outbound)."""
from __future__ import annotations

from pyspark_etl.transfers.configs import AFPS
from pyspark_etl.transfers.sftp_client import run_transfer


def run(local_path: str, dry_run_email: bool = False) -> bool:
    return run_transfer(local_path, AFPS, dry_run_email=dry_run_email)
