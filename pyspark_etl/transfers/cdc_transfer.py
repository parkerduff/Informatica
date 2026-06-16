"""Replaces ``Transfer Scripts/cdc_transfer`` (-> sa-cdcusr/outbound)."""
from __future__ import annotations

from pyspark_etl.transfers.configs import CDC
from pyspark_etl.transfers.sftp_client import run_transfer


def run(local_path: str, dry_run_email: bool = False) -> bool:
    return run_transfer(local_path, CDC, dry_run_email=dry_run_email)
