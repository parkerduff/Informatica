"""Replaces ``Transfer Scripts/nih_cpm_transfer`` (-> sa-nihbiisu/outbound)."""
from __future__ import annotations

from pyspark_etl.transfers.configs import NIH_CPM
from pyspark_etl.transfers.sftp_client import run_transfer


def run(local_path: str, dry_run_email: bool = False) -> bool:
    return run_transfer(local_path, NIH_CPM, dry_run_email=dry_run_email)
