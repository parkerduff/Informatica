"""Generic SFTP transfer + notification, replacing every ksh ``*_transfer`` script.

The original scripts each: checked the file exists, ``sftp``'d it to
``sa-cdirect@m1csv301.hhs.gov:<agency outbound dir>``, emailed the transfer log
and cleaned up. :func:`transfer_file` performs the upload via paramiko and
:func:`run_transfer` adds the existence check and notification around it.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import List, Optional

from pyspark_etl.config.connections import SFTP_DROPBOX, SftpHost
from pyspark_etl.config.notifications import TRANSFER_RECIPIENTS
from pyspark_etl.utils.notifications import send_notification

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransferConfig:
    """Per-agency transfer destination."""

    name: str
    remote_dir: str
    host: SftpHost = SFTP_DROPBOX


def transfer_file(local_path: str, remote_dir: str,
                  host: Optional[SftpHost] = None) -> str:
    """Upload ``local_path`` to ``remote_dir`` on the SFTP host; return remote path."""
    import paramiko

    sftp_host = host or SFTP_DROPBOX
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connect_kwargs = {
        "hostname": sftp_host.host,
        "port": sftp_host.port,
        "username": sftp_host.user,
    }
    if sftp_host.key_path:
        connect_kwargs["key_filename"] = sftp_host.key_path
    elif sftp_host.password:
        connect_kwargs["password"] = sftp_host.password
    ssh.connect(**connect_kwargs)
    try:
        sftp = ssh.open_sftp()
        remote_path = f"{remote_dir.rstrip('/')}/{os.path.basename(local_path)}"
        sftp.put(local_path, remote_path)
        sftp.close()
        logger.info("Transferred %s -> %s:%s", local_path, sftp_host.host, remote_path)
        return remote_path
    finally:
        ssh.close()


def run_transfer(local_path: str, config: TransferConfig,
                 recipients: Optional[List[str]] = None,
                 dry_run_email: bool = False) -> bool:
    """Existence check -> SFTP upload -> email notification (success or failure)."""
    emails = recipients or TRANSFER_RECIPIENTS
    if not os.path.exists(local_path):
        msg = f"{config.name}: file not found: {local_path}"
        logger.error(msg)
        send_notification(f"{config.name} transfer FAILED - file not found",
                          emails, body=msg, dry_run=dry_run_email)
        return False
    try:
        remote_path = transfer_file(local_path, config.remote_dir, config.host)
    except Exception as exc:  # pragma: no cover - network dependent
        logger.error("%s transfer failed: %s", config.name, exc)
        send_notification(f"{config.name} transfer FAILED", emails,
                          body=str(exc), dry_run=dry_run_email)
        return False
    send_notification(
        f"{config.name} transfer completed successfully", emails,
        body=f"Transferred {local_path} to {config.host.host}:{remote_path}",
        dry_run=dry_run_email,
    )
    return True
