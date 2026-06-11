#!/usr/bin/env python3
"""SFTP transfer + file maintenance.

Consolidates the seven near-identical ``Transfer Scripts/`` and the
``Maintenance Scripts/archive_files`` / ``remove_file`` shell scripts into a
single parameterised module. Remote routes come from config (never hardcoded)
and host keys are verified (RejectPolicy, not AutoAddPolicy).
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
from typing import Optional

from utils.notifications import send_notification
from utils.secrets import Config, load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("transfers")


def _client(config: Config):  # pragma: no cover
    import paramiko

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    known_hosts = os.environ.get("BIIS_KNOWN_HOSTS")
    if known_hosts and os.path.exists(known_hosts):
        client.load_host_keys(known_hosts)
    # Verify host keys: reject unknown hosts rather than silently trusting them.
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(
        hostname=config.sftp.host,
        port=config.sftp.port,
        username=config.sftp.user,
        password=config.sftp.password,
    )
    return client


def transfer_file(agency: str, filename: str, config: Config, local_dir: Optional[str] = None) -> str:
    """Upload ``filename`` to the remote route configured for ``agency``."""
    if agency not in config.sftp.routes:
        raise KeyError(f"No SFTP route configured for agency '{agency}'")
    local_dir = local_dir or config.paths.staging
    local_path = os.path.join(local_dir, filename)
    if not os.path.exists(local_path):
        send_notification(
            f"SFTP {agency}: FAILED", f"File not found: {local_path}", config
        )
        raise FileNotFoundError(local_path)

    remote_dir = config.sftp.routes[agency]
    remote_path = f"{remote_dir.rstrip('/')}/{filename}"
    client = _client(config)
    try:
        sftp = client.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
    finally:
        client.close()

    send_notification(
        f"SFTP {agency}: complete", f"Transferred {filename} to {remote_path}", config
    )
    logger.info("transferred %s -> %s", local_path, remote_path)
    return remote_path


def archive_files(input_dir: str, dest_dir: str, pay_period: str) -> int:
    """Move all files from ``input_dir`` to ``dest_dir`` with a PP suffix."""
    os.makedirs(dest_dir, exist_ok=True)
    moved = 0
    for name in os.listdir(input_dir):
        src = os.path.join(input_dir, name)
        if not os.path.isfile(src):
            continue
        base, ext = os.path.splitext(name)
        dst = os.path.join(dest_dir, f"{base}_{pay_period}{ext}")
        shutil.move(src, dst)
        moved += 1
    logger.info("archived %d files from %s to %s", moved, input_dir, dest_dir)
    return moved


def remove_file(input_dir: str, filename: str) -> bool:
    """Delete a file if present; return True if removed, False if absent."""
    path = os.path.join(input_dir, filename)
    if os.path.exists(path):
        os.remove(path)
        logger.info("removed %s", path)
        return True
    logger.info("remove_file: %s not present (skipped)", path)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", default=None)
    ap.add_argument("--agency", required=True)
    ap.add_argument("--filename", required=True)
    args = ap.parse_args()
    transfer_file(args.agency, args.filename, load_config(args.env))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
