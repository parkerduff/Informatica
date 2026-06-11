"""SFTP transfers — migration of the Transfer Scripts (nih_cpm_transfer etc.)
and Maintenance Scripts (archive_files, remove_file)."""
import argparse
import logging
import os
import shutil
import sys

import paramiko

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.notifications import send_notification  # noqa: E402
from utils.secrets import load_config  # noqa: E402

logger = logging.getLogger("sftp_transfer")


def _connect(config: dict) -> paramiko.SFTPClient:
    sftp_cfg = config["sftp"]
    client = paramiko.SSHClient()
    known_hosts = os.path.expanduser("~/.ssh/known_hosts")
    if os.path.exists(known_hosts):
        client.load_host_keys(known_hosts)
    # RejectPolicy by design: host keys must be provisioned via known_hosts
    # (e.g. ssh-keyscan during environment setup); never auto-trust.
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(
        hostname=sftp_cfg["host"],
        port=int(sftp_cfg["port"]),
        username=sftp_cfg["user"],
        password=sftp_cfg["password"],
    )
    return client.open_sftp()


def transfer_file(agency: str, filename: str, config: dict) -> str:
    """Send a staged agency file to its SFTP route; mirrors the legacy
    behaviour of aborting with a notification if the file does not exist."""
    routes = config["sftp"]["routes"]
    if agency not in routes:
        raise ValueError(f"Unknown agency route: {agency}")
    local_path = os.path.join(config["paths"]["staging"], filename)
    if not os.path.exists(local_path):
        send_notification(
            f"Aborting: File {filename} not found!",
            f"No such file at {local_path}, exiting...",
            config,
        )
        raise FileNotFoundError(local_path)
    remote_dir = routes[agency]
    remote_path = f"{remote_dir}/{filename}"
    sftp = _connect(config)
    try:
        sftp.put(local_path, remote_path)
    finally:
        sftp.close()
    send_notification(
        f"File {filename} transferred successfully",
        f"Transferred {local_path} -> {agency}:{remote_path}",
        config,
    )
    return remote_path


def archive_files(input_dir: str, dest_dir: str, pay_period: str, config: dict = None) -> list:
    """Move every file from input_dir to dest_dir, suffixing _P<pay_period>."""
    os.makedirs(dest_dir, exist_ok=True)
    moved = []
    for fname in sorted(os.listdir(input_dir)):
        src = os.path.join(input_dir, fname)
        if not os.path.isfile(src):
            continue
        prefix, ext = os.path.splitext(fname)
        ext = ext or ".txt"
        dest = os.path.join(dest_dir, f"{prefix}_P{pay_period}{ext}")
        shutil.move(src, dest)
        moved.append(dest)
        logger.info("Archived %s -> %s", src, dest)
    return moved


def remove_file(input_dir: str, filename: str) -> bool:
    path = os.path.join(input_dir, filename)
    if os.path.isfile(path):
        os.remove(path)
        logger.info("Removed %s", path)
        return True
    logger.info("File %s not present; nothing to remove", path)
    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=None)
    parser.add_argument("--agency", required=True)
    parser.add_argument("--filename", required=True)
    args = parser.parse_args()
    config = load_config(args.env)
    transfer_file(args.agency, args.filename, config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
