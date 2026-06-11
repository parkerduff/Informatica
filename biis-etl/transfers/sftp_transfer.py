"""SFTP transfer + file maintenance (migrated from Transfer/Maintenance Scripts).

Replaces the per-agency ksh ``*_transfer`` scripts and the ``archive_files`` /
``remove_file`` maintenance scripts with a single parameterised module.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional

from utils import notifications

if TYPE_CHECKING:  # pragma: no cover
    from utils.config import Config


@dataclass(frozen=True)
class Route:
    host: str
    account: str
    remote_dir: str


# Agency routing transcribed from the legacy transfer scripts.
AGENCY_ROUTES: Dict[str, Route] = {
    "nih_cpm": Route("m1csv301.hhs.gov", "sa-cdirect", "/opt/app/jail/sa-nihbiisu/outbound"),
    "nih_les": Route("m1csv301.hhs.gov", "sa-cdirect", "/opt/app/jail/sa-nihlesu/outbound"),
    "oig": Route("m1csv301.hhs.gov", "sa-cdirect", "/opt/app/jail/sa-oigbiisu/outbound"),
    "cdc": Route("m1csv301.hhs.gov", "sa-cdirect", "/opt/app/jail/sa-cdcbiisu/outbound"),
    "afps": Route("m1csv301.hhs.gov", "sa-cdirect", "/opt/app/jail/sa-afpsbiisu/outbound"),
    "fda": Route("m1csv301.hhs.gov", "sa-cdirect", "/opt/app/jail/sa-fdabiisu/outbound"),
}


def get_route(agency: str) -> Route:
    if agency not in AGENCY_ROUTES:
        raise ValueError(f"Unknown agency route: {agency!r}")
    return AGENCY_ROUTES[agency]


def _sftp_client(config: "Config", route: Route):  # pragma: no cover - needs SFTP server
    import paramiko  # type: ignore[import-untyped]

    sftp_cfg = (config.as_dict().get("sftp") or {}) if hasattr(config, "as_dict") else {}
    host = sftp_cfg.get("host", route.host)
    port = int(sftp_cfg.get("port", 22))
    user = sftp_cfg.get("user", route.account)
    password = sftp_cfg.get("password")
    key_path = sftp_cfg.get("key_path")

    transport = paramiko.Transport((host, port))
    if key_path:
        transport.connect(username=user, pkey=paramiko.RSAKey.from_private_key_file(key_path))
    else:
        transport.connect(username=user, password=password)
    return paramiko.SFTPClient.from_transport(transport), transport


def transfer_file(agency: str, local_path: str, config: "Config",
                  remote_dir: Optional[str] = None) -> str:
    """SFTP ``local_path`` to the agency's outbound directory.

    Returns the remote path on success.  Raises ``FileNotFoundError`` (and
    sends a notification) if the local file is missing.
    """
    route = get_route(agency)
    if not os.path.exists(local_path):
        notifications.send_notification(
            f"Aborting: File {local_path} not found!",
            f"No such file at {local_path}; transfer for {agency} aborted.",
            config,
        )
        raise FileNotFoundError(local_path)

    target_dir = remote_dir or route.remote_dir
    remote_path = f"{target_dir.rstrip('/')}/{os.path.basename(local_path)}"

    client, transport = _sftp_client(config, route)  # pragma: no cover
    try:  # pragma: no cover
        client.put(local_path, remote_path)
    finally:  # pragma: no cover
        client.close()
        transport.close()

    notifications.send_notification(  # pragma: no cover
        f"File {local_path} transferred to {route.host} successfully",
        f"Transferred {local_path} -> {remote_path}.",
        config,
    )
    return remote_path  # pragma: no cover


def archive_file(local_path: str, dest_dir: str, pay_period: str) -> str:
    """Move ``file.txt`` to ``dest_dir/file_P<pay_period>.txt`` (archive_files)."""
    os.makedirs(dest_dir, exist_ok=True)
    base = os.path.basename(local_path)
    prefix = base[:-4] if base.endswith(".txt") else base
    dest = os.path.join(dest_dir, f"{prefix}_P{pay_period}.txt")
    os.replace(local_path, dest)
    return dest


def remove_file(path: str) -> bool:
    """Remove ``path`` if it exists; no-op otherwise (remove_file)."""
    if os.path.exists(path):
        os.remove(path)
        return True
    return False
