"""
SFTP File Transfer

Replaces ksh SFTP scripts:
- Transfer Scripts/nih_cpm_transfer
- Transfer Scripts/nih_les_transfer
- Transfer Scripts/oig_transfer
- Transfer Scripts/fda_transfer
- Transfer Scripts/cdc_transfer
- Transfer Scripts/afps_transfer
"""

import logging
import os
import stat

import paramiko

from pyspark_migration.config.settings import SFTP_CONFIG, TRANSFER_DESTINATIONS

logger = logging.getLogger(__name__)


def sftp_transfer(local_path, remote_host=None, remote_path=None,
                  jail_dir=None, destination_key=None):
    """
    Transfer a file via SFTP.

    Replaces the standard ksh pattern:
        /usr/bin/sftp sa-cdirect@m1csv301.hhs.gov <<EOF
        cd /opt/app/jail/sa-nihbiisu/outbound
        put $FNAME
        quit
        EOF

    Parameters
    ----------
    local_path : str
        Full path to the local file to transfer.
    remote_host : str, optional
        SFTP server hostname. Defaults to SFTP_CONFIG['host'].
    remote_path : str, optional
        Remote directory path. Takes precedence over jail_dir and destination_key.
    jail_dir : str, optional
        Remote jail directory (deprecated, use remote_path or destination_key).
    destination_key : str, optional
        Key in TRANSFER_DESTINATIONS ('nih', 'oig', 'fda', 'cdc', 'afps').

    Returns
    -------
    bool
        True if transfer succeeded.

    Raises
    ------
    FileNotFoundError
        If local_path does not exist.
    RuntimeError
        If SFTP transfer fails.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    if remote_host is None:
        remote_host = SFTP_CONFIG["host"]

    if remote_path is None:
        if jail_dir:
            remote_path = jail_dir
        elif destination_key:
            remote_path = TRANSFER_DESTINATIONS.get(destination_key)
            if remote_path is None:
                raise ValueError(
                    f"Unknown destination key: {destination_key}. "
                    f"Valid keys: {list(TRANSFER_DESTINATIONS.keys())}"
                )
        else:
            raise ValueError("Must provide remote_path, jail_dir, or destination_key")

    filename = os.path.basename(local_path)
    remote_file = f"{remote_path}/{filename}"

    logger.info(
        "Transferring %s to %s:%s",
        local_path, remote_host, remote_file,
    )

    transport = None
    sftp = None
    try:
        transport = paramiko.Transport((remote_host, SFTP_CONFIG["port"]))

        if SFTP_CONFIG.get("key_file") and os.path.exists(SFTP_CONFIG["key_file"]):
            pkey = paramiko.RSAKey.from_private_key_file(SFTP_CONFIG["key_file"])
            transport.connect(username=SFTP_CONFIG["username"], pkey=pkey)
        else:
            transport.connect(
                username=SFTP_CONFIG["username"],
                password=SFTP_CONFIG.get("password", ""),
            )

        sftp = paramiko.SFTPClient.from_transport(transport)

        sftp.put(local_path, remote_file)

        remote_stat = sftp.stat(remote_file)
        local_size = os.path.getsize(local_path)
        if remote_stat.st_size != local_size:
            raise RuntimeError(
                f"Size mismatch after transfer: local={local_size}, "
                f"remote={remote_stat.st_size}"
            )

        logger.info(
            "Transfer completed successfully: %s -> %s:%s (%d bytes)",
            local_path, remote_host, remote_file, local_size,
        )
        return True

    except Exception:
        logger.exception(
            "SFTP transfer failed: %s -> %s:%s",
            local_path, remote_host, remote_file,
        )
        raise
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def validate_and_transfer(local_path, destination_key, recipients=None):
    """
    Validate file exists, transfer via SFTP, and send notification email.

    This is the complete replacement for the ksh transfer script pattern:
    1. Validate file exists
    2. SFTP transfer
    3. Send success/failure email

    Parameters
    ----------
    local_path : str
    destination_key : str
    recipients : list of str, optional

    Returns
    -------
    bool
    """
    from pyspark_migration.common.notification import send_failure_email, send_success_email
    from pyspark_migration.config.settings import EMAIL_CONFIG

    if recipients is None:
        recipients = EMAIL_CONFIG["transfer_recipients"]

    filename = os.path.basename(local_path)

    if not os.path.exists(local_path):
        msg = f"No such file at {local_path} exiting..."
        logger.error(msg)
        send_failure_email(
            f"Aborting:File {local_path} not found!",
            msg,
            recipients,
        )
        return False

    try:
        sftp_transfer(local_path, destination_key=destination_key)
        send_success_email(
            f"File {filename} transferred successfully",
            f"File {local_path} transferred to {SFTP_CONFIG['host']} "
            f"({destination_key}) successfully.",
            recipients,
        )
        return True
    except Exception as exc:
        send_failure_email(
            f"File {filename} transfer failed",
            str(exc),
            recipients,
        )
        return False
