"""
SFTP File Transfer Utilities for BIISINT.

Replaces all KSH transfer scripts:
  - Transfer Scripts/nih_cpm_transfer
  - Transfer Scripts/nih_les_transfer
  - Transfer Scripts/nih_transfer_les
  - Transfer Scripts/oig_transfer
  - Transfer Scripts/fda_transfer
  - Transfer Scripts/afps_transfer
  - Transfer Scripts/cdc_transfer

Each original script:
  1. Validates the file exists
  2. Transfers via SFTP to m1csv301.hhs.gov
  3. Sends email notification on success/failure

This module provides a generic transfer function and agency-specific
wrappers that match the original scripts' behavior.
"""

import logging
import os
import subprocess
from datetime import datetime
from typing import Optional

from pyspark.utils.config import AppConfig, SFTPConfig
from pyspark.utils.notifications import send_email

logger = logging.getLogger(__name__)


def sftp_transfer(
    sftp_config: SFTPConfig,
    local_file: str,
    remote_directory: str,
) -> bool:
    """Transfer a file via SFTP to the remote server.

    Replaces the inline SFTP heredoc in the KSH scripts:
        /usr/bin/sftp sa-cdirect@m1csv301.hhs.gov <<EOF
        cd /opt/app/jail/sa-xxxxx/outbound
        put $FNAME
        quit
        EOF

    Args:
        sftp_config: SFTP connection configuration.
        local_file: Absolute path to the local file.
        remote_directory: Remote directory path on the SFTP server.

    Returns:
        True if the transfer succeeded, False otherwise.
    """
    logger.info(
        "Transferring %s to %s:%s",
        local_file, sftp_config.host, remote_directory,
    )

    # Build SFTP batch commands
    sftp_commands = f"cd {remote_directory}\nput {local_file}\nquit\n"

    # Build the sftp command
    sftp_cmd = ["sftp"]

    if sftp_config.key_file:
        sftp_cmd.extend(["-i", sftp_config.key_file])

    sftp_cmd.append(f"{sftp_config.user}@{sftp_config.host}")

    try:
        result = subprocess.run(
            sftp_cmd,
            input=sftp_commands,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode == 0:
            logger.info("SFTP transfer completed successfully")
            return True
        else:
            logger.error(
                "SFTP transfer failed (rc=%d): %s",
                result.returncode, result.stderr,
            )
            return False

    except subprocess.TimeoutExpired:
        logger.error("SFTP transfer timed out after 300 seconds")
        return False
    except Exception:
        logger.exception("SFTP transfer error")
        return False


def transfer_file(
    config: AppConfig,
    filename: str,
    source_dir: str,
    remote_directory: str,
    agency_name: str,
    recipients: Optional[list] = None,
) -> bool:
    """Transfer a file with validation and email notification.

    This is the generic transfer function that replaces the common
    pattern in all KSH transfer scripts.

    Args:
        config: Application configuration.
        filename: Name of the file to transfer.
        source_dir: Local directory containing the file.
        remote_directory: Remote SFTP directory.
        agency_name: Agency name for logging and notifications.
        recipients: Override list of email recipients.

    Returns:
        True if the transfer succeeded, False otherwise.
    """
    if recipients is None:
        recipients = config.email.transfer_recipients

    local_file = os.path.join(source_dir, filename)
    timestamp = datetime.now().strftime("%m/%d/%Y at %H:%M:%S")

    # Validate file exists (replaces: if [ ! -e $FNAME ])
    if not os.path.exists(local_file):
        error_msg = f"No such file at {local_file} exiting..."
        logger.error(error_msg)
        send_email(
            config.email,
            subject=f"Aborting:File {local_file} not found!",
            body=error_msg,
            recipients=recipients,
        )
        return False

    # Log transfer start
    transfer_log = (
        f"Transferring file {local_file} to {config.sftp.host} "
        f"({agency_name} account) on {timestamp}"
    )
    logger.info(transfer_log)

    # Perform SFTP transfer
    success = sftp_transfer(config.sftp, local_file, remote_directory)

    if success:
        completion_msg = (
            f"Transfer to {config.sftp.host} (Dropbox) is now completed "
            f"on {datetime.now().strftime('%m/%d/%Y at %H:%M:%S')}"
        )
        logger.info(completion_msg)

        full_log = f"{transfer_log}\n{completion_msg}"
        send_email(
            config.email,
            subject=(
                f"File {local_file} transferred to "
                f"{config.sftp.host} server successfully"
            ),
            body=full_log,
            recipients=recipients,
        )
    else:
        send_email(
            config.email,
            subject=f"SFTP transfer of {filename} to {agency_name} failed!",
            body=f"Transfer of {local_file} to {agency_name} failed.",
            recipients=recipients,
        )

    return success


# ---------------------------------------------------------------------------
# Agency-specific transfer functions
# Each replaces one KSH transfer script
# ---------------------------------------------------------------------------

def nih_cpm_transfer(config: AppConfig, filename: str) -> bool:
    """Transfer a CPM file to NIH.

    Replaces: Transfer Scripts/nih_cpm_transfer
    """
    return transfer_file(
        config,
        filename=filename,
        source_dir=config.paths.cpm_output_dir,
        remote_directory=config.sftp.nih_outbound,
        agency_name="sa-nihbiisu (NIH CPM)",
    )


def nih_les_transfer(config: AppConfig, filename: str) -> bool:
    """Transfer a LES file to NIH.

    Replaces: Transfer Scripts/nih_les_transfer
    """
    return transfer_file(
        config,
        filename=filename,
        source_dir=config.paths.les_output_dir,
        remote_directory=config.sftp.nih_outbound,
        agency_name="sa-nihlesusr2 (NIH LES)",
    )


def nih_transfer_les(config: AppConfig, filename: str) -> bool:
    """Transfer a LES file to NIH (alternate path).

    Replaces: Transfer Scripts/nih_transfer_les

    Note: This script uses different recipients (PSC-specific) and
    transfers to the same sa-nihbiisu outbound directory.
    """
    psc_recipients = [
        "lloyd.hamilton@psc.hhs.gov",
        "mariappan.muthiah@hhs.gov",
        "karen.williams@psc.hhs.gov",
        "marvin.simon@psc.hhs.gov",
        "robin.cunningham@hhs.gov",
        "minh.tran@hhs.gov",
    ]
    return transfer_file(
        config,
        filename=filename,
        source_dir=config.paths.les_output_dir,
        remote_directory=config.sftp.nih_outbound,
        agency_name="sa-nihbiisu (NIH)",
        recipients=psc_recipients,
    )


def oig_transfer(config: AppConfig, filename: str) -> bool:
    """Transfer a file to OIG.

    Replaces: Transfer Scripts/oig_transfer
    """
    return transfer_file(
        config,
        filename=filename,
        source_dir=config.paths.cpm_output_dir,
        remote_directory=config.sftp.oig_outbound,
        agency_name="sa-oig (OIG)",
    )


def fda_transfer(config: AppConfig, filename: str) -> bool:
    """Transfer a file to FDA.

    Replaces: Transfer Scripts/fda_transfer
    """
    return transfer_file(
        config,
        filename=filename,
        source_dir=config.paths.cpm_output_dir,
        remote_directory=config.sftp.fda_outbound,
        agency_name="sa-fdausr2 (FDA)",
    )


def afps_transfer(config: AppConfig, filename: str) -> bool:
    """Transfer a file to AFPS.

    Replaces: Transfer Scripts/afps_transfer
    """
    return transfer_file(
        config,
        filename=filename,
        source_dir=config.paths.cpm_output_dir,
        remote_directory=config.sftp.afps_outbound,
        agency_name="sa-afps (AFPS)",
    )


def cdc_transfer(config: AppConfig, filename: str) -> bool:
    """Transfer a file to CDC.

    Replaces: Transfer Scripts/cdc_transfer
    """
    return transfer_file(
        config,
        filename=filename,
        source_dir=config.paths.cpm_output_dir,
        remote_directory=config.sftp.cdc_outbound,
        agency_name="sa-cdcusr (CDC)",
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    """Command-line interface for file transfers.

    Usage:
        python -m pyspark.transfers.sftp_transfer <agency> <filename>

    Where <agency> is one of:
        nih_cpm, nih_les, nih_les_alt, oig, fda, afps, cdc
    """
    import sys

    if len(sys.argv) < 3:
        print(
            "Usage: python -m pyspark.transfers.sftp_transfer "
            "<agency> <filename>"
        )
        print("Agencies: nih_cpm, nih_les, nih_les_alt, oig, fda, afps, cdc")
        sys.exit(1)

    agency = sys.argv[1].lower()
    filename = sys.argv[2]
    config = AppConfig()

    transfer_map = {
        "nih_cpm": nih_cpm_transfer,
        "nih_les": nih_les_transfer,
        "nih_les_alt": nih_transfer_les,
        "oig": oig_transfer,
        "fda": fda_transfer,
        "afps": afps_transfer,
        "cdc": cdc_transfer,
    }

    if agency not in transfer_map:
        print(f"Unknown agency: {agency}")
        print(f"Valid agencies: {', '.join(transfer_map.keys())}")
        sys.exit(1)

    success = transfer_map[agency](config, filename)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
