"""Per-agency SFTP destinations (remote outbound dirs taken from the ksh scripts).

All transfers go to ``sa-cdirect@m1csv301.hhs.gov`` (config.connections.SFTP_DROPBOX);
only the remote outbound directory differs per agency.
"""
from __future__ import annotations

from pyspark_etl.transfers.sftp_client import TransferConfig

NIH_CPM = TransferConfig(name="NIH_CPM", remote_dir="/opt/app/jail/sa-nihbiisu/outbound")
NIH_LES = TransferConfig(name="NIH_LES", remote_dir="/opt/app/jail/sa-nihbiisu/outbound")
CDC = TransferConfig(name="CDC", remote_dir="/opt/app/jail/sa-cdcusr/outbound")
OIG = TransferConfig(name="OIG", remote_dir="/opt/app/jail/sa-oig/outbound")
FDA = TransferConfig(name="FDA", remote_dir="/opt/app/jail/sa-fdausr2/outbound")
AFPS = TransferConfig(name="AFPS", remote_dir="/opt/app/jail/sa-afps/outbound")

CONFIGS = {c.name: c for c in (NIH_CPM, NIH_LES, CDC, OIG, FDA, AFPS)}
