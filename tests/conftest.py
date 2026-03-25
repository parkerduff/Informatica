"""
Shared fixtures and constants for BIISINT Informatica test suite.
"""
import os
import pytest

# Repository root directory
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Directory paths
XML_DIR = os.path.join(REPO_ROOT, "XML")
TRANSFER_SCRIPTS_DIR = os.path.join(REPO_ROOT, "Transfer Scripts")
MAINTENANCE_SCRIPTS_DIR = os.path.join(REPO_ROOT, "Maintenance Scripts")

# Expected XML files (Informatica PowerCenter mappings)
EXPECTED_XML_FILES = [
    "EHRP2BIIS_UPDATE",
    "CPM",
    "CPM_NIH",
    "CPM_OIG",
    "CPM_CDC",
    "CPM_AFPS",
    "LES",
    "Pseudossn",
    "Pay_Calendar",
    "COMPTIME",
    "FDA_Leave",
]

# Expected transfer scripts
EXPECTED_TRANSFER_SCRIPTS = [
    "nih_cpm_transfer",
    "nih_les_transfer",
    "nih_transfer_les",
    "oig_transfer",
    "fda_transfer",
    "cdc_transfer",
    "afps_transfer",
]

# Expected maintenance scripts
EXPECTED_MAINTENANCE_SCRIPTS = [
    "remove_file",
    "archive_files",
]

# Expected shell scripts at repo root
EXPECTED_ROOT_SHELL_SCRIPTS = [
    "ehrp2biis_preload",
    "actstage_load",
]

# Expected SQL files at repo root
EXPECTED_SQL_FILES = [
    "ehrp2biis_afterload.sql",
]

# All shell scripts (transfer + maintenance + root)
ALL_SHELL_SCRIPTS = (
    [os.path.join(TRANSFER_SCRIPTS_DIR, s) for s in EXPECTED_TRANSFER_SCRIPTS]
    + [os.path.join(MAINTENANCE_SCRIPTS_DIR, s) for s in EXPECTED_MAINTENANCE_SCRIPTS]
    + [os.path.join(REPO_ROOT, s) for s in EXPECTED_ROOT_SHELL_SCRIPTS]
)

# All XML file paths
ALL_XML_FILES = [os.path.join(XML_DIR, f) for f in EXPECTED_XML_FILES]

# Agency-specific CPM files
AGENCY_CPM_FILES = ["CPM_NIH", "CPM_OIG", "CPM_CDC", "CPM_AFPS"]

# SFTP server hostname
SFTP_SERVER = "m1csv301.hhs.gov"

# SFTP connection account
SFTP_ACCOUNT = "sa-cdirect"

# Expected agency SFTP target directories
EXPECTED_SFTP_TARGETS = {
    "nih_cpm_transfer": "/opt/app/jail/sa-nihbiisu/outbound",
    "nih_les_transfer": "/opt/app/jail/sa-nihbiisu/outbound",
    "nih_transfer_les": "/opt/app/jail/sa-nihbiisu/outbound",
    "oig_transfer": "/opt/app/jail/sa-oig/outbound",
    "fda_transfer": "/opt/app/jail/sa-fdausr2/outbound",
    "cdc_transfer": "/opt/app/jail/sa-cdcusr/outbound",
    "afps_transfer": "/opt/app/jail/sa-afps/outbound",
}

# Expected email notification domains
EXPECTED_EMAIL_DOMAINS = ["hhs.gov", "psc.hhs.gov"]

# Informatica PowerCenter expected XML hierarchy
POWERMART_HIERARCHY = ["POWERMART", "REPOSITORY", "FOLDER"]

# Expected Informatica transformation types
EXPECTED_TRANSFORMATION_TYPES = [
    "Source Qualifier",
    "Expression",
    "Filter",
    "Lookup Procedure",
    "Update Strategy",
]


@pytest.fixture
def repo_root():
    """Return the repository root path."""
    return REPO_ROOT


@pytest.fixture
def xml_dir():
    """Return the XML directory path."""
    return XML_DIR


@pytest.fixture
def transfer_scripts_dir():
    """Return the Transfer Scripts directory path."""
    return TRANSFER_SCRIPTS_DIR


@pytest.fixture
def maintenance_scripts_dir():
    """Return the Maintenance Scripts directory path."""
    return MAINTENANCE_SCRIPTS_DIR
