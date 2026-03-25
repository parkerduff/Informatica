"""
Configuration Tests for BIISINT repository.

Validates:
- Expected directory structure exists
- Required configuration files present
- File naming conventions
- Consistent patterns across agency-specific files
- Repository completeness
"""
import os

import pytest

from tests.conftest import (
    AGENCY_CPM_FILES,
    EXPECTED_MAINTENANCE_SCRIPTS,
    EXPECTED_ROOT_SHELL_SCRIPTS,
    EXPECTED_SQL_FILES,
    EXPECTED_TRANSFER_SCRIPTS,
    EXPECTED_XML_FILES,
    MAINTENANCE_SCRIPTS_DIR,
    REPO_ROOT,
    TRANSFER_SCRIPTS_DIR,
    XML_DIR,
)


class TestDirectoryStructure:
    """Verify expected directory structure exists."""

    def test_xml_directory_exists(self):
        """XML/ directory should exist."""
        assert os.path.isdir(XML_DIR), "XML/ directory missing"

    def test_transfer_scripts_directory_exists(self):
        """Transfer Scripts/ directory should exist."""
        assert os.path.isdir(TRANSFER_SCRIPTS_DIR), (
            "Transfer Scripts/ directory missing"
        )

    def test_maintenance_scripts_directory_exists(self):
        """Maintenance Scripts/ directory should exist."""
        assert os.path.isdir(MAINTENANCE_SCRIPTS_DIR), (
            "Maintenance Scripts/ directory missing"
        )

    def test_repo_root_contains_expected_files(self):
        """Repository root should contain expected files."""
        root_files = os.listdir(REPO_ROOT)
        for expected in ["XML", "Transfer Scripts", "Maintenance Scripts",
                         "ehrp2biis_preload", "ehrp2biis_afterload.sql",
                         "actstage_load", "README.md", "LICENSE"]:
            assert expected in root_files, (
                f"Expected {expected} in repo root"
            )

    def test_xml_directory_contains_all_expected_files(self):
        """XML/ should contain all expected mapping files."""
        xml_files = os.listdir(XML_DIR)
        for expected in EXPECTED_XML_FILES:
            assert expected in xml_files, (
                f"Expected {expected} in XML/ directory"
            )

    def test_transfer_scripts_contains_all_expected_files(self):
        """Transfer Scripts/ should contain all expected scripts."""
        scripts = os.listdir(TRANSFER_SCRIPTS_DIR)
        for expected in EXPECTED_TRANSFER_SCRIPTS:
            assert expected in scripts, (
                f"Expected {expected} in Transfer Scripts/ directory"
            )

    def test_maintenance_scripts_contains_all_expected_files(self):
        """Maintenance Scripts/ should contain all expected scripts."""
        scripts = os.listdir(MAINTENANCE_SCRIPTS_DIR)
        for expected in EXPECTED_MAINTENANCE_SCRIPTS:
            assert expected in scripts, (
                f"Expected {expected} in Maintenance Scripts/"
            )


class TestRequiredFiles:
    """Check for required configuration files."""

    @pytest.mark.parametrize("filename", EXPECTED_ROOT_SHELL_SCRIPTS)
    def test_root_shell_scripts_exist(self, filename):
        """Root-level shell scripts should exist."""
        filepath = os.path.join(REPO_ROOT, filename)
        assert os.path.isfile(filepath), f"Missing root script: {filename}"

    @pytest.mark.parametrize("filename", EXPECTED_SQL_FILES)
    def test_sql_files_exist(self, filename):
        """SQL files should exist."""
        filepath = os.path.join(REPO_ROOT, filename)
        assert os.path.isfile(filepath), f"Missing SQL file: {filename}"

    def test_readme_exists(self):
        """README.md should exist."""
        filepath = os.path.join(REPO_ROOT, "README.md")
        assert os.path.isfile(filepath), "README.md missing"

    def test_license_exists(self):
        """LICENSE file should exist."""
        filepath = os.path.join(REPO_ROOT, "LICENSE")
        assert os.path.isfile(filepath), "LICENSE file missing"

    def test_pseudossn_root_file_exists(self):
        """Pseudossn file should exist at repo root."""
        filepath = os.path.join(REPO_ROOT, "Pseudossn")
        assert os.path.isfile(filepath), "Pseudossn file missing at repo root"


class TestFileNamingConventions:
    """Validate file naming conventions."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_files_no_extension(self, xml_file):
        """XML files in XML/ should not have .xml extension (Informatica convention)."""
        filepath = os.path.join(XML_DIR, xml_file)
        assert os.path.exists(filepath), f"{xml_file} missing"
        assert not xml_file.endswith(".xml"), (
            f"XML file {xml_file} should not have .xml extension"
        )

    def test_sql_files_have_sql_extension(self):
        """SQL files should have .sql extension."""
        for sql_file in EXPECTED_SQL_FILES:
            assert sql_file.endswith(".sql"), (
                f"SQL file {sql_file} should have .sql extension"
            )

    @pytest.mark.parametrize("script", EXPECTED_TRANSFER_SCRIPTS)
    def test_transfer_scripts_no_extension(self, script):
        """Transfer scripts should not have file extension (KSH convention)."""
        assert "." not in script, (
            f"Transfer script {script} should not have extension"
        )

    @pytest.mark.parametrize("script", EXPECTED_MAINTENANCE_SCRIPTS)
    def test_maintenance_scripts_no_extension(self, script):
        """Maintenance scripts should not have file extension."""
        assert "." not in script, (
            f"Maintenance script {script} should not have extension"
        )

    def test_agency_cpm_files_follow_pattern(self):
        """Agency CPM files should follow CPM_{AGENCY} naming pattern."""
        for cpm_file in AGENCY_CPM_FILES:
            assert cpm_file.startswith("CPM_"), (
                f"Agency CPM file {cpm_file} should start with CPM_"
            )

    def test_transfer_scripts_follow_agency_pattern(self):
        """Transfer scripts should follow {agency}_transfer or {agency}_{type}_transfer naming."""
        for script in EXPECTED_TRANSFER_SCRIPTS:
            assert "transfer" in script.lower(), (
                f"Transfer script {script} should contain 'transfer' in name"
            )


class TestAgencyConsistency:
    """Check for consistent patterns across agency-specific files."""

    def test_all_agency_cpm_xml_files_present(self):
        """All agency-specific CPM XML files should be present."""
        for agency_file in AGENCY_CPM_FILES:
            filepath = os.path.join(XML_DIR, agency_file)
            assert os.path.isfile(filepath), (
                f"Agency CPM file {agency_file} missing"
            )

    def test_nih_has_transfer_scripts(self):
        """NIH should have transfer scripts for both CPM and LES."""
        nih_scripts = [s for s in EXPECTED_TRANSFER_SCRIPTS if "nih" in s.lower()]
        assert len(nih_scripts) >= 2, (
            f"NIH should have at least 2 transfer scripts, found {len(nih_scripts)}"
        )

    def test_agency_transfer_coverage(self):
        """Major agencies should have transfer scripts."""
        transfer_content = " ".join(EXPECTED_TRANSFER_SCRIPTS).lower()
        for agency in ["nih", "oig", "fda", "cdc", "afps"]:
            assert agency in transfer_content, (
                f"Agency {agency.upper()} should have a transfer script"
            )

    def test_agency_xml_files_are_non_trivial(self):
        """Agency-specific XML files should have substantial content."""
        for agency_file in AGENCY_CPM_FILES:
            filepath = os.path.join(XML_DIR, agency_file)
            size = os.path.getsize(filepath)
            assert size > 1000, (
                f"Agency file {agency_file} seems too small ({size} bytes)"
            )

    def test_core_cpm_exists(self):
        """Core CPM file should exist as base for agency variants."""
        filepath = os.path.join(XML_DIR, "CPM")
        assert os.path.isfile(filepath), "Core CPM file missing"

    def test_core_cpm_largest(self):
        """Core CPM should be the largest CPM file (contains base mappings)."""
        core_size = os.path.getsize(os.path.join(XML_DIR, "CPM"))
        for agency_file in AGENCY_CPM_FILES:
            agency_size = os.path.getsize(os.path.join(XML_DIR, agency_file))
            assert core_size >= agency_size, (
                f"Core CPM ({core_size}B) should be >= {agency_file} ({agency_size}B)"
            )


class TestFileIntegrity:
    """Verify file integrity across the repository."""

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_files_minimum_size(self, xml_file):
        """XML mapping files should have a minimum size."""
        filepath = os.path.join(XML_DIR, xml_file)
        size = os.path.getsize(filepath)
        assert size > 500, (
            f"XML file {xml_file} is suspiciously small ({size} bytes)"
        )

    @pytest.mark.parametrize("script", EXPECTED_TRANSFER_SCRIPTS)
    def test_transfer_scripts_minimum_size(self, script):
        """Transfer scripts should have a minimum size."""
        filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script)
        size = os.path.getsize(filepath)
        assert size > 100, (
            f"Transfer script {script} is too small ({size} bytes)"
        )

    def test_afterload_sql_substantial(self):
        """afterload.sql should be a substantial file."""
        filepath = os.path.join(REPO_ROOT, "ehrp2biis_afterload.sql")
        size = os.path.getsize(filepath)
        assert size > 1000, (
            f"afterload.sql seems too small ({size} bytes)"
        )

    def test_no_unexpected_files_in_xml_dir(self):
        """XML directory should only contain expected files."""
        actual_files = set(os.listdir(XML_DIR))
        expected_files = set(EXPECTED_XML_FILES)
        unexpected = actual_files - expected_files
        assert len(unexpected) == 0, (
            f"Unexpected files in XML/: {unexpected}"
        )
