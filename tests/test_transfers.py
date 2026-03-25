"""
Transfer Script Tests for BIISINT SFTP file distribution.

Validates:
- Transfer scripts reference valid target directories
- SFTP connection parameters (server, account)
- Error notification email addresses
- File existence validation patterns
- Consistent transfer patterns across agencies
"""
import os
import re

import pytest

from tests.conftest import (
    EXPECTED_SFTP_TARGETS,
    EXPECTED_TRANSFER_SCRIPTS,
    SFTP_ACCOUNT,
    SFTP_SERVER,
    TRANSFER_SCRIPTS_DIR,
)


def read_transfer_script(script_name):
    """Read a transfer script's content."""
    filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script_name)
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


class TestSFTPConnectionParameters:
    """Verify SFTP connection parameters in transfer scripts."""

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_references_sftp_server(self, script_name):
        """Transfer scripts should reference the SFTP server."""
        content = read_transfer_script(script_name)
        assert SFTP_SERVER in content, (
            f"{script_name} should reference SFTP server {SFTP_SERVER}"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_uses_sftp_command(self, script_name):
        """Transfer scripts should use /usr/bin/sftp command."""
        content = read_transfer_script(script_name)
        assert "/usr/bin/sftp" in content, (
            f"{script_name} should use /usr/bin/sftp"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_uses_sa_cdirect_account(self, script_name):
        """Transfer scripts should use sa-cdirect SFTP account."""
        content = read_transfer_script(script_name)
        assert SFTP_ACCOUNT in content, (
            f"{script_name} should use {SFTP_ACCOUNT} account"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_sftp_connection_string_format(self, script_name):
        """Transfer scripts should have proper sftp connection string format."""
        content = read_transfer_script(script_name)
        expected_pattern = f"{SFTP_ACCOUNT}@{SFTP_SERVER}"
        assert expected_pattern in content, (
            f"{script_name} should have connection string {expected_pattern}"
        )


class TestSFTPTargetDirectories:
    """Verify transfer scripts reference valid target directories."""

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_has_cd_to_target_directory(self, script_name):
        """Transfer scripts should cd to an outbound directory."""
        content = read_transfer_script(script_name)
        assert "cd /opt/app/jail/" in content, (
            f"{script_name} should cd to jail directory"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_target_directory_is_outbound(self, script_name):
        """Transfer scripts should target outbound directories."""
        content = read_transfer_script(script_name)
        assert "/outbound" in content, (
            f"{script_name} should target outbound directory"
        )

    @pytest.mark.parametrize(
        "script_name,expected_dir",
        list(EXPECTED_SFTP_TARGETS.items()),
    )
    def test_correct_target_directory(self, script_name, expected_dir):
        """Each transfer script should target the correct agency directory."""
        content = read_transfer_script(script_name)
        assert expected_dir in content, (
            f"{script_name} should target {expected_dir}"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_uses_put_command(self, script_name):
        """Transfer scripts should use put command for file transfer."""
        content = read_transfer_script(script_name)
        assert "put " in content, (
            f"{script_name} should use 'put' command"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_uses_quit_command(self, script_name):
        """Transfer scripts should use quit command after transfer."""
        content = read_transfer_script(script_name)
        assert "quit" in content, (
            f"{script_name} should use 'quit' command"
        )


class TestFileExistenceValidation:
    """Verify file existence validation patterns in transfer scripts."""

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_checks_file_existence(self, script_name):
        """Transfer scripts should check if file exists before transfer."""
        content = read_transfer_script(script_name)
        assert "! -e" in content, (
            f"{script_name} should check file existence with -e"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_references_fname_variable(self, script_name):
        """Transfer scripts should use FNAME variable for file path."""
        content = read_transfer_script(script_name)
        assert "FNAME=" in content, (
            f"{script_name} should define FNAME variable"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_fname_references_data_directory(self, script_name):
        """Transfer scripts FNAME should reference /data/BIISINT/."""
        content = read_transfer_script(script_name)
        assert "/data/BIISINT/" in content, (
            f"{script_name} FNAME should reference /data/BIISINT/"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_accepts_filename_parameter(self, script_name):
        """Transfer scripts should accept filename as parameter ($1)."""
        content = read_transfer_script(script_name)
        assert "$1" in content, (
            f"{script_name} should accept filename as $1 parameter"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_sends_abort_email_on_missing_file(self, script_name):
        """Transfer scripts should send abort email when file not found."""
        content = read_transfer_script(script_name)
        assert "Aborting" in content or "not found" in content.lower(), (
            f"{script_name} should send abort notification for missing files"
        )


class TestTransferNotifications:
    """Verify notification patterns in transfer scripts."""

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_sends_success_email(self, script_name):
        """Transfer scripts should send success notification."""
        content = read_transfer_script(script_name)
        assert "sucessfully" in content.lower() or "successfully" in content.lower(), (
            f"{script_name} should send success notification"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_logs_transfer_start(self, script_name):
        """Transfer scripts should log when transfer starts."""
        content = read_transfer_script(script_name)
        assert "Transfering" in content or "Transfer" in content, (
            f"{script_name} should log transfer start"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_logs_transfer_complete(self, script_name):
        """Transfer scripts should log when transfer completes."""
        content = read_transfer_script(script_name)
        assert "completed" in content.lower(), (
            f"{script_name} should log transfer completion"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_includes_timestamp_in_log(self, script_name):
        """Transfer scripts should include timestamp in log messages."""
        content = read_transfer_script(script_name)
        assert "date" in content, (
            f"{script_name} should include date/timestamp"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_uses_tee_for_logging(self, script_name):
        """Transfer scripts should use tee for logging output."""
        content = read_transfer_script(script_name)
        assert "tee" in content, (
            f"{script_name} should use tee for logging"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_cleans_up_temp_files(self, script_name):
        """Transfer scripts should clean up temporary log files."""
        content = read_transfer_script(script_name)
        assert "rm " in content, (
            f"{script_name} should clean up temp files"
        )


class TestTransferDataPaths:
    """Verify correct data source paths for different agencies."""

    def test_nih_cpm_uses_cpm_directory(self):
        """NIH CPM transfer should use CPM output directory."""
        content = read_transfer_script("nih_cpm_transfer")
        assert "/data/BIISINT/data/int/out/CPM/" in content

    def test_nih_les_uses_les_directory(self):
        """NIH LES transfer should use LES output directory."""
        content = read_transfer_script("nih_les_transfer")
        assert "/data/BIISINT/data/int/out/LES/" in content

    def test_oig_uses_cpm_directory(self):
        """OIG transfer should use CPM output directory."""
        content = read_transfer_script("oig_transfer")
        assert "/data/BIISINT/data/int/out/CPM/" in content

    def test_fda_uses_cpm_directory(self):
        """FDA transfer should use CPM output directory."""
        content = read_transfer_script("fda_transfer")
        assert "/data/BIISINT/data/int/out/CPM/" in content

    def test_cdc_uses_cpm_directory(self):
        """CDC transfer should use CPM output directory."""
        content = read_transfer_script("cdc_transfer")
        assert "/data/BIISINT/data/int/out/CPM/" in content

    def test_afps_uses_cpm_directory(self):
        """AFPS transfer should use CPM output directory."""
        content = read_transfer_script("afps_transfer")
        assert "/data/BIISINT/data/int/out/CPM/" in content


class TestTransferInputValidation:
    """Verify input parameter validation in transfer scripts."""

    def test_oig_validates_parameter(self):
        """OIG transfer should validate input parameter is provided."""
        content = read_transfer_script("oig_transfer")
        assert '-n "$1"' in content, (
            "oig_transfer should validate parameter is non-empty"
        )

    def test_fda_validates_parameter(self):
        """FDA transfer should validate input parameter is provided."""
        content = read_transfer_script("fda_transfer")
        assert '-n "$1"' in content

    def test_cdc_validates_parameter(self):
        """CDC transfer should validate input parameter is provided."""
        content = read_transfer_script("cdc_transfer")
        assert '-n "$1"' in content

    def test_afps_validates_parameter(self):
        """AFPS transfer should validate input parameter is provided."""
        content = read_transfer_script("afps_transfer")
        assert '-n "$1"' in content

    def test_nih_les_validates_parameter(self):
        """NIH LES transfer should validate input parameter."""
        content = read_transfer_script("nih_les_transfer")
        assert '-n "$1"' in content

    def test_scripts_with_validation_show_usage(self):
        """Scripts that validate parameters should show usage on error."""
        for script_name in ["oig_transfer", "fda_transfer", "cdc_transfer",
                            "afps_transfer", "nih_les_transfer", "nih_transfer_les"]:
            content = read_transfer_script(script_name)
            assert "Usage" in content or "usage" in content, (
                f"{script_name} should show usage message"
            )


class TestHeredocPattern:
    """Verify SFTP heredoc pattern in transfer scripts."""

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_uses_heredoc_for_sftp(self, script_name):
        """Transfer scripts should use heredoc (<<EOF) for SFTP commands."""
        content = read_transfer_script(script_name)
        assert "<<EOF" in content, (
            f"{script_name} should use heredoc for SFTP commands"
        )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_heredoc_has_eof_terminator(self, script_name):
        """Transfer scripts heredoc should have EOF terminator."""
        content = read_transfer_script(script_name)
        lines = content.split("\n")
        eof_lines = [l.strip() for l in lines if l.strip() == "EOF"]
        assert len(eof_lines) > 0, (
            f"{script_name} should have EOF terminator for heredoc"
        )
