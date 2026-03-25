"""
Shell Script Tests for KSH scripts in the BIISINT repository.

Validates:
- All KSH scripts have proper shebang lines
- Scripts reference expected environment variables
- SFTP connection patterns in transfer scripts
- Proper error handling (exit codes, error flags)
- Email notification patterns
- Script syntax validation (bash -n)
- Consistent script structure patterns
"""
import os
import re
import subprocess

import pytest

from tests.conftest import (
    ALL_SHELL_SCRIPTS,
    EXPECTED_MAINTENANCE_SCRIPTS,
    EXPECTED_ROOT_SHELL_SCRIPTS,
    EXPECTED_TRANSFER_SCRIPTS,
    MAINTENANCE_SCRIPTS_DIR,
    REPO_ROOT,
    TRANSFER_SCRIPTS_DIR,
)


def read_script(filepath):
    """Read script content."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


class TestShebangLines:
    """Verify all KSH scripts have proper shebang lines."""

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_script_has_shebang(self, script_path):
        """Each script should start with a shebang line."""
        content = read_script(script_path)
        first_line = content.split("\n")[0].strip()
        assert first_line.startswith("#!"), (
            f"Script {os.path.basename(script_path)} missing shebang line"
        )

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_script_has_ksh_shebang(self, script_path):
        """Each script should use ksh interpreter."""
        content = read_script(script_path)
        first_line = content.split("\n")[0].strip()
        assert "ksh" in first_line, (
            f"Script {os.path.basename(script_path)} should use ksh, got: {first_line}"
        )

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_script_shebang_path(self, script_path):
        """Shebang should reference /bin/ksh."""
        content = read_script(script_path)
        first_line = content.split("\n")[0].strip()
        assert first_line == "#!/bin/ksh", (
            f"Expected #!/bin/ksh, got: {first_line}"
        )


class TestEnvironmentVariables:
    """Check scripts reference expected environment variables."""

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_set_home(self, script_name):
        """Root scripts should set HOME variable."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "HOME=" in content or "export HOME" in content, (
            f"{script_name} should set HOME environment variable"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_set_infa_home(self, script_name):
        """Root scripts should set INFA_HOME for Informatica."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "INFA_HOME=" in content, (
            f"{script_name} should set INFA_HOME environment variable"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_set_ld_library_path(self, script_name):
        """Root scripts should set LD_LIBRARY_PATH."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "LD_LIBRARY_PATH=" in content, (
            f"{script_name} should set LD_LIBRARY_PATH"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_set_path(self, script_name):
        """Root scripts should extend PATH for Informatica binaries."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "PATH=" in content or "PATH=$PATH" in content, (
            f"{script_name} should set PATH variable"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_reference_logdir(self, script_name):
        """Root scripts should define a log directory."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "logdir=" in content or "logfile=" in content, (
            f"{script_name} should define log directory/file"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_reference_oracle_home(self, script_name):
        """Root scripts should reference ORACLE_HOME for sqlplus."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "ORACLE_HOME" in content, (
            f"{script_name} should reference ORACLE_HOME"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_set_umask(self, script_name):
        """Root scripts should set umask for file permissions."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "umask" in content, (
            f"{script_name} should set umask for file permissions"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_source_setenv(self, script_name):
        """Root scripts should source SETENV file."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "SETENV" in content, (
            f"{script_name} should source SETENV configuration"
        )


class TestErrorHandling:
    """Validate error handling patterns in scripts."""

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_check_errors(self, script_name):
        """Root scripts should check for errors."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        has_err_flag = "ERR_FLAG" in content
        has_error_check = "ERROR" in content or "SUCCESS" in content
        assert has_err_flag or has_error_check, (
            f"{script_name} should have error checking logic"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_have_conditional_logic(self, script_name):
        """Root scripts should have if/then conditional blocks."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "if " in content and "then" in content, (
            f"{script_name} should have if/then conditional logic"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_TRANSFER_SCRIPTS,
    )
    def test_transfer_scripts_check_file_exists(self, script_name):
        """Transfer scripts should check if file exists before transfer."""
        filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script_name)
        content = read_script(filepath)
        assert "! -e" in content or "-f" in content, (
            f"{script_name} should check file existence"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_TRANSFER_SCRIPTS,
    )
    def test_transfer_scripts_have_exit_on_error(self, script_name):
        """Transfer scripts should exit on missing file."""
        filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script_name)
        content = read_script(filepath)
        assert "exit" in content, (
            f"{script_name} should exit on error conditions"
        )

    def test_preload_has_exit_code(self):
        """ehrp2biis_preload should have exit 0 on success."""
        filepath = os.path.join(REPO_ROOT, "ehrp2biis_preload")
        content = read_script(filepath)
        assert "exit 0" in content, (
            "ehrp2biis_preload should exit 0 on success"
        )

    def test_actstage_has_exit_code(self):
        """actstage_load should have exit 0 on success."""
        filepath = os.path.join(REPO_ROOT, "actstage_load")
        content = read_script(filepath)
        assert "exit 0" in content, (
            "actstage_load should exit 0 on success"
        )


class TestEmailNotifications:
    """Verify email notification patterns in scripts."""

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_have_mail_notifications(self, script_name):
        """Root scripts should send email notifications."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "mailx" in content, (
            f"{script_name} should use mailx for email notifications"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_TRANSFER_SCRIPTS,
    )
    def test_transfer_scripts_have_mail_notifications(self, script_name):
        """Transfer scripts should send email notifications."""
        filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script_name)
        content = read_script(filepath)
        assert "mailx" in content, (
            f"{script_name} should use mailx for email notifications"
        )

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_scripts_define_mail_recipients(self, script_path):
        """Scripts using mailx should define p_mailid recipients."""
        content = read_script(script_path)
        if "mailx" in content:
            assert "p_mailid" in content or "mailid" in content.lower(), (
                f"{os.path.basename(script_path)} should define mail recipients"
            )

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_email_addresses_use_hhs_domain(self, script_path):
        """Email addresses should use hhs.gov domain."""
        content = read_script(script_path)
        emails = re.findall(r"[\w.+-]+@[\w.-]+", content)
        for email in emails:
            assert email.endswith("hhs.gov") or email.endswith("psc.hhs.gov"), (
                f"Email {email} in {os.path.basename(script_path)} "
                "should use hhs.gov domain"
            )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_send_failure_notification(self, script_name):
        """Root scripts should send failure email."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        has_failure_notification = (
            "not complete" in content.lower()
            or "failed" in content.lower()
            or "did not complete" in content.lower()
        )
        assert has_failure_notification, (
            f"{script_name} should send failure notification"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_send_success_notification(self, script_name):
        """Root scripts should send success email."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        has_success_notification = (
            "successfully" in content.lower()
            or "completed" in content.lower()
            or "success" in content.lower()
        )
        assert has_success_notification, (
            f"{script_name} should send success notification"
        )


class TestScriptSyntax:
    """Test script syntax using bash -n."""

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_script_syntax_valid(self, script_path):
        """Script should pass bash -n syntax check."""
        result = subprocess.run(
            ["bash", "-n", script_path],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"Syntax error in {os.path.basename(script_path)}: {result.stderr}"
        )

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_script_has_content(self, script_path):
        """Scripts should not be empty."""
        size = os.path.getsize(script_path)
        assert size > 0, f"Script {os.path.basename(script_path)} is empty"

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_script_no_windows_line_endings(self, script_path):
        """Scripts should not have Windows-style CRLF line endings."""
        with open(script_path, "rb") as f:
            content = f.read()
        assert b"\r\n" not in content, (
            f"Script {os.path.basename(script_path)} has Windows CRLF line endings"
        )


class TestScriptStructure:
    """Validate common structural patterns across scripts."""

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_have_function_definition(self, script_name):
        """Root scripts should define Load_proc function."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "Load_proc" in content, (
            f"{script_name} should define Load_proc function"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_use_sqlplus(self, script_name):
        """Root scripts should use sqlplus for database operations."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "sqlplus" in content, (
            f"{script_name} should use sqlplus for database operations"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_use_spool(self, script_name):
        """Root scripts should spool output to log files."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "spool" in content.lower(), (
            f"{script_name} should spool output to log files"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_read_credentials(self, script_name):
        """Root scripts should read credentials from files (not hardcoded)."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        has_credential_read = (
            "cat $HOME/.use" in content
            or "cat $HOME/.pw" in content
        )
        assert has_credential_read, (
            f"{script_name} should read credentials from files"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_root_scripts_use_timestamp(self, script_name):
        """Root scripts should use timestamp for log naming."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_script(filepath)
        assert "date +" in content, (
            f"{script_name} should use date command for timestamps"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_TRANSFER_SCRIPTS,
    )
    def test_transfer_scripts_have_header_comments(self, script_name):
        """Transfer scripts should have header comment blocks."""
        filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script_name)
        content = read_script(filepath)
        assert "Script Name" in content, (
            f"{script_name} should have header comment with Script Name"
        )
        assert "Creation Date" in content, (
            f"{script_name} should have header comment with Creation Date"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_MAINTENANCE_SCRIPTS,
    )
    def test_maintenance_scripts_have_header_comments(self, script_name):
        """Maintenance scripts should have header comment blocks."""
        filepath = os.path.join(MAINTENANCE_SCRIPTS_DIR, script_name)
        content = read_script(filepath)
        assert "Script Name" in content, (
            f"{script_name} should have header comment with Script Name"
        )
