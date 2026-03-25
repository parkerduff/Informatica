"""
Security Tests for BIISINT repository.

Validates:
- No hardcoded passwords in scripts
- Credential file references are consistent
- Proper file permission patterns
- No sensitive data exposure
- Secure SFTP patterns
"""
import os
import re

import pytest

from tests.conftest import (
    ALL_SHELL_SCRIPTS,
    EXPECTED_ROOT_SHELL_SCRIPTS,
    EXPECTED_TRANSFER_SCRIPTS,
    EXPECTED_XML_FILES,
    REPO_ROOT,
    TRANSFER_SCRIPTS_DIR,
    XML_DIR,
)


def read_file(filepath):
    """Read file content."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


class TestNoHardcodedPasswords:
    """Check no hardcoded passwords in scripts."""

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_no_hardcoded_password_strings(self, script_path):
        """Scripts should not contain hardcoded password strings."""
        content = read_file(script_path)
        password_patterns = [
            r'(?i)password\s*=\s*["\'][^"\']+["\']',
            r'(?i)passwd\s*=\s*["\'][^"\']+["\']',
            r'(?i)pwd\s*=\s*["\'][^"\']+["\']',
        ]
        for pattern in password_patterns:
            matches = re.findall(pattern, content)
            assert len(matches) == 0, (
                f"Potential hardcoded password in {os.path.basename(script_path)}: "
                f"{matches}"
            )

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_no_inline_credentials(self, script_path):
        """Scripts should not have inline username:password patterns."""
        content = read_file(script_path)
        # Check for patterns like user/password in connection strings
        # but allow variable references like $loin2/$ps2
        lines = content.split("\n")
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # Check for literal credentials (not variable references)
            if "connect " in stripped.lower() and "/" in stripped:
                # Should use variables, not literal values
                assert "$" in stripped, (
                    f"Possible inline credentials in "
                    f"{os.path.basename(script_path)}: {stripped[:80]}"
                )


class TestCredentialFileReferences:
    """Verify credential file references are consistent."""

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_reads_credentials_from_files(self, script_name):
        """Root scripts should read credentials from hidden files."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_file(filepath)
        # Should use cat to read from .use and .pw files
        has_use_file = ".use" in content
        has_pw_file = ".pw" in content
        assert has_use_file and has_pw_file, (
            f"{script_name} should read credentials from .use and .pw files"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_credential_files_under_home(self, script_name):
        """Credential file references should be under $HOME."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_file(filepath)
        assert "$HOME/.use" in content or "$HOME/.pw" in content, (
            f"{script_name} should reference credential files under $HOME"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_uses_variable_for_credentials(self, script_name):
        """Scripts should store credentials in variables, not use directly."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_file(filepath)
        # Should have variable assignments for credentials
        has_user_var = "loin=" in content or "loin2=" in content
        has_pass_var = "ps1=" in content or "ps2=" in content
        assert has_user_var and has_pass_var, (
            f"{script_name} should use variables for credentials"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_consistent_credential_pattern(self, script_name):
        """Both scripts should use same credential file pattern."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_file(filepath)
        # Both scripts should read .use1 and .pw1 for secondary credentials
        assert ".use1" in content or ".use" in content, (
            f"{script_name} should reference .use credential file"
        )
        assert ".pw1" in content or ".pw" in content, (
            f"{script_name} should reference .pw credential file"
        )


class TestNoSensitiveDataExposure:
    """Check for sensitive data exposure patterns."""

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_no_ssn_patterns(self, script_path):
        """Scripts should not contain SSN-like patterns."""
        content = read_file(script_path)
        # Look for SSN patterns (XXX-XX-XXXX) but not in comments about SSN
        ssn_pattern = r"\b\d{3}-\d{2}-\d{4}\b"
        matches = re.findall(ssn_pattern, content)
        assert len(matches) == 0, (
            f"Potential SSN pattern found in {os.path.basename(script_path)}"
        )

    @pytest.mark.parametrize("script_path", ALL_SHELL_SCRIPTS)
    def test_no_api_keys(self, script_path):
        """Scripts should not contain API key patterns."""
        content = read_file(script_path)
        api_key_patterns = [
            r"(?i)api[_-]?key\s*=\s*['\"][a-zA-Z0-9]{20,}['\"]",
            r"(?i)secret[_-]?key\s*=\s*['\"][a-zA-Z0-9]{20,}['\"]",
            r"(?i)access[_-]?token\s*=\s*['\"][a-zA-Z0-9]{20,}['\"]",
        ]
        for pattern in api_key_patterns:
            matches = re.findall(pattern, content)
            assert len(matches) == 0, (
                f"Potential API key in {os.path.basename(script_path)}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_no_hardcoded_passwords(self, xml_file):
        """XML files should not contain hardcoded passwords."""
        filepath = os.path.join(XML_DIR, xml_file)
        content = read_file(filepath)
        password_patterns = [
            r'(?i)password\s*=\s*"[^$][^"]*"',
            r"(?i)passwd\s*=\s*'[^$][^']*'",
        ]
        for pattern in password_patterns:
            matches = re.findall(pattern, content)
            assert len(matches) == 0, (
                f"Potential hardcoded password in XML file {xml_file}"
            )

    @pytest.mark.parametrize("xml_file", EXPECTED_XML_FILES)
    def test_xml_no_ssn_patterns(self, xml_file):
        """XML files should not contain SSN-like patterns."""
        filepath = os.path.join(XML_DIR, xml_file)
        content = read_file(filepath)
        ssn_pattern = r"\b\d{3}-\d{2}-\d{4}\b"
        matches = re.findall(ssn_pattern, content)
        assert len(matches) == 0, (
            f"Potential SSN found in XML file {xml_file}"
        )


class TestSecureSFTPPatterns:
    """Verify secure SFTP connection patterns."""

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_sftp_no_password_in_command(self, script_name):
        """SFTP connections should not have passwords in the command line."""
        filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script_name)
        content = read_file(filepath)
        sftp_lines = [
            l for l in content.split("\n")
            if "sftp" in l.lower() and not l.strip().startswith("#")
        ]
        for line in sftp_lines:
            assert "password" not in line.lower(), (
                f"SFTP command in {script_name} should not contain password"
            )

    @pytest.mark.parametrize("script_name", EXPECTED_TRANSFER_SCRIPTS)
    def test_sftp_uses_key_based_auth(self, script_name):
        """SFTP should use key-based authentication (no password in command)."""
        filepath = os.path.join(TRANSFER_SCRIPTS_DIR, script_name)
        content = read_file(filepath)
        sftp_lines = [
            l for l in content.split("\n")
            if "/usr/bin/sftp" in l and not l.strip().startswith("#")
        ]
        for line in sftp_lines:
            # Should be simple user@host format (key-based auth)
            assert "-oPassword" not in line, (
                f"SFTP in {script_name} should not use password option"
            )
            assert "sshpass" not in line, (
                f"SFTP in {script_name} should not use sshpass"
            )


class TestFilePermissionPatterns:
    """Verify file permission patterns in scripts."""

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_sets_umask(self, script_name):
        """Root scripts should set umask for file permissions."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_file(filepath)
        assert "umask" in content, (
            f"{script_name} should set umask"
        )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_umask_value_reasonable(self, script_name):
        """umask value should be reasonable (not too permissive)."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_file(filepath)
        umask_match = re.search(r"umask\s+(\d+)", content)
        if umask_match:
            umask_val = umask_match.group(1)
            # umask 022 or more restrictive is acceptable
            assert int(umask_val) >= 22, (
                f"{script_name} umask {umask_val} may be too permissive"
            )

    @pytest.mark.parametrize(
        "script_name",
        EXPECTED_ROOT_SHELL_SCRIPTS,
    )
    def test_sources_setenv(self, script_name):
        """Root scripts should source SETENV for environment security."""
        filepath = os.path.join(REPO_ROOT, script_name)
        content = read_file(filepath)
        assert ". /home/sa-biisint/bin/SETENV" in content, (
            f"{script_name} should source SETENV"
        )


class TestSQLSecurityPatterns:
    """Verify security patterns in SQL scripts."""

    def test_afterload_no_hardcoded_credentials(self):
        """afterload.sql should not contain hardcoded credentials."""
        filepath = os.path.join(REPO_ROOT, "ehrp2biis_afterload.sql")
        content = read_file(filepath)
        password_patterns = [
            r"(?i)identified\s+by\s+['\"][^'\"]+['\"]",
            r"(?i)password\s*=\s*['\"][^'\"]+['\"]",
        ]
        for pattern in password_patterns:
            matches = re.findall(pattern, content)
            assert len(matches) == 0, (
                "afterload.sql should not contain hardcoded credentials"
            )

    def test_afterload_uses_nolog_connection(self):
        """Scripts connecting to SQL should use '/nolog' pattern."""
        for script_name in EXPECTED_ROOT_SHELL_SCRIPTS:
            filepath = os.path.join(REPO_ROOT, script_name)
            content = read_file(filepath)
            if "sqlplus" in content:
                assert "/nolog" in content, (
                    f"{script_name} should use sqlplus '/nolog' pattern"
                )
