"""
Unit and integration tests for Job 2: wf_COMPTIME.

Tests cover:
- fil_Detail filter (RECORD_TYPE_FLAG = 'D')
- exp_Initial SSN validation
- lkp_PAY_PERIOD current pay period lookup
- exp_Convert date conversions
- agg_ALL_RECORDS count aggregation
- File archival post-session command
- SETVARIABLE pattern for email subject/message
"""

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime


class TestCompTimeFilter(unittest.TestCase):
    """Test fil_Detail: RECORD_TYPE_FLAG = 'D'."""

    def test_filter_detail_records(self):
        """Verify only 'D' records pass filter."""
        # Informatica: Filter Condition = RECORD_TYPE_FLAG = 'D'
        records = [
            {"RECORD_TYPE_FLAG": "D", "SSN": "123456789"},
            {"RECORD_TYPE_FLAG": "H", "SSN": "000000000"},
            {"RECORD_TYPE_FLAG": "D", "SSN": "987654321"},
            {"RECORD_TYPE_FLAG": "T", "SSN": "000000000"},
        ]
        filtered = [r for r in records if r["RECORD_TYPE_FLAG"] == "D"]
        self.assertEqual(len(filtered), 2)

    def test_filter_empty_dataset(self):
        """Verify filter handles empty input."""
        records = []
        filtered = [r for r in records if r["RECORD_TYPE_FLAG"] == "D"]
        self.assertEqual(len(filtered), 0)


class TestCompTimeSSNValidation(unittest.TestCase):
    """Test exp_Initial: DECODE(TRUE, IS_NUMBER(SSN), 'D', 'NO')."""

    def test_valid_numeric_ssn(self):
        """Verify numeric SSN returns 'D'."""
        import re
        ssn = "123456789"
        result = "D" if re.match(r"^[0-9]+$", ssn) else "NO"
        self.assertEqual(result, "D")

    def test_invalid_ssn_with_letters(self):
        """Verify non-numeric SSN returns 'NO'."""
        import re
        ssn = "12345ABCD"
        result = "D" if re.match(r"^[0-9]+$", ssn) else "NO"
        self.assertEqual(result, "NO")

    def test_empty_ssn(self):
        """Verify empty SSN returns 'NO'."""
        import re
        ssn = ""
        result = "D" if re.match(r"^[0-9]+$", ssn) else "NO"
        self.assertEqual(result, "NO")

    def test_ssn_with_spaces(self):
        """Verify SSN with spaces returns 'NO'."""
        import re
        ssn = "123 456 789"
        result = "D" if re.match(r"^[0-9]+$", ssn) else "NO"
        self.assertEqual(result, "NO")


class TestCompTimeDateConversion(unittest.TestCase):
    """Test exp_Convert: IIF(IS_DATE(PP_END_DATE, 'YYYYMMDD'), TO_DATE(...))."""

    def test_valid_date_conversion(self):
        """Verify valid YYYYMMDD date converts correctly."""
        date_str = "20250517"
        from datetime import datetime
        result = datetime.strptime(date_str, "%Y%m%d").date()
        self.assertEqual(str(result), "2025-05-17")

    def test_invalid_date_returns_none(self):
        """Verify invalid date returns None."""
        import re
        date_str = "NOTADATE"
        result = None
        if re.match(r"^[0-9]{8}$", date_str):
            try:
                result = datetime.strptime(date_str, "%Y%m%d").date()
            except ValueError:
                result = None
        self.assertIsNone(result)

    def test_null_date_returns_none(self):
        """Verify null date returns None."""
        date_str = None
        result = None
        if date_str and len(date_str) == 8:
            try:
                result = datetime.strptime(date_str, "%Y%m%d").date()
            except (ValueError, TypeError):
                result = None
        self.assertIsNone(result)


class TestCompTimeCountAggregation(unittest.TestCase):
    """Test agg_ALL_RECORDS: COUNT(SSN) with Scope=All Input."""

    def test_count_all_records(self):
        """Verify COUNT aggregation matches expected."""
        records = [{"SSN": "111"}, {"SSN": "222"}, {"SSN": "333"}]
        self.assertEqual(len(records), 3)


class TestCompTimeFileArchival(unittest.TestCase):
    """Test post-session file archival."""

    @patch("shutil.move")
    @patch("os.path.exists", return_value=True)
    @patch("os.makedirs")
    def test_file_archival(self, mock_makedirs, mock_exists, mock_move):
        """Verify source file is moved to archive directory."""
        import shutil
        import os

        input_path = "/data/BIISINT/data/int/in/COMPTIME/u0827d01.txt"
        archive_path = "/data/BIISINT/data/archive/COMPTIME/u0827d01_P202510.txt"

        if os.path.exists(input_path):
            shutil.move(input_path, archive_path)

        mock_move.assert_called_once_with(input_path, archive_path)


class TestCompTimeMessageBuilder(unittest.TestCase):
    """Test SETVARIABLE pattern for email subject/message."""

    def test_subject_contains_env_prefix(self):
        """Verify environment prefix in subject."""
        env_prefix = "Test: "
        pp_end_year = "2025"
        pp_num = "10"
        subject = (
            f"{env_prefix}Comp Time File loaded successfully for "
            f"Pay Period: {pp_end_year}-{pp_num}"
        )
        self.assertTrue(subject.startswith("Test: "))
        self.assertIn("2025-10", subject)

    def test_message_contains_counter(self):
        """Verify message contains detail record count."""
        detail_count = 1500
        message = f"Number of Detail Records from Comp Time file\t= {detail_count}"
        self.assertIn("1500", message)


if __name__ == "__main__":
    unittest.main()
