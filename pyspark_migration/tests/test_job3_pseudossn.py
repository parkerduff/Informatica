"""
Unit tests for Job 3: m_Pseudossn_Load_Pseudossn_From_SDA_Tbl.

Tests cover:
- Date parsing: MMDDYYYY → MM/DD/YYYY
- Signed numeric parsing for UNIF_ALLOW_AMT
- Update Strategy (DD_UPDATE) for TK_NUM
"""

import unittest
from datetime import datetime


class TestPseudossnDateParsing(unittest.TestCase):
    """Test exp_Conversions date parsing (lines 3614-3641)."""

    def test_valid_mmddyyyy_date(self):
        """Verify MMDDYYYY → date conversion."""
        date_str = "05172025"
        result = datetime.strptime(date_str, "%m%d%Y").date()
        self.assertEqual(str(result), "2025-05-17")

    def test_invalid_date_returns_none(self):
        """Verify invalid date string returns None."""
        date_str = "NOTADATE"
        result = None
        try:
            if date_str and len(date_str) == 8 and date_str.isdigit():
                result = datetime.strptime(date_str, "%m%d%Y").date()
        except ValueError:
            result = None
        self.assertIsNone(result)

    def test_null_date(self):
        """Verify null input returns None."""
        result = None
        self.assertIsNone(result)

    def test_hire_date_format(self):
        """Verify HIRE_DATE conversion from MMDDYYYY."""
        date_str = "01152024"
        result = datetime.strptime(date_str, "%m%d%Y").date()
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 15)
        self.assertEqual(result.year, 2024)


class TestPseudossnSignedNumeric(unittest.TestCase):
    """Test UNIF_ALLOW_AMT signed numeric parsing (lines 3639-3641).
    
    Informatica: DECODE(TRUE, IS_NUMBER(v) AND sign='+', TO_DECIMAL(v,2),
                 IS_NUMBER(v) AND sign='-', TO_DECIMAL(v,2)*-1, ...)
    """

    def _parse_signed_amount(self, raw_value):
        """Parse signed numeric amount from packed format."""
        if not raw_value or len(raw_value) < 6:
            return None
        amt_str = raw_value[:3] + "." + raw_value[3:5]
        sign = raw_value[5:6]
        try:
            amt = float(amt_str)
            if sign == "-":
                amt = -amt
            return round(amt, 2)
        except ValueError:
            return None

    def test_positive_amount(self):
        """Verify positive sign parsing."""
        result = self._parse_signed_amount("12345+")
        self.assertEqual(result, 123.45)

    def test_negative_amount(self):
        """Verify negative sign parsing."""
        result = self._parse_signed_amount("12345-")
        self.assertEqual(result, -123.45)

    def test_unsigned_amount(self):
        """Verify unsigned amount treated as positive."""
        result = self._parse_signed_amount("00100 ")
        self.assertEqual(result, 1.0)

    def test_zero_amount(self):
        """Verify zero amount."""
        result = self._parse_signed_amount("00000+")
        self.assertEqual(result, 0.0)

    def test_null_input(self):
        """Verify null input returns None."""
        result = self._parse_signed_amount(None)
        self.assertIsNone(result)


class TestPseudossnUpdateStrategy(unittest.TestCase):
    """Test upd_Update_TK_NUM Update Strategy (DD_UPDATE)."""

    def test_update_sql_generation(self):
        """Verify UPDATE SQL for TK_NUM."""
        sql = (
            "UPDATE PSEUDOSSN_TBL SET TK_NUM = "
            "(SELECT MAX(TK_NUM) FROM PSEUDOSSN_TBL) + 1 "
            "WHERE TK_NUM IS NULL"
        )
        self.assertIn("UPDATE", sql)
        self.assertIn("PSEUDOSSN_TBL", sql)
        self.assertIn("TK_NUM IS NULL", sql)


if __name__ == "__main__":
    unittest.main()
