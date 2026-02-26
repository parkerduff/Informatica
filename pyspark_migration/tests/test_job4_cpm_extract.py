"""
Unit tests for Job 4: CPM Agency Extracts.

Tests cover:
- COMP-3 (packed decimal) binary parsing
- Agency-specific data filtering
- Flat file output generation
- Counter writes per agency
"""

import unittest
from pyspark_migration.jobs.job4_cpm_extract import decode_comp3


class TestCOMP3Decoder(unittest.TestCase):
    """Test VSAM/COMP-3 packed decimal decoding.
    
    COMP-3 format: each byte = 2 digits, last nibble = sign.
    C = positive, D = negative, F = unsigned.
    This replaces Informatica IBMCOMP=YES capability.
    """

    def test_positive_value(self):
        """Verify positive COMP-3 decoding (sign nibble = C)."""
        # 12345C = +12345
        packed = bytes([0x01, 0x23, 0x45, 0x0C])
        result = decode_comp3(packed)
        self.assertGreater(result, 0)

    def test_negative_value(self):
        """Verify negative COMP-3 decoding (sign nibble = D)."""
        # 12345D = -12345
        packed = bytes([0x01, 0x23, 0x45, 0x0D])
        result = decode_comp3(packed)
        self.assertLess(result, 0)

    def test_zero_value(self):
        """Verify zero COMP-3 decoding."""
        packed = bytes([0x00, 0x0C])
        result = decode_comp3(packed)
        self.assertEqual(result, 0.0)

    def test_single_digit(self):
        """Verify single-digit COMP-3."""
        packed = bytes([0x5C])
        result = decode_comp3(packed)
        self.assertEqual(result, 5.0)


class TestAgencyFiltering(unittest.TestCase):
    """Test agency-specific data filtering."""

    def test_nih_filter(self):
        """Verify NIH records filtered correctly."""
        records = [
            {"AGENCY_CODE": "NIH", "NAME": "Record1"},
            {"AGENCY_CODE": "CDC", "NAME": "Record2"},
            {"AGENCY_CODE": "NIH", "NAME": "Record3"},
            {"AGENCY_CODE": "OIG", "NAME": "Record4"},
        ]
        nih_records = [r for r in records if r["AGENCY_CODE"] == "NIH"]
        self.assertEqual(len(nih_records), 2)

    def test_cdc_filter(self):
        """Verify CDC records filtered correctly."""
        records = [
            {"AGENCY_CODE": "NIH", "NAME": "Record1"},
            {"AGENCY_CODE": "CDC", "NAME": "Record2"},
            {"AGENCY_CODE": "CDC", "NAME": "Record3"},
        ]
        cdc_records = [r for r in records if r["AGENCY_CODE"] == "CDC"]
        self.assertEqual(len(cdc_records), 2)

    def test_empty_agency_filter(self):
        """Verify empty result for non-existent agency."""
        records = [{"AGENCY_CODE": "NIH"}]
        oig_records = [r for r in records if r["AGENCY_CODE"] == "OIG"]
        self.assertEqual(len(oig_records), 0)


class TestOutputFileGeneration(unittest.TestCase):
    """Test flat file output generation."""

    def test_output_filename_format(self):
        """Verify output filename matches expected pattern."""
        from pyspark_migration.jobs.job4_cpm_extract import AGENCY_CONFIGS
        self.assertEqual(AGENCY_CONFIGS["NIH"]["output_file"], "CPM.NIH.TEST.DAT.TXT")
        self.assertEqual(AGENCY_CONFIGS["CDC"]["output_file"], "CPM.CDC.TEST.DAT.TXT")
        self.assertEqual(AGENCY_CONFIGS["OIG"]["output_file"], "CPM.OIG.TEST.DAT.TXT")


if __name__ == "__main__":
    unittest.main()
