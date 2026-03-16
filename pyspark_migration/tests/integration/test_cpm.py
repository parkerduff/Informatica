"""
Integration Tests for CPM Jobs

Tests staging, agency-specific processing.
"""

from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_cpm")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestCPMStaging:
    """Tests for CPM staging."""

    def test_ytd_field_specs_complete(self):
        """YTD field specs cover all required fields."""
        from pyspark_migration.jobs.cpm.staging import YTD_FIELD_SPECS

        field_names = [f[0] for f in YTD_FIELD_SPECS]
        assert "DFAS_PSEUDO_SSN" in field_names
        assert "YTD_GROSS_PAY" in field_names
        assert "PP_END_DATE" in field_names

    def test_mer_field_specs_complete(self):
        """MER field specs cover all required fields."""
        from pyspark_migration.jobs.cpm.staging import MER_FIELD_SPECS

        field_names = [f[0] for f in MER_FIELD_SPECS]
        assert "DFAS_PSEUDO_SSN" in field_names
        assert "EMPLOYEE_NAME" in field_names
        assert "BIRTH_DATE" in field_names

    def test_ytd_signed_fields_list(self):
        """All YTD monetary fields are listed for signed parsing."""
        from pyspark_migration.jobs.cpm.staging import YTD_SIGNED_FIELDS

        assert "YTD_GROSS_PAY" in YTD_SIGNED_FIELDS
        assert "YTD_FED_TAX_DED" in YTD_SIGNED_FIELDS
        assert "CPP_GROSS_PAY" in YTD_SIGNED_FIELDS

    def test_ytd_parsing(self, spark, tmp_path):
        """Parse a sample YTD file."""
        from pyspark_migration.jobs.cpm.staging import YTD_FIELD_SPECS

        # Build a fixed-width line matching YTD specs
        # Minimum: first few fields
        line = "01" + "123456789" + "01" + " " * 400 + "\n"
        file_path = str(tmp_path / "PC_DOEYTD_RDF.TXT")
        with open(file_path, "w") as f:
            f.write(line)

        from pyspark_migration.common.file_utils import parse_fixed_width

        df = parse_fixed_width(spark, file_path, YTD_FIELD_SPECS[:5])
        assert df.count() == 1


class TestCPMNIH:
    """Tests for CPM NIH processing."""

    def test_nih_agency_filter(self, spark):
        """NIH filter captures HE3x agency codes."""
        from pyspark.sql import functions as F

        data = [
            ("111111111", "HE30"),
            ("222222222", "HE38"),
            ("333333333", "HE70"),  # OIG
            ("444444444", "HE20"),  # CDC
        ]
        df = spark.createDataFrame(data, ["DFAS_PSEUDO_SSN", "AGENCY_CODE"])
        nih_df = df.filter(
            F.col("AGENCY_CODE").isin(["HE30", "HE38", "HE39"])
            | F.col("AGENCY_CODE").startswith("HE3")
        )
        assert nih_df.count() == 2


class TestCPMOIG:
    """Tests for CPM OIG processing."""

    def test_oig_agency_filter(self, spark):
        """OIG filter captures HE7x agency codes."""
        from pyspark.sql import functions as F

        data = [
            ("111111111", "HE70"),
            ("222222222", "HE71"),
            ("333333333", "HE30"),  # NIH
        ]
        df = spark.createDataFrame(data, ["DFAS_PSEUDO_SSN", "AGENCY_CODE"])
        oig_df = df.filter(
            F.col("AGENCY_CODE").isin(["HE70", "HE71"])
            | F.col("AGENCY_CODE").startswith("HE7")
        )
        assert oig_df.count() == 2
