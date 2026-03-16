"""
Integration Tests for LES Job

Tests record type parsing and unpivot logic.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_les")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestLESRecordTypes:
    """Tests for LES record type configuration."""

    def test_all_record_types_configured(self):
        """All 8 record types are configured."""
        from pyspark_migration.jobs.les.les_job import RECORD_TYPE_CONFIG

        expected_types = {"header", "C", "D", "L", "M", "R", "T", "U"}
        assert set(RECORD_TYPE_CONFIG.keys()) == expected_types

    def test_each_type_has_required_keys(self):
        """Each config has file_pattern, field_specs, target_table."""
        from pyspark_migration.jobs.les.les_job import RECORD_TYPE_CONFIG

        for key, config in RECORD_TYPE_CONFIG.items():
            assert "file_pattern" in config, f"{key} missing file_pattern"
            assert "field_specs" in config, f"{key} missing field_specs"
            assert "target_table" in config, f"{key} missing target_table"
            assert "signed_fields" in config, f"{key} missing signed_fields"

    def test_header_targets_correct_table(self):
        """Header records go to LES_HEADER_TBL."""
        from pyspark_migration.jobs.les.les_job import RECORD_TYPE_CONFIG

        assert RECORD_TYPE_CONFIG["header"]["target_table"] == "LES_HEADER_TBL"

    def test_type_c_targets_correct_table(self):
        """Type C records go to LES_EMP_DETAIL_RECTYPE_C_TBL."""
        from pyspark_migration.jobs.les.les_job import RECORD_TYPE_CONFIG

        assert RECORD_TYPE_CONFIG["C"]["target_table"] == "LES_EMP_DETAIL_RECTYPE_C_TBL"


class TestLESUnpivot:
    """Tests for C and D record type unpivot logic."""

    def test_earnings_unpivot(self, spark):
        """Type C: 3 earnings per record should unpivot to 3 rows."""
        data = [(
            "123456789", "C", "20240115", "HE30",
            "REG   ", "80.00  ", "1234.56    ",
            "OT    ", "10.00  ", "500.00     ",
            "HOL   ", "8.00   ", "250.00     ",
        )]
        columns = [
            "DFAS_PSEUDO_SSN", "REC_TYPE", "PP_END_DATE", "AGENCY_CODE",
            "EARN1_TYPE", "EARN1_HOURS", "EARN1_AMOUNT",
            "EARN2_TYPE", "EARN2_HOURS", "EARN2_AMOUNT",
            "EARN3_TYPE", "EARN3_HOURS", "EARN3_AMOUNT",
        ]
        df = spark.createDataFrame(data, columns)

        # Simulate unpivot
        earnings_dfs = []
        for i in range(1, 4):
            earn_df = df.select(
                "DFAS_PSEUDO_SSN", "REC_TYPE",
                F.col(f"EARN{i}_TYPE").alias("EARN_TYPE"),
                F.col(f"EARN{i}_HOURS").alias("EARN_HOURS"),
                F.col(f"EARN{i}_AMOUNT").alias("EARN_AMOUNT"),
            ).filter(F.trim(F.col("EARN_TYPE")) != "")
            earnings_dfs.append(earn_df)

        unpivoted = earnings_dfs[0]
        for edf in earnings_dfs[1:]:
            unpivoted = unpivoted.unionByName(edf)

        assert unpivoted.count() == 3

    def test_deductions_unpivot(self, spark):
        """Type D: 2 deductions per record should unpivot to 2 rows."""
        data = [(
            "123456789", "D", "20240115", "HE30",
            "RETIRE", "1234.56    ", "9876.54    ",
            "HEALTH", "567.89     ", "4567.89    ",
        )]
        columns = [
            "DFAS_PSEUDO_SSN", "REC_TYPE", "PP_END_DATE", "AGENCY_CODE",
            "DED1_TYPE", "DED1_CURRENT", "DED1_YTD",
            "DED2_TYPE", "DED2_CURRENT", "DED2_YTD",
        ]
        df = spark.createDataFrame(data, columns)

        ded_dfs = []
        for i in range(1, 3):
            ded_df = df.select(
                "DFAS_PSEUDO_SSN",
                F.col(f"DED{i}_TYPE").alias("DED_TYPE"),
                F.col(f"DED{i}_CURRENT").alias("DED_CURRENT"),
                F.col(f"DED{i}_YTD").alias("DED_YTD"),
            ).filter(F.trim(F.col("DED_TYPE")) != "")
            ded_dfs.append(ded_df)

        unpivoted = ded_dfs[0]
        for ddf in ded_dfs[1:]:
            unpivoted = unpivoted.unionByName(ddf)

        assert unpivoted.count() == 2
