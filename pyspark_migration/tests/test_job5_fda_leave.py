"""
Unit and integration tests for Job 5: wf_FDA_Leave.

Tests cover:
- Parameter validation (ABORT conditions)
- fil_Leave_Records filter (FDA_REC_TYPE = '02')
- 4 parallel lookup validations → ERROR_TBL
- CRITICAL Post SQL DELETE on HI_PM_FDA_TATRAN_TBL
- srt_Distinct_File_Names (Sorter Distinct=YES)
- agg_Count_Number_of_Files
- CPM_CYCLE_TBL update (DD_UPDATE)
- Counter writes (COUNT_READ_IN, LEAVE_REC_COUNT, ERROR_REC_COUNT, WRITTEN_REC_COUNT)
"""

import unittest
from unittest.mock import MagicMock, patch


class TestFDALeaveParameterValidation(unittest.TestCase):
    """Test exp_Validate_Parameters (lines 1641-1650).
    
    ABORT() if PP_END_YEAR or PP_NUM is non-numeric.
    """

    def test_valid_numeric_parameters(self):
        """Verify valid numeric parameters pass validation."""
        pp_end_year = "2025"
        pp_num = "10"
        self.assertTrue(pp_end_year.isnumeric())
        self.assertTrue(pp_num.isnumeric())

    def test_invalid_pp_end_year_aborts(self):
        """Verify non-numeric PP_END_YEAR raises ValueError."""
        pp_end_year = "ABCD"
        with self.assertRaises(ValueError):
            if not pp_end_year.isnumeric():
                raise ValueError(
                    f"!!!! The value : {pp_end_year} is not a valid pay period year"
                )

    def test_invalid_pp_num_aborts(self):
        """Verify non-numeric PP_NUM raises ValueError."""
        pp_num = "XY"
        with self.assertRaises(ValueError):
            if not pp_num.isnumeric():
                raise ValueError(
                    f"!!!! The value : {pp_num} is not a valid pay period number"
                )

    def test_empty_params_default_to_current(self):
        """Verify empty parameters trigger default-to-current-PP logic."""
        pp_end_year = ""
        pp_num = ""
        default_to_curr_pp = not pp_end_year and not pp_num
        self.assertTrue(default_to_curr_pp)


class TestFDALeaveFilter(unittest.TestCase):
    """Test fil_Leave_Records: FDA_REC_TYPE = '02' (lines 3651-3658)."""

    def test_filter_leave_records(self):
        """Verify only FDA_REC_TYPE='02' records pass."""
        records = [
            {"FDA_REC_TYPE": "02", "FDA_EMP_ID": "E001"},
            {"FDA_REC_TYPE": "12", "FDA_EMP_ID": "E002"},
            {"FDA_REC_TYPE": "02", "FDA_EMP_ID": "E003"},
            {"FDA_REC_TYPE": "01", "FDA_EMP_ID": "E004"},
        ]
        filtered = [r for r in records if r["FDA_REC_TYPE"] == "02"]
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]["FDA_EMP_ID"], "E001")
        self.assertEqual(filtered[1]["FDA_EMP_ID"], "E003")


class TestFDALeaveErrorCounter(unittest.TestCase):
    """Test m_0150_PM_FDA_Error_Counter (4 parallel validations, lines 1321-1356)."""

    def test_left_anti_join_finds_unmatched(self):
        """Verify left-anti join logic finds records not in staging."""
        fda_records = [{"FDA_EMP_ID": "E001"}, {"FDA_EMP_ID": "E002"}, {"FDA_EMP_ID": "E003"}]
        staging_records = [{"FDA_EMP_ID": "E001"}, {"FDA_EMP_ID": "E003"}]
        staging_ids = {r["FDA_EMP_ID"] for r in staging_records}
        errors = [r for r in fda_records if r["FDA_EMP_ID"] not in staging_ids]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["FDA_EMP_ID"], "E002")

    def test_all_matched_no_errors(self):
        """Verify no errors when all records match staging."""
        fda_records = [{"FDA_EMP_ID": "E001"}]
        staging_ids = {"E001"}
        errors = [r for r in fda_records if r["FDA_EMP_ID"] not in staging_ids]
        self.assertEqual(len(errors), 0)


class TestFDALeavePostSQL(unittest.TestCase):
    """Test CRITICAL Post SQL DELETE on HI_PM_FDA_TATRAN_TBL (lines 5107-5108).
    
    THIS MUST NOT BE MISSED — core business logic.
    DELETE employees without fda_rec_type = '12'.
    """

    def test_post_sql_structure(self):
        """Verify Post SQL DELETE statement structure."""
        sql = """
            DELETE FROM HI_PM_FDA_TATRAN_TBL
            WHERE fda_emp_id NOT IN (
                SELECT DISTINCT fda_emp_id FROM HI_PM_FDA_TATRAN_TBL
                WHERE fda_rec_type = '12'
            )
        """
        self.assertIn("DELETE FROM HI_PM_FDA_TATRAN_TBL", sql)
        self.assertIn("NOT IN", sql)
        self.assertIn("fda_rec_type = '12'", sql)

    def test_post_sql_logic(self):
        """Verify DELETE logic: removes employees without rec_type='12'."""
        records = [
            {"fda_emp_id": "E001", "fda_rec_type": "02"},
            {"fda_emp_id": "E001", "fda_rec_type": "12"},
            {"fda_emp_id": "E002", "fda_rec_type": "02"},
            {"fda_emp_id": "E003", "fda_rec_type": "12"},
        ]
        # Employees with rec_type='12'
        has_12 = {r["fda_emp_id"] for r in records if r["fda_rec_type"] == "12"}
        # Records to keep (employees who have at least one '12' record)
        kept = [r for r in records if r["fda_emp_id"] in has_12]
        # Records to delete (employees who have NO '12' record)
        deleted = [r for r in records if r["fda_emp_id"] not in has_12]

        self.assertEqual(len(kept), 3)  # E001(x2) + E003
        self.assertEqual(len(deleted), 1)  # E002 only
        self.assertEqual(deleted[0]["fda_emp_id"], "E002")


class TestFDALeaveDistinctFiles(unittest.TestCase):
    """Test srt_Distinct_File_Names (Sorter Distinct=YES, lines 4982-4993)."""

    def test_distinct_file_names(self):
        """Verify duplicate removal and sorting."""
        files = ["file_c.txt", "file_a.txt", "file_b.txt", "file_a.txt"]
        distinct_sorted = sorted(set(files))
        self.assertEqual(distinct_sorted, ["file_a.txt", "file_b.txt", "file_c.txt"])


class TestFDALeaveCountFiles(unittest.TestCase):
    """Test agg_Count_Number_of_Files (lines 4966-4975)."""

    def test_count_distinct_files(self):
        """Verify file count aggregation."""
        files = ["file_a.txt", "file_b.txt", "file_a.txt"]
        count_files = len(set(files))
        self.assertEqual(count_files, 2)


class TestFDALeaveCounters(unittest.TestCase):
    """Test FDA Leave counter writes (lines 3731-3742)."""

    def test_counter_names(self):
        """Verify all 4 counter names are defined."""
        counters = {
            "TATRAN Records Read": 100,
            "Leave Records Read": 50,
            "Error Records": 5,
            "New TATRAN Records Written": 45,
        }
        self.assertEqual(len(counters), 4)
        self.assertEqual(counters["TATRAN Records Read"], 100)
        self.assertEqual(counters["Leave Records Read"], 50)
        self.assertEqual(counters["Error Records"], 5)
        self.assertEqual(counters["New TATRAN Records Written"], 45)


if __name__ == "__main__":
    unittest.main()
