"""
Unit and integration tests for Job 1: wf_Pay_Calendar.

Tests cover:
- Reset pay calendar (DD_UPDATE)
- Set pay calendar (parameter vs date-based routing)
- Verify pay calendar (ABORT() condition)
- Build message (SETVARIABLE pattern)
- Email notification
"""

import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime

from pyspark_migration.jobs.job1_pay_calendar import PayCalendarJob, WORKFLOW_NAME


class TestPayCalendarReset(unittest.TestCase):
    """Test s_Pay_Calendar_Reset_Pay_Calendar session."""

    def setUp(self):
        self.spark = MagicMock()
        self.config = MagicMock()
        self.config.env_prefix = "Test: "
        self.config.email.default_recipients = ["test@hhs.gov"]
        self.db = MagicMock()
        self.email = MagicMock()
        self.job = PayCalendarJob(self.spark, self.config, self.db, self.email)

    def test_reset_updates_curr_pp_flag(self):
        """Verify Reset clears CURR_PP_FLAG='Y' rows (DD_UPDATE)."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        self.db.read_jdbc.return_value = mock_df

        self.job._reset_pay_calendar()

        self.db.execute_sql.assert_called_once()
        call_args = self.db.execute_sql.call_args
        self.assertIn("UPDATE PAY_PERIOD", call_args[0][0])
        self.assertIn("CURR_PP_FLAG = NULL", call_args[0][0])

    def test_reset_metrics_tracking(self):
        """Verify session metrics are captured."""
        mock_df = MagicMock()
        mock_df.count.return_value = 3
        self.db.read_jdbc.return_value = mock_df

        self.job._reset_pay_calendar()

        self.assertEqual(len(self.job.job_metrics.sessions), 1)
        session = self.job.job_metrics.sessions[0]
        self.assertEqual(session.src_success_rows, 3)
        self.assertEqual(session.status, "succeeded")


class TestPayCalendarSet(unittest.TestCase):
    """Test s_Pay_Calendar_Set_Pay_Calendar session."""

    def setUp(self):
        self.spark = MagicMock()
        self.config = MagicMock()
        self.config.env_prefix = "Test: "
        self.config.email.default_recipients = ["test@hhs.gov"]
        self.db = MagicMock()
        self.email = MagicMock()

    @patch.dict("os.environ", {"WF_PP_END_YEAR": "2025", "WF_PP_NUM": "10"})
    def test_parameter_based_path(self):
        """Verify parameter-based routing when WF_PP_END_YEAR/WF_PP_NUM set."""
        job = PayCalendarJob(self.spark, self.config, self.db, self.email)
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_df.first.return_value = {"PP_END_YEAR": 2025, "PP_NUM": 10}
        self.db.read_jdbc.return_value = mock_df

        job._set_pay_calendar()

        call_args = self.db.execute_sql.call_args
        self.assertIn("PP_END_YEAR = :1", call_args[0][0])
        self.assertIn("PP_NUM = :2", call_args[0][0])

    @patch.dict("os.environ", {"WF_PP_END_YEAR": "", "WF_PP_NUM": ""})
    def test_date_based_path(self):
        """Verify date-based routing when parameters empty."""
        job = PayCalendarJob(self.spark, self.config, self.db, self.email)
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_df.first.return_value = {"PP_END_YEAR": 2025, "PP_NUM": 10}
        self.db.read_jdbc.return_value = mock_df

        job._set_pay_calendar()

        call_args = self.db.execute_sql.call_args
        self.assertIn("SYSDATE", call_args[0][0])


class TestPayCalendarVerify(unittest.TestCase):
    """Test s_Pay_Calendar_Verify_Pay_Calendar session (ABORT condition)."""

    def setUp(self):
        self.spark = MagicMock()
        self.config = MagicMock()
        self.config.env_prefix = "Test: "
        self.config.email.default_recipients = ["test@hhs.gov"]
        self.db = MagicMock()
        self.email = MagicMock()
        self.job = PayCalendarJob(self.spark, self.config, self.db, self.email)

    def test_verify_passes_with_one_current_pp(self):
        """Verify passes when exactly 1 CURR_PP_FLAG='Y' row."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        self.db.read_jdbc.return_value = mock_df

        self.job._verify_pay_calendar()  # Should not raise

    def test_verify_aborts_with_zero_current_pp(self):
        """Verify ABORT when 0 CURR_PP_FLAG='Y' rows (replaces ABORT())."""
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        self.db.read_jdbc.return_value = mock_df

        with self.assertRaises(Exception) as ctx:
            self.job._verify_pay_calendar()
        self.assertIn("Expected exactly 1", str(ctx.exception))

    def test_verify_aborts_with_multiple_current_pp(self):
        """Verify ABORT when >1 CURR_PP_FLAG='Y' rows."""
        mock_df = MagicMock()
        mock_df.count.return_value = 3
        self.db.read_jdbc.return_value = mock_df

        with self.assertRaises(Exception) as ctx:
            self.job._verify_pay_calendar()
        self.assertIn("Expected exactly 1", str(ctx.exception))
        self.assertIn("found 3", str(ctx.exception))


class TestPayCalendarBuildMessage(unittest.TestCase):
    """Test s_Pay_Calendar_Build_Message (SETVARIABLE pattern)."""

    def setUp(self):
        self.spark = MagicMock()
        self.config = MagicMock()
        self.config.env_prefix = "Prod: "
        self.config.email.default_recipients = ["test@hhs.gov"]
        self.db = MagicMock()
        self.email = MagicMock()
        self.job = PayCalendarJob(self.spark, self.config, self.db, self.email)

    def test_build_message_sets_subject_and_body(self):
        """Verify SETVARIABLE replacement for $$WF_SUBJECT and $$WF_MESSAGE."""
        mock_df = MagicMock()
        mock_df.first.return_value = {
            "PP_END_YEAR": 2025, "PP_NUM": 10,
            "PP_START_DTE": "2025-05-04", "PP_END_DTE": "2025-05-17"
        }
        mock_df.columns = ["PP_END_YEAR", "PP_NUM", "PP_START_DTE", "PP_END_DTE"]
        self.db.read_jdbc.return_value = mock_df

        self.job._build_message()

        self.assertIn("Prod: ", self.job.wf_subject)
        self.assertIn("2025", self.job.wf_subject)
        self.assertIn("10", self.job.wf_subject)
        self.assertIn("Pay Period", self.job.wf_message)

    def test_env_prefix_in_subject(self):
        """Verify environment prefix appears in email subject."""
        self.config.env_prefix = "Dev: "
        mock_df = MagicMock()
        mock_df.first.return_value = {"PP_END_YEAR": 2025, "PP_NUM": 5}
        mock_df.columns = ["PP_END_YEAR", "PP_NUM"]
        self.db.read_jdbc.return_value = mock_df

        self.job._build_message()

        self.assertTrue(self.job.wf_subject.startswith("Dev: "))


class TestPayCalendarEmail(unittest.TestCase):
    """Test Email_Pay_Calendar task."""

    def setUp(self):
        self.spark = MagicMock()
        self.config = MagicMock()
        self.config.env_prefix = "Test: "
        self.config.email.default_recipients = ["peter.chen@hhs.gov"]
        self.db = MagicMock()
        self.email = MagicMock()
        self.job = PayCalendarJob(self.spark, self.config, self.db, self.email)
        self.job.wf_subject = "Test: Pay Calendar Set"
        self.job.wf_message = "Pay period set to 2025-10"

    def test_email_sent_with_correct_params(self):
        """Verify email is sent with correct subject, body, recipients."""
        self.job._send_email()

        self.email.send_email.assert_called_once_with(
            subject="Test: Pay Calendar Set",
            body="Pay period set to 2025-10",
            recipients=["peter.chen@hhs.gov"]
        )


class TestPayCalendarWorkflow(unittest.TestCase):
    """Integration tests for full wf_Pay_Calendar workflow."""

    def setUp(self):
        self.spark = MagicMock()
        self.config = MagicMock()
        self.config.env_prefix = "Test: "
        self.config.email.default_recipients = ["test@hhs.gov"]
        self.db = MagicMock()
        self.email = MagicMock()

    @patch.dict("os.environ", {"WF_PP_END_YEAR": "2025", "WF_PP_NUM": "10"})
    def test_full_workflow_success(self):
        """Verify complete workflow executes all steps in sequence."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_df.first.return_value = {
            "PP_END_YEAR": 2025, "PP_NUM": 10,
            "PP_START_DTE": "2025-05-04", "PP_END_DTE": "2025-05-17"
        }
        mock_df.columns = ["PP_END_YEAR", "PP_NUM", "PP_START_DTE", "PP_END_DTE"]
        self.db.read_jdbc.return_value = mock_df

        job = PayCalendarJob(self.spark, self.config, self.db, self.email)
        metrics = job.run()

        self.assertEqual(metrics.status, "succeeded")
        self.assertEqual(len(metrics.sessions), 4)  # 4 sessions + email
        self.email.send_email.assert_called_once()

    @patch.dict("os.environ", {"WF_PP_END_YEAR": "2025", "WF_PP_NUM": "10"})
    def test_workflow_step_ordering(self):
        """Verify steps run in correct sequence: Reset → Set → Verify → Message → Email."""
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_df.first.return_value = {"PP_END_YEAR": 2025, "PP_NUM": 10}
        mock_df.columns = ["PP_END_YEAR", "PP_NUM"]
        self.db.read_jdbc.return_value = mock_df

        job = PayCalendarJob(self.spark, self.config, self.db, self.email)
        job.run()

        session_names = [s.session_name for s in job.job_metrics.sessions]
        expected_order = [
            "s_Pay_Calendar_Reset_Pay_Calendar",
            "s_Pay_Calendar_Set_Pay_Calendar",
            "s_Pay_Calendar_Verify_Pay_Calendar",
            "s_Pay_Calendar_Build_Message",
        ]
        self.assertEqual(session_names, expected_order)


if __name__ == "__main__":
    unittest.main()
