"""
Email notification service.

Replaces Informatica email tasks across all workflows:
  - s_BIIS_Pay_Calendar_Email (Pay_Calendar)
  - s_COMPTIME_Email (COMPTIME)
  - s_Pseudossn_Email (Pseudossn)
  - s_FDA_Leave_Email, s_FDA_LEAVE_VALIDATE_EMAIL (FDA_Leave)
  - s_CPM_NIH_EMAIL (CPM_NIH/CPM_CDC)
  - s_EHRP2BIIS_UPDATE_Email (EHRP2BIIS_UPDATE)

Environment prefix logic from Informatica Expression:
  DECODE(SUBSTR($PMRepositoryServiceName, 1, 4),
      'Dev_', 'Dev: ', 'Test', 'Test: ', '')
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from pyspark_migration.common.config import EmailConfig
from pyspark_migration.common.logging_utils import JobMetrics

logger = logging.getLogger(__name__)


class EmailService:
    """Sends email notifications replacing Informatica email tasks.

    Features:
      - Environment prefix in subject (Dev:/Test:/Prod)
      - Success and failure templates
      - Job metrics included in body
      - Configurable enable/disable via EMAIL_ENABLED env var
    """

    def __init__(self, config: Optional[EmailConfig] = None):
        self._config = config or EmailConfig()

    def send(
        self,
        subject: str,
        body: str,
        recipients: Optional[List[str]] = None,
        is_failure: bool = False,
    ) -> bool:
        """Send an email notification.

        Args:
            subject: Email subject (environment prefix auto-added).
            body: Email body text.
            recipients: List of email addresses. Uses defaults if None.
            is_failure: If True, prepends 'FAILURE: ' to subject.

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self._config.enabled:
            logger.info(
                "Email disabled - would send: subject='%s%s'",
                self._config.environment_prefix,
                subject,
            )
            return True

        if recipients is None:
            recipients = [
                r.strip()
                for r in self._config.default_recipients.split(",")
                if r.strip()
            ]

        full_subject = f"{self._config.environment_prefix}"
        if is_failure:
            full_subject += "FAILURE: "
        full_subject += subject

        try:
            msg = MIMEMultipart()
            msg["From"] = self._config.sender
            msg["To"] = ", ".join(recipients)
            msg["Subject"] = full_subject
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(self._config.smtp_host, self._config.smtp_port) as server:
                server.sendmail(self._config.sender, recipients, msg.as_string())

            logger.info("Email sent: subject='%s' to %s", full_subject, recipients)
            return True

        except Exception as exc:
            logger.error("Email FAILED: %s - %s", full_subject, str(exc))
            return False

    def send_job_success(
        self,
        job_name: str,
        metrics: Optional[JobMetrics] = None,
        extra_message: str = "",
        recipients: Optional[List[str]] = None,
    ) -> bool:
        """Send a job success notification.

        Replaces Informatica email task with $$WF_*_EMAIL_MSG body.
        """
        subject = f"{job_name} - Completed Successfully"
        body_parts = [f"Job: {job_name}", "Status: SUCCESS", ""]

        if metrics:
            body_parts.extend([
                f"Total Source Rows: {metrics.total_src_success_rows}",
                f"Total Target Rows: {metrics.total_tgt_success_rows}",
                f"Total Errors: {metrics.total_errors}",
                f"Duration: {metrics.total_duration_seconds:.2f}s",
                f"Sessions: {len(metrics.sessions)}",
                "",
            ])

        if extra_message:
            body_parts.append(extra_message)

        return self.send(subject, "\n".join(body_parts), recipients)

    def send_job_failure(
        self,
        job_name: str,
        error_message: str,
        session_name: str = "",
        metrics: Optional[JobMetrics] = None,
        recipients: Optional[List[str]] = None,
    ) -> bool:
        """Send a job failure notification."""
        subject = f"{job_name} - FAILED"
        body_parts = [
            f"Job: {job_name}",
            "Status: FAILED",
            "",
            f"Error: {error_message}",
        ]

        if session_name:
            body_parts.append(f"Failed Session: {session_name}")

        if metrics:
            body_parts.extend([
                "",
                f"Completed Sessions: {len(metrics.sessions)}",
                f"Failed Sessions: {len(metrics.failed_sessions)}",
            ])

        return self.send(
            subject, "\n".join(body_parts), recipients, is_failure=True
        )
