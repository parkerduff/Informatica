"""
Email notification service for PySpark migration.

Replaces Informatica email tasks:
- Email_Pay_Calendar (XML/Pay_Calendar lines 589-593)
- email_COMPTIME_Complete (XML/COMPTIME)
- FDA Leave email notifications
- EHRP2BIIS success/failure emails
- on_failure_mail session components

Original recipients (ehrp2biis_preload line 8):
  peter.chen@hhs.gov, nathan.knight@hhs.gov, marvin.simon@hhs.gov
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional

from pyspark_migration.common.config import EmailConfig

logger = logging.getLogger(__name__)


class EmailService:
    """Email notification service.
    
    Replaces Informatica Email Task type with attributes:
    - Email User Name (recipient list)
    - Email Subject ($$WF_SUBJECT or hardcoded)
    - Email Text ($$WF_MESSAGE or template with %s, %b, %c, %g placeholders)
    """

    def __init__(self, config: EmailConfig):
        self.config = config

    def send_email(
        self,
        subject: str,
        body: str,
        recipients: Optional[List[str]] = None,
        attachments: Optional[List[str]] = None
    ) -> bool:
        """Send an email notification.
        
        Replaces Informatica email task execution.
        
        Args:
            subject: Email subject (replaces $$WF_SUBJECT)
            body: Email body text (replaces $$WF_MESSAGE)
            recipients: List of recipients (replaces $$WF_*_EMAIL_LIST)
            attachments: Optional file paths to attach
            
        Returns:
            True if email sent successfully
        """
        if recipients is None:
            recipients = self.config.default_recipients

        try:
            msg = MIMEMultipart()
            msg["From"] = self.config.from_address
            msg["To"] = ", ".join(recipients)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            if attachments:
                for filepath in attachments:
                    try:
                        with open(filepath, "r") as f:
                            attachment = MIMEText(f.read())
                            attachment.add_header(
                                "Content-Disposition", "attachment",
                                filename=filepath.split("/")[-1]
                            )
                            msg.attach(attachment)
                    except FileNotFoundError:
                        logger.warning(f"Attachment not found: {filepath}")

            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.send_message(msg)

            logger.info(f"Email sent: '{subject}' to {recipients}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def send_success_email(
        self,
        job_name: str,
        env_prefix: str,
        message: str,
        recipients: Optional[List[str]] = None
    ) -> bool:
        """Send a job success notification.
        
        Replaces Informatica on_success_mail and workflow email tasks.
        """
        subject = f"{env_prefix}{job_name} completed successfully"
        return self.send_email(subject=subject, body=message, recipients=recipients)

    def send_failure_email(
        self,
        job_name: str,
        session_name: str,
        error_message: str,
        start_time: str = "",
        end_time: str = "",
        recipients: Optional[List[str]] = None,
        log_file: Optional[str] = None
    ) -> bool:
        """Send a job failure notification.
        
        Replaces Informatica on_failure_mail session component.
        Original template (XML/Pay_Calendar lines 606-611):
          'Session completed with errors.
           Session name: %s
           Session start time: %b
           Session completion time: %c
           Please read the attached session log for further details.
           %g'
        """
        subject = f"Error in {job_name} - {session_name}"
        body = (
            f"Session completed with errors.\n\n"
            f"Session name: {session_name}\n"
            f"Session start time: {start_time}\n"
            f"Session completion time: {end_time}\n\n"
            f"Error: {error_message}\n\n"
            f"Please read the attached session log for further details."
        )
        attachments = [log_file] if log_file else None
        return self.send_email(
            subject=subject, body=body,
            recipients=recipients, attachments=attachments
        )
