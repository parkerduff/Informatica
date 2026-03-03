"""
Email notification utilities for BIISINT PySpark ETL pipelines.

Replaces:
  - mailx -s "subject" $p_mailid patterns from KSH scripts
  - uuencode $logfile output.txt | mailx patterns
  - Informatica PowerCenter post-session email tasks
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional

from pyspark.utils.config import EmailConfig

logger = logging.getLogger(__name__)


def send_email(
    config: EmailConfig,
    subject: str,
    body: str = "",
    recipients: Optional[list] = None,
    attachment_path: Optional[str] = None,
) -> None:
    """Send an email notification.

    Args:
        config: Email configuration.
        subject: Email subject line.
        body: Email body text.
        recipients: List of recipient email addresses. Falls back to
            ``config.default_recipients`` when *None*.
        attachment_path: Optional path to a file to attach.
    """
    if recipients is None:
        recipients = config.default_recipients

    msg = MIMEMultipart()
    msg["From"] = config.from_address
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    if body:
        msg.attach(MIMEText(body, "plain"))

    if attachment_path:
        try:
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={attachment_path.split('/')[-1]}",
            )
            msg.attach(part)
        except FileNotFoundError:
            logger.warning("Attachment file not found: %s", attachment_path)

    try:
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            server.sendmail(config.from_address, recipients, msg.as_string())
        logger.info("Email sent: %s", subject)
    except Exception:
        logger.exception("Failed to send email: %s", subject)


def send_success_notification(
    config: EmailConfig,
    process_name: str,
    message: str = "",
    log_file: Optional[str] = None,
    recipients: Optional[list] = None,
) -> None:
    """Send a success notification email.

    Args:
        config: Email configuration.
        process_name: Name of the completed process.
        message: Optional body text.
        log_file: Optional path to a log file to attach.
        recipients: Override list of recipients.
    """
    subject = f"{process_name} completed successfully"
    send_email(
        config,
        subject=subject,
        body=message,
        recipients=recipients,
        attachment_path=log_file,
    )


def send_failure_notification(
    config: EmailConfig,
    process_name: str,
    error_message: str = "",
    recipients: Optional[list] = None,
) -> None:
    """Send a failure notification email.

    Args:
        config: Email configuration.
        process_name: Name of the failed process.
        error_message: Description of the error.
        recipients: Override list of recipients.
    """
    subject = f"{process_name} did not complete successfully"
    send_email(
        config,
        subject=subject,
        body=error_message,
        recipients=recipients,
    )
