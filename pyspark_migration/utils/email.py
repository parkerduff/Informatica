"""
Email notification utility.

Replaces the ``mailx`` commands used throughout the KSH orchestration scripts
and the Informatica PowerCenter Email tasks.

Configuration is read from environment variables:
  - SMTP_HOST     : SMTP relay hostname (default: localhost)
  - SMTP_PORT     : SMTP relay port     (default: 25)
  - EMAIL_FROM    : Sender address       (default: biisint@hhs.gov)
"""

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

# Default notification recipients (matches the legacy KSH scripts)
DEFAULT_RECIPIENTS = [
    "peter.chen@hhs.gov",
    "nathan.knight@hhs.gov",
    "marvin.simon@hhs.gov",
]

_SMTP_HOST_ENV = "SMTP_HOST"
_SMTP_PORT_ENV = "SMTP_PORT"
_EMAIL_FROM_ENV = "EMAIL_FROM"


def _get_smtp_host() -> str:
    return os.environ.get(_SMTP_HOST_ENV, "localhost")


def _get_smtp_port() -> int:
    return int(os.environ.get(_SMTP_PORT_ENV, "25"))


def _get_email_from() -> str:
    return os.environ.get(_EMAIL_FROM_ENV, "biisint@hhs.gov")


def send_email(
    subject: str,
    body: str,
    recipients: list = None,
    attachment_path: str = None,
) -> None:
    """Send an email notification.

    Parameters
    ----------
    subject : str
        Email subject line.
    body : str
        Plain-text email body.
    recipients : list[str], optional
        List of recipient email addresses. Defaults to ``DEFAULT_RECIPIENTS``.
    attachment_path : str, optional
        If provided, attach the file at this path to the email (replicates
        the ``uuencode`` + ``mailx`` pattern in the legacy scripts).
    """
    recipients = recipients or DEFAULT_RECIPIENTS
    sender = _get_email_from()

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if attachment_path and os.path.isfile(attachment_path):
        with open(attachment_path, "r") as fh:
            attachment = MIMEText(fh.read())
        attachment.add_header(
            "Content-Disposition",
            "attachment",
            filename=os.path.basename(attachment_path),
        )
        msg.attach(attachment)

    host = _get_smtp_host()
    port = _get_smtp_port()

    logger.info(
        "Sending email to %s via %s:%s — subject: %s",
        recipients, host, port, subject,
    )

    with smtplib.SMTP(host, port) as server:
        server.sendmail(sender, recipients, msg.as_string())

    logger.info("Email sent successfully")


def send_success_email(process_name: str, details: str = "", recipients: list = None) -> None:
    """Convenience wrapper for success notifications."""
    subject = f"{process_name} completed successfully"
    body = details or f"{process_name} finished without errors."
    send_email(subject=subject, body=body, recipients=recipients)


def send_failure_email(process_name: str, error_msg: str = "", recipients: list = None) -> None:
    """Convenience wrapper for failure notifications."""
    subject = f"{process_name} did not complete successfully"
    body = error_msg or f"{process_name} encountered an error. Please investigate."
    send_email(subject=subject, body=body, recipients=recipients)
