"""
Email Notification

Replaces mailx calls from ehrp2biis_preload line 8, actstage_load,
and all Informatica post-session email tasks.
"""

import logging
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from pyspark_migration.config.settings import EMAIL_CONFIG, get_environment

logger = logging.getLogger(__name__)


def _get_environment_prefix():
    """
    Get environment prefix for email subjects.

    Replicates COMPTIME's DECODE(SUBSTR($PMRepositoryServiceName, 1, 4), ...)
    from XML/COMPTIME lines 84-97:
        'Dev_' -> 'Dev: '
        'Test' -> 'Test: '
        'Prod' -> 'Prod: '
    """
    env = get_environment()
    prefix_map = {
        "dev": "Dev: ",
        "test": "Test: ",
        "prod": "Prod: ",
    }
    return prefix_map.get(env, "")


def send_email(subject, body, recipients=None, attachments=None,
               include_env_prefix=True):
    """
    Send an email notification.

    Replaces all mailx calls throughout the shell scripts:
    - ehrp2biis_preload line 60/66
    - actstage_load line 59/62
    - Transfer scripts success/failure emails

    Parameters
    ----------
    subject : str
        Email subject line.
    body : str
        Email body text.
    recipients : list of str, optional
        Email addresses. Defaults to EMAIL_CONFIG['default_recipients'].
    attachments : list of str, optional
        File paths to attach to the email.
    include_env_prefix : bool
        Whether to prepend environment prefix to subject.
    """
    if recipients is None:
        recipients = EMAIL_CONFIG["default_recipients"]

    if include_env_prefix:
        subject = f"{_get_environment_prefix()}{subject}"

    msg = MIMEMultipart()
    msg["From"] = EMAIL_CONFIG["from_address"]
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    if attachments:
        for file_path in attachments:
            if os.path.exists(file_path):
                with open(file_path, "rb") as fp:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(fp.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={os.path.basename(file_path)}",
                )
                msg.attach(part)
            else:
                logger.warning("Attachment not found: %s", file_path)

    try:
        with smtplib.SMTP(
            EMAIL_CONFIG["smtp_host"], EMAIL_CONFIG["smtp_port"]
        ) as server:
            server.sendmail(
                EMAIL_CONFIG["from_address"],
                recipients,
                msg.as_string(),
            )
        logger.info("Email sent: '%s' to %s", subject, recipients)
    except Exception:
        logger.exception("Failed to send email: '%s'", subject)
        raise


def send_success_email(process_name, message, recipients=None, attachments=None):
    """
    Send a success notification email.

    Parameters
    ----------
    process_name : str
    message : str
    recipients : list of str, optional
    attachments : list of str, optional
    """
    subject = f"{process_name} completed successfully"
    send_email(subject, message, recipients, attachments)


def send_failure_email(process_name, error_message, recipients=None):
    """
    Send a failure notification email.

    Parameters
    ----------
    process_name : str
    error_message : str
    recipients : list of str, optional
    """
    subject = f"{process_name} did not complete successfully"
    body = f"Process: {process_name}\nError: {error_message}\n"
    send_email(subject, body, recipients)
