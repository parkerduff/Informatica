"""Email notifications via ``smtplib`` -- replaces all ``mailx`` calls.

The original scripts used ``mailx -s "<subject>" <recipients>`` and, on success,
``uuencode <logfile> | mailx`` to attach the run log. :func:`send_notification`
covers both cases (plain body and optional file attachment).
"""
from __future__ import annotations

import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from pyspark_etl.config.notifications import SMTP

logger = logging.getLogger(__name__)


def send_notification(
    subject: str,
    recipients: List[str],
    body: str = "",
    attachment_path: Optional[str] = None,
    dry_run: bool = False,
) -> bool:
    """Send an email; optionally attach a file (e.g. the run log).

    Returns ``True`` when the message was sent (or would be, under ``dry_run``).
    Failures are logged and swallowed so a notification problem never aborts an
    otherwise-successful ETL run -- matching the fire-and-forget ``mailx`` usage.
    """
    msg = MIMEMultipart()
    msg["From"] = SMTP.sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body or subject, "plain"))

    if attachment_path:
        try:
            with open(attachment_path, "rb") as fh:
                part = MIMEApplication(fh.read())
            part.add_header(
                "Content-Disposition", "attachment",
                filename=attachment_path.rsplit("/", 1)[-1],
            )
            msg.attach(part)
        except OSError as exc:
            logger.warning("Could not attach %s: %s", attachment_path, exc)

    if dry_run:
        logger.info("[dry-run] would email %s: %s", recipients, subject)
        return True

    try:
        with smtplib.SMTP(SMTP.host, SMTP.port) as server:
            if SMTP.use_tls:
                server.starttls()
            if SMTP.user:
                server.login(SMTP.user, SMTP.password)
            server.sendmail(SMTP.sender, recipients, msg.as_string())
        logger.info("Sent notification %r to %s", subject, recipients)
        return True
    except Exception as exc:  # pragma: no cover - network dependent
        logger.error("Failed to send notification %r: %s", subject, exc)
        return False
