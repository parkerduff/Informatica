"""Notification dispatch. Replaces the mailx calls in the Korn shell scripts.

Recipients always come from config (never hard-coded), preserving the original
distribution lists while removing the embedded HHS addresses from source code.
"""
from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText
from typing import List

from utils.secrets import Config

logger = logging.getLogger(__name__)


def _recipients(config: Config) -> List[str]:
    return [r.strip() for r in config.notifications.recipients.split(",") if r.strip()]


def send_notification(subject: str, body: str, config: Config) -> None:
    """Dispatch a notification via the configured provider.

    provider ``log`` -> stdout/logger (dev/test)
    provider ``ses`` -> AWS Simple Email Service
    provider ``smtp`` -> SMTP relay
    """
    provider = config.notifications.provider
    recipients = _recipients(config)

    if provider == "log":
        logger.info("NOTIFY [to=%s] %s\n%s", ",".join(recipients), subject, body)
        return

    if provider == "ses":  # pragma: no cover
        import boto3

        client = boto3.client("ses", region_name=config.notifications.region)
        client.send_email(
            Source=config.notifications.sender or "biis-noreply@hhs.gov",
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body}},
            },
        )
        logger.info("Sent SES notification %r to %s", subject, recipients)
        return

    if provider == "smtp":  # pragma: no cover
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = config.notifications.sender or "biis-noreply@hhs.gov"
        msg["To"] = ", ".join(recipients)
        host = config.notifications.smtp_host or "localhost"
        with smtplib.SMTP(host, config.notifications.smtp_port) as server:
            server.send_message(msg)
        logger.info("Sent SMTP notification %r to %s", subject, recipients)
        return

    raise ValueError(f"Unknown notification provider {provider!r}")
