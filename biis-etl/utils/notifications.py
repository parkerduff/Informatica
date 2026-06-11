"""Notification dispatch replacing the legacy mailx-based status emails."""
import logging
import smtplib
from email.message import EmailMessage

logger = logging.getLogger(__name__)


def send_notification(subject: str, body: str, config: dict) -> None:
    notif = config.get("notifications", {})
    provider = notif.get("provider", "log")
    recipients = notif.get("recipients", "")
    if provider == "log":
        logger.info("NOTIFICATION to=%s subject=%s body=%s", recipients, subject, body)
        return
    if provider == "ses":
        import boto3

        client = boto3.client("ses", region_name=notif.get("region", "us-east-1"))
        client.send_email(
            Source=notif.get("sender", "noreply@biis.local"),
            Destination={"ToAddresses": [r.strip() for r in recipients.split(",")]},
            Message={"Subject": {"Data": subject}, "Body": {"Text": {"Data": body}}},
        )
        return
    if provider == "smtp":
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = notif.get("sender", "noreply@biis.local")
        msg["To"] = recipients
        msg.set_content(body)
        with smtplib.SMTP(notif.get("smtp_host", "localhost"), int(notif.get("smtp_port", 25))) as smtp:
            smtp.send_message(msg)
        return
    raise ValueError(f"Unknown notification provider: {provider}")
