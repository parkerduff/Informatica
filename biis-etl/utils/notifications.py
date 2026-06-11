"""Notification dispatch.

Replaces the ``mailx`` / ``uuencode`` calls in the legacy ksh scripts.

* ``mock`` -> append messages to an in-memory list (asserted in tests).
* ``smtp`` -> send a real email via smtplib (production).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:  # pragma: no cover
    from utils.config import Config


@dataclass
class Notification:
    subject: str
    body: str
    recipients: List[str]


# Captured notifications when provider == "mock".
SENT: List[Notification] = []


def send_notification(subject: str, body: str, config: "Config", recipients=None) -> Notification:
    """Send (or capture) a notification and return it."""
    recipients = recipients or config.notifications.get("recipients", ["biis-ops@hhs.gov"])
    note = Notification(subject=subject, body=body, recipients=list(recipients))
    provider = config.notifications.get("provider", "mock")
    if provider == "mock":
        SENT.append(note)
        return note
    if provider == "smtp":  # pragma: no cover - requires SMTP server
        import smtplib
        from email.mime.text import MIMEText

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = config.notifications.get("sender", "biis-noreply@hhs.gov")
        msg["To"] = ", ".join(note.recipients)
        host = config.notifications.get("smtp_host", "localhost")
        with smtplib.SMTP(host) as server:
            server.send_message(msg)
        return note
    raise ValueError(f"Unknown notifications provider: {provider!r}")


def reset() -> None:
    """Clear captured mock notifications (used between tests)."""
    SENT.clear()
