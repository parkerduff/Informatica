"""Email notification utility.

Replaces the ``mailx`` / ``uuencode`` calls in the original shell scripts with
``smtplib``-based delivery, including optional log-file attachments.
"""
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional

from config.notifications import DEFAULT_RECIPIENTS, SMTP_FROM, SMTP_HOST, SMTP_PORT


def send_notification(subject: str, recipients: Optional[List[str]] = None,
                      body: str = "", attachment_path: Optional[str] = None) -> None:
    """Send an email notification with an optional attachment.

    Replaces ``mailx`` calls. ``recipients`` defaults to DEFAULT_RECIPIENTS.
    """
    recipients = recipients or DEFAULT_RECIPIENTS

    message = MIMEMultipart()
    message["From"] = SMTP_FROM
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    if attachment_path:
        path = Path(attachment_path)
        if path.is_file():
            with path.open("rb") as fh:
                part = MIMEApplication(fh.read(), Name=path.name)
            part["Content-Disposition"] = 'attachment; filename="%s"' % path.name
            message.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.sendmail(SMTP_FROM, recipients, message.as_string())
