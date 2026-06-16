"""Email notification configuration.

Replaces the hard-coded ``mailx`` recipient lists used by the original shell
scripts.
"""
import os

from dotenv import load_dotenv

load_dotenv()

DEFAULT_RECIPIENTS = [
    "peter.chen@hhs.gov",
    "nathan.knight@hhs.gov",
    "marvin.simon@hhs.gov",
]

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "25"))
SMTP_FROM = os.environ.get("SMTP_FROM", "biis-etl@hhs.gov")
