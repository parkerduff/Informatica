"""Email notification configuration (replaces the ``mailx`` calls in the ksh scripts)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _emails(name: str, default: List[str]) -> List[str]:
    raw = os.environ.get(name)
    if not raw:
        return default
    return [e.strip() for e in raw.replace(",", " ").split() if e.strip()]


@dataclass(frozen=True)
class SmtpConfig:
    host: str = field(default_factory=lambda: _env("SMTP_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(_env("SMTP_PORT", "25")))
    user: str = field(default_factory=lambda: _env("SMTP_USER", ""))
    password: str = field(default_factory=lambda: _env("SMTP_PASSWORD", ""))
    use_tls: bool = field(default_factory=lambda: _env("SMTP_TLS", "false").lower() == "true")
    sender: str = field(default_factory=lambda: _env("SMTP_SENDER", "biisint@hhs.gov"))


SMTP = SmtpConfig()

# Default distribution lists, taken verbatim from the original scripts.
# ehrp2biis_preload / actstage_load.
EHRP2BIIS_RECIPIENTS = _emails(
    "EHRP2BIIS_RECIPIENTS",
    ["peter.chen@hhs.gov", "nathan.knight@hhs.gov", "marvin.simon@hhs.gov"],
)

# Transfer scripts (nih/cdc/oig/fda/afps).
TRANSFER_RECIPIENTS = _emails(
    "TRANSFER_RECIPIENTS",
    [
        "mariappan.muthiah@hhs.gov",
        "nathan.knight@hhs.gov",
        "karen.williams@hhs.gov",
        "marvin.simon@hhs.gov",
        "robin.cunningham@hhs.gov",
        "minh.tran@hhs.gov",
    ],
)
