"""
Logging and metrics capture utilities for PySpark migration.

Replaces Informatica session-level metrics tracking:
- $<session>.StartTime, EndTime
- $<session>.SrcSuccessRows, SrcFailedRows
- $<session>.TgtSuccessRows, TgtFailedRows
- $<session>.TotalTransErrors
- $<session>.FirstErrorCode, FirstErrorMsg
(Confirmed in XML/EHRP2BIIS_UPDATE lines 2791-2803, XML/COMPTIME lines 963-975)

Also replaces Informatica performance data collection:
- Collect performance data = YES/NO (XML/COMPTIME lines 944-946)
- Write performance data to repository = YES/NO
"""

import logging
import os
import sys
import json
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional


def setup_logging(job_name: str, log_dir: str = "/home/sa-biisint/data/int/log") -> logging.Logger:
    """Set up logging for an ETL job.
    
    Replaces Informatica session log files:
    - Session Log File Name (e.g., s_Pay_Calendar_Reset_Pay_Calendar.log)
    - Workflow Log File Name (e.g., wf_EHRP2BIIS_UPDATE.log)
    """
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    log_file = os.path.join(log_dir, f"{job_name}_{timestamp}.log")

    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized for {job_name} -> {log_file}")
    return logger


@dataclass
class SessionMetrics:
    """Capture session-level metrics for before/after comparison.
    
    Maps directly to Informatica workflow variables:
    - $<session>.StartTime → start_time
    - $<session>.EndTime → end_time
    - $<session>.SrcSuccessRows → src_success_rows
    - $<session>.SrcFailedRows → src_failed_rows
    - $<session>.TgtSuccessRows → tgt_success_rows
    - $<session>.TgtFailedRows → tgt_failed_rows
    - $<session>.TotalTransErrors → total_trans_errors
    - $<session>.FirstErrorCode → first_error_code
    - $<session>.FirstErrorMsg → first_error_msg
    """
    session_name: str = ""
    mapping_name: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    src_success_rows: int = 0
    src_failed_rows: int = 0
    tgt_success_rows: int = 0
    tgt_failed_rows: int = 0
    total_trans_errors: int = 0
    first_error_code: int = 0
    first_error_msg: str = ""
    status: str = "NOT_STARTED"

    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds (EndTime - StartTime)."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def duration_minutes(self) -> float:
        """Calculate duration in minutes."""
        return self.duration_seconds / 60.0

    def mark_started(self) -> None:
        self.start_time = datetime.now()
        self.status = "RUNNING"

    def mark_succeeded(self) -> None:
        self.end_time = datetime.now()
        self.status = "SUCCEEDED"

    def mark_failed(self, error_code: int = -1, error_msg: str = "") -> None:
        self.end_time = datetime.now()
        self.status = "FAILED"
        if not self.first_error_code:
            self.first_error_code = error_code
        if not self.first_error_msg:
            self.first_error_msg = error_msg

    def to_dict(self) -> dict:
        """Convert to dictionary for reporting."""
        d = asdict(self)
        d["start_time"] = str(self.start_time) if self.start_time else ""
        d["end_time"] = str(self.end_time) if self.end_time else ""
        d["duration_seconds"] = self.duration_seconds
        d["duration_minutes"] = self.duration_minutes
        return d


@dataclass
class JobMetrics:
    """Capture job-level metrics for the performance comparison template.
    
    Aggregates all session metrics for a single workflow run.
    Used to populate Section 6 Before/After Performance tables.
    """
    job_name: str = ""
    workflow_name: str = ""
    sessions: list = field(default_factory=list)
    job_start_time: Optional[datetime] = None
    job_end_time: Optional[datetime] = None
    status: str = "NOT_STARTED"

    def add_session(self, session: SessionMetrics) -> None:
        self.sessions.append(session)

    def mark_started(self) -> None:
        self.job_start_time = datetime.now()
        self.status = "RUNNING"

    def mark_succeeded(self) -> None:
        self.job_end_time = datetime.now()
        self.status = "SUCCEEDED"

    def mark_failed(self) -> None:
        self.job_end_time = datetime.now()
        self.status = "FAILED"

    @property
    def total_src_success_rows(self) -> int:
        return sum(s.src_success_rows for s in self.sessions)

    @property
    def total_tgt_success_rows(self) -> int:
        return sum(s.tgt_success_rows for s in self.sessions)

    @property
    def total_duration_minutes(self) -> float:
        if self.job_start_time and self.job_end_time:
            return (self.job_end_time - self.job_start_time).total_seconds() / 60.0
        return 0.0

    def to_report(self) -> dict:
        """Generate a performance report for the Word document."""
        return {
            "job_name": self.job_name,
            "workflow_name": self.workflow_name,
            "status": self.status,
            "job_start_time": str(self.job_start_time) if self.job_start_time else "",
            "job_end_time": str(self.job_end_time) if self.job_end_time else "",
            "total_duration_minutes": round(self.total_duration_minutes, 2),
            "total_src_success_rows": self.total_src_success_rows,
            "total_tgt_success_rows": self.total_tgt_success_rows,
            "sessions": [s.to_dict() for s in self.sessions]
        }

    def summary(self) -> str:
        """Generate a human-readable summary string."""
        lines = [
            f"Job: {self.job_name} ({self.workflow_name})",
            f"Status: {self.status}",
            f"Duration: {self.total_duration_minutes:.2f} minutes",
            f"Total Source Rows: {self.total_src_success_rows}",
            f"Total Target Rows: {self.total_tgt_success_rows}",
            f"Sessions: {len(self.sessions)}",
        ]
        for s in self.sessions:
            lines.append(
                f"  - {s.session_name}: {s.status} "
                f"(src={s.src_success_rows}, tgt={s.tgt_success_rows})"
            )
        return "\n".join(lines)

    def save_report(self, filepath: str) -> None:
        """Save performance report to a JSON file."""
        report = self.to_report()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)
