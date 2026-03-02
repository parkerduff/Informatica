"""
Session-level and job-level metrics tracking.

Replaces Informatica built-in session variables that were NOT being collected
(Collect performance data = NO across all 40 sessions). The PySpark migration
adds what Informatica was missing: timing, row counts, error tracking, and
throughput measurement for every session.

Informatica session variables replaced:
  $session.SrcSuccessRows  -> SessionMetrics.src_success_rows
  $session.SrcFailedRows   -> SessionMetrics.src_failed_rows
  $session.TgtSuccessRows  -> SessionMetrics.tgt_success_rows
  $session.TgtFailedRows   -> SessionMetrics.tgt_failed_rows
  $session.TotalTransErrors-> SessionMetrics.total_trans_errors
  $session.FirstErrorCode  -> SessionMetrics.first_error_code
  $session.FirstErrorMsg   -> SessionMetrics.first_error_msg
  $session.StartTime       -> SessionMetrics.start_time
  $session.EndTime         -> SessionMetrics.end_time
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class SessionMetrics:
    """Tracks metrics for a single session (mapping execution).

    Every PySpark session method must create a SessionMetrics instance,
    call start() before processing, and stop() after processing completes.
    """

    session_name: str
    mapping_name: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    src_success_rows: int = 0
    src_failed_rows: int = 0
    tgt_success_rows: int = 0
    tgt_failed_rows: int = 0
    total_trans_errors: int = 0
    first_error_code: int = 0
    first_error_msg: str = ""
    status: str = "NOT_STARTED"
    _start_ns: int = field(default=0, repr=False)

    def start(self) -> "SessionMetrics":
        """Mark session as started. Call before processing."""
        self.start_time = datetime.now()
        self._start_ns = time.perf_counter_ns()
        self.status = "RUNNING"
        logger.info(
            "Session STARTED: %s (mapping: %s)",
            self.session_name,
            self.mapping_name,
        )
        return self

    def stop(self, success: bool = True) -> "SessionMetrics":
        """Mark session as completed. Call after processing."""
        self.end_time = datetime.now()
        elapsed_ns = time.perf_counter_ns() - self._start_ns
        self.duration_seconds = elapsed_ns / 1_000_000_000
        self.status = "SUCCEEDED" if success else "FAILED"
        logger.info(
            "Session %s: %s (duration=%.2fs, src_ok=%d, src_fail=%d, "
            "tgt_ok=%d, tgt_fail=%d, errors=%d)",
            self.status,
            self.session_name,
            self.duration_seconds,
            self.src_success_rows,
            self.src_failed_rows,
            self.tgt_success_rows,
            self.tgt_failed_rows,
            self.total_trans_errors,
        )
        return self

    def record_error(self, error_code: int, error_msg: str) -> None:
        """Record an error. Only the first error code/msg are kept."""
        self.total_trans_errors += 1
        if self.first_error_code == 0:
            self.first_error_code = error_code
            self.first_error_msg = error_msg

    @property
    def throughput_rows_per_sec(self) -> float:
        """Source rows processed per second."""
        if self.duration_seconds > 0:
            return self.src_success_rows / self.duration_seconds
        return 0.0

    def reconciliation_check(self) -> bool:
        """Verify row count reconciliation: source = target + errors + rejects.

        Returns True if balanced, False if not.
        """
        total_out = (
            self.tgt_success_rows + self.tgt_failed_rows + self.total_trans_errors
        )
        balanced = self.src_success_rows == total_out
        if not balanced:
            logger.warning(
                "ROW COUNT MISMATCH in %s: src=%d != tgt_ok=%d + tgt_fail=%d "
                "+ errors=%d (total_out=%d)",
                self.session_name,
                self.src_success_rows,
                self.tgt_success_rows,
                self.tgt_failed_rows,
                self.total_trans_errors,
                total_out,
            )
        return balanced

    def to_dict(self) -> Dict[str, object]:
        """Convert to dictionary for JSON serialization."""
        return {
            "session_name": self.session_name,
            "mapping_name": self.mapping_name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": round(self.duration_seconds, 3),
            "src_success_rows": self.src_success_rows,
            "src_failed_rows": self.src_failed_rows,
            "tgt_success_rows": self.tgt_success_rows,
            "tgt_failed_rows": self.tgt_failed_rows,
            "total_trans_errors": self.total_trans_errors,
            "first_error_code": self.first_error_code,
            "first_error_msg": self.first_error_msg,
            "status": self.status,
            "throughput_rows_per_sec": round(self.throughput_rows_per_sec, 1),
        }


@dataclass
class JobMetrics:
    """Aggregates SessionMetrics across all sessions in a workflow."""

    job_name: str
    workflow_name: str = ""
    sessions: List[SessionMetrics] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "NOT_STARTED"

    def start(self) -> "JobMetrics":
        """Mark job as started."""
        self.start_time = datetime.now()
        self.status = "RUNNING"
        logger.info("Job STARTED: %s (workflow: %s)", self.job_name, self.workflow_name)
        return self

    def stop(self, success: bool = True) -> "JobMetrics":
        """Mark job as completed."""
        self.end_time = datetime.now()
        self.status = "SUCCEEDED" if success else "FAILED"
        logger.info(
            "Job %s: %s (sessions=%d, total_src=%d, total_tgt=%d, "
            "total_errors=%d, duration=%.2fs)",
            self.status,
            self.job_name,
            len(self.sessions),
            self.total_src_success_rows,
            self.total_tgt_success_rows,
            self.total_errors,
            self.total_duration_seconds,
        )
        return self

    def add_session(self, metrics: SessionMetrics) -> None:
        """Add a completed session's metrics."""
        self.sessions.append(metrics)

    @property
    def total_src_success_rows(self) -> int:
        return sum(s.src_success_rows for s in self.sessions)

    @property
    def total_tgt_success_rows(self) -> int:
        return sum(s.tgt_success_rows for s in self.sessions)

    @property
    def total_errors(self) -> int:
        return sum(s.total_trans_errors for s in self.sessions)

    @property
    def total_duration_seconds(self) -> float:
        return sum(s.duration_seconds for s in self.sessions)

    @property
    def failed_sessions(self) -> List[SessionMetrics]:
        return [s for s in self.sessions if s.status == "FAILED"]

    def to_dict(self) -> Dict[str, object]:
        """Convert to dictionary for JSON serialization."""
        return {
            "job_name": self.job_name,
            "workflow_name": self.workflow_name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "total_src_success_rows": self.total_src_success_rows,
            "total_tgt_success_rows": self.total_tgt_success_rows,
            "total_errors": self.total_errors,
            "total_duration_seconds": round(self.total_duration_seconds, 3),
            "sessions": [s.to_dict() for s in self.sessions],
        }
