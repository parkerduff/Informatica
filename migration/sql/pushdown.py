#!/usr/bin/env python3
"""
sql.pushdown -- wrap the legacy Oracle PL/SQL pre/post steps as callable modules.

Per the migration decision (default assumption: the Oracle warehouse stays
DB-resident), the PL/SQL logic in the repository's source-of-truth scripts is
**not** rewritten into Spark. Instead it is invoked as push-down SQL steps by the
orchestration (Step Functions -> a JDBC/SQL task), before/after the Glue mapping
job runs.

This module:
  * references the ORIGINAL scripts by path (never modifies them),
  * extracts the executable SQL statements (dropping SQL*Plus-only directives
    such as SPOOL / COL / @includes),
  * exposes them as ordered :class:`SqlStep` objects for orchestration + report,
  * can apply the portable statements against a local SQLite/Postgres shim so the
    local end-to-end pipeline can exercise the pre/post-load steps.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Source-of-truth legacy scripts (kept unmodified in the repo root).
LEGACY_SCRIPTS = {
    "ehrp2biis_preload": os.path.join(REPO_ROOT, "ehrp2biis_preload"),
    "ehrp2biis_afterload": os.path.join(REPO_ROOT, "ehrp2biis_afterload.sql"),
    "actstage_load": os.path.join(REPO_ROOT, "actstage_load"),
}

# Which push-down steps run before vs after the mapping job, per workflow family.
PUSHDOWN_PLAN = {
    "EHRP2BIIS_UPDATE": {
        "preload": ["ehrp2biis_preload"],      # ksh: env + Informatica preload
        "afterload": ["ehrp2biis_afterload"],  # PL/SQL post-load fixups
    },
    "actstage": {
        "load": ["actstage_load"],
    },
}

_SQLPLUS_NOISE = re.compile(
    r"^\s*(SPOOL|COL\b|COLUMN\b|SET\b|@|PROMPT|WHENEVER|DEFINE|EXIT\b|/\s*$)",
    re.IGNORECASE,
)


@dataclass
class SqlStep:
    name: str
    source_path: str
    statements: List[str] = field(default_factory=list)
    note: str = ""

    @property
    def statement_count(self) -> int:
        return len(self.statements)


def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    lines = []
    for ln in text.splitlines():
        ln = re.sub(r"--.*$", "", ln)
        lines.append(ln)
    return "\n".join(lines)


def extract_sql(path: str) -> List[str]:
    """Extract executable SQL statements from a .sql or embedded-SQL script."""
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        raw = fh.read()
    raw = _strip_comments(raw)
    # keep only lines that are not SQL*Plus/shell noise
    kept = [ln for ln in raw.splitlines() if not _SQLPLUS_NOISE.match(ln)]
    body = "\n".join(kept)
    stmts = []
    for chunk in body.split(";"):
        s = chunk.strip()
        if not s:
            continue
        if not re.match(r"(?is)^(update|insert|delete|merge|create|drop|alter|"
                        r"truncate|commit|begin|declare|grant|select)\b", s):
            continue
        stmts.append(s + ";")
    return stmts


def load_step(name: str) -> SqlStep:
    path = LEGACY_SCRIPTS.get(name, "")
    stmts = extract_sql(path)
    note = ""
    if path.endswith(".sql"):
        note = "Oracle PL/SQL post-load fixups (push-down)."
    else:
        note = "ksh driver: Informatica/env setup; SQL push-down where present."
    return SqlStep(name=name, source_path=path, statements=stmts, note=note)


def plan_steps(family: str, phase: str) -> List[SqlStep]:
    names = PUSHDOWN_PLAN.get(family, {}).get(phase, [])
    return [load_step(n) for n in names]


# --- portable statement classification / local application -----------------

_PORTABLE = re.compile(r"(?is)^(update|delete|insert|create table|drop table|"
                       r"truncate|commit)\b")


def _is_portable(stmt: str) -> bool:
    return bool(_PORTABLE.match(stmt.strip()))


def apply_local(step: SqlStep, conn: Any) -> Dict[str, Any]:
    """Best-effort apply of the portable statements against a DBAPI connection.

    Oracle-specific statements that a local SQLite/Postgres shim cannot run are
    skipped and reported (they still run push-down against the real warehouse).
    """
    applied, skipped, errors = 0, 0, []
    cur = conn.cursor()
    for stmt in step.statements:
        if stmt.strip().lower().startswith("commit"):
            conn.commit()
            applied += 1
            continue
        if not _is_portable(stmt):
            skipped += 1
            continue
        try:
            cur.execute(stmt)
            applied += 1
        except Exception as exc:  # noqa: BLE001 - report, keep going
            skipped += 1
            errors.append(f"{type(exc).__name__}: {exc}")
    conn.commit()
    return {"step": step.name, "applied": applied, "skipped": skipped,
            "errors": errors, "total": step.statement_count}


def describe_all() -> List[Dict[str, Any]]:
    out = []
    for name in LEGACY_SCRIPTS:
        step = load_step(name)
        out.append({"name": name, "source": os.path.relpath(step.source_path, REPO_ROOT),
                    "statements": step.statement_count, "note": step.note})
    return out


if __name__ == "__main__":
    import json
    print(json.dumps(describe_all(), indent=2))
