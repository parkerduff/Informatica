"""Create the full BIIS schema in the target database.

Generates ``CREATE TABLE`` statements from the central schema registry
(:mod:`utils.schemas`) for the active backend (sqlite or SQL Server) and also
writes the rendered SQL Server DDL to ``sql/ddl/`` as a reviewable artifact.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import db  # noqa: E402
from utils.config import get_config  # noqa: E402
from utils.schemas import TABLES, sqlite_type, sqlserver_type  # noqa: E402

DDL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql", "ddl")


def render_create(table: str, dialect: str) -> str:
    type_fn = sqlite_type if dialect == "sqlite" else sqlserver_type
    cols = ",\n    ".join(f"{c[0]} {type_fn(c)}" for c in TABLES[table])
    return f"CREATE TABLE {table} (\n    {cols}\n)"


def write_sqlserver_ddl() -> None:
    os.makedirs(DDL_DIR, exist_ok=True)
    for i, table in enumerate(TABLES, start=1):
        path = os.path.join(DDL_DIR, f"{i:02d}_{table.lower()}.sql")
        with open(path, "w") as fh:
            fh.write("-- Auto-generated from utils.schemas for SQL Server\n")
            fh.write(f"IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.{table};\nGO\n")
            fh.write(render_create(table, "sqlserver") + ";\nGO\n")


def apply(env: str) -> None:
    cfg = get_config(env)
    for table in TABLES:
        if cfg.backend == "sqlite":
            db.execute(cfg, f"DROP TABLE IF EXISTS {table}")
            db.execute(cfg, render_create(table, "sqlite"))
        else:  # pragma: no cover - SQL Server
            db.execute(cfg, f"IF OBJECT_ID('dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.{table}")
            db.execute(cfg, render_create(table, "sqlserver"))
    print(f"Applied DDL for {len(TABLES)} tables to {cfg.backend}")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Apply BIIS DDL")
    p.add_argument("--env", default="test", choices=["test", "prod"])
    p.add_argument("--emit-sql", action="store_true", help="Also write sql/ddl/*.sql artifacts")
    args = p.parse_args(argv)
    apply(args.env)
    if args.emit_sql or args.env == "test":
        write_sqlserver_ddl()


if __name__ == "__main__":
    main()
