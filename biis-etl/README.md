# BIIS ETL — PySpark Migration

Migration of the Informatica PowerCenter 9.6.1 BIIS ETL jobs to PySpark with
SQL Server as the target database and Airflow for orchestration. Mainframe
(PWX) output remains as-is; Oracle stored procedures are ported to T-SQL.

## Quick start

```bash
cd biis-etl
make all        # clean -> setup -> build -> lint -> db-up -> db-seed -> run-all -> test -> validate
```

Requirements: Python 3.11, Java 11, Docker (SQL Server 2022 + SFTP test
containers), `msodbcsql18` (ODBC Driver 18 for SQL Server).

## Layout

| Path | Contents |
|------|----------|
| `schemas/` | JSON schema registry extracted from the PowerCenter XML exports — single source of truth for DDL, Spark schemas and golden data |
| `sql/ddl/` | SQL Server DDL (generated from the schema registry) |
| `sql/stored_procs/` | T-SQL ports of the Oracle procedures (stubs pending PL/SQL source extraction) |
| `jobs/` | PySpark jobs: `pay_calendar`, `comptime`, `pseudossn`, `fda_leave`, `ehrp2biis/` (preload, etl, afterload), `cpm/` (NIH, OIG, CDC) |
| `transfers/` | SFTP transfer + maintenance scripts (paramiko, RejectPolicy) |
| `dags/` | Airflow DAGs (one per workflow) |
| `scripts/` | DDL apply, golden-data generation/seeding, reconciliation CLI |
| `tests/` | unit (>=90% coverage), functional (live DB), regression (zero-diff golden gate), performance (pytest-benchmark) |
| `config/` | `dev.yaml`, `test.yaml`, `prod.yaml` |

## Migrated workflows

| Informatica workflow | PySpark job | DAG |
|---|---|---|
| wf_Pay_Calendar | `jobs/pay_calendar.py` | `pay_calendar` |
| wf_COMPTIME | `jobs/comptime.py` | `comptime` |
| wf_Pseudossn | `jobs/pseudossn.py` | `pseudossn` |
| wf_FDA_Leave | `jobs/fda_leave.py` | `fda_leave` |
| wf_EHRP2BIIS_UPDATE (+ pre/afterload SQL) | `jobs/ehrp2biis/` | `ehrp2biis` |
| wf_CPM_NIH / wf_CPM_OIG / wf_CPM_CDC | `jobs/cpm/` | `cpm` |
| Transfer & maintenance scripts | `transfers/sftp_transfer.py` | `transfer` |

## Validation

- `make db-seed` regenerates the synthetic golden fixtures for the current run
  date and seeds the input tables/files.
- `make run-all` executes every job in dependency order against the local
  SQL Server.
- `make validate` reconciles every output table against its golden CSV with a
  full-outer join on business keys; the gate fails on ANY diff
  (`reports/reconciliation/`).

## Environments

`--env {dev,test,prod}` selects `config/<env>.yaml` (also via `BIIS_ENV`).
`test` uses the local Docker SQL Server (`sa`, database `biis_test`) and the
local SFTP container. Secrets come from the config in `local` mode and AWS
Secrets Manager in `aws` mode; notifications are logged locally and sent via
SES/SMTP in higher environments.

## Stubs pending source extraction

The eight `sql/stored_procs/*.sql` procedures and the EHRP2BIIS preload step01
raise informational `STUB` messages until the Oracle PL/SQL source is
extracted; the pipeline structure, sequencing and transactions are in place.
