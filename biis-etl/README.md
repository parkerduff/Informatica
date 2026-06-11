# BIIS ETL — Informatica → PySpark Migration

This directory contains the PySpark migration of two Informatica PowerCenter 9.6.1
workflows from the `parkerduff/Informatica` repository, validated end-to-end against
a Dockerised SQL Server.

| Informatica workflow | Source XML | PySpark job |
|----------------------|-----------|-------------|
| `wf_Pay_Calendar` (4 sessions) | `XML/Pay_Calendar` | `jobs/pay_calendar.py` |
| `wf_COMPTIME` (3 sessions) | `XML/COMPTIME` | `jobs/comptime.py` |

## Primary deliverable

`reports/migration_validation_report.html` — a self-contained HTML report that
shows all tests passing, source-to-target transformation mappings, data quality
checks, and sample post-migration data. Open it in any browser.

## Layout

```
biis-etl/
├── docker-compose.yml      # SQL Server 2019 container
├── requirements.txt
├── setup.py                # installable package (jobs, utils)
├── Makefile                # make all -> full pipeline + report
├── config/test.yaml        # DB + Spark config
├── sql/ddl.sql             # PAY_PERIOD, COMP_TIME_DAILY_TBL, COUNTER_TBL, audit
├── jobs/                   # pay_calendar.py, comptime.py
├── utils/                  # config.py, db.py, validation.py
├── tests/                  # unit/ (30+), functional/ (e2e), fixtures/
├── reports/                # generate_html_report.py + HTML output
└── scripts/apply_ddl.py    # create schema + seed PAY_PERIOD
```

## Quick start

```bash
cd biis-etl
make all          # clean, setup venv, start DB, seed, run jobs, test, build report
```

Or step by step:

```bash
make setup        # create .venv and install deps
make db-up        # start the SQL Server container
make db-seed      # apply DDL + seed PAY_PERIOD
make run-jobs     # run both PySpark jobs
make test         # unit (>=90% cov) + functional tests
make report       # generate reports/migration_validation_report.html
```

## Requirements

- Docker (for SQL Server 2019)
- Python 3.9+, Java 8/11/17 (for PySpark)
- Microsoft ODBC Driver 18 for SQL Server (`pyodbc`)

Install the ODBC driver on Ubuntu 22.04:

```bash
curl -sSL -o /tmp/p.deb https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb
sudo dpkg -i /tmp/p.deb
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

## Migration notes

- **I/O strategy**: reads are materialised into Spark DataFrames and all
  transformation logic (filters, expressions, date conversions, aggregations)
  runs in PySpark; writes/updates use `pyodbc` to mirror the Informatica
  Update Strategy (`DD_UPDATE`) and target loads.
- **Pay Calendar** mirrors the Router `rtr_Parameter_Non_Parameter` (parameter
  path vs. `TRUNC(SESSSTARTTIME)` date path), the verify `DECODE`/`ABORT` logic,
  and the `LPAD(TO_CHAR(PP_NUM), 2, '0')` message formatting.
- **COMPTIME** mirrors `IS_NUMBER(SSN)` record-type detection (header/trailer
  filtering), `TO_DATE(..., 'YYYYMMDD')` conversions, `PP_YEAR_NUM` derivation,
  and the `COUNT(SSN)` aggregator that populates `COUNTER_TBL`.
