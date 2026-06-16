# BIIS / EHRP PySpark ETL

PySpark + Python re-implementation of the BIIS / EHRP **Informatica PowerCenter
9.6.1** project (the Korn-shell wrappers, SQL*Plus scripts and Informatica XML
mappings in the repository root).

## Layout

```
pyspark_etl/
├── config/        connections, notifications, generated schemas
├── transforms/    date / signed-numeric / record-type parsing + fixed-width IO
├── jobs/          one subpackage per Informatica folder/workflow
├── transfers/     paramiko SFTP (replaces the 7 *_transfer ksh scripts)
├── maintenance/   archive_files / remove_file
├── utils/         logging, email (smtplib), Oracle JDBC + SQL execution, Spark
├── sql/           SQL kept as-is (preload step01, afterload PL/SQL)
└── main.py        CLI orchestrator
```

## Mapping from legacy artifacts

| Legacy artifact                              | New module                                   |
| -------------------------------------------- | -------------------------------------------- |
| `XML/Pseudossn` (`m_Pseudossn_Load_..._SDA`) | `jobs/pseudossn/load_from_sda.py`            |
| `ehrp2biis_preload` (ksh + step01)           | `jobs/ehrp2biis/preload.py` + `sql/`         |
| `XML/EHRP2BIIS_UPDATE`                        | `jobs/ehrp2biis/update.py`                   |
| `ehrp2biis_afterload.sql`                    | `jobs/ehrp2biis/afterload.py` + `sql/`       |
| `XML/CPM_NIH` / `CPM_CDC` / `CPM_OIG` / `CPM_AFPS` | `jobs/cpm/{nih,cdc,oig,afps}_payroll.py` |
| `XML/FDA_Leave`                              | `jobs/cpm/fda_leave.py`                       |
| `XML/COMPTIME`                              | `jobs/comptime/load.py`                       |
| `XML/Pay_Calendar`                          | `jobs/pay_calendar/update.py`                 |
| `Transfer Scripts/*`                        | `transfers/*`                                 |
| `Maintenance Scripts/*`                     | `maintenance/*`                               |

The schema modules under `config/schemas/` are **auto-derived** from the XML
source/target definitions (field names, physical offsets/lengths, datatypes) and
should be regenerated rather than hand-edited.

## Configuration

All credentials and paths come from environment variables (see `.env.example`);
nothing sensitive is committed. The original `$HOME/.use` / `$HOME/.pw` files and
hard-coded paths from `bin/SETENV` are replaced by these variables.

## Install & test

```bash
cd pyspark_etl
pip install -e ".[dev]"
python -m pytest tests/ -v
ruff check .
```

## Run

```bash
python -m pyspark_etl.main pseudossn-load --input-file /data/.../sda_file
python -m pyspark_etl.main ehrp2biis-full
python -m pyspark_etl.main cpm-nih --pay-period 12 --year 2024
python -m pyspark_etl.main comptime-load --input-file /data/.../U0287D01
python -m pyspark_etl.main pay-calendar-set --pay-period 12 --year 2024
python -m pyspark_etl.main transfer --agency NIH_CPM --file output.txt
python -m pyspark_etl.main archive --input-dir IN --dest-dir OUT --pay-period 12
```

Reading/writing Oracle requires the Oracle JDBC driver jar; point
`ORACLE_JDBC_JAR` at it so the Spark session picks it up.

## Notes / limitations

* `sql/ehrp2biis_preload_step01.sql` is a placeholder — the real `step01` lives
  on the BIIS server and is not in this repo. Drop it in (or repoint the preload
  job) before running for real.
* The afterload PL/SQL is executed verbatim via JDBC, statement-by-statement;
  `EXEC proc;` shorthand is rewritten to `BEGIN proc; END;` for `oracledb`.
* The CPM agency extracts reproduce the Source-Qualifier pay-period + agency
  filters exactly; supply a per-field output `layout` to
  `transforms.fixed_width_writer.write_fixed_width` to emit the precise
  agency record format.
```
