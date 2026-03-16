# BIIS PySpark Migration

PySpark-based replacement for the HHS BIIS (Business Intelligence Information System) Informatica PowerCenter ETL system.

## Overview

This project migrates 9 Informatica PowerCenter job families to PySpark:

| # | Job Family | Source | Description |
|---|-----------|--------|-------------|
| 1 | Pay Calendar | `XML/Pay_Calendar` | Reset/Set/Verify current pay period |
| 2 | PseudoSSN | `Pseudossn` | SSN de-identification from SDA files |
| 3 | COMPTIME | `XML/COMPTIME` | Compensatory time daily load |
| 4 | EHRP2BIIS | `ehrp2biis_preload`, `XML/EHRP2BIIS_UPDATE`, `ehrp2biis_afterload.sql` | Personnel action processing |
| 5 | CPM | `XML/CPM_NIH`, `XML/CPM_OIG`, `XML/CPM_CDC` | Payroll processing per agency |
| 6 | LES | `XML/LES` | Leave and Earnings Statement processing |
| 7 | FDA Validation | `XML/FDA_Leave` | FDA leave record validation |
| 8 | File Transfers | `Transfer Scripts/*` | SFTP distribution to agencies |
| 9 | Orchestration | N/A | Airflow DAG for job sequencing |

## Project Structure

```
pyspark_migration/
├── config/             # Database connections, file paths, environment configs
├── common/             # Shared utilities (Spark session, DB helpers, parsers)
├── jobs/               # Job implementations by family
│   ├── pay_calendar/
│   ├── pseudossn/
│   ├── comptime/
│   ├── ehrp2biis/
│   ├── cpm/
│   ├── les/
│   ├── fda_validation/
│   └── file_transfers/
├── tests/              # Unit, integration, and regression tests
├── dags/               # Airflow DAG definitions
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.9+
- Apache Spark 3.4+
- Oracle JDBC Driver (ojdbc8.jar)
- Oracle Database access (ORA_BIIS, ORA_BIISPRD_SRC)
- SFTP access to m1csv301.hhs.gov

## Setup

1. Install dependencies:
   ```bash
   pip install -r pyspark_migration/requirements.txt
   ```

2. Configure environment variables (or use env files):
   ```bash
   export BIIS_ENVIRONMENT=dev  # dev, test, or prod
   export ORA_BIIS_URL=jdbc:oracle:thin:@//host:1521/BIIS
   export ORA_BIIS_USER=username
   export ORA_BIIS_PASSWORD=password
   export ORA_BIISPRD_SRC_URL=jdbc:oracle:thin:@//host:1521/BIISPRD
   export ORA_BIISPRD_SRC_USER=username
   export ORA_BIISPRD_SRC_PASSWORD=password
   ```

3. Place Oracle JDBC JAR:
   ```bash
   export ORACLE_JDBC_JAR=/path/to/ojdbc8.jar
   ```

## Running Jobs

Each job can be run independently via CLI:

```bash
# Pay Calendar
python -m pyspark_migration.jobs.pay_calendar.pay_calendar_job --pp-num 5 --pp-end-year 2024

# PseudoSSN
python -m pyspark_migration.jobs.pseudossn.pseudossn_job --input-file /path/to/sda_file.dat

# COMPTIME
python -m pyspark_migration.jobs.comptime.comptime_job --input-file /path/to/U0287D01

# EHRP2BIIS (3-step pipeline)
python -m pyspark_migration.jobs.ehrp2biis.preload
python -m pyspark_migration.jobs.ehrp2biis.main_etl
python -m pyspark_migration.jobs.ehrp2biis.afterload

# CPM (staging then agency-specific)
python -m pyspark_migration.jobs.cpm.staging
python -m pyspark_migration.jobs.cpm.cpm_nih
python -m pyspark_migration.jobs.cpm.cpm_oig
python -m pyspark_migration.jobs.cpm.cpm_cdc
python -m pyspark_migration.jobs.cpm.cpm_afps

# LES
python -m pyspark_migration.jobs.les.les_job

# FDA Validation
python -m pyspark_migration.jobs.fda_validation.fda_validation_job

# File Transfers
python -m pyspark_migration.jobs.file_transfers.transfer_jobs --action all
```

## Orchestration

The Airflow DAG (`dags/biis_etl_dag.py`) orchestrates all jobs with proper dependencies:

```
pay_calendar >> pseudossn >> [comptime, ehrp2biis]
ehrp2biis >> cpm_staging >> [cpm_nih, cpm_oig, cpm_cdc, cpm_afps]
cpm_agencies >> fda_validation >> les >> file_transfers >> maintenance
```

## Testing

```bash
# Run all tests
pytest pyspark_migration/tests/

# Run unit tests only
pytest pyspark_migration/tests/unit/

# Run integration tests
pytest pyspark_migration/tests/integration/

# Run with coverage
pytest --cov=pyspark_migration pyspark_migration/tests/
```

## Key Design Decisions

1. **Oracle JDBC for reads/writes**: Uses `spark.read.jdbc()` and `df.write.jdbc()` for bulk data operations
2. **cx_Oracle for procedures**: Stored procedures and DDL executed via cx_Oracle (not expressible as DataFrame ops)
3. **COBOL signed numeric UDFs**: Custom PySpark UDFs replicate mainframe overpunch sign encoding
4. **Broadcast join for pay period**: Small PAY_PERIOD table broadcast to all workers for enrichment
5. **Fixed-width parsing via substring**: Replaces Informatica flat file Source Qualifiers with `F.substring()` extraction

## Database Connections

| Connection | Purpose |
|-----------|---------|
| ORA_BIISPRD_SRC | Source: EHRP PS_GVT_JOB, PeopleSoft tables |
| ORA_BIIS | Target: BIIS data warehouse (all target tables) |

## File Transfer Destinations

| Agency | Jail Directory |
|--------|---------------|
| NIH | /opt/app/jail/sa-nihbiisu/outbound |
| OIG | /opt/app/jail/sa-oig/outbound |
| FDA | /opt/app/jail/sa-fdausr2/outbound |
| CDC | /opt/app/jail/sa-cdcbiis/outbound |
| AFPS | /opt/app/jail/sa-afps/outbound |

## Migration from Informatica

This project replaces the following Informatica PowerCenter artifacts:
- **Workflows**: wf_Pay_Calendar, wf_COMPTIME, wf_Pseudossn, wf_EHRP2BIIS, wf_CPM_*, wf_LES
- **Shell scripts**: ehrp2biis_preload, actstage_load, transfer scripts
- **SQL scripts**: ehrp2biis_afterload.sql, step01
- **XML definitions**: All files in XML/ directory
