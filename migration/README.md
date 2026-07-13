# Informatica PowerCenter &rarr; AWS Glue Migration

This directory contains a complete, self-contained migration of the eight
PowerCenter workflows in this repository to an **AWS Glue** stack (PySpark for
wide/high-volume feeds, Glue Python Shell for light reference feeds), orchestrated
with **Step Functions + EventBridge**, with a local execution harness, mass
synthetic data generation, a spec-derived golden baseline, automated field-level
reconciliation, and two offline HTML reports.

> The existing `XML/`, shell, and SQL files in the repo root are the
> **source-of-truth spec** and are never modified. Everything generated lives
> under `migration/`.

## Layout

```
migration/
  parser/          parse_powermart.py       XML -> machine-readable spec JSON
  spec/            <folder>.json            parsed specs (source-of-truth contract)
                   classification.md        Spark vs Python Shell rationale
  lib/             infa_compat.py           verbatim Informatica function semantics
                   infa_expr.py             expression parser/evaluator
                   records.py               fixed-width / delimited / relational readers
                   engine_pandas.py         pure-Python engine (Python Shell jobs)
                   engine_spark.py          PySpark engine (wide feeds; repartition + broadcast)
                   mapping_resolver.py      CONNECTOR-graph field lineage
                   pipeline.py              PipelineDef / SourceDef contracts
  jobs/            configs.py               per-folder pipeline metadata
                   <folder>/job.py          Glue entrypoint per folder (8)
                   runner.py                shared job runner (engine selection + IO)
  sql/             pushdown.py              PL/SQL push-down wrappers (DB-resident)
  orchestration/   generate_orchestration.py -> statemachines/*.asl.json (Step Functions)
  iac/             *.tf                     Terraform (Glue, SFN, EventBridge, SNS, S3)
  local/           run_pipeline.py          end-to-end local driver (parallel branches)
                   sql_shim.py              SQLite/Postgres target warehouse shim
                   docker-compose.yml       LocalStack + Postgres (optional)
  synth/           generate.py              mass synthetic data generator
  baseline/        reference.py             spec-derived golden baseline (independent)
  recon/           reconcile.py             field-level reconciliation harness
  reports/         generate_reports.py      -> migration_report.html + data_analysis_report.html
  tests/           migration unit + integration tests
  run_local.sh     one-command local end-to-end
```

## The eight jobs

| Folder | Engine | Source | Target |
|--------|--------|--------|--------|
| `Pseudossn` | Glue PySpark | `PSEUDOSSN_FILE` (fixed-width) | `PSEUDOSSN_TBL` |
| `EHRP2BIIS_UPDATE` | Glue PySpark | `NWK_NEW_EHRP_ACTIONS_TBL` (+`PS_GVT_JOB` lookup) | `NWK_ACTION_PRIMARY_TBL` |
| `CPM_NIH` | Glue PySpark | `CPM_NEWPAY_TBL` | `NIH_PAYROLL_MASTER` |
| `CPM_OIG` | Glue PySpark | `CPM_NEWPAY_TBL` | `SKPAYROLL_MASTER` |
| `CPM_CDC` | Glue PySpark | `CPM_NEWPAY_TBL` | `WS_PAY_OUT_REC` |
| `FDA_Leave` | Glue PySpark | `HI_PM_FDA_TATRAN_FLAT` (fixed-width) | `HI_PM_FDA_TATRAN_TBL` |
| `Pay_Calendar` | Glue Python Shell | `PAY_PERIOD` | `PAY_PERIOD` |
| `COMPTIME` | Glue Python Shell | `U0287D01` (delimited) | `COMP_TIME_DAILY_TBL` |

See [`spec/classification.md`](spec/classification.md) for the rationale.

## Run locally (no AWS required)

```bash
# one-shot: parse -> synth -> run (parallel) -> reconcile -> reports
./migration/run_local.sh functional      # small hand-verifiable set
./migration/run_local.sh performance     # mass-volume set
./migration/run_local.sh both

# or step by step
python3 migration/parser/parse_powermart.py
python3 migration/synth/generate.py --mode functional
python3 migration/local/run_pipeline.py --mode functional
python3 migration/reports/generate_reports.py
```

Requirements: Python 3.10+, `pyspark==3.5.1`, `faker`. Docker is optional — if
present, `run_local.sh` starts LocalStack (S3/Step Functions/SNS) + Postgres for a
fuller AWS-like rehearsal; otherwise the pure-Python path (local Spark + SQLite
shim) runs everything offline.

Outputs:
* `migration/local/out/<mode>/<folder>/` — target + `ERROR_TBL` + `COUNTER_TBL` CSVs
* `migration/reports/migration_report.html`
* `migration/reports/data_analysis_report.html`

Both reports are single files with inline CSS + inline SVG charts and open fully
offline in any browser.

## Deploy to real AWS (Terraform)

```bash
cd migration/iac
terraform init
terraform apply \
  -var="region=us-east-1" \
  -var="data_bucket=<your-data-bucket>" \
  -var="code_bucket=<your-code-bucket>" \
  -var='alert_emails=["ops@example.gov"]'
```

Before `apply`, upload the job scripts + a zipped `migration/lib` to the code
bucket (`s3://<code_bucket>/jobs/<folder>/job.py`, `s3://<code_bucket>/lib/migration_lib.zip`).
Terraform provisions:

* Glue jobs (`glueetl` for PySpark feeds, `pythonshell` for reference feeds) + a
  push-down SQL job,
* Step Functions state machines (master + one per workflow family) from the
  generated ASL, with **Parallel** branches for the independent agency feeds,
* an EventBridge rule for the biweekly pay-period schedule,
* an SNS alert topic (the `mailx` replacement),
* S3 buckets with an archive lifecycle rule (the `archive_files`/`remove_file`
  replacement).

`terraform validate` passes on the committed configuration.

## Orchestration & parallelization

`ksh`+cron &rarr; Step Functions + EventBridge. Independent agency feeds
(NIH/OIG/CDC/FDA) run as concurrent Step Functions **Parallel** branches — the
analog of Informatica child sessions. Each dependent feed runs
`preload SQL &rarr; Glue mapping job &rarr; afterload SQL &rarr; SFTP delivery`.
Within PySpark jobs, wide records are repartitioned on natural keys (e.g.
`EMPLID`, `PSEUDO_SSN`).

## PL/SQL push-down

Per the default stakeholder assumption (**Oracle warehouse stays DB-resident**),
the legacy PL/SQL (`ehrp2biis_preload`, `ehrp2biis_afterload.sql`, `actstage_load`)
is **not** rewritten into Spark. `migration/sql/pushdown.py` wraps the original
scripts as ordered SQL steps invoked by orchestration (a JDBC/SQL task against the
warehouse). The local shim applies the portable subset against SQLite/Postgres.

## Parallel-run cutover strategy

1. Deploy the Glue stack alongside the live Informatica pipeline.
2. For each pay period, feed **the same input files** to both systems.
3. Run `migration/recon/reconcile.py` on the two output sets (row counts,
   field-level values honoring precision/scale, NULL parity, dedup/latest-record
   parity, error-routing parity, counter/audit parity).
4. Require **N consecutive clean pay periods** (recommended N = 3) with zero
   unexplained discrepancies.
5. Cut over; keep the legacy pipeline warm for one additional cycle as rollback.

## Assumptions & limitations

* **Baseline = spec-derived expected output**, independently coded from the XML
  mapping expressions — *not* a live PowerCenter engine run (the engine was
  unavailable). This is the reconciliation golden dataset.
* **Oracle warehouse location** is unresolved; default assumption is DB-resident
  PL/SQL push-down (RDS for Oracle / on-prem). If the target becomes Redshift,
  the push-down SQL steps would be migrated rather than invoked in place.
* **Live AWS availability** is not assumed. Local execution + IaC are delivered so
  the pipeline runs with or without an AWS account.
* Local performance volumes are scaled to be laptop-safe (tens of thousands of
  rows; wide CPM feeds capped lower). On real Glue these scale to millions via DPU
  workers and key-based repartitioning — the code path is identical.
