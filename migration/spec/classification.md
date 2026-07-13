# Job Classification — Glue PySpark vs Glue Python Shell

Each PowerCenter folder is converted to a Glue job. The execution engine is
chosen from the **data volume**, **record width**, and **transformation
intensity** of the primary mapping (all figures are read from the parsed
`migration/spec/<folder>.json`).

| Folder | Engine | Primary source | Src fields | Target | Tgt fields | Rationale |
|--------|--------|----------------|-----------:|--------|-----------:|-----------|
| `Pseudossn` | **Glue PySpark** | `PSEUDOSSN_FILE` (fixed-width) | 66 | `PSEUDOSSN_TBL` | 63 | Wide fixed-width payroll feed; overpunch signed decoding, multi-format date reformatting, Sorter "latest-record-wins" dedup (Window + `row_number`), header/trailer validation. High volume. |
| `EHRP2BIIS_UPDATE` | **Glue PySpark** | `NWK_NEW_EHRP_ACTIONS_TBL` + `PS_GVT_JOB` | 4 + 246 | `NWK_ACTION_PRIMARY_TBL` | 260 | Wide join/enrichment against a 246-column HR table (broadcast lookup); high row counts. |
| `CPM_NIH` | **Glue PySpark** | `CPM_NEWPAY_TBL` | 501 | `NIH_PAYROLL_MASTER` | 534 | Very wide (500+ column) payroll master extract; benefits from partitioned parallel processing. |
| `CPM_OIG` | **Glue PySpark** | `CPM_NEWPAY_TBL` | 501 | `SKPAYROLL_MASTER` | 287 | Wide payroll extract with signed-decimal reformatting. |
| `CPM_CDC` | **Glue PySpark** | `CPM_NEWPAY_TBL` | 501 | `WS_PAY_OUT_REC` | 736 | Widest target (736 columns); heavy per-row derivation. |
| `FDA_Leave` | **Glue PySpark** | `HI_PM_FDA_TATRAN_FLAT` (fixed-width) | 6 | `HI_PM_FDA_TATRAN_TBL` | 8 | Time-and-attendance transaction feed; volume-driven, fixed-width parsing. |
| `Pay_Calendar` | **Glue Python Shell** | `PAY_PERIOD` | 10 | `PAY_PERIOD` | 8 | Small reference/verification feed (a single 10-column calendar table). No Spark overhead justified. |
| `COMPTIME` | **Glue Python Shell** | `U0287D01` (delimited) | 12 | `COMP_TIME_DAILY_TBL` | 15 | Light daily comp-time file with a pay-period lookup and message/counter build. Reference-scale volume. |

## Decision rule

```
if primary feed is wide (>~50 cols) OR high-volume OR needs Spark-scale
   dedup/join/repartition:
       -> Glue PySpark   (migration/lib/engine_spark.py)
else (small reference / validation / calendar feed):
       -> Glue Python Shell (migration/lib/engine_pandas.py)
```

Both engines share the exact same transformation semantics
(`migration/lib/infa_compat.py` + `migration/lib/infa_expr.py`), so classifying
a job one way or the other never changes its output — it only changes the
runtime. This is verified by the reconciliation harness: every job's converted
output matches the independently-coded golden baseline field-for-field.

## Notes

* `CPM_*` share the `CPM_NEWPAY_TBL` source (a NewPay payroll master). The three
  agency variants (NIH/OIG/CDC) differ only in their output layout and are
  independent → they run as **parallel Step Functions branches**.
* PL/SQL pre/post logic (`ehrp2biis_preload`, `ehrp2biis_afterload.sql`,
  `actstage_load`) is **not** re-implemented in Spark; it is kept DB-resident and
  invoked as push-down SQL steps by the orchestration (see `migration/sql/`).
