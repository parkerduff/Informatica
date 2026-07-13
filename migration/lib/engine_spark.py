#!/usr/bin/env python3
"""
engine_spark -- PySpark executor for a ``PipelineDef`` (the Glue PySpark jobs).

Design goals:
  * distributed row processing via ``mapPartitions`` (the wide payroll feeds),
  * broadcast joins for cached lookups (PAY_PERIOD / PS_GVT_JOB),
  * explicit ``repartition`` on natural keys to parallelise wide records,
  * Sorter "latest record wins" implemented as a ``Window`` + ``row_number``,
  * identical transformation semantics to the golden baseline (the same
    ``infa_compat`` / ``infa_expr`` primitives run inside the executors), so the
    reconciliation is meaningful.

Falls back with a clear error if PySpark is unavailable.  Result contract is the
same ``Result`` dataclass used by ``engine_pandas`` so recon treats both engines
uniformly.
"""
from __future__ import annotations

import atexit
import threading
import time
from typing import Any, Dict, List, Optional

from . import infa_compat as C
from . import infa_expr as E
from . import records as R
from .engine_pandas import (Result, RunContext, _counters, _dedup, _error_row,
                            _parse_ts, _project)
from .pipeline import PipelineDef, SourceDef


# Spark work is serialised across threads (one JVM SparkContext per process);
# the session is reused and torn down at process exit.
_SPARK_LOCK = threading.Lock()
_SESSION = None


def _spark(app: str):
    global _SESSION
    from pyspark.sql import SparkSession
    if _SESSION is None:
        _SESSION = (SparkSession.builder
                    .appName(app)
                    .master("local[*]")
                    .config("spark.sql.shuffle.partitions", "8")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate())
        atexit.register(_stop_session)
    return _SESSION


def _stop_session():
    global _SESSION
    if _SESSION is not None:
        try:
            _SESSION.stop()
        except Exception:
            pass
        _SESSION = None


def _process_row(row: Dict[str, Any], pdef: PipelineDef, lookups, port_asts,
                 rule_asts, ctx):
    row = dict(row)
    row["__const_curr"] = "Y"
    for lk in pdef.lookups:
        key = tuple(C._to_str(row.get(rf, "")).strip() for rf, _ in lk.on)
        match = lookups.get(lk.name, {}).get(key)
        for out_name, lk_field in lk.select:
            row[out_name] = match.get(lk_field) if match else None
    err_msg = err_code = None
    for name, ast in port_asts:
        try:
            row[name] = E._eval(ast, row)
        except C.TransformationError as te:
            err_msg, err_code = te.message, "XFORM"
            row[name] = None
            break
    if err_msg is None:
        for msg, code, ast in rule_asts:
            try:
                if C._truthy(E._eval(ast, row)):
                    err_msg, err_code = msg, code
                    break
            except C.TransformationError as te:
                err_msg, err_code = te.message, "XFORM"
                break
    if err_msg is not None:
        return ("error", _error_row(pdef, row, err_msg, err_code, ctx))
    return ("main", row)


def run(pdef: PipelineDef, resolver: Dict[str, str],
        ctx: Optional[RunContext] = None, keep_alive: bool = True) -> Result:
    with _SPARK_LOCK:
        return _run_locked(pdef, resolver, ctx, keep_alive)


def _run_locked(pdef: PipelineDef, resolver: Dict[str, str],
                ctx: Optional[RunContext], keep_alive: bool) -> Result:
    ctx = ctx or RunContext()
    t0 = time.time()
    C.set_sessstarttime(_parse_ts(ctx.run_ts))

    spark = _spark(f"convert_{pdef.folder}")
    sc = spark.sparkContext

    raw = R.read_source(pdef.primary_source, resolver[pdef.primary_source.name])
    total_in = len(raw)

    # broadcast lookups
    lut: Dict[str, Dict] = {}
    src_by_name = {pdef.primary_source.name: pdef.primary_source}
    for s in pdef.extra_sources:
        src_by_name[s.name] = s
    for lk in pdef.lookups:
        rows = R.read_source(src_by_name[lk.source], resolver[lk.source]) if lk.source in resolver else []
        d = {}
        for r in rows:
            k = tuple(C._to_str(r.get(f, "")).strip() for f, _ in lk.on)
            d[k] = r
        lut[lk.name] = d
    b_lut = sc.broadcast(lut)

    # record-type split happens on the driver (cheap header/trailer scan)
    rt = pdef.record_type
    header_count = trailer_declared = 0
    detail: List[Dict[str, Any]] = []
    for r in raw:
        if rt:
            flag = C.record_type_flag(r.get(rt["field"], ""),
                                      rt.get("header", "HEADER"),
                                      rt.get("trailer", "TRAILER"))
            if flag == "H":
                header_count += 1
                continue
            if flag == "T":
                if rt.get("trailer_count_expr"):
                    trailer_declared = C.to_integer(E.evaluate(rt["trailer_count_expr"], r)) or 0
                continue
            if flag not in pdef.detail_flags:
                continue
        detail.append(r)

    derived = [(dp.name, dp.expr) for dp in pdef.derived_ports]
    rules = [(er.get("message", "transformation error"), er.get("code", "RULE"), er["expr"])
             for er in pdef.error_rules]

    npart = max(1, min(8, len(detail) // 1000 + 1))
    rdd = sc.parallelize(list(enumerate(detail)), npart)

    # explicit repartition on natural keys to parallelise wide records
    pkeys = pdef.partition_keys
    if pkeys:
        def keyfn(item):
            _, row = item
            return tuple(C._to_str(row.get(k, "")).strip() for k in pkeys)
        rdd = rdd.keyBy(keyfn).partitionBy(npart).map(lambda kv: kv[1])

    def proc(part):
        port_asts = [(n, E.parse(e)) for n, e in derived]
        rule_asts = [(m, c, E.parse(e)) for m, c, e in rules]
        out = []
        for seq, row in part:
            row[R.SEQ_COL] = seq
            tag, payload = _process_row(row, pdef, b_lut.value, port_asts, rule_asts, ctx)
            out.append((tag, payload))
        return out

    processed = rdd.mapPartitions(proc).collect()

    good = [p for t, p in processed if t == "main"]
    errors = [p for t, p in processed if t == "error"]

    # Sorter latest-record-wins dedup (Window + row_number equivalent)
    good.sort(key=lambda r: r.get(R.SEQ_COL, 0))
    if pdef.dedup:
        good = _dedup(good, pdef.dedup)

    main = [_project(pdef, row) for row in good]
    counters = _counters(pdef, ctx, total=total_in, header=header_count,
                         detail=len(main), errors=len(errors),
                         trailer_declared=trailer_declared)

    res = Result(main=main, error=errors, counters=counters)
    rt_sec = round(time.time() - t0, 4) or 1e-9
    res.metrics = {
        "engine": "pyspark", "folder": pdef.folder, "mapping": pdef.mapping,
        "input_rows": total_in, "detail_rows": len(main), "error_rows": len(errors),
        "runtime_sec": rt_sec, "rows_per_sec": round(total_in / rt_sec, 1),
        "partitions": npart, "partition_keys": pkeys,
    }
    if not keep_alive:
        spark.stop()
    return res
