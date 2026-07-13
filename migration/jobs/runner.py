#!/usr/bin/env python3
"""
jobs.runner -- shared entrypoint for the converted Glue jobs.

Every ``migration/jobs/<folder>/job.py`` is a thin wrapper that calls
``run_job(<FOLDER>)``.  The runner:
  * builds the folder's ``PipelineDef`` from its spec (via ``configs``),
  * selects the execution engine from the classification
    (``pyspark`` -> ``engine_spark``, ``python_shell`` -> ``engine_pandas``),
  * resolves input paths (local dir or S3 prefix parameters mirroring the
    Informatica ``$Param_Root_Directory/data/int/in/<FOLDER>`` layout),
  * writes main / ERROR_TBL / COUNTER_TBL outputs and a run-metrics sidecar.

Runs identically on real Glue and locally (thin runtime shim).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from migration.jobs import configs
from migration.lib.engine_pandas import RunContext
from migration.lib.pipeline import PipelineDef


def _resolver_from_dir(pdef: PipelineDef, in_dir: str) -> Dict[str, str]:
    resolver: Dict[str, str] = {}
    wanted = [pdef.primary_source.name] + [s.name for s in pdef.extra_sources]
    # PAY_PERIOD is always a candidate reference source
    for name in wanted + ["PAY_PERIOD"]:
        for ext in (".dat", ".csv", ".txt"):
            cand = os.path.join(in_dir, f"{name}{ext}")
            if os.path.exists(cand):
                resolver[name] = cand
                break
    return resolver


def _write_csv(path: str, rows: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        cols = columns or []
    else:
        cols = columns or list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: ("" if r.get(c) is None else r.get(c)) for c in cols})


def run_job(folder: str, in_dir: str, out_dir: str,
            engine: Optional[str] = None,
            ctx: Optional[RunContext] = None) -> Dict[str, Any]:
    pdef = configs.build(folder)
    engine = engine or pdef.engine
    resolver = _resolver_from_dir(pdef, in_dir)

    if engine == "pyspark":
        try:
            from migration.lib import engine_spark as EG
            result = EG.run(pdef, resolver, ctx)
        except Exception as exc:  # pragma: no cover - spark optional locally
            sys.stderr.write(f"[runner] spark unavailable ({exc}); using python engine\n")
            from migration.lib import engine_pandas as EG
            result = EG.run(pdef, resolver, ctx)
    else:
        from migration.lib import engine_pandas as EG
        result = EG.run(pdef, resolver, ctx)

    fout = os.path.join(out_dir, folder)
    tgt_cols = [c.name for c in pdef.target.columns]
    _write_csv(os.path.join(fout, f"{pdef.target.name}.csv"), result.main, tgt_cols)
    _write_csv(os.path.join(fout, f"{pdef.error_target}.csv"), result.error)
    _write_csv(os.path.join(fout, f"{pdef.counter_target}.csv"), result.counters)
    with open(os.path.join(fout, "_metrics.json"), "w") as fh:
        json.dump(result.metrics, fh, indent=2)

    print(f"[job] {folder:18s} engine={result.metrics.get('engine'):11s} "
          f"in={result.metrics.get('input_rows')} main={len(result.main)} "
          f"err={len(result.error)} rps={result.metrics.get('rows_per_sec')}")
    return result


def main(argv: List[str], folder: Optional[str] = None) -> int:
    ap = argparse.ArgumentParser(description="Run a converted Informatica->Glue job")
    ap.add_argument("--folder", default=folder, required=folder is None)
    ap.add_argument("--in-dir", required=True, help="input dir or S3 prefix (local shim)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--engine", choices=["pyspark", "python_shell"], default=None)
    args, _ = ap.parse_known_args(argv[1:])
    run_job(args.folder, args.in_dir, args.out_dir, args.engine)
    return 0


def result_metrics(result) -> Dict[str, Any]:
    return result.metrics


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
