#!/usr/bin/env bash
#
# run_local.sh -- provision the local stack, generate synthetic data, and execute
# the full orchestrated pipeline end-to-end (converted jobs + golden baseline +
# field-level reconciliation + HTML reports), entirely offline.
#
# Usage:
#   ./migration/run_local.sh [functional|performance|both]   (default: functional)
#
# No live AWS is required. If docker + docker-compose are available the LocalStack
# / Postgres stack is started for a fuller AWS-like rehearsal; otherwise the
# pure-Python path (local Spark + engines + SQLite shim) runs everything.
set -euo pipefail

MODE="${1:-functional}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
cd "$REPO_ROOT"

PY="${PYTHON:-python3}"
export PYTHONPATH="$REPO_ROOT:${PYTHONPATH:-}"

echo "==> [1/6] Parsing source metadata -> specs"
$PY migration/parser/parse_powermart.py

echo "==> [2/6] Generating Step Functions orchestration (ASL)"
$PY migration/orchestration/generate_orchestration.py

echo "==> [3/6] Optional: start LocalStack + Postgres (if docker available)"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  (cd migration/local && docker compose up -d >/dev/null 2>&1) \
    && echo "    LocalStack + Postgres started" \
    || echo "    docker present but compose failed; continuing pure-Python"
else
  echo "    docker not available; using pure-Python local stack"
fi

run_mode () {
  local m="$1"
  echo "==> [4/6] ($m) Generating synthetic data"
  $PY migration/synth/generate.py --mode "$m"

  echo "==> [5/6] ($m) Running pipeline (parallel branches) + reconciliation"
  $PY migration/local/run_pipeline.py --mode "$m"

  echo "==> ($m) Loading targets into local warehouse shim + push-down afterload"
  $PY migration/local/sql_shim.py --out-dir "migration/local/out/$m" \
      --db "migration/local/warehouse_$m.db" || true
}

if [ "$MODE" = "both" ]; then
  run_mode functional
  run_mode performance
else
  run_mode "$MODE"
fi

echo "==> [6/6] Generating self-contained HTML reports"
$PY migration/reports/generate_reports.py

echo ""
echo "Done. Reports:"
echo "  migration/reports/migration_report.html"
echo "  migration/reports/data_analysis_report.html"
