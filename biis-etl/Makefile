.PHONY: all setup build lint test test-unit test-functional test-regression test-performance \
        db-seed run-all run-pay-calendar run-comptime run-pseudossn run-fda-leave \
        run-ehrp2biis run-cpm validate clean generate-golden \
        docker-up docker-down airflow-validate \
        phase-1 phase-2 phase-3 phase-4 phase-5 phase-6

# ---- CONFIGURATION ----
# The default `test` backend is a self-contained SQLite database + local landing
# directory (see utils/config.py), so the whole build -> run -> test -> validate
# loop runs anywhere with no Docker / SQL Server required.  docker-compose.yml is
# provided for the optional SQL Server + Airflow path (see docker-up / airflow-validate).
PYTHON ?= python3
VENV := .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest
FLAKE8 := $(VENV)/bin/flake8
MYPY := $(VENV)/bin/mypy
ENV ?= test
RUN_DATE ?= 2026-06-11
GOLDEN := tests/fixtures/golden
REPORTS := reports
export SETUPTOOLS_USE_DISTUTILS = local

# ---- MASTER TARGET: setup + compile + run + test + validate ----
all: setup build lint db-seed run-all test validate
	@echo "=========================================="
	@echo "ALL PHASES PASSED - ZERO DEPRECATION"
	@echo "=========================================="

# ---- STEP 1: SETUP (virtual env + dependencies) ----
setup:
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .
	@echo "Verifying core libraries import..."
	$(PY) -c "import pyspark; print(f'PySpark {pyspark.__version__} OK')"
	$(PY) -c "import pandas; print('pandas OK')"
	$(PY) -c "import chispa; print('chispa OK')"

# ---- STEP 2: BUILD (compile all .py files, catch syntax errors) ----
build:
	@echo "Compiling all Python sources..."
	$(PY) -m compileall -q jobs utils transfers scripts dags
	@echo "All files compile OK"

# ---- STEP 3: LINT (style + type check) ----
lint:
	$(FLAKE8) jobs/ utils/ transfers/ scripts/ dags/ tests/ --max-line-length=120
	$(MYPY) jobs/ utils/ transfers/ --ignore-missing-imports
	@echo "Lint OK"

# ---- STEP 4: SEED DB (DDL + golden input data) ----
db-seed:
	@echo "Creating schema..."
	$(PY) scripts/apply_ddl.py --env $(ENV)
	@echo "Seeding golden input datasets..."
	$(PY) scripts/seed_golden_data.py --env $(ENV) --fixtures-dir $(GOLDEN)/
	@echo "Verifying seed..."
	$(PY) scripts/verify_seed.py --env $(ENV)
	@echo "DB seeded OK"

# ---- STEP 5: RUN ALL JOBS ----
run-all: run-pay-calendar run-comptime run-pseudossn run-fda-leave run-ehrp2biis run-cpm
	@echo "All jobs ran OK"

run-pay-calendar:
	@echo "=== Running Pay Calendar ==="
	$(PY) jobs/pay_calendar.py --env $(ENV) --run-date $(RUN_DATE)
run-comptime:
	@echo "=== Running COMPTIME ==="
	$(PY) jobs/comptime.py --env $(ENV) --file-path $(GOLDEN)/comptime_input.csv --run-date $(RUN_DATE)
run-pseudossn:
	@echo "=== Running PseudoSSN ==="
	$(PY) jobs/pseudossn.py --env $(ENV) --file-path $(GOLDEN)/pseudossn_input.dat --run-date $(RUN_DATE)
run-fda-leave:
	@echo "=== Running FDA Leave ==="
	$(PY) jobs/fda_leave.py --env $(ENV) --run-date $(RUN_DATE)
run-ehrp2biis:
	@echo "=== Running EHRP2BIIS (preload -> etl -> afterload) ==="
	$(PY) jobs/ehrp2biis/preload.py --env $(ENV) --run-date $(RUN_DATE)
	$(PY) jobs/ehrp2biis/etl.py --env $(ENV) --run-date $(RUN_DATE)
	$(PY) jobs/ehrp2biis/afterload.py --env $(ENV) --run-date $(RUN_DATE)
run-cpm:
	@echo "=== Running CPM (NIH, OIG, CDC) ==="
	$(PY) jobs/cpm/cpm_nih.py --env $(ENV) --run-date $(RUN_DATE)
	$(PY) jobs/cpm/cpm_oig.py --env $(ENV) --run-date $(RUN_DATE)
	$(PY) jobs/cpm/cpm_cdc.py --env $(ENV) --run-date $(RUN_DATE)

# ---- STEP 6: RUN ALL TESTS ----
test: test-unit test-functional test-regression test-performance
	@echo "All tests passed"

test-unit:
	@echo "=== Unit Tests ==="
	$(PYTEST) tests/unit/ -v

test-functional:
	@echo "=== Functional Tests (end-to-end per job) ==="
	$(PYTEST) tests/functional/ -v

test-regression:
	@echo "=== Regression Tests (golden dataset comparison) ==="
	$(PYTEST) tests/regression/ -v --tb=long

test-performance:
	@echo "=== Performance Tests (benchmark vs Informatica baselines) ==="
	$(PYTEST) tests/performance/ -v --benchmark-json=$(REPORTS)/benchmark.json

# ---- STEP 7: VALIDATE (source vs target reconciliation) ----
validate:
	@echo "=== Source-Target Reconciliation ==="
	$(PY) scripts/reconcile.py --env $(ENV) --modules all --report-dir $(REPORTS)/reconciliation/
	@echo "=== Checking for zero diffs ==="
	$(PY) scripts/check_reconciliation.py --report-dir $(REPORTS)/reconciliation/
	@echo "VALIDATION PASSED: Zero business logic deprecation"

# ---- Regenerate the synthetic golden fixtures ----
generate-golden:
	$(PY) scripts/generate_synthetic_golden.py

# ---- Optional: SQL Server + Airflow via Docker ----
docker-up:
	docker compose -f docker-compose.yml up -d --wait

docker-down:
	docker compose -f docker-compose.yml down -v 2>/dev/null || true

airflow-validate:
	docker compose -f docker-compose.yml run --rm airflow

# ---- CLEANUP ----
clean:
	rm -rf $(REPORTS) .pytest_cache .mypy_cache .biis-test
	find . -path ./$(VENV) -prune -o -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -path ./$(VENV) -prune -o -name "*.pyc" -delete 2>/dev/null || true

# ---- PER-PHASE GATES (incremental; each phase includes the previous ones) ----
phase-1: setup build lint db-seed run-pay-calendar
	$(PYTEST) tests/unit/test_pay_calendar.py tests/functional/test_pay_calendar_e2e.py -v
	$(PY) scripts/reconcile.py --env $(ENV) --modules pay_calendar --report-dir $(REPORTS)/reconciliation/
	@echo "PHASE 1 (Pay Calendar) GATE PASSED"

phase-2: phase-1 run-comptime
	$(PYTEST) tests/unit/test_comptime.py tests/functional/test_comptime_e2e.py -v
	$(PY) scripts/reconcile.py --env $(ENV) --modules comptime --report-dir $(REPORTS)/reconciliation/
	@echo "PHASE 2 (COMPTIME) GATE PASSED"

phase-3: phase-2 run-pseudossn
	$(PYTEST) tests/unit/test_pseudossn.py tests/functional/test_pseudossn_e2e.py -v
	$(PY) scripts/reconcile.py --env $(ENV) --modules pseudossn --report-dir $(REPORTS)/reconciliation/
	@echo "PHASE 3 (PseudoSSN) GATE PASSED"

phase-4: phase-3 run-fda-leave
	$(PYTEST) tests/unit/test_fda_leave.py tests/functional/test_fda_leave_e2e.py -v
	$(PY) scripts/reconcile.py --env $(ENV) --modules fda_leave --report-dir $(REPORTS)/reconciliation/
	@echo "PHASE 4 (FDA Leave) GATE PASSED"

phase-5: phase-4 run-ehrp2biis
	$(PYTEST) tests/unit/test_ehrp2biis_expressions.py tests/unit/test_afterload_sql.py tests/functional/test_ehrp2biis_e2e.py -v
	$(PY) scripts/reconcile.py --env $(ENV) --modules ehrp2biis --report-dir $(REPORTS)/reconciliation/
	@echo "PHASE 5 (EHRP2BIIS) GATE PASSED"

phase-6: phase-5 run-cpm
	$(PYTEST) tests/unit/test_cpm_transforms.py tests/functional/test_cpm_e2e.py -v
	$(PY) scripts/reconcile.py --env $(ENV) --modules cpm_nih,cpm_oig,cpm_cdc --report-dir $(REPORTS)/reconciliation/
	@echo "PHASE 6 (CPM) GATE PASSED"
