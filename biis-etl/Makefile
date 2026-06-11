# BIIS ETL - master build/compile/run/test/validate entry point.
# Single source of truth for the full local validation loop proving zero
# deprecation of business logic from the Informatica PowerCenter source state.

SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV        := venv
PY          := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
PYTEST      := $(VENV)/bin/pytest
FLAKE8      := $(VENV)/bin/flake8
MYPY        := $(VENV)/bin/mypy
SPARK_SUBMIT := $(VENV)/bin/spark-submit

BIIS_ENV    ?= test
RUN_DATE    ?= $(shell date +%F)
GOLDEN_DIR  := tests/fixtures/golden
REPORT_DIR  := reports

export BIIS_ENV
export PYTHONPATH := $(CURDIR)

# Spark needs the SQL Server JDBC driver on its classpath; pulled via packages.
SPARK_PKGS  := com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11
SPARK_OPTS  := --packages $(SPARK_PKGS) --master local[2]

.PHONY: help all setup build lint db-up db-wait db-seed run-all test \
        test-unit test-functional test-regression test-performance \
        validate clean golden \
        phase-1 phase-2 phase-3 phase-4 phase-5 phase-6

help:
	@echo "BIIS ETL targets:"
	@echo "  make all              full loop: clean->setup->build->lint->db-up->db-seed->run-all->test->validate"
	@echo "  make setup            create venv, install deps, verify core imports"
	@echo "  make build            py_compile every job/util/transfer module"
	@echo "  make lint             flake8 + mypy"
	@echo "  make db-up            docker compose up + wait for SQL Server"
	@echo "  make db-seed          apply DDL, seed golden data, verify seed"
	@echo "  make golden           regenerate synthetic golden fixtures"
	@echo "  make run-all          spark-submit every job in dependency order"
	@echo "  make test             unit + functional + regression + performance"
	@echo "  make validate         reconcile all modules; fail on any non-zero diff"
	@echo "  make clean            docker compose down -v; remove venv/reports/caches"
	@echo "  make phase-1..6       cumulative per-phase gates"

# ---------------------------------------------------------------------------
# Build infrastructure
# ---------------------------------------------------------------------------
$(VENV)/.installed: requirements.txt
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip wheel setuptools
	$(PIP) install -r requirements.txt
	touch $(VENV)/.installed

setup: $(VENV)/.installed
	@echo "Verifying core imports..."
	$(PY) -c "import pyspark, pyodbc, paramiko, chispa, yaml, boto3, pandas; print('imports OK')"

build: setup
	@echo "Compiling all Python sources..."
	$(PY) -m py_compile $$(find jobs utils transfers scripts dags -name '*.py')
	@echo "build OK"

lint: setup
	$(FLAKE8) jobs utils transfers scripts dags
	$(MYPY) jobs utils transfers dags
	@echo "lint OK"

# ---------------------------------------------------------------------------
# Database lifecycle
# ---------------------------------------------------------------------------
db-up:
	docker compose up -d sqlserver sftp localstack
	$(MAKE) db-wait

db-wait: setup
	$(PY) scripts/wait_for_db.py --timeout 90

db-seed: setup
	$(PY) scripts/apply_ddl.py --env $(BIIS_ENV)
	$(PY) scripts/seed_golden_data.py --fixtures-dir $(GOLDEN_DIR) --env $(BIIS_ENV)
	$(PY) scripts/verify_seed.py --env $(BIIS_ENV)

golden: setup
	$(PY) scripts/generate_synthetic_golden.py --out $(GOLDEN_DIR)

# ---------------------------------------------------------------------------
# Job execution (dependency order)
# ---------------------------------------------------------------------------
run-all: setup
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/pay_calendar.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/comptime.py --env $(BIIS_ENV) --file-path $(GOLDEN_DIR)/comptime_input.csv
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/pseudossn.py --env $(BIIS_ENV) --file-path $(GOLDEN_DIR)/pseudossn_input.dat
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/fda_leave.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/ehrp2biis/preload.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/ehrp2biis/etl.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/ehrp2biis/afterload.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/cpm/cpm_nih.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/cpm/cpm_oig.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(SPARK_SUBMIT) $(SPARK_OPTS) jobs/cpm/cpm_cdc.py --env $(BIIS_ENV) --run-date $(RUN_DATE)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
test: test-unit test-functional test-regression test-performance

test-unit: setup
	mkdir -p $(REPORT_DIR)
	$(PYTEST) tests/unit/ -m unit -v --cov --cov-report=term-missing \
		--cov-report=html:$(REPORT_DIR)/coverage --cov-fail-under=90

test-functional: setup
	$(PYTEST) tests/functional/ -m functional -v

test-regression: setup
	$(PYTEST) tests/regression/ -m regression -v

test-performance: setup
	mkdir -p $(REPORT_DIR)
	$(PYTEST) tests/performance/ -m performance -v \
		--benchmark-json=$(REPORT_DIR)/benchmark.json

# ---------------------------------------------------------------------------
# Reconciliation - the final zero-deprecation gate
# ---------------------------------------------------------------------------
validate: setup
	$(PY) scripts/reconcile.py --env $(BIIS_ENV) --report-dir $(REPORT_DIR)/reconciliation
	$(PY) scripts/check_reconciliation.py --report-dir $(REPORT_DIR)/reconciliation

# ---------------------------------------------------------------------------
# Full loop + cleanup
# ---------------------------------------------------------------------------
all: clean setup build lint db-up db-seed run-all test validate
	@echo "================================================================"
	@echo " make all complete - zero deprecation validated for $(BIIS_ENV)"
	@echo "================================================================"

clean:
	-docker compose down -v
	rm -rf $(VENV) $(REPORT_DIR)
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type d -name '.pytest_cache' -prune -exec rm -rf {} +
	find . -type d -name '.mypy_cache' -prune -exec rm -rf {} +
	rm -rf /tmp/biis

# ---------------------------------------------------------------------------
# Per-phase cumulative gates. Each phase re-runs all previous phases so a
# later phase cannot silently regress an earlier one.
# ---------------------------------------------------------------------------
phase-1: build lint
	@echo "PHASE 1 (scaffold/build/lint) PASSED"

phase-2: phase-1 db-up db-seed
	@echo "PHASE 2 (schema DDL + seed) PASSED"

phase-3: phase-2
	$(PYTEST) tests/unit/ -m unit -v --cov --cov-fail-under=90
	@echo "PHASE 3 (utils + unit tests) PASSED"

phase-4: phase-3 run-all
	@echo "PHASE 4 (jobs run end-to-end) PASSED"

phase-5: phase-4 test-functional test-regression
	@echo "PHASE 5 (functional + regression) PASSED"

phase-6: phase-5 test-performance validate
	@echo "PHASE 6 (performance + reconciliation) PASSED"
