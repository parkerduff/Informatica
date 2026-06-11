SHELL := /bin/bash
VENV := venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
export BIIS_ENV ?= test
export PYTHONPATH := $(CURDIR)
RUN_DATE ?= $(shell date -u +%Y-%m-%d)

.PHONY: all setup build lint db-up db-seed run-all test test-unit test-functional \
        test-regression test-performance validate clean \
        phase-1 phase-2 phase-3 phase-4 phase-5 phase-6

all: clean setup build lint db-up db-seed run-all test validate

setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PY) -c "import pyspark, pyodbc, paramiko, chispa; print('deps OK')"

build:
	$(PY) -m compileall -q jobs utils transfers scripts dags

lint:
	$(VENV)/bin/flake8 jobs utils transfers scripts dags tests --max-line-length=120 --extend-ignore=E203,W503
	$(VENV)/bin/mypy jobs utils transfers scripts --ignore-missing-imports

db-up:
	docker compose up -d sqlserver sftp
	$(PY) scripts/wait_for_db.py --timeout 120
	mkdir -p ~/.ssh && touch ~/.ssh/known_hosts
	-for i in 1 2 3 4 5; do ssh-keyscan -p 2222 localhost >> ~/.ssh/known_hosts 2>/dev/null && break || sleep 2; done

db-seed:
	$(PY) scripts/apply_ddl.py --env $(BIIS_ENV)
	$(PY) scripts/generate_synthetic_golden.py --run-date $(RUN_DATE)
	$(PY) scripts/seed_golden_data.py --fixtures-dir tests/fixtures/golden/
	$(PY) scripts/verify_seed.py

run-all:
	$(PY) jobs/pay_calendar.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/comptime.py --env $(BIIS_ENV) --file-path /tmp/biis/landing/comptime_input.csv
	$(PY) jobs/pseudossn.py --env $(BIIS_ENV) --file-path /tmp/biis/landing/pseudossn_input.dat
	$(PY) jobs/fda_leave.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/ehrp2biis/preload.py --env $(BIIS_ENV)
	$(PY) jobs/ehrp2biis/etl.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/ehrp2biis/afterload.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/cpm/cpm_nih.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/cpm/cpm_oig.py --env $(BIIS_ENV) --run-date $(RUN_DATE)
	$(PY) jobs/cpm/cpm_cdc.py --env $(BIIS_ENV) --run-date $(RUN_DATE)

test: test-unit test-functional test-regression test-performance

test-unit:
	$(PYTEST) tests/unit/ -v --cov=jobs --cov=utils --cov=transfers --cov-fail-under=90

test-functional:
	$(PYTEST) tests/functional/ -v

test-regression:
	$(PYTEST) tests/regression/ -v

test-performance:
	mkdir -p reports
	$(PYTEST) tests/performance/ -v --benchmark-json=reports/benchmark.json

validate:
	$(PY) scripts/reconcile.py --env $(BIIS_ENV) --report-dir reports/reconciliation/
	$(PY) scripts/check_reconciliation.py --report-dir reports/reconciliation/

clean:
	-docker compose down -v
	rm -rf $(VENV) reports .pytest_cache .mypy_cache .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +

phase-1: setup build lint
phase-2: phase-1 db-up db-seed
phase-3: phase-2 run-all
phase-4: phase-3 test-unit
phase-5: phase-4 test-functional test-regression test-performance
phase-6: phase-5 validate
