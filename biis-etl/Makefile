.PHONY: all setup db-up db-seed run-jobs test report clean

PYTHON := python3
VENV := .venv
PYTEST := $(VENV)/bin/pytest
PIP := $(VENV)/bin/pip

all: clean setup db-up db-seed run-jobs test report
	@echo "=========================================="
	@echo "MIGRATION COMPLETE -- Open reports/migration_validation_report.html"
	@echo "=========================================="

setup:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

db-up:
	docker compose up -d --wait
	$(VENV)/bin/python -c "import time; time.sleep(15); print('SQL Server ready')"

db-seed:
	$(VENV)/bin/python scripts/apply_ddl.py

run-jobs:
	$(VENV)/bin/python -m jobs.pay_calendar --env test --run-date 2026-06-11
	$(VENV)/bin/python -m jobs.comptime --env test --file-path tests/fixtures/comptime_input.csv

test:
	$(PYTEST) tests/unit/ -v --cov=jobs --cov=utils --cov-report=json:reports/coverage.json --cov-fail-under=90 --tb=short --junitxml=reports/junit_unit.xml
	$(PYTEST) tests/functional/ -v --tb=short --junitxml=reports/junit_functional.xml

report:
	$(VENV)/bin/python reports/generate_html_report.py

clean:
	docker compose down -v 2>/dev/null || true
	rm -rf $(VENV) reports/*.html reports/*.xml reports/*.json
