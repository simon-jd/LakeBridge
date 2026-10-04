PYTHON ?= .venv/bin/python
LB ?= .venv/bin/lakebridge

.PHONY: install test integration lint demo benchmark compose-up compose-down
install:
	uv sync --frozen --extra dev --extra consumer --python 3.11
	$(PYTHON) scripts/bootstrap.py

test:
	$(PYTHON) -m pytest -m 'not integration' -q

integration:
	LAKEBRIDGE_INTEGRATION=1 $(PYTHON) -m pytest -m integration -q

lint:
	.venv/bin/ruff check src scripts tests orchestration
	.venv/bin/ruff format --check src scripts tests orchestration

demo:
	$(LB) --root artifacts/demo --namespace demo demo

benchmark:
	$(LB) --root artifacts/performance --namespace performance benchmark

compose-up:
	docker compose up -d postgres minio minio-init

compose-down:
	docker compose --profile airflow down
