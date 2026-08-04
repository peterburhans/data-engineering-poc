COMPOSE := docker compose
DBT := $(COMPOSE) run --rm --entrypoint /home/airflow/dbt_venv/bin/dbt airflow-apiserver
DBT_ARGS := --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

DOCKER_GID ?= 1001
export DOCKER_GID

.PHONY: init infra up events backfill pipeline test lint dbt metrics logs down reset check-env

init:
	@test -e .env || cp .env.example .env

check-env:
	@grep -q '^LOCALSTACK_AUTH_TOKEN=.' .env 2>/dev/null || \
		(echo "Set LOCALSTACK_AUTH_TOKEN in .env"; exit 1)

infra: check-env
	$(COMPOSE) up -d localstack
	$(COMPOSE) run --no-deps --rm terraform fmt -check -recursive
	$(COMPOSE) run --no-deps --rm terraform init
	$(COMPOSE) run --no-deps --rm terraform validate
	$(COMPOSE) run --rm terraform apply -auto-approve

up: infra
	$(COMPOSE) build glue-runner
	$(COMPOSE) up -d --build marquez-db marquez-api marquez control-db mooncake airflow-init
	$(DBT) build $(DBT_ARGS)
	$(COMPOSE) up -d airflow-apiserver airflow-scheduler airflow-dag-processor
	$(COMPOSE) up -d --build superset-init
	$(COMPOSE) up -d superset

events:
	$(COMPOSE) up -d --build mock-data-service

backfill:
	$(COMPOSE) run --rm --no-deps --build mock-data-service \
		python -m mock_data_service.main backfill --days $${DAYS:-365}

pipeline:
	$(COMPOSE) exec airflow-apiserver airflow dags trigger smart_meter_warehouse \
		--conf '{"processing_mode":"'$${MODE:-incremental}'","backfill_start":"'$${START:-}'","backfill_end":"'$${END:-}'","backfill_grain":"'$${GRAIN:-day}'"}'
	$(COMPOSE) exec airflow-apiserver airflow dags unpause -y smart_meter_warehouse

test:
	$(COMPOSE) run --no-deps --rm --build mock-data-service pytest -p no:cacheprovider
	$(COMPOSE) run --no-deps --rm -v .:/workspace -w /workspace \
		-e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/workspace/jobs/glue/lib:/workspace/libs/data_contracts \
		mock-data-service pytest -p no:cacheprovider jobs/glue/tests

lint:
	$(COMPOSE) run --no-deps --rm --build -v .:/workspace -w /workspace mock-data-service \
		ruff check --no-cache jobs/glue services/mock-data-service platform/superset scripts

dbt:
	$(DBT) build $(DBT_ARGS)

metrics:
	$(DBT) parse $(DBT_ARGS)
	$(COMPOSE) run --rm --workdir /opt/airflow/dbt \
		--entrypoint /home/airflow/dbt_venv/bin/mf airflow-apiserver list metrics

logs:
	$(COMPOSE) logs -f

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v
