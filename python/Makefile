install:
	poetry install

lint:
	poetry run flake8
	poetry run black --check --diff .
	poetry run isort --check-only .

format:
	poetry run isort .
	poetry run black .

test:
	poetry run mypy -p bccp -p tests
	poetry run coverage run -m pytest --failed-first -vv
	poetry run coverage report
	poetry run coverage html

test-unit:
	poetry run mypy -p bccp -p tests
	poetry run pytest tests/unit -v


test-integration:
	poetry run mypy -p bccp -p tests
	poetry run pytest tests/integration -v

test-e2e:
	COMPOSE_PROJECT_NAME=bccp-test poetry run docker-compose --profile no_s3 up -d
	sleep 15
	RABBITMQ_CONNECTION_STRING=amqp://guest:guest@localhost:5672/%2F \
	MINIO_ENDPOINT=localhost:9000 \
	MINIO_ACCESS_KEY=minioadmin \
	MINIO_SECRET_KEY=minioadmin \
	poetry run pytest tests/e2e -v
	COMPOSE_PROJECT_NAME=bccp-test poetry run docker-compose --profile no_s3 down

