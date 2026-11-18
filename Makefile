.PHONY: check test test-integration test-all migrate upgrade run
check:
	uv run --no-sync ruff format --check src tests
	uv run --no-sync ruff check src tests
	uv run --no-sync mypy src tests
test:
	uv run --no-sync pytest tests/unit -q
test-integration:
	uv run --no-sync pytest tests/integration -q
test-all: test test-integration
migrate:
	uv run --no-sync alembic revision --autogenerate -m "$(MSG)"
upgrade:
	uv run --no-sync alembic upgrade head
run:
	uv run --no-sync python -m src
