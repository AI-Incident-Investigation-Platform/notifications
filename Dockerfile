FROM python:3.14.8-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

FROM python:3.14.8-slim
RUN useradd --uid 1000 --create-home app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --chown=app:app src ./src
COPY --chown=app:app alembic.ini ./alembic.ini
COPY --chown=app:app alembic ./alembic
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER app
EXPOSE 8010
CMD ["python", "-m", "src"]
