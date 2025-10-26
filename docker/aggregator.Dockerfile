FROM astral/uv:0.9-python3.14-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY src/aggregator ./src/aggregator

RUN uv sync --frozen --no-dev

FROM python:3.14-slim-bookworm

WORKDIR /app

RUN adduser --disabled-password --gecos "" --no-create-home appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app

COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/src/aggregator /app/src/aggregator

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER appuser

EXPOSE 8000

ENTRYPOINT ["fastapi"]
CMD ["run", "src/aggregator/app/main.py", "--host", "0.0.0.0", "--port", "8000"]
