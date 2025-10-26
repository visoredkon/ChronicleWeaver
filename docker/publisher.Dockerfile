FROM astral/uv:0.9-python3.14-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY src/publisher ./src/publisher

RUN uv sync --frozen --no-dev

FROM python:3.14-slim-bookworm

WORKDIR /app

RUN adduser --disabled-password --gecos "" --no-create-home appuser

COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/src/publisher /app/src/publisher

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER appuser

ENTRYPOINT ["python"]
CMD ["-m", "src.publisher.app.main"]
