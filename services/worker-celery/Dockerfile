# Imagem do worker-celery: uv instala dependências travadas (uv.lock).
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY app ./app
# venv do uv no PATH: o compose escolhe o processo (bridge ou celery) via `command`.
ENV PATH="/app/.venv/bin:$PATH"
