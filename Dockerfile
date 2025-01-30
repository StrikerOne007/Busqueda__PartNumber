FROM python:3.12.8-slim-bullseye AS base

# Instala las dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*


FROM base AS builder
COPY --from=ghcr.io/astral-sh/uv:0.5.20 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /app
COPY uv.lock pyproject.toml /app/
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-install-project --no-dev
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev

FROM base
# Copia el binario `uv` desde la etapa builder
COPY --from=builder /bin/uv /bin/uv
# Copia la aplicación y el entorno virtual
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app
CMD ["uv", "run", "vs_code_busqueda_partnumber.py"]
