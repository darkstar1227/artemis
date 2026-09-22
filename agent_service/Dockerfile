FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

# SQLite central collector store — mount a volume here to persist it.
VOLUME ["/app/data"]

EXPOSE 8787
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8787"]
