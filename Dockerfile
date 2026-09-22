# Builds the `artemis` watch/supervision binary. The container also needs
# `uv` at runtime because Store::run_analyzer shells out to
# `uv run --project analyzer analyzer/analyze.py ...` for each incident.
FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY --from=builder /build/target/release/artemis /usr/local/bin/artemis
COPY analyzer ./analyzer
# Pre-provision the interpreter analyzer/.python-version pins and warm its
# venv at build time, so recording the first incident doesn't pay a ~30MB
# CPython download + venv creation cost at runtime.
RUN uv python install 3.14 && uv run --project analyzer python3 -c ""

# Mount the target project's source at /project and its config at
# /app/artemis.toml (cwd/analyzer paths in the config should point there).
VOLUME ["/project", "/app/incidents"]

ENTRYPOINT ["artemis"]
CMD ["watch"]
