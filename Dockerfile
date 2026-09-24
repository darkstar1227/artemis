# Builds the `artemis` watch/supervision binary. The container also needs
# `uv` at runtime because Store::run_analyzer shells out to
# `uv run --project analyzer analyzer/analyze.py ...` for each incident.
FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git procps \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Static `docker` CLI (client only, no daemon) so stage1/stage3 run_bash and
# health_check_command can drive containers on the host via a mounted
# /var/run/docker.sock — needed for deployments that monitor
# already-running docker-compose services rather than spawning `command`
# themselves (e.g. `docker compose logs -f <service>` as the watched
# process). Harmless/unused if a given deployment never calls `docker`.
ARG TARGETARCH
# docker.com doesn't publish a checksum manifest for these static tarballs, so
# the hashes below were computed by hand from the 27.3.1 release at build-time
# of this Dockerfile and are pinned here as a tamper/integrity check.
RUN ARCH=$(case "${TARGETARCH:-$(dpkg --print-architecture)}" in amd64) echo x86_64 ;; arm64) echo aarch64 ;; *) echo "${TARGETARCH}" ;; esac) \
    && SHA256=$(case "${ARCH}" in \
         x86_64) echo 9b4f6fe406e50f9085ee474c451e2bb5adb119a03591f467922d3b4e2ddf31d3 ;; \
         aarch64) echo 4da6a6c7502b7ab561675a5ff5ac192d9b49d76d0b8847cf17ade246122279f4 ;; \
         *) echo "" ;; \
       esac) \
    && curl -LsSf "https://download.docker.com/linux/static/stable/${ARCH}/docker-27.3.1.tgz" -o /tmp/docker.tgz \
    && echo "${SHA256}  /tmp/docker.tgz" | sha256sum -c - \
    && tar -xzf /tmp/docker.tgz -C /tmp \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && rm -rf /tmp/docker.tgz /tmp/docker

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
