FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

# pipeline.py's stage3 step shells out to `git -C <cwd> diff` directly (not
# through tools.py's run_bash whitelist) to capture the code diff for the
# report — without git installed that subprocess.run raises and the whole
# /escalate call errors out after stage3 already ran. Mounted project
# directories (see docker-compose.example.yml) are owned by the host user,
# not this image's root, so git's dubious-ownership check would otherwise
# refuse every repo under /projects; `safe.directory '*'` trusts any mounted
# path since this container only ever operates on repos the operator chose
# to mount here.
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && git config --global --add safe.directory '*'

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

# SQLite central collector store — mount a volume here to persist it.
VOLUME ["/app/data"]

EXPOSE 8787
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8787"]
