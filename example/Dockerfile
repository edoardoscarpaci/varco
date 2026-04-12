# ── Varco Post Example — Dockerfile ──────────────────────────────────────────
#
# Multi-stage build:
#   builder  — install all Python dependencies with uv
#   runtime  — minimal image with source + venv; uses `uv run` to launch
#
# Build (from workspace root):
#   docker build -t varco-example -f example/Dockerfile .
#
# Run (requires DATABASE_URL and VARCO_REDIS_URL env vars):
#   docker run -p 8000:8000 \
#     -e DATABASE_URL="postgresql+asyncpg://varco:varco@db/varco" \
#     -e VARCO_REDIS_URL="redis://redis:6379" \
#     -e VARCO_REDIS_CACHE_URL="redis://redis:6379" \
#     varco-example
#
# DESIGN: uv run in CMD over bare executable path
#   ✅ uv run activates the correct venv automatically — no PATH gymnastics.
#   ✅ Works regardless of where uv places the venv on different platforms.
#   ✅ Consistent with how the example is run in development (uv run …).
#   ❌ uv binary must be present in the runtime image — adds ~10 MB.
#
# DESIGN: two-stage RUN in builder (workspace sync + pip install)
#   uvicorn and asyncpg are listed in example/pyproject.toml under
#   [dependency-groups] dev, which `uv sync --no-dev` skips.  Rather than
#   moving them to [project.dependencies] (which would change the workspace
#   lock file), we add them explicitly with `uv pip install` after the sync.
#   ✅ No changes to pyproject.toml or uv.lock required.
#   ✅ Builder layer is still cached when only source changes (not deps).
#   ❌ Two RUN layers instead of one — acceptable for a quickstart Dockerfile.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

# Install uv.  Pinned to a stable release for reproducible layer caching.
COPY --from=ghcr.io/astral-sh/uv:0.7.2 /uv /usr/local/bin/uv

WORKDIR /workspace

# Copy workspace-level lock and config files first.
# These rarely change — Docker caches this layer aggressively.
COPY pyproject.toml uv.lock ./

# Copy all workspace package directories.
# varco_core, varco_fastapi, varco_sa, varco_redis are direct dependencies.
# varco_kafka, varco_beanie, varco_memcached, varco_ws are workspace members
# that uv requires to resolve the lock file even if unused by the example.
COPY varco_core/      ./varco_core/
COPY varco_fastapi/   ./varco_fastapi/
COPY varco_sa/        ./varco_sa/
COPY varco_redis/     ./varco_redis/
COPY varco_kafka/     ./varco_kafka/
COPY varco_beanie/    ./varco_beanie/
COPY varco_memcached/ ./varco_memcached/
COPY varco_ws/        ./varco_ws/
COPY example/         ./example/
# Copy vendored wheels — providify 0.1.6 is vendored here because uv.lock
# stores a relative path to the wheel (Docker-compatible, no external deps).
COPY vendor/          ./vendor/
# Step 1: sync the workspace — installs all varco_* packages into .venv.
# --no-dev: skip dev-only dependencies (pytest, testcontainers, httpx).
# --frozen: fail if uv.lock is stale — prevents silent dependency drift.
RUN uv sync --frozen --no-dev --all-packages

# Step 2: install runtime deps that live in example's dev group.
# uvicorn and asyncpg are declared as dev deps in example/pyproject.toml
# (they were grouped with pytest/testcontainers for local development).
# We install them separately so they are present in the production image.
RUN uv pip install uvicorn asyncpg

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime
ENV PATH="/workspace/.venv/bin:$PATH"
ENV PYTHONPATH="/workspace/example"
# Create a non-root user for the app process.
# Running as root inside a container is a security risk — defence-in-depth.
RUN addgroup --system varco && adduser --system --ingroup varco varco

WORKDIR /workspace

# Copy the entire workspace (source + installed venv) from the builder.
# The venv at /workspace/.venv contains all installed packages.
COPY --from=builder --chown=varco:varco /workspace /workspace

# Switch to the non-root user before running the server.
USER varco

# Expose the default uvicorn port.
EXPOSE 8000

# Health check — /auth/me requires no token and always responds 200.
# Shell-form HEALTHCHECK so $PATH is available for python lookup.
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c \
        "import urllib.request; urllib.request.urlopen('http://localhost:8000/auth/me')" \
    || exit 1

# Start the ASGI server via `uv run`.
# uv run sets VIRTUAL_ENV=/workspace/.venv automatically (finds it via .python-version
# or the nearest pyproject.toml) and adds its bin/ to PATH before exec-ing uvicorn.
#
# --host 0.0.0.0: bind to all interfaces (required inside Docker).
# --workers 1: single worker — increase for production (behind a load balancer).
CMD ["uvicorn", "example.app:create_app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1"]
