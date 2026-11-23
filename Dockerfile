# One image serves every role of the port allocation service:
#   * worker   (default CMD below, or compose `command:` with app.main:app)
#   * entry    (compose `command:` with --factory app.entry:create_entry_app)
#   * standalone single instance (default CMD, no SHARD_* env vars)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv/app

# Install dependencies first so application edits don't invalidate the layer.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Run as the unprivileged user shipped with the slim image.
USER nobody

EXPOSE 8080

# Default: standalone single instance (or one sharded worker when SHARD_INDEX
# / SHARD_COUNT are set). Every process that holds leases must run with
# --workers 1: the lease store lives in process memory, and the cluster
# scales by sharding peers across processes, never by replicating a process.
# The entry (app.entry:app) is stateless and is started via docker-compose.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
