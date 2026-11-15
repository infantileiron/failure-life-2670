import os
from pathlib import Path
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:////tmp/failure-life.sqlite")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
JWT_SECRET = os.getenv("JWT_SECRET", "failure-life-development-secret-change-this")
SEED_DEMO = os.getenv("SEED_DEMO", "true").lower() == "true"
TASK_MODE = os.getenv("TASK_MODE", "redis")
ARTIFACT_DIR = Path(os.getenv("ARTIFACT_DIR", "/tmp/failure-life-artifacts"))
# A claimed run holds a lease that the worker refreshes with a heartbeat. A
# run whose lease expired is treated as interrupted and recomputed with the
# same run id.
RUN_LEASE_SECONDS = float(os.getenv("RUN_LEASE_SECONDS", "45"))
RUN_MAX_ATTEMPTS = max(1, int(os.getenv("RUN_MAX_ATTEMPTS", "3")))
RUN_RECOVERY_INTERVAL = float(os.getenv("RUN_RECOVERY_INTERVAL", "10"))
RUN_HEARTBEAT_FILE = os.getenv("RUN_HEARTBEAT_FILE", "/tmp/worker-heartbeat")
