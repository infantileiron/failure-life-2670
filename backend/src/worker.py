import logging, time
from pathlib import Path
import redis
from src.config.settings import REDIS_URL, RUN_HEARTBEAT_FILE, RUN_RECOVERY_INTERVAL
from src.services.run import execute, recover_pending
from src.utils.repository import init_db

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("worker")


def heartbeat():
    try:
        Path(RUN_HEARTBEAT_FILE).write_text(str(time.time()))
    except OSError:
        LOG.warning("could not write worker heartbeat file")


def main():
    init_db()
    client = redis.Redis.from_url(REDIS_URL)
    last_recovery = 0.0
    while True:
        heartbeat()
        try:
            # Recovery runs on its own cadence and does not depend on a job
            # arriving, so a run killed mid-computation is picked up again.
            if time.time() - last_recovery >= RUN_RECOVERY_INTERVAL:
                last_recovery = time.time()
                for run_id in recover_pending(client):
                    LOG.info("recovered run %s", run_id)
            item = client.blpop("failure-life:jobs", timeout=3)
            if item:
                execute(item[1].decode())
        except Exception:
            LOG.exception("worker failure")
            time.sleep(2)


if __name__ == "__main__":
    main()
