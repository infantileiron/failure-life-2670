import hashlib, json, logging, os, socket, threading, time, uuid
from pathlib import Path
from fastapi import HTTPException
from src.models.entities import SurvivalRun
from src.utils import repository as repo
from src.utils.identity import new_id, now
from src.services.statistics import analyze
from src.config.settings import (
    ARTIFACT_DIR, REDIS_URL, TASK_MODE, RUN_LEASE_SECONDS, RUN_MAX_ATTEMPTS,
    RUN_RECOVERY_INTERVAL,
)

LOG = logging.getLogger(__name__)
QUEUE_KEY = "failure-life:jobs"
RETRYABLE_ERRORS = (OSError, ConnectionError, TimeoutError)


def worker_identity():
    return f"{socket.gethostname()}:{os.getpid()}"


def _new_token():
    return uuid.uuid4().hex


def _normalize(run):
    """Fill and repair lifecycle fields for legacy or freshly built rows."""
    if "attempts" not in run:
        run["attempts"] = 1 if run.get("status") in ("running", "completed", "failed") else 0
    run["attempts"] = max(0, int(run.get("attempts") or 0))
    run["max_attempts"] = max(1, int(run.get("max_attempts") or RUN_MAX_ATTEMPTS))
    defaults = {
        "attempt_token": "",
        "last_error": run.get("error", "") if run.get("status") == "failed" else "",
        "last_error_at": "",
        "retryable": False,
        "final_status": "completed" if run.get("status") == "completed" else ("failed" if run.get("status") == "failed" else ""),
        "worker": "",
        "lease_until": 0.0,
        "heartbeat_at": 0.0,
        "recovery_at": 0.0,
        "started_at": "",
        "finished_at": "",
        "updated_at": run.get("created_at", ""),
        "published_at": "",
        "artifact": "",
        "artifact_sha256": "",
        "artifact_size": 0,
    }
    for key, value in defaults.items():
        run.setdefault(key, value)
    return run


def _public(run):
    run = dict(run)
    run.pop("attempt_token", None)
    run["artifact_present"] = (
        run.get("status") != "completed"
        or (ARTIFACT_DIR / (run.get("artifact") or (run.get("id", "") + ".json"))).is_file()
    )
    return run


def list_runs():
    return [_public(_normalize(run)) for run in repo.all_rows("runs")]


def detail(id):
    return _public(_normalize(repo.get("runs", id)))


def _artifact_path(run_id):
    return ARTIFACT_DIR / (run_id + ".json")


def artifact_missing(run):
    """True when the row claims completion but the published file is absent."""
    if run.get("status") != "completed":
        return False
    name = run.get("artifact") or (run.get("id", "") + ".json")
    return not (ARTIFACT_DIR / name).is_file()


def artifact_checksum(run):
    return run.get("artifact_sha256", "") if run.get("status") == "completed" else ""


def _write_attempt_artifact(run_id, token, result):
    """Stage a result outside the published filename.

    A late or superseded attempt may write its staging file but can never touch
    the published artifact before it wins the database publication guard.
    """
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    staging = ARTIFACT_DIR / ".staging"
    staging.mkdir(parents=True, exist_ok=True)
    tmp = staging / f"{run_id}.{token}.json"
    payload = json.dumps(result, ensure_ascii=False, allow_nan=False)
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return tmp, digest, len(payload.encode("utf-8"))


def write_artifact(run_id, result, token="public"):
    """Write the published result file and return its metadata.

    The file is replaced atomically so a partial write is never visible under
    the published name.
    """
    tmp, digest, size = _write_attempt_artifact(run_id, token, result)
    final = _artifact_path(run_id)
    os.replace(tmp, final)
    return {"artifact": final.name, "artifact_sha256": digest, "artifact_size": size}


def _publish_artifact(run_id, token, tmp, digest, size, result):
    """Move the staged artifact into place, then persist completion.

    The database row is locked and re-checked while holding the transaction, so
    a stale attempt cannot replace an already published result. The file is
    moved before the status becomes "completed"; any failure leaves the run
    non-completed.
    """
    final = _artifact_path(run_id)
    with repo.transaction() as (conn, q):
        current = repo.lock_get(conn, "runs", run_id, q)
        if not current:
            return False, None, "missing"
        if current.get("status") != "running" or current.get("attempt_token") != token:
            return False, _normalize(current), "superseded"
        os.replace(tmp, final)
        try:
            directory_fd = os.open(str(ARTIFACT_DIR), os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
        current["result"] = result
        current["artifact"] = final.name
        current["artifact_sha256"] = digest
        current["artifact_size"] = size
        current["published_at"] = now()
        current["status"] = "completed"
        current["final_status"] = "completed"
        current["retryable"] = False
        current["error"] = ""
        # last_error/last_error_at are deliberately kept as an audit trail of
        # the attempts that preceded this successful publication.
        current["worker"] = ""
        current["lease_until"] = 0.0
        current["finished_at"] = current["published_at"]
        current["updated_at"] = current["published_at"]
        repo.locked_put(conn, "runs", current, q)
    return True, _normalize(current), ""


def _mark_failure(run_id, token, exc):
    retryable = isinstance(exc, RETRYABLE_ERRORS) or not isinstance(
        exc, (ValueError, KeyError, TypeError, HTTPException)
    )
    message = str(exc) or exc.__class__.__name__
    with repo.transaction() as (conn, q):
        current = repo.lock_get(conn, "runs", run_id, q)
        if not current:
            return None
        if current.get("status") != "running" or current.get("attempt_token") != token:
            return _normalize(current)
        stamp = now()
        attempts = int(current.get("attempts", 0))
        max_attempts = int(current.get("max_attempts", RUN_MAX_ATTEMPTS))
        exhausted = attempts >= max_attempts
        current["last_error"] = message
        current["last_error_at"] = stamp
        current["error"] = message
        current["updated_at"] = stamp
        current["worker"] = ""
        current["lease_until"] = 0.0
        current["heartbeat_at"] = 0.0
        current["retryable"] = bool(retryable and not exhausted)
        if retryable and not exhausted:
            # Keep the run pending for the recovery sweep; page shows failure
            # reason and retry count but never "completed".
            current["status"] = "queued"
            current["final_status"] = ""
            current["recovery_at"] = time.time()
        else:
            current["status"] = "failed"
            current["final_status"] = "failed"
            current["finished_at"] = stamp
        repo.locked_put(conn, "runs", current, q)
    return _normalize(current)


def claim(run_id, worker=None):
    """Atomically take ownership of a run.

    Returns the claimed run, or None when the run is terminal, owned by another
    live worker, or already published. Every claim issues a fresh attempt token
    so older workers can be rejected at publication time.
    """
    worker = worker or worker_identity()
    stamp = now()
    with repo.transaction() as (conn, q):
        run = repo.lock_get(conn, "runs", run_id, q)
        if run is None:
            # No row is equivalent to a terminal/unknown run: no claim.
            return None
        run = _normalize(run)
        status = run.get("status")
        retryable = status == "failed" and run.get("retryable") and not run.get("final_status")
        if status not in ("queued", "running", "failed"):
            return None
        if status == "failed" and not retryable:
            return None
        if status == "running" and float(run.get("lease_until") or 0) > time.time():
            # Another live worker owns this run; do not double-claim.
            return None
        attempts = int(run.get("attempts", 0))
        max_attempts = int(run.get("max_attempts", RUN_MAX_ATTEMPTS))
        if attempts >= max_attempts:
            run["status"] = "failed"
            run["final_status"] = "failed"
            run["retryable"] = False
            run["worker"] = ""
            run["lease_until"] = 0.0
            run["finished_at"] = stamp
            run["updated_at"] = stamp
            run["last_error"] = run.get("last_error") or "运行已中断且达到最大重试次数"
            run["last_error_at"] = run.get("last_error_at") or stamp
            repo.locked_put(conn, "runs", run, q)
            return None
        token = _new_token()
        run["status"] = "running"
        run["attempts"] = attempts + 1
        run["max_attempts"] = max_attempts
        run["attempt_token"] = token
        run["worker"] = worker
        run["started_at"] = run.get("started_at") or stamp
        run["heartbeat_at"] = time.time()
        run["lease_until"] = time.time() + RUN_LEASE_SECONDS
        run["final_status"] = ""
        run["retryable"] = False
        run["updated_at"] = stamp
        repo.locked_put(conn, "runs", run, q)
    return _normalize(run)


def _heartbeat(run_id, token, stop):
    interval = max(1.0, RUN_LEASE_SECONDS / 3.0)
    while not stop.wait(interval):
        try:
            with repo.transaction() as (conn, q):
                current = repo.lock_get(conn, "runs", run_id, q)
                if not current or current.get("status") != "running" or current.get("attempt_token") != token:
                    return
                current["heartbeat_at"] = time.time()
                current["lease_until"] = time.time() + RUN_LEASE_SECONDS
                repo.locked_put(conn, "runs", current, q)
        except Exception:
            LOG.exception("run heartbeat failed; lease will expire and recovery will retry")
            return


def execute(id, worker=None):
    """Compute and publish one run, reusing the existing run id."""
    run = claim(id, worker)
    if run is None:
        try:
            return detail(id)
        except HTTPException:
            # A stale job for a deleted/unknown run must not kill the worker.
            return None
    token = run["attempt_token"]
    stop = threading.Event()
    beat = threading.Thread(target=_heartbeat, args=(id, token, stop), daemon=True)
    beat.start()
    staged = None
    try:
        study = repo.get("studies", run["study_id"])
        rule = repo.get("rules", study["rule_id"])
        dataset = repo.get("datasets", study["dataset_id"])
        result = analyze(dataset["rows"], rule, study)
        staged, digest, size = _write_attempt_artifact(id, token, result)
        published, current, reason = _publish_artifact(id, token, staged, digest, size, result)
        if not published:
            try:
                if staged.exists():
                    staged.unlink()
            except OSError:
                pass
            if reason == "superseded":
                LOG.info("discarded late result for run %s (attempt %s)", id, token)
            return detail(id)
        staged = None
        return detail(id)
    except Exception as exc:
        LOG.exception("run %s failed on attempt", id)
        _mark_failure(id, token, exc)
        return detail(id)
    finally:
        stop.set()
        beat.join(timeout=2)
        if staged is not None:
            try:
                if staged.exists():
                    staged.unlink()
            except OSError:
                pass


def run_to_completion(run_id, worker=None):
    """Execute inline until the run settles (single-process/TASK_MODE=inline).

    In Redis mode the recovery sweep drives retries; without it an inline run
    that hits a retryable error would otherwise stay queued forever. The loop is
    bounded because every successful claim increments the attempt counter.
    """
    result = execute(run_id, worker)
    while (
        result
        and result.get("status") == "queued"
        and int(result.get("attempts", 0)) < int(result.get("max_attempts", RUN_MAX_ATTEMPTS))
    ):
        result = execute(run_id, worker)
    return result


def enqueue(run_id, client=None):
    """Push a run id unless an identical job is already waiting.

    Recovery re-runs on a fixed cadence, so without this de-duplication a
    worker outage would keep appending the same id and grow the queue without
    bound. Duplicate executions are already harmless (claim is atomic), this
    just keeps the queue bounded.
    """
    import redis
    client = client or redis.Redis.from_url(REDIS_URL)
    try:
        if client.lpos(QUEUE_KEY, run_id) is not None:
            return False
    except (AttributeError, redis.ResponseError):
        pass
    client.rpush(QUEUE_KEY, run_id)
    return True


def recover_pending(client=None):
    """Requeue pending, interrupted and retryable runs without creating new ids.

    * queued runs are re-enqueued so a lost Redis job cannot strand them;
    * running runs whose lease expired are treated as interrupted and queued
      again under the same run id;
    * retryable failures and published runs whose artifact went missing are
      repaired as well.

    Completed results stored before artifact tracking existed are reconciled by
    rewriting the artifact from the stored result, never by inventing a new one.
    """
    stamp = time.time()
    requeue = []
    repairs = []
    with repo.transaction() as (conn, q):
        if not repo._is_sqlite():
            # One recovery sweep at a time; without this two workers could both
            # decide to requeue the same run.
            conn.execute("SELECT pg_advisory_xact_lock(2302)")
        select = "SELECT id,payload FROM runs ORDER BY id"
        if not repo._is_sqlite():
            # Lock every row we may rewrite so a concurrent claim/publish
            # cannot be clobbered by this sweep's stale payload.
            select += " FOR UPDATE"
        rows = conn.execute(select).fetchall()
        for raw_id, payload in rows:
            run = _normalize(json.loads(payload))
            status = run.get("status")
            attempts = int(run.get("attempts", 0))
            max_attempts = int(run.get("max_attempts", RUN_MAX_ATTEMPTS))
            changed = False
            if status == "queued":
                if attempts >= max_attempts:
                    run["status"] = "failed"
                    run["final_status"] = "failed"
                    run["retryable"] = False
                    run["last_error"] = run.get("last_error") or "运行排队后达到最大重试次数"
                    run["last_error_at"] = run.get("last_error_at") or now()
                    run["finished_at"] = now()
                    changed = True
                elif stamp - float(run.get("recovery_at") or 0) >= RUN_RECOVERY_INTERVAL:
                    run["recovery_at"] = stamp
                    changed = True
                    requeue.append(run["id"])
            elif status == "running" and float(run.get("lease_until") or 0) <= stamp:
                run["worker"] = ""
                run["lease_until"] = 0.0
                if attempts >= max_attempts:
                    run["status"] = "failed"
                    run["final_status"] = "failed"
                    run["retryable"] = False
                    run["last_error"] = run.get("last_error") or "运行中断且达到最大重试次数"
                    run["last_error_at"] = run.get("last_error_at") or now()
                    run["finished_at"] = now()
                else:
                    run["status"] = "queued"
                    run["final_status"] = ""
                    run["last_error"] = "计算进程中断，已重新排队并使用原运行编号重算"
                    run["last_error_at"] = now()
                    run["error"] = run["last_error"]
                    run["recovery_at"] = stamp
                    requeue.append(run["id"])
                run["updated_at"] = now()
                changed = True
            elif status == "failed" and run.get("retryable") and not run.get("final_status"):
                if attempts >= max_attempts:
                    run["retryable"] = False
                    run["final_status"] = "failed"
                    run["finished_at"] = now()
                else:
                    run["status"] = "queued"
                    run["retryable"] = False
                    run["recovery_at"] = stamp
                    requeue.append(run["id"])
                run["updated_at"] = now()
                changed = True
            elif status == "completed":
                path = _artifact_path(run["id"])
                if not path.is_file():
                    result = run.get("result") or {}
                    if result:
                        repairs.append((run, result))
                    elif attempts >= max_attempts:
                        run["status"] = "failed"
                        run["final_status"] = "failed"
                        run["last_error"] = "结果文件缺失且达到最大重试次数"
                        run["last_error_at"] = now()
                        run["finished_at"] = now()
                        run["updated_at"] = now()
                        changed = True
                    else:
                        run["status"] = "queued"
                        run["final_status"] = ""
                        run["last_error"] = "结果文件缺失，已重新排队并使用原运行编号重算"
                        run["last_error_at"] = now()
                        run["recovery_at"] = stamp
                        requeue.append(run["id"])
                        run["updated_at"] = now()
                        changed = True
            if changed:
                repo.locked_put(conn, "runs", run, q)
        # A repaired run is written after the artifact is confirmed on disk.
        for run, result in repairs:
            try:
                tmp, digest, size = _write_attempt_artifact(run["id"], "repair", result)
                final = _artifact_path(run["id"])
                os.replace(tmp, final)
                run["artifact"] = final.name
                run["artifact_sha256"] = digest
                run["artifact_size"] = size
                run["updated_at"] = now()
                repo.locked_put(conn, "runs", run, q)
            except OSError as exc:
                LOG.exception("could not repair artifact for run %s", run["id"])
                if int(run.get("attempts", 0)) >= int(run.get("max_attempts", RUN_MAX_ATTEMPTS)):
                    run["status"] = "failed"
                    run["final_status"] = "failed"
                    run["retryable"] = False
                    run["finished_at"] = now()
                else:
                    run["status"] = "queued"
                    run["final_status"] = ""
                    run["retryable"] = False
                    run["recovery_at"] = stamp
                    requeue.append(run["id"])
                run["last_error"] = f"结果文件写入失败：{exc}"
                run["last_error_at"] = now()
                run["updated_at"] = now()
                repo.locked_put(conn, "runs", run, q)
    for run_id in requeue:
        try:
            enqueue(run_id, client)
        except Exception:
            LOG.exception("could not requeue run %s; it remains pending for the next sweep", run_id)
    return requeue


def create(data):
    repo.get("studies", data.study_id)
    if sum(r["status"] in ("queued", "running") for r in repo.all_rows("runs")) >= 10:
        raise ValueError("运行队列已满，请等待已有任务完成")
    run = repo.put("runs", SurvivalRun(new_id("run"), data.study_id, created_at=now()))
    run = _normalize(run)
    if TASK_MODE == "inline":
        return run_to_completion(run["id"])
    try:
        enqueue(run["id"])
    except Exception:
        # Keep the run pending instead of failing it: the recovery sweep will
        # enqueue it once Redis returns, under the same run id.
        LOG.exception("could not enqueue new run %s; recovery will retry", run["id"])
        run["status"] = "queued"
        run["final_status"] = ""
        run["error"] = "队列暂不可用，恢复扫描会在Redis恢复后用原运行编号重算"
        run["last_error"] = run["error"]
        run["last_error_at"] = now()
        run["retryable"] = False
        run["recovery_at"] = 0.0
        repo.put("runs", run)
        raise ValueError(run["error"])
    return run


def request_retry(run_id):
    """Manually reset a failed run for another attempt, keeping its id.

    Only retryable/enqueued states are touched; a completed or live run is
    never reopened. Returns the updated run or None when retries are exhausted.
    """
    with repo.transaction() as (conn, q):
        run = repo.lock_get(conn, "runs", run_id, q)
        if not run:
            return None
        run = _normalize(run)
        if run["status"] in ("completed", "running"):
            return run
        max_attempts = int(run.get("max_attempts", RUN_MAX_ATTEMPTS))
        if int(run.get("attempts", 0)) >= max_attempts:
            return None
        stamp = now()
        run["status"] = "queued"
        run["final_status"] = ""
        run["retryable"] = False
        run["recovery_at"] = 0.0
        run["worker"] = ""
        run["lease_until"] = 0.0
        run["updated_at"] = stamp
        repo.locked_put(conn, "runs", run, q)
    try:
        enqueue(run_id)
    except Exception:
        LOG.exception("could not requeue run %s after manual retry", run_id)
    return _normalize(run)
