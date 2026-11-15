import json, sqlite3, threading
from contextlib import contextmanager
from dataclasses import asdict
from fastapi import HTTPException
from src.config.settings import DATABASE_URL
TABLES = ("datasets", "rules", "studies", "runs", "reviews", "reports", "users", "audit")
_LOCK = threading.RLock()


def _is_sqlite():
    return DATABASE_URL.startswith("sqlite:///")


def connect():
    if _is_sqlite():
        conn = sqlite3.connect(
            DATABASE_URL.removeprefix("sqlite:///"), timeout=30, isolation_level=None
        )
        conn.execute("PRAGMA busy_timeout=30000")
        return conn, "?"
    import psycopg
    return psycopg.connect(DATABASE_URL), "%s"


def init_db():
    conn,_ = connect()
    try:
        if not _is_sqlite():
            conn.execute("SELECT pg_advisory_xact_lock(2301)")
        for table in TABLES:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        conn.commit()
    finally: conn.close()


@contextmanager
def transaction():
    """Open a write transaction that serialises read-modify-write cycles.

    SQLite uses BEGIN IMMEDIATE so concurrent workers cannot both pass a
    compare-and-set read. PostgreSQL relies on SELECT ... FOR UPDATE plus the
    normal transaction isolation.
    """
    conn,q = connect()
    try:
        if _is_sqlite():
            conn.execute("BEGIN IMMEDIATE")
        yield conn,q
        conn.commit()
    except BaseException:
        try: conn.rollback()
        except Exception: pass
        raise
    finally: conn.close()


@contextmanager
def read_transaction():
    conn,q = connect()
    try:
        yield conn,q
    finally: conn.close()


def all_rows(table):
    assert table in TABLES
    conn,_ = connect()
    try: return [json.loads(row[0]) for row in conn.execute(f"SELECT payload FROM {table} ORDER BY id").fetchall()]
    finally: conn.close()


def get(table, id):
    assert table in TABLES
    conn,q = connect()
    try:
        row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}", (id,)).fetchone()
        if row is None: raise HTTPException(404, "记录不存在")
        return json.loads(row[0])
    finally: conn.close()


def _locked_select(conn, table, id, q):
    sql = f"SELECT payload FROM {table} WHERE id={q}"
    if not _is_sqlite(): sql += " FOR UPDATE"
    row = conn.execute(sql, (id,)).fetchone()
    return None if row is None else json.loads(row[0])


def lock_get(conn, table, id, q=None):
    """Read a row inside a transaction, taking a row lock on PostgreSQL."""
    assert table in TABLES
    if q is None: q = "%s" if not _is_sqlite() else "?"
    return _locked_select(conn, table, id, q)


def _write(conn, table, item, q, insert_only=False):
    if not isinstance(item, dict): item = asdict(item)
    clause = "DO NOTHING" if insert_only else "DO UPDATE SET payload=excluded.payload"
    conn.execute(
        f"INSERT INTO {table} (id,payload) VALUES ({q},{q}) ON CONFLICT(id) {clause}",
        (item["id"], json.dumps(item, ensure_ascii=False, allow_nan=False)),
    )
    return item


def locked_put(conn, table, item, q=None, insert_only=False):
    assert table in TABLES
    if q is None: q = "%s" if not _is_sqlite() else "?"
    return _write(conn, table, item, q, insert_only)


def put(table, item, insert_only=False):
    assert table in TABLES
    if not isinstance(item, dict): item = asdict(item)
    conn,q = connect()
    try:
        with _LOCK:
            item = _write(conn, table, item, q, insert_only)
            conn.commit()
        return item
    finally: conn.close()
