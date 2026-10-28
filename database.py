"""
SQLite persistence shared by the dashboard (app.py) and the worker (worker.py).

Holds job metadata and per-message checkpoints only, never email content.
Every connection runs in WAL mode with foreign keys and a busy timeout, and
transactions stay short: no write transaction is held during network calls.
"""
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import config

SCHEMA_VERSION = 1

# ------------------------------------------------------------------ states

JOB_STATES = {"queued", "running", "retrying", "pausing", "paused", "cancelling",
              "cancelled", "completed", "completed_with_errors", "failed"}
JOB_TERMINAL = {"cancelled", "completed", "completed_with_errors", "failed"}
JOB_ACTIVE = {"running", "retrying", "pausing", "cancelling"}

# The one place job transitions are defined. "running -> queued" is crash recovery.
JOB_TRANSITIONS = {
    "queued": {"running", "paused", "cancelled"},
    "running": {"retrying", "pausing", "cancelling", "completed", "completed_with_errors", "failed",
                "paused", "cancelled", "queued"},
    "retrying": {"running", "pausing", "cancelling", "failed", "paused", "cancelled", "queued"},
    "pausing": {"paused", "cancelling", "cancelled", "queued"},
    "paused": {"queued", "cancelled"},
    "cancelling": {"cancelled"},
    "cancelled": {"queued"},
    "completed": {"queued"},
    "completed_with_errors": {"queued"},
    "failed": {"queued"},
}

ACCOUNT_STATES = {"queued", "running", "retrying", "completed", "completed_with_errors", "failed"}

# needs_review: an interrupted upload (or a partial match on the destination)
# that cannot be proven either way. Never re-uploaded without an operator decision.
MESSAGE_STATES = {"pending", "in_progress", "copied_unverified", "verified", "retrying",
                  "failed", "skipped", "needs_review"}


class InvalidTransition(RuntimeError):
    pass


# ------------------------------------------------------------------ schema

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS migration_jobs (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    source_host TEXT NOT NULL,
    source_port INTEGER NOT NULL,
    source_security TEXT NOT NULL DEFAULT 'ssl',
    destination_host TEXT NOT NULL,
    destination_port INTEGER NOT NULL,
    destination_security TEXT NOT NULL DEFAULT 'ssl',
    status TEXT NOT NULL DEFAULT 'queued',
    dry_run INTEGER NOT NULL DEFAULT 0,
    max_parallel_accounts INTEGER NOT NULL DEFAULT 2,
    skip_folders_json TEXT NOT NULL DEFAULT '[]',
    large_message_threshold_bytes INTEGER NOT NULL DEFAULT 26214400,
    pass_number INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    updated_at REAL NOT NULL,
    last_heartbeat_at REAL,
    pause_requested INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS migration_accounts (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES migration_jobs(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    source_user TEXT NOT NULL,
    destination_user TEXT NOT NULL,
    encrypted_source_password TEXT,
    encrypted_destination_password TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    current_folder TEXT,
    messages_scanned INTEGER NOT NULL DEFAULT 0,
    messages_verified INTEGER NOT NULL DEFAULT 0,
    messages_failed INTEGER NOT NULL DEFAULT 0,
    bytes_verified INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    reconnect_count INTEGER NOT NULL DEFAULT 0,
    attachments INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_activity_at REAL,
    next_retry_at REAL,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    updated_at REAL NOT NULL,
    UNIQUE (job_id, source_user)
);

CREATE TABLE IF NOT EXISTS migration_folders (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES migration_accounts(id) ON DELETE CASCADE,
    source_folder_name TEXT NOT NULL,
    destination_folder_name TEXT NOT NULL,
    source_uidvalidity INTEGER,
    source_uidnext_observed INTEGER,
    destination_uidvalidity INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    source_messages INTEGER NOT NULL DEFAULT 0,
    source_bytes INTEGER NOT NULL DEFAULT 0,
    destination_messages INTEGER,
    attachments INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_scan_at REAL,
    last_verified_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (account_id, source_folder_name),
    UNIQUE (account_id, destination_folder_name)
);

CREATE TABLE IF NOT EXISTS migration_messages (
    id INTEGER PRIMARY KEY,
    folder_id INTEGER NOT NULL REFERENCES migration_folders(id) ON DELETE CASCADE,
    source_uidvalidity INTEGER NOT NULL,
    source_uid INTEGER NOT NULL,
    source_rfc822_size INTEGER NOT NULL DEFAULT 0,
    source_internal_date TEXT,
    source_internal_epoch INTEGER,
    source_message_id_header TEXT,
    source_flags TEXT,
    source_content_fingerprint TEXT,
    destination_uidvalidity INTEGER,
    destination_uid INTEGER,
    append_attempt_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    skip_reason TEXT,
    error_category TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    copied_at REAL,
    verified_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (folder_id, source_uidvalidity, source_uid)
);
CREATE INDEX IF NOT EXISTS ix_messages_status_folder ON migration_messages(status, folder_id);
CREATE INDEX IF NOT EXISTS ix_messages_folder_status ON migration_messages(folder_id, status);
CREATE INDEX IF NOT EXISTS ix_messages_copied_at ON migration_messages(copied_at);

CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY,
    job_id INTEGER REFERENCES migration_jobs(id) ON DELETE CASCADE,
    account_id INTEGER,
    folder_id INTEGER,
    message_id INTEGER,
    created_at REAL NOT NULL,
    level TEXT NOT NULL,
    category TEXT NOT NULL,
    retryable INTEGER,
    attempt INTEGER,
    details TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_job ON job_events(job_id, id);

CREATE TABLE IF NOT EXISTS worker_heartbeat (
    worker_id TEXT PRIMARY KEY,
    pid INTEGER,
    hostname TEXT,
    started_at REAL NOT NULL,
    last_heartbeat_at REAL NOT NULL,
    current_job_id INTEGER
);
"""


def now():
    return time.time()


def connect(path=None):
    path = path or config.db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(path=None):
    conn = connect(path)
    conn.executescript("BEGIN IMMEDIATE;" + SCHEMA + "COMMIT;")  # idempotent CREATE IF NOT EXISTS
    with tx(conn):
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        if row is None:
            conn.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION,))
        elif row["version"] > SCHEMA_VERSION:
            raise RuntimeError(f"Database schema v{row['version']} is newer than this code (v{SCHEMA_VERSION})")
    try:
        os.chmod(path or config.db_path(), 0o600)
    except OSError:
        pass
    return conn


@contextmanager
def tx(conn):
    """Short write transaction. BEGIN IMMEDIATE takes the write lock up front."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


# ------------------------------------------------------------------ events

def event(conn, job_id, level, category, details="", account_id=None, folder_id=None,
          message_id=None, retryable=None, attempt=None):
    conn.execute(
        "INSERT INTO job_events (job_id, account_id, folder_id, message_id, created_at, level, category,"
        " retryable, attempt, details) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (job_id, account_id, folder_id, message_id, now(), level, category,
         None if retryable is None else int(bool(retryable)), attempt, (details or "")[:500]),
    )


# ------------------------------------------------------------------ jobs

def create_job(conn, name, cfg, accounts):
    """cfg: source/destination host, port, security, dry_run, max_parallel_accounts,
    skip_folders, large_message_threshold_bytes. accounts: dicts with source_user,
    destination_user, encrypted_source_password, encrypted_destination_password.
    The job is created 'queued': the operator's create button is the explicit start."""
    t = now()
    with tx(conn):
        cur = conn.execute(
            "INSERT INTO migration_jobs (name, source_host, source_port, source_security, destination_host,"
            " destination_port, destination_security, status, dry_run, max_parallel_accounts,"
            " skip_folders_json, large_message_threshold_bytes, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,'queued',?,?,?,?,?,?)",
            (name, cfg["source_host"], cfg["source_port"], cfg.get("source_security", "ssl"),
             cfg["destination_host"], cfg["destination_port"], cfg.get("destination_security", "ssl"),
             int(cfg.get("dry_run", False)), int(cfg.get("max_parallel_accounts", 2)),
             json.dumps(sorted(cfg.get("skip_folders", []))),
             int(cfg.get("large_message_threshold_bytes", 25 * 1024 * 1024)), t, t),
        )
        job_id = cur.lastrowid
        for pos, a in enumerate(accounts):
            conn.execute(
                "INSERT INTO migration_accounts (job_id, position, source_user, destination_user,"
                " encrypted_source_password, encrypted_destination_password, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (job_id, pos, a["source_user"], a["destination_user"], a["encrypted_source_password"],
                 a["encrypted_destination_password"], t, t),
            )
        event(conn, job_id, "info", "job_created", f"{len(accounts)} account(s), dry_run={bool(cfg.get('dry_run'))}")
    return job_id


def get_job(conn, job_id):
    return conn.execute("SELECT * FROM migration_jobs WHERE id=?", (job_id,)).fetchone()


def _set_job_status(conn, job_id, new, **fields):
    """Must be called inside tx(). Enforces JOB_TRANSITIONS."""
    row = conn.execute("SELECT status FROM migration_jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        raise InvalidTransition(f"job {job_id} does not exist")
    old = row["status"]
    if new != old and new not in JOB_TRANSITIONS[old]:
        raise InvalidTransition(f"job {job_id}: {old} -> {new} is not allowed")
    sets = {"status": new, "updated_at": now(), **fields}
    conn.execute(f"UPDATE migration_jobs SET {', '.join(k + '=?' for k in sets)} WHERE id=?",
                 (*sets.values(), job_id))
    if new != old:
        event(conn, job_id, "info", "job_status", f"{old} -> {new}")
    return old


def set_job_status(conn, job_id, new, **fields):
    with tx(conn):
        return _set_job_status(conn, job_id, new, **fields)


def claim_next_job(conn):
    """Atomically move the oldest queued job to running and return it."""
    with tx(conn):
        row = conn.execute(
            "SELECT id FROM migration_jobs WHERE status='queued' ORDER BY created_at, id LIMIT 1").fetchone()
        if row is None:
            return None
        t = now()
        _set_job_status(conn, row["id"], "running", started_at=t, last_heartbeat_at=t, finished_at=None)
    return get_job(conn, row["id"])


def request_pause(conn, job_id):
    with tx(conn):
        status = conn.execute("SELECT status FROM migration_jobs WHERE id=?", (job_id,)).fetchone()["status"]
        if status == "queued":
            _set_job_status(conn, job_id, "paused")
        elif status in ("running", "retrying"):
            _set_job_status(conn, job_id, "pausing", pause_requested=1)
        else:
            raise InvalidTransition(f"cannot pause a {status} job")


def request_resume(conn, job_id):
    with tx(conn):
        _set_job_status(conn, job_id, "queued", pause_requested=0)


def request_cancel(conn, job_id):
    with tx(conn):
        status = conn.execute("SELECT status FROM migration_jobs WHERE id=?", (job_id,)).fetchone()["status"]
        if status in ("queued", "paused"):
            _set_job_status(conn, job_id, "cancelled", finished_at=now())
        elif status in ("running", "retrying", "pausing"):
            _set_job_status(conn, job_id, "cancelling", cancel_requested=1)
        else:
            raise InvalidTransition(f"cannot cancel a {status} job")


def request_sync_again(conn, job_id):
    """Second pass (e.g. after MX cutover): requeue every account and retry failed
    messages. Verified messages stay verified; folders are rescanned for new mail.
    Destination mail is never deleted."""
    t = now()
    with tx(conn):
        job = conn.execute("SELECT status, pass_number FROM migration_jobs WHERE id=?", (job_id,)).fetchone()
        if job["status"] not in JOB_TERMINAL:
            raise InvalidTransition(f"cannot sync a {job['status']} job; wait for it to finish or pause")
        _set_job_status(conn, job_id, "queued", pause_requested=0, cancel_requested=0, finished_at=None,
                        last_error=None, pass_number=job["pass_number"] + 1)
        conn.execute("UPDATE migration_accounts SET status='queued', last_error=NULL, finished_at=NULL,"
                     " retry_count=0, next_retry_at=NULL, updated_at=? WHERE job_id=?", (t, job_id))
        conn.execute(
            "UPDATE migration_messages SET status='pending', attempts=0, last_error=NULL, error_category=NULL,"
            " updated_at=? WHERE status='failed' AND folder_id IN (SELECT f.id FROM migration_folders f"
            " JOIN migration_accounts a ON a.id=f.account_id WHERE a.job_id=?)", (t, job_id))


def resolve_review(conn, job_id, message_ids, action):
    """Operator decision for needs_review messages.
    action 'copy': re-upload (may create a duplicate). 'accept': treat as present, never copy."""
    if action not in ("copy", "accept"):
        raise ValueError(action)
    t = now()
    with tx(conn):
        for mid in message_ids:
            row = conn.execute(
                "SELECT m.id FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
                " JOIN migration_accounts a ON a.id=f.account_id"
                " WHERE m.id=? AND a.job_id=? AND m.status='needs_review'", (mid, job_id)).fetchone()
            if row is None:
                continue
            if action == "copy":
                conn.execute("UPDATE migration_messages SET status='pending', append_attempt_id=NULL,"
                             " last_error='operator chose re-copy', updated_at=? WHERE id=?", (t, mid))
            else:
                conn.execute("UPDATE migration_messages SET status='skipped', skip_reason='accepted_by_operator',"
                             " updated_at=? WHERE id=?", (t, mid))
            event(conn, job_id, "warning", "review_resolved", f"message {mid}: {action}", message_id=mid)


def clone_job(conn, job_id, dry_run):
    """New job with the same settings and (still encrypted) credentials, e.g.
    'start the real migration from this dry run'."""
    job = get_job(conn, job_id)
    accounts = conn.execute("SELECT * FROM migration_accounts WHERE job_id=? ORDER BY position", (job_id,)).fetchall()
    if any(a["encrypted_source_password"] is None for a in accounts):
        raise RuntimeError("Credentials of this job were purged; create a new job instead.")
    cfg = dict(source_host=job["source_host"], source_port=job["source_port"],
               source_security=job["source_security"], destination_host=job["destination_host"],
               destination_port=job["destination_port"], destination_security=job["destination_security"],
               dry_run=dry_run, max_parallel_accounts=job["max_parallel_accounts"],
               skip_folders=json.loads(job["skip_folders_json"]),
               large_message_threshold_bytes=job["large_message_threshold_bytes"])
    name = job["name"].replace("Dry run", "Migration") if not dry_run else job["name"]
    return create_job(conn, name, cfg, [dict(a) for a in accounts])


def purge_credentials(conn, job_id=None):
    t = now()
    with tx(conn):
        if job_id is None:
            n = conn.execute("UPDATE migration_accounts SET encrypted_source_password=NULL,"
                             " encrypted_destination_password=NULL, updated_at=?", (t,)).rowcount
        else:
            n = conn.execute("UPDATE migration_accounts SET encrypted_source_password=NULL,"
                             " encrypted_destination_password=NULL, updated_at=? WHERE job_id=?",
                             (t, job_id)).rowcount
        event(conn, job_id, "warning", "credentials_purged", f"{n} account(s)")
    return n


# ------------------------------------------------------------------ worker heartbeat

def new_worker_id():
    return uuid.uuid4().hex[:12]


def other_live_worker(conn, worker_id):
    cutoff = now() - config.HEARTBEAT_STALE_SECONDS
    return conn.execute("SELECT * FROM worker_heartbeat WHERE worker_id != ? AND last_heartbeat_at > ?",
                        (worker_id, cutoff)).fetchone()


def heartbeat(conn, worker_id, pid, hostname, job_id=None, started_at=None):
    t = now()
    with tx(conn):
        conn.execute(
            "INSERT INTO worker_heartbeat (worker_id, pid, hostname, started_at, last_heartbeat_at, current_job_id)"
            " VALUES (?,?,?,?,?,?) ON CONFLICT(worker_id) DO UPDATE SET last_heartbeat_at=excluded.last_heartbeat_at,"
            " current_job_id=excluded.current_job_id", (worker_id, pid, hostname, started_at or t, t, job_id))
        if job_id is not None:
            conn.execute("UPDATE migration_jobs SET last_heartbeat_at=? WHERE id=?", (t, job_id))


def latest_heartbeat(conn):
    return conn.execute("SELECT * FROM worker_heartbeat ORDER BY last_heartbeat_at DESC LIMIT 1").fetchone()


def recover_after_crash(conn):
    """Run once at worker start (only one worker runs at a time). Anything left
    mid-flight by a dead worker is requeued; in_progress messages are kept as-is so
    the worker reconciles them against the destination before any re-upload."""
    t = now()
    recovered = []
    with tx(conn):
        for job in conn.execute("SELECT id, status FROM migration_jobs WHERE status IN"
                                " ('running','retrying','pausing','cancelling')").fetchall():
            target = {"pausing": "paused", "cancelling": "cancelled"}.get(job["status"], "queued")
            extra = {"finished_at": t} if target == "cancelled" else {}
            _set_job_status(conn, job["id"], target, **extra)
            event(conn, job["id"], "warning", "recovered", f"worker restarted; job was {job['status']}")
            recovered.append(job["id"])
        conn.execute("UPDATE migration_accounts SET status='queued', current_folder=NULL, updated_at=?"
                     " WHERE status IN ('running','retrying')", (t,))
    return recovered


# ------------------------------------------------------------------ reporting (no secrets)

def job_message_stats(conn, job_id):
    rows = conn.execute(
        "SELECT a.id AS account_id, m.status, COUNT(*) AS n, COALESCE(SUM(m.source_rfc822_size),0) AS bytes"
        " FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
        " JOIN migration_accounts a ON a.id=f.account_id WHERE a.job_id=? GROUP BY a.id, m.status",
        (job_id,)).fetchall()
    out = {}
    for r in rows:
        out.setdefault(r["account_id"], {})[r["status"]] = (r["n"], r["bytes"])
    return out


def recent_throughput(conn, job_id, window=600):
    """Bytes/sec actually uploaded by this job over the last `window` seconds (measured)."""
    since = now() - window
    row = conn.execute(
        "SELECT COALESCE(SUM(m.source_rfc822_size),0) AS b, MIN(m.copied_at) AS first"
        " FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
        " JOIN migration_accounts a ON a.id=f.account_id WHERE a.job_id=? AND m.copied_at>=?",
        (job_id, since)).fetchone()
    if not row["b"] or row["first"] is None:
        return 0.0
    return row["b"] / max(now() - row["first"], 30)


SAFE_ACCOUNT_COLUMNS = ("source_user", "destination_user", "status", "current_folder", "messages_scanned",
                        "messages_verified", "messages_failed", "bytes_verified", "retry_count",
                        "reconnect_count", "attachments", "last_error", "last_activity_at")


def accounts_for_report(conn, job_id):
    cols = ", ".join(SAFE_ACCOUNT_COLUMNS)
    return conn.execute(f"SELECT id, {cols} FROM migration_accounts WHERE job_id=? ORDER BY position",
                        (job_id,)).fetchall()


def problem_messages(conn, job_id, large_threshold=None, limit=5000):
    where = "m.status IN ('failed','needs_review') OR (m.status='skipped' AND m.skip_reason='deleted_on_source')"
    params = [job_id]
    if large_threshold:
        where += " OR m.source_rfc822_size >= ?"
        params.append(large_threshold)
    return conn.execute(
        "SELECT m.id, a.source_user, f.source_folder_name, f.destination_folder_name, m.source_uid,"
        " m.source_rfc822_size, m.source_internal_date, m.source_message_id_header, m.status,"
        " m.skip_reason, m.error_category, m.attempts, m.last_error"
        " FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
        " JOIN migration_accounts a ON a.id=f.account_id"
        f" WHERE a.job_id=? AND ({where}) ORDER BY a.position, f.source_folder_name, m.source_uid LIMIT ?",
        (*params, limit)).fetchall()
