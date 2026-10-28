import threading

import pytest

import crypto
import database as db
from conftest import PASSWORD


def test_schema_wal_and_version(env):
    c = env.conn
    assert c.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert c.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert c.execute("SELECT version FROM schema_version").fetchone()[0] == db.SCHEMA_VERSION
    db.init_db(env.db_path)  # idempotent
    assert c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0] == 1


def test_invalid_transition_rejected(env):
    job = env.create_job()
    with pytest.raises(db.InvalidTransition):
        db.set_job_status(env.conn, job, "completed")  # queued -> completed is not allowed
    db.set_job_status(env.conn, job, "running")
    db.set_job_status(env.conn, job, "completed")
    with pytest.raises(db.InvalidTransition):
        db.set_job_status(env.conn, job, "running")


def test_claim_is_atomic(env):
    env.create_job()
    results = []

    def claim():
        conn = db.connect(env.db_path)
        results.append(db.claim_next_job(conn))

    threads = [threading.Thread(target=claim) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(r is not None for r in results) == 1


def test_wal_concurrent_read_during_write(env):
    job = env.create_job()
    reader = db.connect(env.db_path)
    env.conn.execute("BEGIN IMMEDIATE")
    env.conn.execute("UPDATE migration_jobs SET name='x' WHERE id=?", (job,))
    # reader is not blocked by the open write transaction and sees the committed state
    assert reader.execute("SELECT name FROM migration_jobs WHERE id=?", (job,)).fetchone()[0] == "Test job"
    env.conn.execute("COMMIT")
    assert reader.execute("SELECT name FROM migration_jobs WHERE id=?", (job,)).fetchone()[0] == "x"


def test_pause_resume_cancel_flow(env):
    job = env.create_job()
    db.request_pause(env.conn, job)                       # queued -> paused directly
    assert db.get_job(env.conn, job)["status"] == "paused"
    db.request_resume(env.conn, job)
    assert db.get_job(env.conn, job)["status"] == "queued"
    db.claim_next_job(env.conn)
    db.request_pause(env.conn, job)                       # running -> pausing (worker finishes it)
    j = db.get_job(env.conn, job)
    assert j["status"] == "pausing" and j["pause_requested"] == 1
    db.request_cancel(env.conn, job)
    assert db.get_job(env.conn, job)["status"] == "cancelling"


def test_recover_after_crash_requeues(env):
    job = env.create_job()
    db.claim_next_job(env.conn)
    env.conn.execute("UPDATE migration_accounts SET status='running' WHERE job_id=?", (job,))
    assert db.recover_after_crash(env.conn) == [job]
    assert db.get_job(env.conn, job)["status"] == "queued"
    assert env.account(job)["status"] == "queued"


def test_passwords_encrypted_at_rest(env):
    env.create_job()
    env.conn.execute("PRAGMA wal_checkpoint(FULL)")
    blob = open(env.db_path, "rb").read()
    assert PASSWORD.encode() not in blob
    stored = env.conn.execute("SELECT encrypted_source_password FROM migration_accounts").fetchone()[0]
    assert crypto.decrypt(stored) == PASSWORD


def test_purge_credentials(env):
    job = env.create_job()
    assert db.purge_credentials(env.conn, job) == 1
    assert env.account(job)["encrypted_source_password"] is None
    with pytest.raises(RuntimeError):
        db.clone_job(env.conn, job, dry_run=False)


def test_report_columns_have_no_secrets(env):
    job = env.create_job()
    row = db.accounts_for_report(env.conn, job)[0]
    assert not any("password" in k for k in row.keys())
