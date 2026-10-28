"""End-to-end worker runs against fake servers: copy, verify, resume, no duplicates."""
import pytest

import database as db
from fakeimap import SOURCE_MUTATIONS, SimulatedCrash, make_msg

U = "a@test"


def seed(env, n=5, folder="INBOX"):
    for i in range(n):
        env.src.add(U, folder, make_msg(f"{folder}-{i}"), flags=["\\Seen"] if i % 2 else [],
                    idate=f"0{i % 9 + 1}-Feb-2024 10:00:00 +0800")


def dest_raw(env, folder="INBOX"):
    return env.dst.all_raw(U, folder)


def assert_no_duplicates(env, folder="INBOX"):
    raws = dest_raw(env, folder)
    assert len(raws) == len(set(raws)), "duplicate messages on destination"


def test_full_copy_verifies_and_preserves_flags_dates(env):
    seed(env, 6)
    seed(env, 3, "INBOX.Sent")
    job = env.create_job()
    j = env.run(job)
    assert j["status"] == "completed"
    assert sorted(dest_raw(env)) == sorted(env.src.all_raw(U, "INBOX"))
    assert sorted(dest_raw(env, "INBOX.Sent")) == sorted(env.src.all_raw(U, "INBOX.Sent"))
    src_msgs = {m.raw: m for m in env.src.folder(U, "INBOX").msgs.values()}
    for m in env.dst.folder(U, "INBOX").msgs.values():
        assert m.idate == src_msgs[m.raw].idate
        assert m.flags == src_msgs[m.raw].flags
    assert len(env.messages(job, "verified")) == 9
    assert env.account(job)["messages_verified"] == 9


def test_source_is_never_modified(env):
    seed(env, 4)
    job = env.create_job()
    env.run(job)
    src_cmds = {c for _, c, _ in env.src.commands}
    assert not (src_cmds & SOURCE_MUTATIONS), src_cmds
    assert "EXAMINE" in src_cmds


def test_rerun_does_not_duplicate_and_copies_new_mail(env):
    seed(env, 4)
    job = env.create_job()
    env.run(job)
    env.src.add(U, "INBOX", make_msg("late-arrival"))     # mail delivered to old host after first pass
    db.request_sync_again(env.conn, job)
    j = env.run(job)
    assert j["status"] == "completed" and j["pass_number"] == 2
    assert env.dst.count(U, "INBOX") == 5
    assert_no_duplicates(env)


def test_preexisting_copies_from_v1_are_not_duplicated(env):
    seed(env, 4)
    for m in list(env.src.folder(U, "INBOX").msgs.values())[:2]:  # v1 already copied two
        env.dst.add(U, "INBOX", m.raw, m.flags, m.idate)
    job = env.create_job()
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 4
    assert_no_duplicates(env)


@pytest.mark.parametrize("op", ["append_after", "append_before", "fetch_body"])
def test_worker_crash_then_restart_resumes_without_duplicates(env, op):
    """Crash (like SIGKILL) after the APPEND landed but before the checkpoint,
    before the APPEND, and while reading from the source."""
    seed(env, 6)
    server = env.src if op == "fetch_body" else env.dst
    server.fault(op, mode="crash", match=(lambda c: c.get("uid") == 3) if op == "fetch_body" else None)
    if op != "fetch_body":
        calls = {"n": 0}

        def third(ctx):
            calls["n"] += 1
            return calls["n"] == 3
        server.faults[-1]["match"] = third
    job = env.create_job()
    w1 = env.worker()
    w1.startup()
    with pytest.raises(SimulatedCrash):
        w1.run_once()
    j = db.get_job(env.conn, job)
    assert j["status"] in ("running", "retrying")         # left behind by the dead worker
    if op == "append_after":
        assert len(env.messages(job, "in_progress")) == 1  # uncertain upload recorded durably
    j = env.run(job)                                      # a fresh worker process
    assert j["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 6
    assert_no_duplicates(env)
    if op == "append_after":
        cats = [r[0] for r in env.conn.execute("SELECT category FROM job_events")]
        assert "uncertain_upload_found" in cats and "recovered" in cats


def test_pause_and_resume(env):
    seed(env, 5)
    job = env.create_job()
    calls = {"n": 0}

    def pause_on_third(ctx):
        calls["n"] += 1
        if calls["n"] == 3:
            db.request_pause(db.connect(env.db_path), job)
        return False
    env.dst.fault("append_before", match=pause_on_third, count=99)
    env.run(job)
    j = db.get_job(env.conn, job)
    assert j["status"] == "paused"
    assert 0 < env.dst.count(U, "INBOX") < 5
    db.request_resume(env.conn, job)
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 5
    assert_no_duplicates(env)


def test_cancel(env):
    seed(env, 5)
    job = env.create_job()
    env.dst.fault("append_before", match=lambda c: db.request_cancel(db.connect(env.db_path), job) and False)
    env.run(job)
    assert db.get_job(env.conn, job)["status"] == "cancelled"


def test_uidvalidity_change_between_passes(env):
    seed(env, 4)
    job = env.create_job()
    env.run(job)
    env.src.reset_uidvalidity(U, "INBOX")               # server rebuilt its index: all UIDs renumbered
    env.src.add(U, "INBOX", make_msg("after-rebuild"))
    db.request_sync_again(env.conn, job)
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 5
    assert_no_duplicates(env)
    cats = [r[0] for r in env.conn.execute("SELECT category FROM job_events")]
    assert "uidvalidity_changed" in cats


def test_message_deleted_on_source_is_reported(env):
    seed(env, 4)
    job = env.create_job()
    victim = 2

    def delete_it(ctx):
        env.src.folder(U, "INBOX").msgs.pop(victim, None)
        return False
    env.src.fault("fetch_body", match=delete_it, count=1)
    j = env.run(job)
    assert j["status"] == "completed_with_errors"
    skipped = env.messages(job, "skipped")
    assert len(skipped) == 1 and skipped[0]["skip_reason"] == "deleted_on_source"
    assert env.dst.count(U, "INBOX") == 3


def test_dry_run_counts_without_writing(env):
    seed(env, 3)
    env.src.add(U, "INBOX.Junk", make_msg("spam1", size=1000))
    m = next(iter(env.src.folder(U, "INBOX").msgs.values()))
    env.dst.add(U, "INBOX", m.raw, m.flags, m.idate)       # one already present
    job = env.create_job(dry_run=True)
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 1                  # nothing written
    assert "INBOX.Junk" in env.dst.mailboxes[U] and env.dst.count(U, "INBOX.Junk") == 0
    assert not [c for u, c, _ in env.dst.commands if c in ("APPEND", "CREATE")]
    pending = env.messages(job, "pending")
    assert len(pending) == 3                               # 2 inbox + 1 junk would be copied
    assert len([r for r in env.messages(job, "skipped") if r["skip_reason"] == "already_on_destination"]) == 1


def test_clone_dry_run_into_migration(env):
    seed(env, 2)
    dry = env.create_job(dry_run=True)
    env.run(dry)
    real = db.clone_job(env.conn, dry, dry_run=False)
    assert env.run(real)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 2


def test_graceful_shutdown_requeues_and_resumes(env):
    """systemctl stop / restart (SIGTERM): finish the current message, requeue the job."""
    seed(env, 5)
    job = env.create_job()
    w = env.worker()
    calls = {"n": 0}

    def stop_on_second(ctx):
        calls["n"] += 1
        if calls["n"] == 2:
            w.shutdown.set()
        return False
    env.dst.fault("append_before", match=stop_on_second, count=99)
    w.startup()
    w.run_once()
    assert db.get_job(env.conn, job)["status"] == "queued"
    assert env.messages(job, "in_progress") == []
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 5
    assert_no_duplicates(env)
