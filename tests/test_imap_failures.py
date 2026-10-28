"""Network failures, server rejections, ambiguous uploads, credentials hygiene."""
import imaplib
import logging
import ssl

import pytest

import database as db
import mailmove as mm
from conftest import PASSWORD
from fakeimap import make_msg

U = "a@test"


def seed(env, n=4):
    for i in range(n):
        env.src.add(U, "INBOX", make_msg(i))


def no_dupes(env, folder="INBOX"):
    raws = env.dst.all_raw(U, folder)
    return len(raws) == len(set(raws))


def test_source_offline_for_a_while_then_back(env):
    seed(env)
    env.src.down = 6                                     # six refused connection attempts
    job = env.create_job()
    j = env.run(job)
    assert j["status"] == "completed"
    acc = env.account(job)
    assert acc["retry_count"] >= 6 and acc["reconnect_count"] >= 6
    assert env.dst.count(U, "INBOX") == 4
    statuses = [r[0] for r in env.conn.execute("SELECT details FROM job_events WHERE category='job_status'")]
    assert any("running -> retrying" in s for s in statuses)


def test_job_never_completes_while_source_is_down(env):
    seed(env)
    env.src.down = True
    job = env.create_job()
    w = env.worker()
    w.startup()
    import threading
    t = threading.Thread(target=w.run_once)
    t.start()
    import time
    time.sleep(1.0)
    assert db.get_job(env.conn, job)["status"] == "retrying"
    assert env.account(job)["status"] == "retrying"
    db.request_pause(env.conn, job)                      # operator can still pause a retrying job
    t.join(10)
    assert db.get_job(env.conn, job)["status"] == "paused"
    env.src.down = 0
    db.request_resume(env.conn, job)
    assert env.run(job)["status"] == "completed"


@pytest.mark.parametrize("op,mode", [("append_after", "abort"), ("append_after", "timeout"),
                                      ("append_before", "abort"), ("fetch_body", "abort"),
                                      ("fetch_meta", "abort"), ("select", "abort")])
def test_disconnects_are_retried_without_duplicates(env, op, mode):
    seed(env, 5)
    (env.src if op in ("fetch_body", "fetch_meta") else env.dst).fault(op, mode=mode, count=2)
    job = env.create_job()
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 5
    assert no_dupes(env)


def test_lost_ack_without_uidplus_is_matched_by_content(make_env):
    env = make_env(dst_kwargs={"uidplus": False})
    seed(env, 3)
    env.dst.fault("append_after", count=1)
    job = env.create_job()
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 3 and no_dupes(env)
    assert len(env.messages(job, "verified")) == 3


def test_ambiguous_interrupted_upload_goes_to_review_not_duplicated(env):
    """Destination rewrites messages (size changes), so after a lost ACK the copy
    can't be matched exactly: flag for review instead of uploading again."""
    seed(env, 2)
    env.dst.alter_on_append = lambda raw: b"X-Added-By-Server: 1\r\n" + raw
    env.dst.fault("append_after", count=1)
    job = env.create_job()
    j = env.run(job)
    assert j["status"] == "completed_with_errors"
    review = env.messages(job, "needs_review")
    assert len(review) == 1 and review[0]["error_category"] == "ambiguous"
    assert env.dst.count(U, "INBOX") == 2                # no duplicate was created
    # the other (acknowledged) one is verified via APPENDUID + Message-ID despite the size change
    assert len(env.messages(job, "verified")) == 1
    # operator accepts it as present
    db.resolve_review(env.conn, job, [review[0]["id"]], "accept")
    assert env.messages(job, "needs_review") == []


def test_duplicate_message_ids_on_source_are_all_copied(env):
    for i in range(3):
        env.src.add(U, "INBOX", make_msg(i, mid="<same@id>", body=f"different body {i}".encode()))
    job = env.create_job()
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 3
    db.request_sync_again(env.conn, job)
    env.run(job)
    assert env.dst.count(U, "INBOX") == 3


def test_identical_repeated_messages_are_all_copied_once_each(env):
    raw = make_msg(1)
    for _ in range(2):
        env.src.add(U, "INBOX", raw)
    job = env.create_job()
    env.run(job)
    db.request_sync_again(env.conn, job)
    env.run(job)
    assert env.dst.count(U, "INBOX") == 2


def test_missing_message_id(env):
    for i in range(3):
        env.src.add(U, "INBOX", make_msg(i, mid=False, size=i), idate=f"0{i + 1}-Mar-2024 09:00:00 +0000")
    env.dst.fault("append_after", count=1)
    job = env.create_job()
    assert env.run(job)["status"] == "completed"
    db.request_sync_again(env.conn, job)
    env.run(job)
    assert env.dst.count(U, "INBOX") == 3 and no_dupes(env)


def test_oversized_message_reported_not_silently_skipped(env):
    seed(env, 2)
    env.src.add(U, "INBOX", make_msg("big", size=50_000))
    env.dst.max_size = 10_000
    job = env.create_job()
    j = env.run(job)
    assert j["status"] == "completed_with_errors"
    failed = env.messages(job, "failed")
    assert len(failed) == 1 and failed[0]["error_category"] == "too_large"
    assert db.problem_messages(env.conn, job)[0]["error_category"] == "too_large"
    assert env.dst.count(U, "INBOX") == 2


def test_quota_exceeded_fails_account_with_remediation(env):
    seed(env, 5)
    env.dst.quota_bytes = 300
    job = env.create_job()
    j = env.run(job)
    assert j["status"] == "failed"
    acc = env.account(job)
    assert acc["status"] == "failed" and "quota" in acc["last_error"].lower()
    assert "increase its quota" in acc["last_error"].lower()


def test_throttling_is_retried(env):
    seed(env, 3)
    env.dst.fault("append_no", mode="[UNAVAILABLE] Too many connections, try again later", count=2)
    job = env.create_job()
    assert env.run(job)["status"] == "completed"
    assert env.dst.count(U, "INBOX") == 3


def test_bad_credentials_fail_after_limited_attempts(env):
    seed(env, 1)
    env.src.users[U] = "different"
    job = env.create_job()
    j = env.run(job)
    assert j["status"] == "failed"
    acc = env.account(job)
    assert acc["status"] == "failed" and acc["last_error"].startswith("auth")
    assert acc["retry_count"] == 2                        # 3 attempts in total


def test_tls_context_verifies_certificates(monkeypatch):
    seen = {}

    class Capture(Exception):
        pass

    def fake_ssl(host, port, ssl_context=None, timeout=None):
        seen.update(host=host, port=port, ctx=ssl_context, timeout=timeout)
        raise Capture()

    monkeypatch.setattr(imaplib, "IMAP4_SSL", fake_ssl)
    with pytest.raises(Capture):
        mm.Mailbox("imap.example.test", 993, "u", "p")
    ctx = seen["ctx"]
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname is True
    assert seen["port"] == 993 and seen["timeout"] == 30


@pytest.mark.parametrize("port,security", [(143, "ssl"), (143, "starttls"), (993, "plain")])
def test_only_imaps_993_is_allowed(port, security):
    with pytest.raises(ValueError):
        mm.Mailbox("imap.example.test", port, "u", "p", security=security)


def test_certificate_errors_are_permanent_with_guidance():
    err = ssl.SSLCertVerificationError(1, "certificate verify failed: Hostname mismatch")
    assert mm.classify(err) == ("tls_certificate", False)
    assert "Connect Devices" in mm.REMEDIATION["tls_certificate"]


def test_no_plaintext_password_in_db_events_or_logs(env, caplog):
    caplog.set_level(logging.DEBUG)
    seed(env, 2)
    env.dst.fault("append_after", count=1)
    env.dst.fault("append_no", mode=f"[ALERT] echo {PASSWORD}", count=1)   # server echoes it back
    job = env.create_job()
    env.run(job)
    env.conn.execute("PRAGMA wal_checkpoint(FULL)")
    blob = open(env.db_path, "rb").read()
    assert PASSWORD.encode() not in blob
    assert PASSWORD not in caplog.text
    for r in db.problem_messages(env.conn, job):
        assert PASSWORD not in " ".join(str(v) for v in r)


def test_classify():
    assert mm.classify(ConnectionResetError())[1] is True
    assert mm.classify(imaplib.IMAP4.abort("EOF")) == ("connection", True)
    assert mm.classify(imaplib.IMAP4.error("[AUTHENTICATIONFAILED] x")) == ("auth", False)
    assert mm.classify_text("[OVERQUOTA] full") == ("quota", False)
    assert mm.classify_text("[TOOBIG] no") == ("too_large", False)
    assert mm.classify_text("[UNAVAILABLE] busy") == ("throttled", True)
