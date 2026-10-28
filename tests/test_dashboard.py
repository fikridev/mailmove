"""Dashboard smoke tests (Streamlit AppTest): job creation stores only encrypted
passwords, and job pages render for every state without exceptions."""
import os

import pytest
from streamlit.testing.v1 import AppTest

import database as db
from conftest import PASSWORD
from fakeimap import make_msg

APP = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app.py")


@pytest.fixture
def app_env(env, monkeypatch):
    monkeypatch.setenv("MAILMOVE_DB", env.db_path)
    return env


def test_create_dry_run_job_from_form(app_env):
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception
    at.text_input(key="src_host").input("old.example.test")
    at.text_input(key="dst_host").input("new.example.test")
    form = [t for t in at.text_input if t.label == "Source email"][0]
    form.input("a@test")
    [t for t in at.text_input if t.label == "Source password"][0].input(PASSWORD)
    [b for b in at.button if b.label == "Add account"][0].click().run()
    assert not at.exception
    [b for b in at.button if b.label == "🔍 Start dry run"][0].click().run()
    assert not at.exception
    job = app_env.conn.execute("SELECT * FROM migration_jobs").fetchone()
    assert job and job["dry_run"] == 1 and job["source_port"] == 993
    acc = app_env.conn.execute("SELECT * FROM migration_accounts").fetchone()
    assert acc["encrypted_source_password"] and PASSWORD not in acc["encrypted_source_password"]
    assert at.session_state["pending_accounts"] == []        # plaintext dropped from the session


@pytest.mark.parametrize("dry", [True, False])
def test_job_view_renders_after_run(app_env, dry):
    env = app_env
    for i in range(3):
        env.src.add("a@test", "INBOX", make_msg(i))
    env.src.add("a@test", "INBOX", make_msg("big", size=30_000))
    env.dst.max_size = 20_000
    job = env.create_job(dry_run=dry, threshold=10_000)
    env.run(job)
    at = AppTest.from_file(APP, default_timeout=30)
    at.session_state["job_id"] = job
    at.run()
    assert not at.exception, at.exception
    labels = [m.label for m in at.metric]
    assert ("Emails to copy" in labels) if dry else ("Verified" in labels and "Needs review" in labels)
    text = " ".join(str(e.value) for e in at.get("markdown")) + " ".join(str(x.value) for x in at.dataframe)
    assert PASSWORD not in text


def test_job_view_while_retrying(app_env):
    env = app_env
    job = env.create_job()
    db.claim_next_job(env.conn)
    env.conn.execute("UPDATE migration_accounts SET status='retrying', last_error='connection_refused: x'")
    db.set_job_status(env.conn, job, "retrying")
    at = AppTest.from_file(APP, default_timeout=30)
    at.session_state["job_id"] = job
    at.run()
    assert not at.exception
    assert any("worker is not running" in w.value for w in at.warning)
