import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cryptography.fernet import Fernet  # noqa: E402

os.environ["MAILMOVE_KEY"] = Fernet.generate_key().decode()
os.environ["MAILMOVE_BACKOFF_BASE"] = "0.01"
os.environ["MAILMOVE_BACKOFF_MAX"] = "0.05"

import config  # noqa: E402

config.BACKOFF_BASE = 0.01
config.BACKOFF_MAX = 0.05

import crypto  # noqa: E402
import database as db  # noqa: E402
import mailmove as mm  # noqa: E402
import worker as wk  # noqa: E402
from fakeimap import FakeConn, FakeServer  # noqa: E402

SRC_HOST, DST_HOST = "old.example.test", "new.example.test"
PASSWORD = "S3cret-Pa55word!"


class Env:
    """A source + destination fake server pair, a temp DB, and helpers to run jobs."""

    def __init__(self, tmp_path, users=("a@test",), dst_kwargs=None, src_kwargs=None):
        self.users = list(users)
        self.src = FakeServer({u: PASSWORD for u in users}, **(src_kwargs or {}))
        self.dst = FakeServer({u: PASSWORD for u in users}, **({"delim": ".", "prefix": "INBOX."} | (dst_kwargs or {})))
        self.db_path = str(tmp_path / "mm.db")
        servers = {SRC_HOST: self.src, DST_HOST: self.dst}

        class FakeMailbox(mm.Mailbox):
            def _open(self):
                return FakeConn(servers[self.host], self.host)

        self.Mailbox = FakeMailbox
        self.conn = db.init_db(self.db_path)

    def worker(self):
        return wk.Worker(self.db_path, mailbox_factory=self.Mailbox)

    def create_job(self, dry_run=False, skip=(), parallel=2, threshold=25 * 1024 * 1024):
        cfg = dict(source_host=SRC_HOST, source_port=993, destination_host=DST_HOST, destination_port=993,
                   dry_run=dry_run, max_parallel_accounts=parallel, skip_folders=list(skip),
                   large_message_threshold_bytes=threshold)
        accounts = [dict(source_user=u, destination_user=u, encrypted_source_password=crypto.encrypt(PASSWORD),
                         encrypted_destination_password=crypto.encrypt(PASSWORD)) for u in self.users]
        return db.create_job(self.conn, "Test job", cfg, accounts)

    def run(self, job_id=None, worker=None):
        w = worker or self.worker()
        w.startup()
        while w.run_once():
            pass
        return db.get_job(self.conn, job_id) if job_id else None

    def messages(self, job_id, status=None):
        q = ("SELECT m.* FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
             " JOIN migration_accounts a ON a.id=f.account_id WHERE a.job_id=?")
        params = [job_id]
        if status:
            q += " AND m.status=?"
            params.append(status)
        return self.conn.execute(q, params).fetchall()

    def account(self, job_id, user=None):
        return self.conn.execute("SELECT * FROM migration_accounts WHERE job_id=? AND source_user=?",
                                 (job_id, user or self.users[0])).fetchone()


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


@pytest.fixture
def make_env(tmp_path):
    return lambda **kw: Env(tmp_path, **kw)
