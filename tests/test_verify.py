"""scripts/verify_migration.py: independent read-only comparison."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from fakeimap import SOURCE_MUTATIONS, make_msg  # noqa: E402
from verify_migration import verify_job  # noqa: E402

U = "a@test"


def test_verify_reports_ok_then_missing(env):
    for i in range(4):
        env.src.add(U, "INBOX", make_msg(i))
    job = env.create_job()
    env.run(job)
    rows = {r["source_folder"]: r for r in verify_job(env.conn, job, env.Mailbox)}
    assert rows["INBOX"]["result"] == "OK" and rows["INBOX"]["matched"] == 4
    env.dst.folder(U, "INBOX").msgs.pop(1)              # someone deleted one on the destination
    rows = {r["source_folder"]: r for r in verify_job(env.conn, job, env.Mailbox)}
    assert rows["INBOX"]["result"] == "MISSING" and rows["INBOX"]["missing"] == 1
    assert not ({c for _, c, _ in env.src.commands} & SOURCE_MUTATIONS)
