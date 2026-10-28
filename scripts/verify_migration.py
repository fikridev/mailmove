#!/usr/bin/env python3
"""Independent, read-only comparison of source and destination for a job.

For every folder the job mapped, both sides are opened with EXAMINE and each
source message (Message-ID + size + INTERNALDATE) is matched against an
unclaimed destination message. Nothing is written to either server.

    python scripts/verify_migration.py --job 3 [--csv report.csv]

"missing" = source messages with no exact match on the destination. Some
servers rewrite messages on upload, so a small "missing" count can be a size
change rather than lost mail; compare with the dashboard's needs_review list.
"""
import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import crypto  # noqa: E402
import database as db  # noqa: E402
import mailmove as mm  # noqa: E402

FIELDS = ["account", "source_folder", "destination_folder", "source_messages", "source_bytes",
          "destination_messages", "matched", "missing", "db_verified", "result"]


def verify_job(conn, job_id, mailbox_factory=mm.Mailbox):
    job = db.get_job(conn, job_id)
    out = []
    for acc in conn.execute("SELECT * FROM migration_accounts WHERE job_id=? ORDER BY position", (job_id,)):
        if acc["encrypted_source_password"] is None:
            out.append({"account": acc["source_user"], "result": "credentials purged"})
            continue
        try:
            src = mailbox_factory(job["source_host"], job["source_port"], acc["source_user"],
                                  crypto.decrypt(acc["encrypted_source_password"]))
            dst = mailbox_factory(job["destination_host"], job["destination_port"], acc["destination_user"],
                                  crypto.decrypt(acc["encrypted_destination_password"]))
        except Exception as e:
            out.append({"account": acc["source_user"], "result": f"cannot connect: {mm.classify(e)[0]}"})
            continue
        try:
            for f in conn.execute("SELECT * FROM migration_folders WHERE account_id=? ORDER BY source_folder_name",
                                  (acc["id"],)):
                row = {"account": acc["source_user"], "source_folder": f["source_folder_name"],
                       "destination_folder": f["destination_folder_name"]}
                row["db_verified"] = conn.execute("SELECT COUNT(*) FROM migration_messages WHERE folder_id=? AND"
                                                  " status='verified'", (f["id"],)).fetchone()[0]
                try:
                    info = src.examine(f["source_folder_name"])
                    metas = list(src.fetch_meta(src.uid_search_all())) if info["exists"] else []
                except Exception as e:
                    row["result"] = f"source folder unavailable: {mm.sanitize(e)}"
                    out.append(row)
                    continue
                try:
                    _, index = mm.load_dest_index(dst, f["destination_folder_name"])
                except Exception:
                    index = mm.DestIndex()
                matched = sum(1 for m in metas if index.take_exact(m["mid"], m["size"], m["epoch"]))
                row.update(source_messages=len(metas), source_bytes=sum(m["size"] for m in metas),
                           destination_messages=len(index), matched=matched, missing=len(metas) - matched)
                row["result"] = "OK" if row["missing"] == 0 else "MISSING"
                out.append(row)
        finally:
            src.close()
            dst.close()
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--job", type=int, required=True)
    ap.add_argument("--csv", help="write the report to this CSV file")
    args = ap.parse_args()
    conn = db.init_db()
    rows = verify_job(conn, args.job)
    for r in rows:
        print(f"{r.get('account', ''):32} {r.get('source_folder', ''):28} src={r.get('source_messages', '-'):>6} "
              f"dst={r.get('destination_messages', '-'):>6} missing={r.get('missing', '-'):>5}  {r['result']}")
    missing = sum(r.get("missing") or 0 for r in rows)
    bad = [r for r in rows if r["result"] != "OK"]
    print(f"\n{len(rows)} folder(s) checked, {missing} source message(s) without an exact destination match, "
          f"{len(bad)} folder(s) need attention.")
    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"Report written to {args.csv}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
