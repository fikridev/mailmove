#!/usr/bin/env python3
"""Remove stored (encrypted) mailbox passwords after the migration is finished.

    python scripts/purge_credentials.py --job 3     # one job
    python scripts/purge_credentials.py --all       # every job
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import database as db  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--job", type=int)
    g.add_argument("--all", action="store_true")
    args = ap.parse_args()
    conn = db.init_db()
    n = db.purge_credentials(conn, None if args.all else args.job)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.execute("VACUUM")  # rewrite the file so old encrypted values do not linger in free pages
    print(f"Purged credentials for {n} account row(s).")


if __name__ == "__main__":
    main()
