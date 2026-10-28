#!/usr/bin/env python3
"""Create / upgrade the MailMove SQLite database, or generate an encryption key.

    python scripts/init_db.py              # create schema at $MAILMOVE_DB
    python scripts/init_db.py --gen-key    # print a new MAILMOVE_KEY (store it in the env file)
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402
import crypto  # noqa: E402
import database as db  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gen-key", action="store_true", help="print a new Fernet key and exit")
    args = ap.parse_args()
    if args.gen_key:
        print(crypto.generate_key())
        return
    conn = db.init_db()
    v = conn.execute("SELECT version FROM schema_version").fetchone()[0]
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    print(f"Database ready: {config.db_path()} (schema v{v}, journal {mode})")
    if not crypto.key_available():
        print("WARNING: MAILMOVE_KEY is not set or invalid; jobs cannot be created or run.")


if __name__ == "__main__":
    main()
