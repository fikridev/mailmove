"""
Runtime settings, read from the environment.

On the VPS these come from /etc/mailmove/mailmove.env (systemd EnvironmentFile).
For local development a .env file next to this one is loaded if present.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _load_dotenv():
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()


def db_path():
    return os.environ.get("MAILMOVE_DB", str(ROOT / "data" / "mailmove.db"))


def env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


# Retry backoff: 5s, 10s, 20s ... capped at 5 minutes, with jitter.
BACKOFF_BASE = env_float("MAILMOVE_BACKOFF_BASE", 5)
BACKOFF_MAX = env_float("MAILMOVE_BACKOFF_MAX", 300)
# Socket timeout for the IMAP connect and for each read/write (not the whole transfer).
IMAP_TIMEOUT = env_float("MAILMOVE_IMAP_TIMEOUT", 30)
# imaplib keeps a whole message in memory; refuse anything bigger than this.
MAX_MESSAGE_BYTES = int(env_float("MAILMOVE_MAX_MESSAGE_MB", 150) * 1024 * 1024)
# Worker considered dead if its heartbeat is older than this.
HEARTBEAT_STALE_SECONDS = env_float("MAILMOVE_HEARTBEAT_STALE", 60)
POLL_SECONDS = env_float("MAILMOVE_POLL_SECONDS", 2)
