"""
Mailbox password encryption (Fernet, AES-128-CBC + HMAC).

The key lives in MAILMOVE_KEY (environment file outside the repo and outside
SQLite). Generate one with:  python scripts/init_db.py --gen-key
"""
import os

from cryptography.fernet import Fernet, InvalidToken

import config  # noqa: F401  (loads .env for local development)


class KeyError_(RuntimeError):
    pass


def generate_key():
    return Fernet.generate_key().decode()


def _fernet():
    key = os.environ.get("MAILMOVE_KEY", "").strip()
    if not key:
        raise KeyError_("MAILMOVE_KEY is not set. Generate one with: python scripts/init_db.py --gen-key")
    try:
        return Fernet(key.encode())
    except ValueError as e:
        raise KeyError_(f"MAILMOVE_KEY is not a valid Fernet key: {e}") from None


def key_available():
    try:
        _fernet()
        return True
    except KeyError_:
        return False


def encrypt(plaintext):
    if plaintext is None:
        return None
    return _fernet().encrypt(plaintext.encode()).decode()


def decrypt(token):
    if token is None:
        return None
    try:
        return _fernet().decrypt(token.encode()).decode()
    except InvalidToken:
        raise KeyError_("Stored password cannot be decrypted (wrong MAILMOVE_KEY?)") from None
