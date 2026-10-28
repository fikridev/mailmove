# MailMove v2

Copies mailboxes from one IMAP host to another (e.g. an old cPanel host to a new one), safely and resumably.

- **Dashboard** (`app.py`, Streamlit): create jobs, enter accounts, watch progress, pause/resume/cancel, review problems.
- **Worker** (`worker.py`): does all copying. Separate process, so a dashboard restart never stops a migration.
- **SQLite** (`database.py`): jobs, accounts, folders and one checkpoint row per message. No email content is stored.

```
Mac browser --SSH tunnel--> Streamlit (127.0.0.1:8501) --- SQLite (WAL) --- worker.py
                                                                              |
                                                       old IMAP (read-only) -> new IMAP
```

## What it guarantees, and what it does not

| Guarantee | How |
|---|---|
| Source is never changed | Folders opened with `EXAMINE`; bodies read with `BODY.PEEK[]`; no STORE/EXPUNGE/COPY/MOVE/DELETE. Covered by tests. |
| Encrypted, verified connections | IMAPS port 993 only, `ssl.create_default_context()` (certificate chain, expiry and hostname checked). No STARTTLS, no plaintext fallback. |
| Survives outages and restarts | Every message has a checkpoint. Network errors retry forever with backoff (5s, 10s, ... 5 min). Worker crash, `systemctl restart` or a reboot resumes where it stopped. |
| No duplicates from interrupted uploads | The attempt is saved **before** each APPEND. After a crash or dropped connection, the destination folder is searched before anything is re-uploaded. Exact match (Message-ID + size + date) means verified. Nothing similar means re-upload. Only a partial match means **needs_review**, never re-uploaded automatically. |
| Honest completion | `copied` is not `verified`: after each folder, every upload is confirmed on the destination (APPENDUID when the server supports UIDPLUS, otherwise content match). A job is `completed` only when nothing is pending, failed or under review; otherwise `completed_with_errors`. |
| Folders kept separate | Every source folder gets its own destination folder (Junk and spam are not merged; name clashes get a ` (2)` suffix). Nothing is skipped by default. |
| Passwords protected | Fernet-encrypted in SQLite, key in an env file outside the repo and DB. Never logged, never exported. CSV uploads are parsed in memory only. |

**Limits (documented, not hidden):**
- Exactly-once delivery cannot be proven on every IMAP server. Without UIDPLUS, "verified" means a destination message with the same Message-ID, size and INTERNALDATE exists. Servers that rewrite messages on upload change the size; those are verified by Message-ID when it is unique, otherwise flagged for review.
- Two genuinely different emails with identical Message-ID, size and date are indistinguishable. This is extremely rare.
- Each message is held in RAM while copied (imaplib limitation). Messages above `MAILMOVE_MAX_MESSAGE_MB` (default 150 MB) are refused and reported.
- Folder names are passed through byte-for-byte (IMAP modified UTF-7), which is correct for cPanel/Dovecot on both sides.
- IMAP does not carry forwarders, aliases, filters, autoresponders or passwords. Recreate those on the new host by hand.

## Local development

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
echo "MAILMOVE_KEY=$(.venv/bin/python scripts/init_db.py --gen-key)" > .env   # .env is gitignored
.venv/bin/python scripts/init_db.py              # creates data/mailmove.db
.venv/bin/python worker.py                       # terminal 1
.venv/bin/streamlit run app.py                   # terminal 2
.venv/bin/python -m pytest tests -q              # 58 tests, about 4 seconds, no network
```

## Deploy to the DigitalOcean droplet (Ubuntu 24.04)

Run on the **droplet** as root (or with sudo). `YOUR_DROPLET_IP` is the droplet's address.

**1. Packages, service user, directories**
```bash
apt update && apt install -y python3 python3-venv python3-pip sqlite3 rsync
useradd --system --create-home --home-dir /var/lib/mailmove --shell /usr/sbin/nologin mailmove || true
mkdir -p /opt/mailmove /var/lib/mailmove/data /var/lib/mailmove/backup /etc/mailmove
chown -R mailmove:mailmove /var/lib/mailmove && chmod 700 /var/lib/mailmove/data /var/lib/mailmove/backup
```

**2. Copy the code** (run on your **Mac**, in this folder)
```bash
rsync -av --delete --exclude .git --exclude .venv --exclude data --exclude logs --exclude __pycache__ \
      --exclude .env --exclude '*.csv' --exclude '*.db*' ./ root@YOUR_DROPLET_IP:/opt/mailmove/
```

**3. Virtualenv** (droplet). The code stays owned by root, so the service user cannot modify it.
```bash
cd /opt/mailmove
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
chown -R root:root /opt/mailmove && chmod -R go-w /opt/mailmove
```

**4. Secrets file** (droplet)
```bash
KEY=$(/opt/mailmove/.venv/bin/python /opt/mailmove/scripts/init_db.py --gen-key)
printf 'MAILMOVE_KEY=%s\nMAILMOVE_DB=/var/lib/mailmove/data/mailmove.db\n' "$KEY" > /etc/mailmove/mailmove.env
unset KEY
chown root:mailmove /etc/mailmove /etc/mailmove/mailmove.env
chmod 750 /etc/mailmove && chmod 640 /etc/mailmove/mailmove.env
```
Keep a copy of the key somewhere safe (a password manager). Without it, stored passwords cannot be decrypted.

**5. Database**
```bash
sudo -u mailmove bash -c 'set -a; . /etc/mailmove/mailmove.env; set +a; /opt/mailmove/.venv/bin/python /opt/mailmove/scripts/init_db.py'
```

**6. systemd**
```bash
cp /opt/mailmove/services/mailmove-*.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now mailmove-worker mailmove-dashboard
systemctl status mailmove-worker mailmove-dashboard --no-pager
journalctl -u mailmove-worker -f          # live worker log (Ctrl+C to leave)
```

**7. Open the dashboard from your Mac** (port 8501 is never exposed publicly)
```bash
ssh -N -L 8501:127.0.0.1:8501 root@YOUR_DROPLET_IP
```
Then browse to http://localhost:8501. Later, prefer a non-root SSH user with key authentication.

**Updating code during a migration:** rsync the code again, then restart only what changed. Restarting the **dashboard** is always safe. Restarting the **worker** is also safe (it finishes the current message, requeues, and resumes), but avoid it during the bulk copy unless needed.

## Operations

| Task | Command |
|---|---|
| Worker log | `journalctl -u mailmove-worker -f` |
| Restart dashboard | `systemctl restart mailmove-dashboard` |
| Back up DB (safe while running) | `sudo -u mailmove sqlite3 /var/lib/mailmove/data/mailmove.db ".backup /var/lib/mailmove/backup/mm-$(date +%F-%H%M).db"` |
| Independent verification | `sudo -u mailmove bash -c 'set -a; . /etc/mailmove/mailmove.env; set +a; cd /opt/mailmove; .venv/bin/python scripts/verify_migration.py --job N --csv /var/lib/mailmove/verify-N.csv'` |
| Purge stored passwords | same pattern with `scripts/purge_credentials.py --all` |
| Limit journal size | `journalctl --vacuum-size=500M` (or set `SystemMaxUse=500M` in `/etc/systemd/journald.conf`) |

## Troubleshooting

| Symptom | Meaning / fix |
|---|---|
| `tls_certificate` error | The certificate doesn't match the hostname, or has expired. **Do not disable verification.** Use the secure incoming hostname from cPanel, under Email Accounts, then Connect Devices (often `serverNNN.provider.com`). Test it: `openssl s_client -connect HOST:993 -servername HOST -verify_return_error </dev/null` |
| `auth` error | Wrong email/password (tried 3 times, then the account stops). Fix it, then create a new job for that account. |
| Job stays `retrying` | The old host is down or throttling. That's expected; it resumes by itself. Check `journalctl` for the reason. |
| `quota` | Destination mailbox is full. Raise the quota on the new host, then click **Sync again**. |
| `needs_review` emails | An interrupted upload couldn't be proven present or absent. Look in that destination folder. If it's there, choose **Accept as present**; if not, choose **Copy anyway**, then **Sync again**. |
| Dashboard says the worker is not running | `systemctl status mailmove-worker`, then `journalctl -u mailmove-worker -n 50` |

## Files

`app.py` dashboard · `worker.py` engine · `mailmove.py` IMAP primitives · `database.py` schema and state machine · `crypto.py` encryption · `config.py` settings · `scripts/` init, verify, purge · `services/` systemd units · `tests/` fake IMAP server with fault injection · `OPERATIONS.md` migration runbook.
