# MailMove v2 — Technical Specification & Claude Code Implementation Guide

**Status:** Implementation specification  
**Target:** DigitalOcean Ubuntu 24.04 LTS, Python 3.12  
**Priority:** Urgent migration of approximately 16.12 GB of mail from an unstable cPanel host before 31 October 2026  
**Project model:** One repository, one VPS, one SQLite database, two independently supervised processes

> **Instructions to Claude Code:** Inspect the existing repository before editing. Preserve working functionality. Do not assume that the existing IMAP engine is correct or resumable until verified. Deliver a small, safe, testable implementation in phases. Never run a production migration, change DNS, delete mail, or expose mailbox credentials without explicit operator approval.

## 1. Context and objectives

The current application, **MailMove**, copies mailboxes between IMAP hosts. The supplied `app.py` is a Streamlit UI that imports `mailmove.py` as `mm`. It currently supports source and destination IMAP host/port settings, a CSV of accounts (`src_user`, `src_pass`, `dst_user`, `dst_pass`), a connection test, dry run, up to eight concurrent mailbox workers, optional folder skipping, large-message warnings, progress metrics, logs, and repeat synchronization. The Streamlit UI starts a migration with `threading.Thread(..., daemon=True)`, and runs a `ThreadPoolExecutor` within that background thread. Progress and job state are held in memory. This creates a risk that a Streamlit restart or VPS reboot interrupts the job and loses UI progress.

**Known, not yet verified:** `mailmove.py` implements the IMAP transfer, deduplication, and estimates. Claude Code must inspect this file and its tests before making claims about its behavior. The previously supplied `app.py` is the only engine-adjacent source reviewed for this specification.

### Primary objectives

1. Transfer mail from an old, intermittently available cPanel IMAP server to a new hosting provider.
2. Keep the source strictly read-only and preserve messages, folders, internal dates, and flags where supported.
3. Automatically retry transient failures and safely resume after worker crashes, UI restarts, or VPS reboots.
4. Avoid duplicate destination messages, especially when IMAP `APPEND` succeeds but the acknowledgement is lost.
5. Provide clear progress, failed-message reporting, and post-transfer verification.
6. Minimize time to a working first migration. Avoid feature expansion that delays the deadline.

### Non-goals

- Migrating website files, databases, cPanel account settings, mail passwords, filters, aliases, forwarders, or DNS automatically.
- Replacing the destination hosting provider's mail service.
- Distributed workers or a multi-tenant public SaaS in this version.
- Guaranteeing exactly-once delivery across arbitrary IMAP servers. Explicitly document limitations.

## 2. Architecture and repository

Use **one Git repository** and deploy one checkout at `/opt/mailmove`. Run two independent systemd services from the same Python virtual environment:

```text
Mac / browser
   |
   | SSH tunnel to localhost:8501
   v
Streamlit dashboard (app.py) ---- SQLite (WAL) ---- Python worker (worker.py)
                                                      |
                                               IMAP source -> IMAP destination
```

The dashboard **must not perform migrations in a daemon thread**. It creates jobs and reads state. The worker claims and executes jobs independently. Both processes access SQLite on the same local SSD. `systemd` restarts either process after failure. SQLite is for metadata and checkpoints, **not raw email content**.

Suggested layout (adjust to the existing codebase rather than rewriting unnecessarily):

```text
mailmove/
├── app.py
├── mailmove.py
├── worker.py
├── database.py
├── models.py                 # optional, only if it simplifies implementation
├── crypto.py                 # credential encryption / decryption
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── services/
│   ├── mailmove-dashboard.service
│   └── mailmove-worker.service
├── scripts/
│   ├── init_db.py
│   └── verify_migration.py
├── tests/
│   ├── test_database.py
│   ├── test_resume.py
│   ├── test_imap_failures.py
│   └── test_folder_mapping.py
└── data/                     # runtime only; not committed
    └── mailmove.db
```

## 3. Migration lifecycle

Job states: `queued`, `running`, `retrying`, `pausing`, `paused`, `cancelling`, `cancelled`, `completed`, `completed_with_errors`, `failed`. Specify valid transitions and enforce them in one place.

Account states: `queued`, `running`, `retrying`, `completed`, `completed_with_errors`, `failed`.

Message states: `pending`, `in_progress`, `copied_unverified`, `verified`, `retrying`, `failed`, `skipped`.

- Creating a job saves configuration and account references; it does not start network activity unless the operator explicitly starts the job.
- A worker atomically claims eligible work. With one worker process, avoid elaborate distributed leasing; still persist a worker heartbeat and recover stale `running` work on startup.
- Pause/cancel is cooperative and checks flags between messages and during retry waits. An in-flight `APPEND` may complete before pause takes effect.
- On startup, recover unfinished work, reconcile uncertain uploads, then continue.
- When all accounts are processed, run verification before marking the job completed. Missing or ambiguous messages must be visible in the report.
- Support a second synchronization pass after MX cutover to copy late-arriving mail. Never delete destination mail during this pass.

## 4. SQLite schema

Use migrations or a schema version table. Enable `PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, and a nonzero `busy_timeout` for each connection. Keep transactions short; do not hold a SQLite write transaction during network calls.

### `migration_jobs`

`id` UUID/INTEGER PK, `name`, `source_host`, `source_port`, `destination_host`, `destination_port`, `status`, `dry_run`, `max_parallel_accounts`, `skip_folders_json`, `large_message_threshold_bytes`, `created_at`, `started_at`, `finished_at`, `updated_at`, `last_heartbeat_at`, `pause_requested`, `cancel_requested`, `last_error`.

### `migration_accounts`

`id`, `job_id`, `source_user`, `destination_user`, `encrypted_source_password`, `encrypted_destination_password`, `status`, `current_folder`, `messages_scanned`, `messages_verified`, `messages_failed`, `bytes_verified`, `retry_count`, `last_error`, timestamps. Never expose encrypted password fields to UI exports.

### `migration_folders`

`id`, `account_id`, `source_folder_name`, `destination_folder_name`, `source_uidvalidity`, `source_uidnext_observed`, `destination_uidvalidity`, `status`, counts, last_scan_at`, `last_verified_at`. Store folder names losslessly (including UTF-7/UTF-8 handling). Track source `UIDVALIDITY` changes as an explicit reconciliation event, not an automatic reset of all checkpoints.

### `migration_messages`

`id`, `folder_id`, `source_uidvalidity`, `source_uid`, `source_rfc822_size`, `source_internal_date`, `source_message_id_header`, `source_content_fingerprint` (when practical), `destination_uidvalidity`, `destination_uid`, `append_attempt_id`, `status`, `attempts`, `last_error`, `verified_at`, timestamps. Unique constraint on `(folder_id, source_uidvalidity, source_uid)`; indexes for `(status, folder_id)` and pending/retry lookups. Destination UIDs may be unknown if server lacks UIDPLUS; design for this.

### `migration_errors` / `job_events`

Store job/account/folder/message identifiers, timestamp, sanitized error category, retryable flag, attempt number, and short sanitized details. Never log passwords, message bodies, authentication tokens, or full email content.

**Scale:** Tens or hundreds of thousands of message metadata rows are acceptable for a single SQLite instance with proper indexing and batched writes. Do not cache all messages in Python memory.

## 5. IMAP correctness requirements

### 5.1 Connection and TLS

- Default to IMAPS port 993 and verify TLS certificates and hostnames.
- Use explicit connect/read/write timeouts and keepalive or reconnect where supported.
- Support source/destination hostnames independent of the mail domain; this matters before MX cutover.
- Never silently fall back to insecure IMAP. STARTTLS port 143 may be supported only if implemented and tested with mandatory TLS.
- Limit parallelism to **two accounts initially** on the unstable source. Make concurrency configurable (1–8), but do not increase it without measurement.

### 5.2 Source must remain read-only

- Use `EXAMINE` or equivalent read-only mailbox selection when possible.
- Do not issue `STORE`, `EXPUNGE`, `MOVE`, `COPY`, `DELETE`, or mutate flags on the source.
- Be aware that some `FETCH` forms can set `\\Seen`; use non-mutating `BODY.PEEK[]` semantics.

### 5.3 Folder discovery and mapping

- Discover all selectable source folders, namespace prefixes, delimiters, and special-use flags.
- Preserve Inbox, Sent, Drafts, Archive, Trash, Junk/Spam, and custom folders unless explicitly excluded.
- Handle destination folder naming, delimiter, namespace, and special-use conventions. Never flatten two different source folders onto the same destination name.
- Preserve non-ASCII names and hierarchy. Test Gmail-like and cPanel/Dovecot-like folder conventions where relevant.
- Default to skipping **nothing** for the urgent migration.

### 5.4 Message transfer

- Use stable source UID with UIDVALIDITY for checkpoint identity; do not use IMAP sequence numbers.
- Fetch raw RFC822 content without marking source messages read.
- Preserve `INTERNALDATE` and supported flags in destination `APPEND`.
- Stream or spool individual messages using bounded memory. If a library requires a complete message in memory, enforce a safe per-message size limit and use secure temporary files for larger messages; never load entire mailboxes into memory.
- Handle oversized messages rejected by the destination with a clear failure report; do not silently skip.
- Do not assume source UID equals destination UID. Use UIDPLUS `APPENDUID` when available; otherwise reconcile using defensible destination-side evidence.

### 5.5 Idempotency and uncertain APPEND results

This is a critical requirement. A timeout after `APPEND` may mean the destination accepted the message even though the client never received confirmation. Blind retry can duplicate mail.

Implement a durable attempt state **before** upload. On an uncertain result, inspect the destination folder for the candidate message before retrying. Use destination UID mappings when available; otherwise use multiple identifying attributes (e.g., size, internal date, Message-ID, and content fingerprint when feasible). **Message-ID alone is not unique or reliable.** If the match is ambiguous, report it for reconciliation rather than silently append a duplicate.

Mark a message `verified` only when there is sufficient destination-side evidence. Document what verification can and cannot guarantee on servers with limited IMAP capabilities. Prefer correctness over a false 100% completion indicator.

### 5.6 UIDVALIDITY and mailbox changes

- If source UIDVALIDITY changes, stop relying on previous UID checkpoints for that folder and perform a safe rescan/reconciliation.
- Handle source messages arriving during the migration. A final sync pass is mandatory.
- If source messages disappear before being copied, report the discrepancy instead of claiming success.
- Do not delete destination messages merely because they no longer exist on the source.

## 6. Retry and recovery

Classify errors into transient (connection reset, timeout, temporary server busy, throttling) and permanent/actionable (invalid credentials, missing mailbox, invalid TLS certificate, destination quota exceeded, message too large). Use exponential backoff with jitter, e.g., 5s, 10s, 20s, 40s, up to 5 minutes, and respect server throttling. Avoid infinite tight loops. Permanent errors should pause the affected account or message and surface a clear remediation.

- Reconnect both source and destination sessions as needed; reselect folders and revalidate UIDVALIDITY.
- Save progress after verified messages or small batches.
- A worker crash/reboot must not require restarting from zero.
- Persist enough information to reconcile any message that was in flight.
- `systemd Restart=on-failure` restarts the worker process; the application itself must recover database state and retry network errors.
- Log reconnect attempts and the last successful activity time.
- When old hosting is offline for hours, job remains `retrying`; it must not be declared completed.

## 7. Streamlit dashboard requirements

Retain existing UX where practical. Replace `threading.Thread(..., daemon=True)` migration execution with DB-backed job creation and control. UI features:

1. Create/edit job with source and destination IMAP host/port and TLS settings.
2. Add mailbox rows manually or upload CSV with existing column names.
3. Test both endpoints and report authentication/TLS/folder discovery errors safely.
4. Dry run that counts messages/bytes/large messages; clearly distinguish estimates from measured throughput.
5. Start, pause, resume, cancel, and rerun final synchronization.
6. Per-job and per-account status, current folder, verified messages, failed messages, bytes transferred, reconnect count, speed, ETA, last successful activity, and last error.
7. Failed/oversized message list with safe metadata, retry controls, and downloadable sanitized reports.
8. Display uncertainty honestly: counts can change while scanning, ETA is approximate, and `copied` is not equivalent to `verified`.
9. No plaintext password display after initial entry; no passwords in CSV exports or logs.
10. UI remains usable and shows persistent state after refresh or Streamlit restart.

**Credential CSV warning:** CSV uploads contain plaintext passwords. Parse only in memory, encrypt immediately for storage, never write raw CSV to disk, and recommend deleting the source CSV after use. Use password-type inputs where possible.

## 8. Secrets and operational security

- Use an authenticated private dashboard. Preferred immediate access: Streamlit listening on `127.0.0.1:8501`, with an SSH tunnel from the operator's Mac.
- Do not expose port 8501 publicly or bind to `0.0.0.0` without a separate authenticated TLS reverse proxy.
- Store a Fernet or equivalent encryption key in a root-protected environment file outside the repository and outside SQLite; permissions restricted to the service user as appropriate.
- Do not place plaintext passwords in process command arguments, logs, Git history, shell history, or exported CSVs.
- Run services as a dedicated unprivileged `mailmove` Linux user, not root.
- Restrict database, log, runtime, and key-file permissions. Exclude secrets and SQLite DB/WAL/SHM files from Git.
- Do not disable TLS verification to resolve certificate errors. Verify hostnames from cPanel **Connect Devices** on each provider.
- Provide a documented procedure to purge stored credentials and temporary files after successful migration.

## 9. DigitalOcean deployment

**Target:** Ubuntu 24.04, Python 3.12.3, Basic Droplet with 2GB RAM. Region Singapore is reasonable for Malaysian hosting; measure actual routes. The Droplet is already provisioned. Kernel upgrade may require a reboot before deployment.

### 9.1 Install prerequisites

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git sqlite3
```

### 9.2 Dedicated account and project directory

```bash
sudo useradd --system --create-home --home-dir /var/lib/mailmove --shell /usr/sbin/nologin mailmove || true
sudo mkdir -p /opt/mailmove /var/lib/mailmove/data /var/log/mailmove
sudo chown -R mailmove:mailmove /var/lib/mailmove /var/log/mailmove
# Deploy source into /opt/mailmove from Git using an appropriate deploy user.
# Ensure service user can read code, but cannot modify protected secrets or repository history.
```

### 9.3 Virtual environment

```bash
cd /opt/mailmove
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Use pinned/tested dependencies and a repeatable deployment procedure. Create an environment file (e.g., `/etc/mailmove/mailmove.env`) with protected permissions. Define a persistent DB path such as `/var/lib/mailmove/data/mailmove.db`.

### 9.4 systemd

Create **two** units:

- `mailmove-worker.service`: `ExecStart=/opt/mailmove/.venv/bin/python /opt/mailmove/worker.py`, `User=mailmove`, `WorkingDirectory=/opt/mailmove`, `EnvironmentFile=/etc/mailmove/mailmove.env`, `Restart=on-failure`, `RestartSec=5`, `After=network-online.target`.
- `mailmove-dashboard.service`: `ExecStart=/opt/mailmove/.venv/bin/streamlit run /opt/mailmove/app.py --server.address 127.0.0.1 --server.port 8501 --server.headless true`, same unprivileged user and environment, with `Restart=on-failure`.

Use suitable systemd hardening, but test writable paths and DNS/network access before enabling strict restrictions. Avoid a service restart on every dashboard code update while a migration is in flight.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now mailmove-worker mailmove-dashboard
sudo systemctl status mailmove-worker mailmove-dashboard
sudo journalctl -u mailmove-worker -f
```

### 9.5 Private access from Mac

```bash
ssh -L 8501:127.0.0.1:8501 root@YOUR_DROPLET_IP
```

Then open `http://localhost:8501` locally. After initial setup, prefer a non-root SSH user and key-based authentication.

### 9.6 Backup and recovery

- Back up SQLite with the SQLite online backup API or `sqlite3 .backup`, not a naive copy of the DB while WAL writes are active.
- Keep backup files and credentials protected; avoid storing unencrypted secret material in broad-access snapshots.
- Verify the worker resumes after a controlled process restart and after a reboot before starting the bulk migration.
- Ensure disk capacity for SQLite, logs, and temporary per-message spooling. Set log rotation and cleanup policies.

## 10. Testing and acceptance criteria

### Automated tests

- DB schema creation, migrations, WAL concurrent read/write, atomic job claims.
- Pause/resume/cancel and stale-job recovery.
- Source connection timeout, mid-fetch disconnect, destination disconnect, throttling, and long offline period.
- Crash immediately before `APPEND`, during `APPEND`, and after successful `APPEND` but before checkpoint.
- UIDVALIDITY change and source mailbox changes.
- Duplicate Message-ID, missing Message-ID, repeated messages, ambiguous destination matches.
- Folder delimiter/namespace changes, UTF-8 folder names, flags, and internal dates.
- Oversized messages, destination quota errors, and bad credentials.
- No plaintext credentials in logs, reports, DB, or UI exports.

### Manual acceptance test before bulk migration

1. Test both IMAP connections with a small mailbox.
2. Run dry run and compare counts/size with source where possible.
3. Migrate a small mailbox and verify Inbox, Sent, custom folders, flags, dates, and representative attachments.
4. Interrupt the old IMAP connection and confirm automatic retry.
5. Restart Streamlit; verify the worker continues.
6. Restart the worker; verify no known duplicate messages are created and progress resumes.
7. Rerun the same job; confirm it does not duplicate verified messages.
8. Inspect the verification report and resolve errors.
9. Only then migrate the remaining mailboxes.

### Definition of done

- Dashboard and worker run as separate systemd services.
- Restarting dashboard does not stop migration.
- Worker restart resumes from persistent checkpoints with uncertain uploads reconciled.
- Source remains read-only.
- Successful messages are destination-verified; failures and ambiguities are explicitly reported.
- Final sync can safely run after MX cutover.
- Deployment and operator runbook are reproducible.

## 11. Urgent operating plan (28–31 October 2026)

**Immediately:** Ask the old hosting provider for an account backup or temporary service extension. Check destination mailbox capacity and quotas. Confirm all destination email accounts exist and obtain source/destination IMAP hostnames from each cPanel's **Connect Devices** screen. Do not depend on DNS mail hostnames that will change during cutover.

**28 October:** Deploy minimal reliable worker, perform one small mailbox migration, test retry and resume. Do not spend the evening polishing the dashboard.

**29 October:** Start bulk migration with 2 parallel mailboxes. Monitor source CPU/IMAP responsiveness and actual throughput. Increase concurrency only if the source remains healthy.

**30 October:** Verify folder/message counts and attachments, resolve failures, switch MX after the initial copy is sufficiently complete, and run another synchronization to catch mail delivered to the old host. Check DNS TTL and delivery on both sides.

**31 October:** Complete final reconciliation before service termination, export sanitized migration reports, preserve independent backups, and confirm users can send/receive on the new provider. Remember that IMAP does not migrate cPanel forwarders, aliases, filters, or credentials.

**Contingency:** If the source is intermittently unavailable, leave the worker in retrying mode and prioritize critical mailboxes. If shutdown is imminent, request the host's direct cPanel account backup or provider-assisted transfer. A migration tool cannot recover data after the old server permanently disappears.

## 12. Implementation phases for Claude Code

### Phase 0 — Audit (mandatory)

Inspect `app.py`, `mailmove.py`, `requirements.txt`, tests, and deployment scripts. Produce a short audit listing reusable parts, correctness risks, current deduplication approach, and the smallest viable changes. **Do not assume a feature exists because the UI labels claim it.**

### Phase 1 — Persistence and worker (highest priority)

Implement SQLite schema, encryption, independent worker, job lifecycle, systemd units, and Streamlit integration. Add tests for process restart and DB state recovery. Preserve existing account CSV format and basic dashboard.

### Phase 2 — Safe IMAP resume

Implement robust retries, source UIDVALIDITY handling, bounded memory, destination reconciliation for uncertain APPEND, and verifiable checkpoints. Test using controlled IMAP failure simulations. Do not assert exactly-once semantics without proof.

### Phase 3 — Operational dashboard and verification

Add job controls, progress, failure reports, final sync, and comparison/verification tools. Keep scope focused on completing the urgent migration.

### Phase 4 — Production hardening

Finalize unprivileged service deployment, secret management, backup, log rotation, and documentation. Demonstrate a small-mailbox acceptance test before bulk execution.

## 13. Required deliverables

1. Updated source code in the existing single repository.
2. Database schema/migrations and initialization script.
3. Independent worker and two systemd unit files.
4. Updated Streamlit dashboard with persistent state.
5. Automated tests and a report of test results.
6. `README.md` with local setup, VPS deployment, secure access, recovery, and troubleshooting.
7. `OPERATIONS.md` with the exact runbook for the 16GB migration, verification, MX cutover, and final sync.
8. Explicit list of remaining risks and any features deferred because of the deadline.

## 14. Final instruction to Claude Code

**Start by auditing the existing code, not by rewriting it.** Show the implementation plan and file-level changes. Then implement Phase 1 and Phase 2, run tests, and provide deployment commands. Do not execute any destructive or production-affecting operation without approval. Prioritize safe, resumable mail transfer over UI enhancements or architectural complexity.
