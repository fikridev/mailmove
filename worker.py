#!/usr/bin/env python3
"""
worker.py : MailMove v2 migration worker (run by systemd as mailmove-worker).

Claims queued jobs from SQLite and copies mail account by account, with every
message checkpointed in migration_messages:

  pending -> in_progress (saved BEFORE the APPEND) -> copied_unverified -> verified

If the worker dies or the connection drops during an APPEND, the message stays
'in_progress'. Before anything is uploaded again, the destination folder is
searched for it:
  * exact match (Message-ID + size + INTERNALDATE)       -> verified, no re-upload
  * nothing similar on the destination                   -> safe to upload again
  * only a partial match (same Message-ID, or similar)   -> needs_review, NOT re-uploaded

Transient network errors (old host offline, timeouts, throttling) are retried
forever with exponential backoff (5s .. 5 min + jitter); the job shows
'retrying' and is never marked completed while mail is still missing.
"""
import hashlib
import json
import logging
import os
import random
import signal
import socket
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import config
import crypto
import database as db
import mailmove as mm

log = logging.getLogger("mailmove.worker")

ACTIVE_MESSAGE_STATES = ("pending", "retrying", "in_progress")
MAX_MESSAGE_ATTEMPTS = 3
AUTH_ATTEMPTS = 3


class StopRequested(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class TransientError(Exception):
    pass


class PermanentAccountError(Exception):
    def __init__(self, category, message):
        super().__init__(message)
        self.category = category


def backoff_delay(failures):
    base = config.BACKOFF_BASE * (2 ** min(max(failures - 1, 0), 16))  # bounded: outages can last hours
    delay = min(base, config.BACKOFF_MAX)
    return delay * random.uniform(0.8, 1.2)


# ------------------------------------------------------------------ account runner

class AccountRunner:
    def __init__(self, worker, job, account_id):
        self.worker = worker
        self.job = job
        self.job_id = job["id"]
        self.dry_run = bool(job["dry_run"])
        self.skip = set(json.loads(job["skip_folders_json"]))
        self.conn = db.connect(worker.db_path)
        self.account_id = account_id
        acc = self.conn.execute("SELECT * FROM migration_accounts WHERE id=?", (account_id,)).fetchone()
        self.user = acc["source_user"]
        self.dst_user = acc["destination_user"]
        self.src_pw = crypto.decrypt(acc["encrypted_source_password"])
        self.dst_pw = crypto.decrypt(acc["encrypted_destination_password"])
        self.secrets = (self.src_pw, self.dst_pw)
        self.src = self.dst = None
        self.progressed = False
        self.folder_errors = 0

    # -------------------------------------------------------------- helpers

    def clean(self, text):
        return mm.sanitize(text, self.secrets)

    def event(self, level, category, details="", **kw):
        details = self.clean(details)
        with db.tx(self.conn):
            db.event(self.conn, self.job_id, level, category, details, account_id=self.account_id, **kw)
        getattr(log, "warning" if level == "warning" else "error" if level == "error" else "info")(
            "[job %s] %s %s: %s", self.job_id, self.user, category, details)

    def set_account(self, **fields):
        fields["updated_at"] = db.now()
        with db.tx(self.conn):
            self.conn.execute(
                f"UPDATE migration_accounts SET {', '.join(k + '=?' for k in fields)} WHERE id=?",
                (*fields.values(), self.account_id))

    def update_message(self, mid, **fields):
        fields["updated_at"] = db.now()
        with db.tx(self.conn):
            self.conn.execute(
                f"UPDATE migration_messages SET {', '.join(k + '=?' for k in fields)} WHERE id=?",
                (*fields.values(), mid))

    def check_stop(self):
        if self.worker.shutdown.is_set():
            raise StopRequested("shutdown")
        row = self.conn.execute("SELECT pause_requested, cancel_requested FROM migration_jobs WHERE id=?",
                                (self.job_id,)).fetchone()
        if row["cancel_requested"]:
            raise StopRequested("cancel")
        if row["pause_requested"]:
            raise StopRequested("pause")

    def sleep_interruptible(self, seconds):
        end = time.time() + seconds
        while True:
            self.check_stop()
            left = end - time.time()
            if left <= 0:
                return
            time.sleep(min(1.0, left))

    def refresh_counters(self):
        row = self.conn.execute(
            "SELECT COUNT(*) AS scanned,"
            " SUM(m.status='verified') AS verified,"
            " SUM(m.status IN ('failed','needs_review')) AS failed,"
            " COALESCE(SUM(CASE WHEN m.status='verified' THEN m.source_rfc822_size END),0) AS bytes"
            " FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
            " WHERE f.account_id=? AND (m.skip_reason IS NULL OR m.skip_reason NOT IN"
            " ('superseded_uidvalidity'))", (self.account_id,)).fetchone()
        att = self.conn.execute("SELECT COALESCE(SUM(attachments),0) AS a FROM migration_folders"
                                " WHERE account_id=?", (self.account_id,)).fetchone()["a"]
        self.set_account(messages_scanned=row["scanned"] or 0, messages_verified=row["verified"] or 0,
                         messages_failed=row["failed"] or 0, bytes_verified=row["bytes"] or 0,
                         attachments=att)

    def close(self):
        for m in (self.src, self.dst):
            if m:
                m.close()
        self.src = self.dst = None

    # -------------------------------------------------------------- main loop

    def run(self):
        self.set_account(status="running", started_at=db.now(), last_error=None, next_retry_at=None)
        failures = auth_failures = 0
        try:
            while True:
                self.progressed = False
                try:
                    self.check_stop()
                    self.connect()
                    self.process_all_folders()
                    self.finish()
                    return
                except StopRequested:
                    self.set_account(status="queued", current_folder=None)
                    return
                except PermanentAccountError as e:
                    self.fail(e.category, str(e))
                    return
                except Exception as e:  # network / server errors
                    cat, retryable = ("throttled", True) if isinstance(e, TransientError) else mm.classify(e)
                    if cat == "auth":
                        auth_failures += 1
                        retryable = auth_failures < AUTH_ATTEMPTS
                    if not retryable:
                        if cat == "internal":
                            log.exception("[job %s] %s internal error", self.job_id, self.user)
                        self.fail(cat, f"{type(e).__name__}: {e}")
                        return
                    failures = 1 if self.progressed else failures + 1
                    delay = backoff_delay(failures)
                    with db.tx(self.conn):
                        self.conn.execute(
                            "UPDATE migration_accounts SET status='retrying', retry_count=retry_count+1,"
                            " last_error=?, next_retry_at=?, updated_at=? WHERE id=?",
                            (self.clean(f"{cat}: {e}"), db.now() + delay, db.now(), self.account_id))
                        db.event(self.conn, self.job_id, "warning", cat, self.clean(str(e)),
                                 account_id=self.account_id, retryable=True, attempt=failures)
                    self.worker._sync_job_status(self.job_id, self.conn)
                    log.warning("[job %s] %s %s, retry #%d in %.0fs: %s", self.job_id, self.user, cat,
                                failures, delay, self.clean(str(e)))
                finally:
                    self.close()
                try:
                    self.sleep_interruptible(delay)
                except StopRequested:
                    self.set_account(status="queued", current_folder=None)
                    return
                with db.tx(self.conn):
                    self.conn.execute("UPDATE migration_accounts SET reconnect_count=reconnect_count+1,"
                                      " status='running', updated_at=? WHERE id=?", (db.now(), self.account_id))
        finally:
            self.conn.close()

    def connect(self):
        j = self.job
        self.src = self.worker.mailbox_factory(j["source_host"], j["source_port"], self.user, self.src_pw,
                                               security=j["source_security"])
        self.dst = self.worker.mailbox_factory(j["destination_host"], j["destination_port"], self.dst_user,
                                               self.dst_pw, security=j["destination_security"])
        self.set_account(status="running", last_error=None, next_retry_at=None, last_activity_at=db.now())
        self.worker._sync_job_status(self.job_id, self.conn)
        log.info("[job %s] %s connected (dest UIDPLUS=%s)", self.job_id, self.user, self.dst.uidplus)

    def fail(self, category, message):
        hint = mm.REMEDIATION.get(category, "")
        text = self.clean(f"{category}: {message}" + (f" | {hint}" if hint else ""))
        self.set_account(status="failed", last_error=text, finished_at=db.now(), current_folder=None)
        self.event("error", category, text, retryable=False)
        try:
            self.refresh_counters()
        except Exception:
            pass

    def finish(self):
        self.refresh_counters()
        bad = self.conn.execute(
            "SELECT COUNT(*) AS n FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
            " WHERE f.account_id=? AND (m.status IN ('failed','needs_review','pending','retrying','in_progress'"
            ",'copied_unverified') OR (m.status='skipped' AND m.skip_reason='deleted_on_source'))",
            (self.account_id,)).fetchone()["n"]
        if self.dry_run:
            bad = 0
        folder_bad = self.conn.execute("SELECT COUNT(*) AS n FROM migration_folders WHERE account_id=?"
                                       " AND status IN ('error','missing_on_source')",
                                       (self.account_id,)).fetchone()["n"]
        status = "completed_with_errors" if (bad or folder_bad) else "completed"
        self.set_account(status=status, finished_at=db.now(), current_folder=None)
        self.event("info", "account_done", f"{status}")

    # -------------------------------------------------------------- folders

    def process_all_folders(self):
        rows = self.conn.execute("SELECT source_folder_name, destination_folder_name FROM migration_folders"
                                 " WHERE account_id=?", (self.account_id,)).fetchall()
        existing = {r["source_folder_name"]: r["destination_folder_name"] for r in rows}
        plan = mm.plan_folders(self.src, self.dst, self.skip, existing)
        planned = {s for s, _, _ in plan}
        for gone in set(existing) - set(self.src.folders):
            with db.tx(self.conn):
                self.conn.execute("UPDATE migration_folders SET status='missing_on_source', updated_at=?"
                                  " WHERE account_id=? AND source_folder_name=?", (db.now(), self.account_id, gone))
            self.event("warning", "folder_missing_on_source", gone)
        for sname, _flags, dname in plan:
            self.check_stop()
            fid = self.folder_row(sname, dname)
            try:
                self.process_folder(fid, sname, dname)
            except mm.ImapNo as e:  # folder-level NO (cannot open/create): report, continue with the rest
                cat, _ = mm.classify(e)
                if cat == "throttled":
                    raise
                with db.tx(self.conn):
                    self.conn.execute("UPDATE migration_folders SET status='error', last_error=?, updated_at=?"
                                      " WHERE id=?", (self.clean(e), db.now(), fid))
                self.event("error", "folder_error", f"{sname}: {e}", folder_id=fid)
        return planned

    def folder_row(self, sname, dname):
        t = db.now()
        with db.tx(self.conn):
            self.conn.execute(
                "INSERT OR IGNORE INTO migration_folders (account_id, source_folder_name, destination_folder_name,"
                " created_at, updated_at) VALUES (?,?,?,?,?)", (self.account_id, sname, dname, t, t))
        return self.conn.execute("SELECT id FROM migration_folders WHERE account_id=? AND source_folder_name=?",
                                 (self.account_id, sname)).fetchone()["id"]

    def folder(self, fid):
        return self.conn.execute("SELECT * FROM migration_folders WHERE id=?", (fid,)).fetchone()

    def counts(self, fid, uv):
        rows = self.conn.execute("SELECT status, COUNT(*) AS n FROM migration_messages WHERE folder_id=?"
                                 " AND source_uidvalidity=? GROUP BY status", (fid, uv)).fetchall()
        return {r["status"]: r["n"] for r in rows}

    def process_folder(self, fid, sname, dname):
        self.set_account(current_folder=sname, last_activity_at=db.now())
        uv = self.scan_source(fid, sname)
        if self.dry_run:
            self.dry_run_folder(fid, sname, dname, uv)
            return
        c = self.counts(fid, uv)
        work = sum(c.get(s, 0) for s in (*ACTIVE_MESSAGE_STATES, "copied_unverified"))
        if not work and self.folder(fid)["last_verified_at"]:
            self.mark_folder_done(fid, uv)
            return

        self.dst.ensure_folder(dname)
        dest_uv, index = mm.load_dest_index(self.dst, dname)
        old_dest_uv = self.folder(fid)["destination_uidvalidity"]
        with db.tx(self.conn):
            if old_dest_uv is not None and old_dest_uv != dest_uv:
                # destination UIDs recorded earlier are meaningless now; rematch by content
                self.conn.execute("UPDATE migration_messages SET destination_uid=NULL, destination_uidvalidity=NULL"
                                  " WHERE folder_id=?", (fid,))
                db.event(self.conn, self.job_id, "warning", "dest_uidvalidity_changed", dname,
                         account_id=self.account_id, folder_id=fid)
            self.conn.execute("UPDATE migration_folders SET destination_uidvalidity=?, updated_at=? WHERE id=?",
                              (dest_uv, db.now(), fid))

        self.reconcile(fid, uv, dest_uv, index)
        self.src.examine(sname)  # re-select the source folder (read-only) before fetching bodies
        self.copy_pending(fid, sname, dname, uv)

        dest_uv, index = mm.load_dest_index(self.dst, dname)
        self.verify(fid, uv, dest_uv, index)
        with db.tx(self.conn):
            self.conn.execute("UPDATE migration_folders SET destination_messages=?, last_verified_at=?, updated_at=?"
                              " WHERE id=?", (len(index), db.now(), db.now(), fid))
        self.mark_folder_done(fid, uv)

    def mark_folder_done(self, fid, uv):
        c = self.counts(fid, uv)
        bad = sum(c.get(s, 0) for s in ("failed", "needs_review", "pending", "retrying", "in_progress",
                                         "copied_unverified"))
        with db.tx(self.conn):
            self.conn.execute("UPDATE migration_folders SET status=?, last_error=NULL, updated_at=? WHERE id=?",
                              ("done_with_errors" if bad else "done", db.now(), fid))
        self.refresh_counters()

    # -------------------------------------------------------------- scan

    def scan_source(self, fid, sname):
        """Record every source message (UID, size, date, Message-ID) for this folder.
        Returns the folder's current source UIDVALIDITY."""
        info = self.src.examine(sname)
        uv = info["uidvalidity"]
        folder = self.folder(fid)
        t = db.now()
        if folder["source_uidvalidity"] is not None and folder["source_uidvalidity"] != uv:
            with db.tx(self.conn):
                n = self.conn.execute(
                    "UPDATE migration_messages SET status='skipped', skip_reason='superseded_uidvalidity',"
                    " updated_at=? WHERE folder_id=? AND source_uidvalidity!=? AND status!='skipped'",
                    (t, fid, uv)).rowcount
                db.event(self.conn, self.job_id, "warning", "uidvalidity_changed",
                         f"{sname}: {folder['source_uidvalidity']} -> {uv}; {n} old checkpoints superseded, "
                         "folder rescanned and matched against the destination",
                         account_id=self.account_id, folder_id=fid)

        src_uids = self.src.uid_search_all() if info["exists"] else []
        known = {r[0] for r in self.conn.execute(
            "SELECT source_uid FROM migration_messages WHERE folder_id=? AND source_uidvalidity=?", (fid, uv))}
        new = [u for u in src_uids if u not in known]
        batch = []

        def flush():
            if not batch:
                return
            with db.tx(self.conn):
                self.conn.executemany(
                    "INSERT OR IGNORE INTO migration_messages (folder_id, source_uidvalidity, source_uid,"
                    " source_rfc822_size, source_internal_date, source_internal_epoch, source_message_id_header,"
                    " source_flags, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,'pending',?,?)", batch)
            batch.clear()

        for m in self.src.fetch_meta(new):
            batch.append((fid, uv, m["uid"], m["size"], m["idate"], m["epoch"], m["mid"], m["flags"], t, t))
            if len(batch) >= 500:
                flush()
        flush()

        gone = known - set(src_uids)
        if gone:
            with db.tx(self.conn):
                n = 0
                for chunk in _chunks(sorted(gone), 500):
                    n += self.conn.execute(
                        f"UPDATE migration_messages SET status='skipped', skip_reason='deleted_on_source',"
                        f" updated_at=? WHERE folder_id=? AND source_uidvalidity=? AND status IN ('pending','retrying')"
                        f" AND source_uid IN ({','.join('?' * len(chunk))})", (t, fid, uv, *chunk)).rowcount
            if n:
                self.event("warning", "deleted_on_source",
                           f"{sname}: {n} message(s) disappeared from the source before they were copied",
                           folder_id=fid)

        size = self.conn.execute(
            "SELECT COALESCE(SUM(source_rfc822_size),0) AS b FROM migration_messages WHERE folder_id=?"
            " AND source_uidvalidity=? AND (skip_reason IS NULL OR skip_reason NOT IN"
            " ('deleted_on_source','superseded_uidvalidity'))", (fid, uv)).fetchone()["b"]
        with db.tx(self.conn):
            self.conn.execute(
                "UPDATE migration_folders SET source_uidvalidity=?, source_uidnext_observed=?, source_messages=?,"
                " source_bytes=?, status='scanned', last_scan_at=?, updated_at=? WHERE id=?",
                (uv, info["uidnext"], len(src_uids), size, t, t, fid))
        log.info("[job %s] %s %s: %d on source (%d new)", self.job_id, self.user, sname, len(src_uids), len(new))
        return uv

    def iter_messages(self, fid, uv, statuses, page=500):
        """Page through messages by id so no folder is ever held in memory in full."""
        last = 0
        marks = ",".join("?" * len(statuses))
        while True:
            rows = self.conn.execute(
                f"SELECT * FROM migration_messages WHERE folder_id=? AND source_uidvalidity=? AND status IN ({marks})"
                f" AND id>? ORDER BY id LIMIT ?", (fid, uv, *statuses, last, page)).fetchall()
            if not rows:
                return
            yield from rows
            last = rows[-1]["id"]

    # -------------------------------------------------------------- dry run

    def dry_run_folder(self, fid, sname, dname, uv):
        if dname in self.dst.folders or dname.upper() == "INBOX":
            _, index = mm.load_dest_index(self.dst, dname)
        else:
            index = mm.DestIndex()
        to_copy, present = [], 0
        t = db.now()
        updates = []
        for r in self.iter_messages(fid, uv, ("pending", "retrying")):
            if index.take_exact(r["source_message_id_header"], r["source_rfc822_size"], r["source_internal_epoch"]):
                present += 1
                updates.append(r["id"])
            else:
                to_copy.append(r["source_uid"])
        with db.tx(self.conn):
            for chunk in _chunks(updates, 500):
                self.conn.execute(f"UPDATE migration_messages SET status='skipped', skip_reason='already_on_destination',"
                                  f" updated_at=? WHERE id IN ({','.join('?' * len(chunk))})", (t, *chunk))
        self.src.examine(sname)
        attachments = self.src.scan_attachments(to_copy) if to_copy else 0
        with db.tx(self.conn):
            self.conn.execute("UPDATE migration_folders SET attachments=?, destination_messages=?, status='done',"
                              " updated_at=? WHERE id=?", (attachments, len(index), t, fid))
        self.refresh_counters()
        log.info("[job %s] %s %s: dry run, %d to copy, %d already on destination, %d attachments",
                 self.job_id, self.user, sname, len(to_copy), present, attachments)

    # -------------------------------------------------------------- reconcile (before upload)

    def reconcile(self, fid, uv, dest_uv, index):
        """Match unfinished source messages against what is already in the destination
        folder. Runs before any upload, so interrupted uploads and copies made by an
        earlier run (or MailMove v1) are never uploaded twice."""
        for r in self.iter_messages(fid, uv, ("verified", "copied_unverified")):
            if r["destination_uid"] and r["destination_uidvalidity"] == dest_uv and r["destination_uid"] in index.by_uid:
                index.claim(r["destination_uid"])
            elif r["status"] == "copied_unverified":
                uid = index.take_exact(r["source_message_id_header"], r["source_rfc822_size"],
                                       r["source_internal_epoch"])
                if uid:
                    self.mark_verified(r["id"], dest_uv, uid)
        t = db.now()
        # interrupted uploads get first claim on matching destination copies
        for states in (("in_progress",), ("pending", "retrying")):
            for r in self.iter_messages(fid, uv, states):
                mid, size, epoch = r["source_message_id_header"], r["source_rfc822_size"], r["source_internal_epoch"]
                uid = index.take_exact(mid, size, epoch)
                if uid:
                    self.mark_verified(r["id"], dest_uv, uid)
                    if r["status"] == "in_progress":
                        self.event("info", "uncertain_upload_found",
                                   f"uid {r['source_uid']}: interrupted upload is on the destination; not re-uploaded",
                                   folder_id=fid, message_id=r["id"])
                    continue
                partial = index.unclaimed_by_mid(mid) if mid else (
                    index.unclaimed_similar_no_mid(size, epoch) if r["status"] == "in_progress" else [])
                if partial:
                    why = ("interrupted upload; destination has a similar message (same Message-ID or size/date, "
                           "not an exact match). Not re-uploaded to avoid a duplicate.") \
                        if r["status"] == "in_progress" else \
                        "destination already has a message with this Message-ID but different size/date"
                    self.update_message(r["id"], status="needs_review", error_category="ambiguous",
                                        last_error=why)
                    self.event("warning", "needs_review", f"uid {r['source_uid']}: {why}",
                               folder_id=fid, message_id=r["id"])
                elif r["status"] == "in_progress":
                    self.update_message(r["id"], status="retrying", append_attempt_id=None,
                                        last_error="interrupted upload not found on destination; will upload again")
        return t

    def mark_verified(self, msg_id, dest_uv, dest_uid):
        t = db.now()
        self.update_message(msg_id, status="verified", destination_uidvalidity=dest_uv, destination_uid=dest_uid,
                            verified_at=t, last_error=None, error_category=None)
        self.progressed = True

    # -------------------------------------------------------------- copy

    def copy_pending(self, fid, sname, dname, uv):
        for _round in range(MAX_MESSAGE_ATTEMPTS):
            did = False
            for r in self.iter_messages(fid, uv, ("pending", "retrying"), page=200):
                self.check_stop()
                did = True
                self.copy_one(fid, r, dname)
            if not did:
                break
        # anything still retrying after all rounds is a permanent failure
        with db.tx(self.conn):
            self.conn.execute("UPDATE migration_messages SET status='failed', updated_at=? WHERE folder_id=?"
                              " AND source_uidvalidity=? AND status='retrying' AND attempts>=?",
                              (db.now(), fid, uv, MAX_MESSAGE_ATTEMPTS))

    def copy_one(self, fid, r, dname):
        uid = r["source_uid"]
        if r["source_rfc822_size"] > config.MAX_MESSAGE_BYTES:
            self.update_message(r["id"], status="failed", error_category="too_large",
                                last_error=f"{mm.fmt_size(r['source_rfc822_size'])} exceeds the tool limit "
                                           f"({mm.fmt_size(config.MAX_MESSAGE_BYTES)}, MAILMOVE_MAX_MESSAGE_MB)")
            return
        got = self.src.fetch_message(uid)
        if got is None:
            self.update_message(r["id"], status="skipped", skip_reason="deleted_on_source")
            self.event("warning", "deleted_on_source", f"uid {uid} disappeared before it was copied",
                       folder_id=fid, message_id=r["id"])
            return
        flags, idate, raw = got
        # durable attempt record BEFORE the upload: if we die during APPEND, the
        # message stays in_progress and is reconciled before any retry
        self.update_message(r["id"], status="in_progress", append_attempt_id=uuid.uuid4().hex,
                            attempts=r["attempts"] + 1, source_content_fingerprint=hashlib.sha256(raw).hexdigest())
        ok, duv, duid, text = self.dst.append(dname, flags, idate or r["source_internal_date"], raw)
        # (connection errors propagate from append: the message stays in_progress)
        if ok:
            self.update_message(r["id"], status="copied_unverified", copied_at=db.now(),
                                destination_uidvalidity=duv, destination_uid=duid, last_error=None)
            self.progressed = True
            self.set_account(last_activity_at=db.now())
            return
        # server answered NO: the message was definitely NOT stored
        cat, retryable = mm.classify_text(text)
        if cat == "quota":
            self.update_message(r["id"], status="pending", append_attempt_id=None)
            raise PermanentAccountError("quota", self.clean(text))
        if retryable:
            self.update_message(r["id"], status="retrying", append_attempt_id=None, error_category=cat,
                                last_error=self.clean(text))
            raise TransientError(f"{cat}: {text}")
        final = cat == "too_large" or r["attempts"] + 1 >= MAX_MESSAGE_ATTEMPTS
        self.update_message(r["id"], status="failed" if final else "retrying", append_attempt_id=None,
                            error_category=cat, last_error=self.clean(text))
        self.event("error" if final else "warning", cat, f"uid {uid} ({mm.fmt_size(r['source_rfc822_size'])}): {text}",
                   folder_id=fid, message_id=r["id"], retryable=not final, attempt=r["attempts"] + 1)

    # -------------------------------------------------------------- verify (after upload)

    def verify(self, fid, uv, dest_uv, index):
        """Confirm every uploaded message is really in the destination folder."""
        for r in self.iter_messages(fid, uv, ("verified",)):
            if r["destination_uid"] and r["destination_uidvalidity"] == dest_uv:
                index.claim(r["destination_uid"])
        for r in self.iter_messages(fid, uv, ("copied_unverified",)):
            mid, size, epoch = r["source_message_id_header"], r["source_rfc822_size"], r["source_internal_epoch"]
            duid = r["destination_uid"]
            if duid and r["destination_uidvalidity"] == dest_uv and duid in index.by_uid \
                    and duid not in index.claimed and index.by_uid[duid][0] == mid:
                index.claim(duid)
                self.mark_verified(r["id"], dest_uv, duid)
                continue
            uid = index.take_exact(mid, size, epoch)
            if uid:
                self.mark_verified(r["id"], dest_uv, uid)
                continue
            cands = index.unclaimed_by_mid(mid)
            if mid and len(cands) == 1:  # server altered size/date on store; Message-ID is unique here
                index.claim(cands[0])
                self.mark_verified(r["id"], dest_uv, cands[0])
                self.event("info", "verified_by_message_id",
                           f"uid {r['source_uid']}: destination size/date differ from source", folder_id=fid,
                           message_id=r["id"])
                continue
            why = "upload acknowledged but the message was not found on the destination"
            self.update_message(r["id"], status="needs_review", error_category="not_found_after_upload",
                                last_error=why)
            self.event("error", "needs_review", f"uid {r['source_uid']}: {why}", folder_id=fid, message_id=r["id"])


def _pid_alive(pid):
    if not pid or pid == os.getpid():
        return False  # same process: the previous Worker object is gone
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _chunks(seq, n):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ------------------------------------------------------------------ worker

class Worker:
    def __init__(self, db_path=None, mailbox_factory=None):
        self.db_path = db_path or config.db_path()
        self.mailbox_factory = mailbox_factory or mm.Mailbox
        self.worker_id = db.new_worker_id()
        self.shutdown = threading.Event()
        self.started_at = db.now()
        self.current_job = None
        self.conn = db.init_db(self.db_path)

    def beat(self):
        db.heartbeat(self.conn, self.worker_id, os.getpid(), socket.gethostname(), self.current_job, self.started_at)

    def _heartbeat_loop(self):
        conn = db.connect(self.db_path)
        while not self.shutdown.wait(10):
            try:
                db.heartbeat(conn, self.worker_id, os.getpid(), socket.gethostname(), self.current_job,
                             self.started_at)
            except Exception as e:
                log.warning("heartbeat failed: %s", e)

    def startup(self):
        other = db.other_live_worker(self.conn, self.worker_id)
        if other and other["hostname"] == socket.gethostname() and not _pid_alive(other["pid"]):
            other = None  # crashed moments ago (systemd restart); its heartbeat is just not stale yet
        if other:
            raise SystemExit(f"Another worker (pid {other['pid']} on {other['hostname']}) is alive; refusing to start.")
        if not crypto.key_available():
            raise SystemExit("MAILMOVE_KEY is missing or invalid; cannot decrypt mailbox passwords.")
        self.beat()
        for job_id in db.recover_after_crash(self.conn):
            log.warning("recovered job %s after restart; it will resume", job_id)

    def run_forever(self):
        self.startup()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        log.info("worker %s started (db %s)", self.worker_id, self.db_path)
        while not self.shutdown.is_set():
            if not self.run_once():
                self.shutdown.wait(config.POLL_SECONDS)
        with db.tx(self.conn):
            self.conn.execute("DELETE FROM worker_heartbeat WHERE worker_id=?", (self.worker_id,))
        log.info("worker stopped")

    def run_once(self):
        job = db.claim_next_job(self.conn)
        if job is None:
            return False
        self.run_job(job)
        return True

    def run_job(self, job):
        job_id = job["id"]
        self.current_job = job_id
        self.beat()
        log.info("job %s started: %s (dry_run=%s, pass %s)", job_id, job["name"], bool(job["dry_run"]),
                 job["pass_number"])
        accounts = [r["id"] for r in self.conn.execute(
            "SELECT id FROM migration_accounts WHERE job_id=? AND status IN ('queued','running','retrying')"
            " ORDER BY position", (job_id,))]
        with ThreadPoolExecutor(max_workers=max(1, job["max_parallel_accounts"])) as pool:
            futures = {pool.submit(self._run_account, job, a) for a in accounts}
            pending = set(futures)
            while pending:
                done, pending = wait(pending, timeout=5, return_when=FIRST_COMPLETED)
                self._sync_job_status(job_id)
            for f in futures:
                f.result()  # re-raise anything that escaped (e.g. a simulated crash in tests)
        self._finish_job(job_id)
        self.current_job = None

    def _run_account(self, job, account_id):
        try:
            AccountRunner(self, job, account_id).run()
        except Exception as e:  # never let one account's bug take down the others
            log.exception("account %s crashed", account_id)
            conn = db.connect(self.db_path)
            with db.tx(conn):
                conn.execute("UPDATE migration_accounts SET status='failed', last_error=?, updated_at=? WHERE id=?",
                             (mm.sanitize(f"internal: {type(e).__name__}: {e}"), db.now(), account_id))
            conn.close()

    def _sync_job_status(self, job_id, conn=None):
        """Show 'retrying' on the job while any account is waiting to reconnect."""
        conn = conn or self.conn
        with db.tx(conn):
            status = conn.execute("SELECT status FROM migration_jobs WHERE id=?", (job_id,)).fetchone()["status"]
            retrying = conn.execute("SELECT COUNT(*) AS n FROM migration_accounts WHERE job_id=?"
                                    " AND status='retrying'", (job_id,)).fetchone()["n"]
            if status == "running" and retrying:
                db._set_job_status(conn, job_id, "retrying")
            elif status == "retrying" and not retrying:
                db._set_job_status(conn, job_id, "running")
            conn.execute("UPDATE migration_jobs SET last_heartbeat_at=? WHERE id=?", (db.now(), job_id))

    def _finish_job(self, job_id):
        with db.tx(self.conn):
            job = self.conn.execute("SELECT * FROM migration_jobs WHERE id=?", (job_id,)).fetchone()
            statuses = [r["status"] for r in self.conn.execute(
                "SELECT status FROM migration_accounts WHERE job_id=?", (job_id,))]
            t = db.now()
            if job["cancel_requested"]:
                if job["status"] != "cancelling":
                    db._set_job_status(self.conn, job_id, "cancelling")
                db._set_job_status(self.conn, job_id, "cancelled", finished_at=t, cancel_requested=0)
            elif job["pause_requested"]:
                if job["status"] != "pausing":
                    db._set_job_status(self.conn, job_id, "pausing")
                db._set_job_status(self.conn, job_id, "paused", pause_requested=0)
            elif self.shutdown.is_set() or any(s in ("queued", "running", "retrying") for s in statuses):
                db._set_job_status(self.conn, job_id, "queued")  # resume on next start
            elif statuses and all(s == "failed" for s in statuses):
                db._set_job_status(self.conn, job_id, "failed", finished_at=t,
                                   last_error="every account failed; see account errors")
            elif any(s in ("failed", "completed_with_errors") for s in statuses):
                db._set_job_status(self.conn, job_id, "completed_with_errors", finished_at=t)
            else:
                db._set_job_status(self.conn, job_id, "completed", finished_at=t)
        log.info("job %s finished: %s", job_id, db.get_job(self.conn, job_id)["status"])


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    w = Worker()

    def stop(signum, _frame):
        log.info("signal %s: finishing current message, then stopping", signum)
        w.shutdown.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    w.run_forever()


if __name__ == "__main__":
    main()
