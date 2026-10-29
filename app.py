"""
MailMove v2 : Streamlit dashboard.

The dashboard never copies mail itself. It saves jobs (with encrypted
passwords) to SQLite and shows their progress; worker.py does the copying,
so restarting or closing this page does not stop a migration.

Run (VPS): systemd unit mailmove-dashboard, bound to 127.0.0.1:8501 (SSH tunnel)
Run (local): streamlit run app.py   (with worker.py running in another terminal)
"""
import io
import json
import time

import pandas as pd
import streamlit as st

import config
import crypto
import database as db
import mailmove as mm

st.set_page_config(page_title="MailMove", page_icon="📬", layout="wide")

STATUS_ICON = {"queued": "⏳ queued", "running": "🔄 running", "retrying": "🔁 retrying", "pausing": "⏸ pausing",
               "paused": "⏸ paused", "cancelling": "⏹ cancelling", "cancelled": "⏹ cancelled",
               "completed": "✅ completed", "completed_with_errors": "⚠️ completed with errors", "failed": "❌ failed"}


@st.cache_resource
def get_conn(path):
    return db.init_db(path)


conn = get_conn(config.db_path())
ss = st.session_state
ss.setdefault("pending_accounts", [])   # plaintext only in this browser session, until the job is created
ss.setdefault("upload_id", None)
ss.setdefault("up_n", 0)
ss.setdefault("job_id", None)


def ago(ts):
    if not ts:
        return ""
    s = time.time() - ts
    return "just now" if s < 5 else f"{mm.fmt_duration(s)} ago" if s >= 60 else f"{int(s)}s ago"


# ------------------------------------------------------------------ header

st.title("📬 MailMove")
st.caption("Copies mailboxes between two hosts over IMAP (TLS, port 993). The old server is read-only. "
           "Progress is saved, so the copy resumes after any disconnect, restart or reboot.")

if not crypto.key_available():
    st.error("MAILMOVE_KEY is not set, so passwords cannot be stored. On the VPS put it in "
             "/etc/mailmove/mailmove.env; locally put it in .env. Generate one with "
             "`python scripts/init_db.py --gen-key`.")
    st.stop()

hb = db.latest_heartbeat(conn)
if hb is None or time.time() - hb["last_heartbeat_at"] > config.HEARTBEAT_STALE_SECONDS:
    st.warning("⚠️ The worker is not running, so queued jobs will not start. "
               "VPS: `sudo systemctl start mailmove-worker`. Local: `python worker.py`.")

tab_new, tab_jobs = st.tabs(["➕ New job", "📊 Jobs"])

# ------------------------------------------------------------------ new job

with tab_new:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("From (old hosting)")
        src_host = st.text_input("IMAP host", placeholder="server123.oldhost.com", key="src_host",
                                 help="Use the 'Incoming server' from cPanel > Email Accounts > Connect Devices.")
    with c2:
        st.subheader("To (new hosting)")
        dst_host = st.text_input("IMAP host ", placeholder="server456.newhost.com", key="dst_host")
    st.caption("🔒 IMAPS on port 993 only, with certificate and hostname verification. No insecure fallback.")

    with st.expander("Options"):
        o1, o2, o3 = st.columns(3)
        workers = o1.slider("Mailboxes in parallel", 1, 8, 2,
                            help="Start with 2 on an unstable host. Increase only if it stays healthy.")
        skip = o2.multiselect("Skip folders", ["trash", "junk", "drafts", "archive"], default=[],
                              help="Nothing is skipped by default.")
        big_mb = o3.number_input("Label emails larger than (MB)", 1, 500, 25,
                                 help="Label only. These emails are still copied automatically, with no approval "
                                      "needed. They are listed so you can check them if the new host rejects one "
                                      "as too large.")

    st.subheader("Accounts")
    up = st.file_uploader("Upload CSV (src_user, src_pass, dst_user, dst_pass). Read in memory only, never saved.",
                          type="csv", key=f"uploader_{ss.up_n}")
    if up is not None and up.file_id != ss.upload_id:
        try:
            df = pd.read_csv(io.BytesIO(up.getvalue()), dtype=str, encoding="utf-8-sig").fillna("")
            df.columns = [c.strip().lower() for c in df.columns]
            if not {"src_user", "src_pass"} <= set(df.columns):
                st.error("CSV needs at least the columns src_user and src_pass.")
            else:
                added = 0
                for rec in df.to_dict("records"):
                    su = rec.get("src_user", "").strip()
                    if su:
                        ss.pending_accounts.append({"src_user": su, "src_pass": rec.get("src_pass", ""),
                                                    "dst_user": rec.get("dst_user", "").strip(),
                                                    "dst_pass": rec.get("dst_pass", "")})
                        added += 1
                st.success(f"Added {added} account(s). Delete the CSV file from your computer after use.")
            ss.upload_id = up.file_id
        except Exception as e:
            st.error(f"Could not read CSV: {mm.sanitize(e)}")

    with st.form("add_account", clear_on_submit=True):
        f1, f2, f3, f4 = st.columns(4)
        a_su = f1.text_input("Source email")
        a_sp = f2.text_input("Source password", type="password")
        a_du = f3.text_input("Destination email", help="Blank = same as source")
        a_dp = f4.text_input("Destination password", type="password", help="Blank = same as source")
        if st.form_submit_button("Add account") and a_su.strip():
            ss.pending_accounts.append({"src_user": a_su.strip(), "src_pass": a_sp,
                                        "dst_user": a_du.strip(), "dst_pass": a_dp})

    accts = ss.pending_accounts
    if accts:
        st.dataframe(pd.DataFrame([{"Source email": a["src_user"], "Destination email": a["dst_user"] or a["src_user"],
                                    "Source password": "••••" if a["src_pass"] else "missing",
                                    "Destination password": "••••" if a["dst_pass"] else "same as source"}
                                   for a in accts]), hide_index=True, width="stretch")
        r1, r2 = st.columns([3, 1])
        remove = r1.multiselect("Remove", [a["src_user"] for a in accts], label_visibility="collapsed",
                                placeholder="Select accounts to remove")
        if r2.button("Remove selected", disabled=not remove):
            ss.pending_accounts = [a for a in accts if a["src_user"] not in remove]
            st.rerun()

    def problems():
        out = []
        if not src_host.strip():
            out.append("Source host is empty.")
        if not dst_host.strip():
            out.append("Destination host is empty.")
        if not accts:
            out.append("Add at least one account.")
        for a in accts:
            if not a["src_pass"]:
                out.append(f"{a['src_user']}: source password missing.")
        users = [a["src_user"].lower() for a in accts]
        dupes = sorted({u for u in users if users.count(u) > 1})
        if dupes:
            out.append("Duplicate accounts: " + ", ".join(dupes))
        return out

    b1, b2, b3, _ = st.columns([1, 1, 1, 2])
    if b1.button("🔌 Test connection", width="stretch", disabled=not accts):
        a = accts[0]
        for side, host, user, pw in (("Source", src_host.strip(), a["src_user"], a["src_pass"]),
                                     ("Destination", dst_host.strip(), a["dst_user"] or a["src_user"],
                                      a["dst_pass"] or a["src_pass"])):
            try:
                with st.spinner(f"{side}: logging in as {user}..."):
                    mb = mm.Mailbox(host, 993, user, pw)
                st.success(f"{side}: TLS certificate valid, logged in as {user}. {len(mb.folders)} folders "
                           f"(separator '{mb.delim}', prefix '{mb.prefix or 'none'}', UIDPLUS: "
                           f"{'yes' if mb.uidplus else 'no'})")
                st.code("\n".join(sorted(mb.folders)), language=None)
                mb.close()
            except Exception as e:
                cat, _ = mm.classify(e)
                st.error(f"{side}: {cat}: {mm.sanitize(e, (pw,))}")
                if mm.REMEDIATION.get(cat):
                    st.info(mm.REMEDIATION[cat])

    def create(dry):
        errs = problems()
        if errs:
            for p in errs:
                st.error(p)
            return
        cfg = dict(source_host=src_host.strip(), source_port=993, destination_host=dst_host.strip(),
                   destination_port=993, dry_run=dry, max_parallel_accounts=workers, skip_folders=skip,
                   large_message_threshold_bytes=int(big_mb) * mm.MB)
        rows = [dict(source_user=a["src_user"], destination_user=a["dst_user"] or a["src_user"],
                     encrypted_source_password=crypto.encrypt(a["src_pass"]),
                     encrypted_destination_password=crypto.encrypt(a["dst_pass"] or a["src_pass"])) for a in accts]
        name = f"{'Dry run' if dry else 'Migration'} {src_host.strip()} → {dst_host.strip()} ({len(rows)} accounts)"
        ss.job_id = db.create_job(conn, name, cfg, rows)
        ss.pending_accounts = []      # plaintext passwords leave the session once encrypted
        ss.up_n += 1
        st.success(f"Job #{ss.job_id} queued. Open the Jobs tab to follow it.")

    if b2.button("🔍 Start dry run", width="stretch"):
        create(True)
    if b3.button("▶ Start migration", type="primary", width="stretch"):
        create(False)
    st.caption("Dry run counts emails, size, attachments and large emails without copying anything.")

# ------------------------------------------------------------------ jobs

with tab_jobs:
    jobs = conn.execute("SELECT id, name, status, dry_run FROM migration_jobs ORDER BY id DESC LIMIT 50").fetchall()
    if not jobs:
        st.info("No jobs yet.")
    else:
        ids = [j["id"] for j in jobs]
        labels = {j["id"]: f"#{j['id']} {STATUS_ICON.get(j['status'], j['status'])} · {j['name']}" for j in jobs}
        default = ids.index(ss.job_id) if ss.job_id in ids else 0
        ss.job_id = st.selectbox("Job", ids, index=default, format_func=lambda i: labels[i])

        @st.fragment(run_every=3)
        def job_view(job_id):
            c = db.connect()
            job = db.get_job(c, job_id)
            dry = bool(job["dry_run"])
            st.subheader(f"{'Dry run' if dry else 'Migration'} #{job_id}: {STATUS_ICON.get(job['status'], job['status'])}"
                         + (f" (pass {job['pass_number']})" if job["pass_number"] > 1 else ""))
            st.caption(f"{job['source_host']} → {job['destination_host']} · {job['max_parallel_accounts']} in parallel · "
                       f"skip: {', '.join(json.loads(job['skip_folders_json'])) or 'nothing'} · "
                       f"worker heartbeat {ago(job['last_heartbeat_at'])}")
            if job["last_error"]:
                st.error(job["last_error"])

            # controls
            k1, k2, k3, k4, _ = st.columns([1, 1, 1, 1.4, 1.6])
            s = job["status"]
            try:
                if k1.button("⏸ Pause", disabled=s not in ("queued", "running", "retrying"), key=f"p{job_id}"):
                    db.request_pause(c, job_id)
                    st.rerun()
                if k2.button("▶ Resume", disabled=s != "paused", key=f"r{job_id}"):
                    db.request_resume(c, job_id)
                    st.rerun()
                if k3.button("⏹ Cancel", disabled=s not in ("queued", "paused", "running", "retrying", "pausing"),
                             key=f"c{job_id}"):
                    db.request_cancel(c, job_id)
                    st.rerun()
                if not dry and k4.button("🔁 Sync again", disabled=s not in db.JOB_TERMINAL, key=f"s{job_id}",
                                         help="Rescan every folder and copy only new or failed mail. Use after "
                                              "the MX cutover. Never deletes destination mail."):
                    db.request_sync_again(c, job_id)
                    st.rerun()
                if dry and k4.button("▶ Start migration from this", disabled=s not in db.JOB_TERMINAL,
                                     key=f"m{job_id}", type="primary"):
                    ss.job_id = db.clone_job(c, job_id, dry_run=False)
                    st.rerun(scope="app")
            except (db.InvalidTransition, RuntimeError) as e:
                st.error(str(e))

            stats = db.job_message_stats(c, job_id)
            accounts = db.accounts_for_report(c, job_id)

            def tot(status, idx=0, acc=None):
                rows = [stats.get(acc, {})] if acc else stats.values()
                return sum(r.get(status, (0, 0))[idx] for r in rows)

            if dry:
                to_copy, to_bytes = tot("pending"), tot("pending", 1)
                present = c.execute(
                    "SELECT COUNT(*) FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
                    " JOIN migration_accounts a ON a.id=f.account_id WHERE a.job_id=? AND m.skip_reason="
                    "'already_on_destination'", (job_id,)).fetchone()[0]
                large = c.execute(
                    "SELECT COUNT(*) FROM migration_messages m JOIN migration_folders f ON f.id=m.folder_id"
                    " JOIN migration_accounts a ON a.id=f.account_id WHERE a.job_id=? AND m.status='pending'"
                    " AND m.source_rfc822_size>=?", (job_id, job["large_message_threshold_bytes"])).fetchone()[0]
                m = st.columns(5)
                m[0].metric("Accounts done", f"{sum(a['status'] in ('completed', 'completed_with_errors', 'failed') for a in accounts)}/{len(accounts)}")
                m[1].metric("Emails to copy", f"{to_copy:,}")
                m[2].metric("Size to copy", mm.fmt_size(to_bytes))
                m[3].metric("Attachments", f"{sum(a['attachments'] for a in accounts):,}")
                m[4].metric(f"Emails ≥ {job['large_message_threshold_bytes'] // mm.MB} MB", f"{large:,}")
                if present:
                    st.caption(f"{present:,} email(s) already exist on the destination and will not be copied again.")
                if to_copy:
                    per = [{"bytes": tot("pending", 1, a["id"]), "count": tot("pending", 0, a["id"])} for a in accounts]
                    fast, slow = mm.estimate_seconds(per, job["max_parallel_accounts"])
                    st.info(f"⏱ **Estimate** (not measured): {mm.fmt_duration(fast)} to {mm.fmt_duration(slow)}, "
                            "assuming 1 to 4 MB/s. The real speed shows during the migration.")
            else:
                verified, vbytes = tot("verified"), tot("verified", 1)
                unverified = tot("copied_unverified")
                remaining = sum(tot(x) for x in ("pending", "retrying", "in_progress"))
                rem_bytes = sum(tot(x, 1) for x in ("pending", "retrying", "in_progress"))
                failed, review = tot("failed"), tot("needs_review")
                total = verified + unverified + remaining + failed + review
                speed = db.recent_throughput(c, job_id)
                m = st.columns(6)
                m[0].metric("Verified", f"{verified:,} / {total:,}")
                m[1].metric("Size verified", mm.fmt_size(vbytes))
                m[2].metric("Remaining", f"{remaining:,} ({mm.fmt_size(rem_bytes)})")
                m[3].metric("Failed", f"{failed:,}")
                m[4].metric("Needs review", f"{review:,}")
                if s in ("running", "retrying") and speed and rem_bytes:
                    m[5].metric("Time left (approx.)", mm.fmt_duration(rem_bytes / speed),
                                f"{speed / mm.MB:.2f} MB/s measured", delta_color="off")
                else:
                    m[5].metric("Speed (last 10 min)", f"{speed / mm.MB:.2f} MB/s" if speed else "–")
                if total:
                    st.progress(min(verified / total, 1.0),
                                text=f"{verified:,} verified on the destination"
                                     + (f", {unverified:,} uploaded and awaiting verification" if unverified else "")
                                     + ". Totals grow while folders are scanned.")

            rows = []
            for a in accounts:
                st_ = stats.get(a["id"], {})
                g = lambda k, i=0: st_.get(k, (0, 0))[i]  # noqa: E731
                r = {"Account": a["source_user"], "Status": STATUS_ICON.get(a["status"], a["status"]),
                     "Folder": a["current_folder"] or ""}
                if dry:
                    r.update({"To copy": g("pending"), "Size": mm.fmt_size(g("pending", 1)),
                              "Attachments": a["attachments"]})
                else:
                    r.update({"Scanned": a["messages_scanned"], "Verified": g("verified"),
                              "Size verified": mm.fmt_size(g("verified", 1)),
                              "Remaining": g("pending") + g("retrying") + g("in_progress"),
                              "Failed": g("failed"), "Review": g("needs_review"),
                              "Reconnects": a["reconnect_count"]})
                r.update({"Last activity": ago(a["last_activity_at"]), "Last error": a["last_error"] or ""})
                rows.append(r)
            table = pd.DataFrame(rows)
            st.dataframe(table, hide_index=True, width="stretch")

            probs = db.problem_messages(c, job_id, job["large_message_threshold_bytes"])
            pdf = pd.DataFrame([{"id": p["id"], "Account": p["source_user"], "Folder": p["source_folder_name"],
                                 "UID": p["source_uid"], "Size (MB)": round(p["source_rfc822_size"] / mm.MB, 1),
                                 "Date": p["source_internal_date"], "Message-ID": p["source_message_id_header"],
                                 "Status": p["status"], "Category": p["error_category"] or p["skip_reason"] or "",
                                 "Error": p["last_error"] or ""} for p in probs])
            if len(pdf):
                with st.expander(f"⚠️ Problem and large emails ({len(pdf)})", expanded=not dry and bool(
                        (pdf["Status"].isin(["failed", "needs_review"])).any())):
                    st.caption("**needs_review**: an interrupted upload could not be proven present or absent, "
                               "or a similar email already exists. It was NOT re-uploaded, to avoid a duplicate. "
                               "Check the destination, then choose below.")
                    st.dataframe(pdf.drop(columns=["id"]), hide_index=True, width="stretch")
                    rv = pdf[pdf["Status"] == "needs_review"]
                    if len(rv):
                        x1, x2, x3 = st.columns([2, 1, 1])
                        pick = x1.multiselect("Needs-review emails", rv["id"].tolist(), key=f"rv{job_id}",
                                              format_func=lambda i: f"#{i} {rv.set_index('id').loc[i, 'Folder']} "
                                                                    f"uid {rv.set_index('id').loc[i, 'UID']}")
                        if x2.button("Accept as present", disabled=not pick, key=f"ra{job_id}"):
                            db.resolve_review(c, job_id, pick, "accept")
                            st.rerun()
                        if x3.button("Copy anyway (may duplicate)", disabled=not pick, key=f"rc{job_id}"):
                            db.resolve_review(c, job_id, pick, "copy")
                            st.toast("Queued for copy. Click Sync again when the job has finished.")

            with st.expander("Event log"):
                ev = c.execute("SELECT created_at, level, category, details FROM job_events WHERE job_id=?"
                               " ORDER BY id DESC LIMIT 200", (job_id,)).fetchall()
                st.code("\n".join(f"{time.strftime('%m-%d %H:%M:%S', time.localtime(e['created_at']))} "
                                  f"{e['level'].upper():7} {e['category']}: {e['details']}" for e in ev) or "–",
                        language=None)

            d1, d2, _ = st.columns([1, 1, 3])
            d1.download_button("⬇ Account report", table.to_csv(index=False).encode(),
                               f"mailmove_job{job_id}_accounts.csv", "text/csv", key=f"da{job_id}")
            d2.download_button("⬇ Problem emails", pdf.drop(columns=["id"], errors="ignore").to_csv(index=False).encode(),
                               f"mailmove_job{job_id}_problems.csv", "text/csv", key=f"dp{job_id}")
            c.close()

        job_view(ss.job_id)

    with st.expander("🧹 Purge stored passwords"):
        st.caption("After the final sync is verified, remove every encrypted password from the database. "
                   "Jobs and reports are kept, but they can no longer run.")
        if st.button("Purge all stored passwords", type="secondary"):
            n = db.purge_credentials(conn)
            st.success(f"Purged credentials for {n} account row(s).")
