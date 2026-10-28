"""
MailMove : Streamlit front end for mailmove.py
Copy mailboxes between any two IMAP servers (different hosting).

Run:
    pip install -r requirements.txt
    streamlit run app.py
"""
import io
import logging
import threading
import time
import types
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import streamlit as st

import mailmove as mm

st.set_page_config(page_title="MailMove", page_icon="📬", layout="wide")

COLUMNS = ["src_user", "src_pass", "dst_user", "dst_pass"]
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


# ------------------------------------------------------------------ job state

class UILogHandler(logging.Handler):
    """Sends script log lines into the running job so the UI can show them."""

    def __init__(self, job):
        super().__init__()
        self.job = job
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s", "%H:%M:%S"))

    def emit(self, record):
        try:
            self.job.logs.append(self.format(record))
        except Exception:
            pass


class Job:
    def __init__(self, rows, cfg):
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.dry_run = cfg["dry_run"]
        self.workers = cfg["workers"]
        self.big_mb = cfg["big_mb"]
        self.started = time.time()
        self.finished = None
        self.logs = deque(maxlen=800)
        self.big = []
        self.accounts = {
            r["src_user"]: {"status": "queued", "folder": "", "to_copy": 0, "copied": 0,
                            "failed": 0, "bytes_todo": 0, "bytes_done": 0, "attachments": 0,
                            "big": 0, "error": ""}
            for r in rows
        }

    def hook(self, user, event, *data):
        with self.lock:
            a = self.accounts[user]
            if event == "start":
                a["status"] = "running"
            elif event == "folder":
                name, n, nbytes, natt, big = data
                a["folder"] = name
                a["to_copy"] += n
                a["bytes_todo"] += nbytes
                a["attachments"] += natt
                a["big"] += len(big)
                self.big += [dict(b, account=user) for b in big]
            elif event in ("copied", "failed"):
                a[event] += 1
                a["bytes_done"] += data[1]

    def finish_account(self, res):
        with self.lock:
            a = self.accounts[res["account"]]
            a.update(status=res["status"], error=res["error"], folder="")
            if self.dry_run:
                a["copied"] = 0
            a["failed"] = res["failed"]

    def snapshot(self):
        with self.lock:
            return {k: dict(v) for k, v in self.accounts.items()}, list(self.big)

    @property
    def running(self):
        return self.finished is None


def run_job(job, rows, args):
    def one(row):
        if job.stop.is_set():
            return {"account": row["src_user"], "folders": 0, "copied": 0,
                    "failed": 0, "status": "stopped", "error": ""}
        return mm.migrate_account(row, args)

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(one, r) for r in rows]
            for fut in as_completed(futures):
                try:
                    job.finish_account(fut.result())
                except Exception as e:  # should not happen, migrate_account catches
                    job.logs.append(f"ERROR worker crashed: {e}")
    finally:
        job.finished = time.time()


def start_job(rows, cfg):
    job = Job(rows, cfg)
    # route every account's log lines into this job (replacing any previous run's handler)
    for r in rows:
        log = mm.make_logger(r["src_user"], str(LOG_DIR))
        for h in list(log.handlers):
            if isinstance(h, UILogHandler):
                log.removeHandler(h)
        log.addHandler(UILogHandler(job))
    args = types.SimpleNamespace(
        src_host=cfg["src_host"], src_port=cfg["src_port"],
        dst_host=cfg["dst_host"], dst_port=cfg["dst_port"],
        workers=cfg["workers"], skip=set(cfg["skip"]), dry_run=cfg["dry_run"], big_mb=cfg["big_mb"],
        log_dir=str(LOG_DIR), progress=job.hook, stop_event=job.stop,
    )
    threading.Thread(target=run_job, args=(job, rows, args), daemon=True).start()
    return job


# ------------------------------------------------------------------ helpers

def clean_rows(df):
    rows = []
    for rec in df.fillna("").to_dict("records"):
        rec = {c: str(rec.get(c, "")).strip() for c in COLUMNS}
        if rec["src_user"]:
            rows.append(rec)
    return rows


def validate(cfg, rows):
    problems = []
    if not cfg["src_host"]:
        problems.append("Source host is empty.")
    if not cfg["dst_host"]:
        problems.append("Destination host is empty.")
    if not rows:
        problems.append("Add at least one account.")
    for r in rows:
        if not r["src_pass"]:
            problems.append(f"{r['src_user']}: source password missing.")
    users = [r["src_user"].lower() for r in rows]
    dupes = {u for u in users if users.count(u) > 1}
    if dupes:
        problems.append("Duplicate accounts: " + ", ".join(sorted(dupes)))
    return problems


def test_connection(cfg, row):
    out = {}
    for side, host, port, user, pw in (
        ("Source", cfg["src_host"], cfg["src_port"], row["src_user"], row["src_pass"]),
        ("Destination", cfg["dst_host"], cfg["dst_port"],
         row["dst_user"] or row["src_user"], row["dst_pass"] or row["src_pass"]),
    ):
        try:
            mb = mm.Mailbox(host, port, user, pw, timeout=30)
            out[side] = (True, mb)
            mb.close()
        except Exception as e:
            out[side] = (False, str(e))
    return out


# ------------------------------------------------------------------ session

ss = st.session_state
ss.setdefault("accounts_df", pd.DataFrame([{c: "" for c in COLUMNS}]))
ss.setdefault("upload_id", None)
ss.setdefault("up_n", 0)
ss.setdefault("job", None)
job = ss.job
busy = job is not None and job.running

# ------------------------------------------------------------------ layout

st.title("📬 MailMove")
st.caption("Copy every mailbox from one email host to another over IMAP. "
           "The old server is read only, and re-running only copies what is missing.")

c1, c2 = st.columns(2)
with c1:
    st.subheader("From (old hosting)")
    src_host = st.text_input("IMAP host", placeholder="mail.oldhost.com", key="src_host", disabled=busy)
    src_port = st.number_input("Port", value=993, step=1, key="src_port", disabled=busy,
                               help="993 = SSL (normal). 143 = STARTTLS.")
with c2:
    st.subheader("To (new hosting)")
    dst_host = st.text_input("IMAP host", placeholder="mail.newhost.com", key="dst_host", disabled=busy)
    dst_port = st.number_input("Port", value=993, step=1, key="dst_port", disabled=busy)

with st.expander("Options", expanded=False):
    o1, o2, o3, o4 = st.columns(4)
    workers = o1.slider("Mailboxes in parallel", 1, 8, 2, disabled=busy,
                        help="Keep low (2 to 3) on shared hosting to avoid throttling.")
    skip = o2.multiselect("Skip folders", ["trash", "junk", "drafts", "archive"],
                          default=[], disabled=busy,
                          help="Nothing is skipped by default. \"junk\" also covers Spam folders.")
    big_mb = o3.number_input("Large email warning (MB)", 1, 500, 25, disabled=busy,
                             help="Emails this big are listed, since many hosts reject over 25 to 50 MB.")
    dry_run = o4.toggle("Dry run (count only, copy nothing)", value=True, disabled=busy,
                        help="Also counts size and attachments, and estimates copy time.")

st.subheader("Accounts")
up = st.file_uploader("Upload CSV (src_user, src_pass, dst_user, dst_pass)", type="csv", disabled=busy,
                      key=f"uploader_{ss.up_n}")
if up is not None and up.file_id != ss.upload_id:
    try:
        df = pd.read_csv(io.BytesIO(up.getvalue()), dtype=str, encoding="utf-8-sig").fillna("")
        df.columns = [c.strip().lower() for c in df.columns]
        missing = [c for c in ("src_user", "src_pass") if c not in df.columns]
        if missing:
            st.error(f"CSV is missing column(s): {', '.join(missing)}")
        else:
            for c in COLUMNS:
                if c not in df.columns:
                    df[c] = ""
            ss.accounts_df = df[COLUMNS]
            ss.upload_id = up.file_id
    except Exception as e:
        st.error(f"Could not read CSV: {e}")

edited = st.data_editor(
    ss.accounts_df,
    num_rows="dynamic",
    width="stretch",
    disabled=busy,
    key=f"editor_{ss.upload_id}",
    column_config={
        "src_user": st.column_config.TextColumn("Source email", required=True),
        "src_pass": st.column_config.TextColumn("Source password"),
        "dst_user": st.column_config.TextColumn("Destination email", help="Blank = same as source"),
        "dst_pass": st.column_config.TextColumn("Destination password", help="Blank = same as source"),
    },
)
rows = clean_rows(edited)
st.caption(f"{len(rows)} account(s). Leave destination fields blank to reuse the source login.")

cfg = dict(src_host=src_host.strip(), src_port=int(src_port), dst_host=dst_host.strip(),
           dst_port=int(dst_port), workers=workers, skip=skip, dry_run=dry_run, big_mb=int(big_mb))

b1, b2, b3, _ = st.columns([1, 1, 1, 3])
test_clicked = b1.button("🔌 Test connection", disabled=busy, width="stretch")
start_clicked = b2.button("▶ Dry run" if dry_run else "▶ Start migration", type="primary",
                          disabled=busy, width="stretch")
stop_clicked = b3.button("■ Stop", disabled=not busy, width="stretch")

if test_clicked:
    problems = [p for p in validate(cfg, rows) if "Duplicate" not in p]
    if problems:
        for p in problems:
            st.error(p)
    else:
        with st.spinner(f"Logging in as {rows[0]['src_user']} on both servers..."):
            res = test_connection(cfg, rows[0])
        tc1, tc2 = st.columns(2)
        for col, side in ((tc1, "Source"), (tc2, "Destination")):
            ok, val = res[side]
            with col:
                if ok:
                    st.success(f"{side}: connected, {len(val.folders)} folders "
                               f"(separator '{val.delim}', prefix '{val.prefix or 'none'}')")
                    st.code("\n".join(sorted(val.folders)) or "(none)", language=None)
                else:
                    st.error(f"{side}: {val}")

if start_clicked:
    problems = validate(cfg, rows)
    if problems:
        for p in problems:
            st.error(p)
    else:
        ss.job = start_job(rows, cfg)
        st.rerun()

if stop_clicked and job:
    job.stop.set()
    st.toast("Stopping after the current message...")


# ------------------------------------------------------------------ progress

def render_progress():
    job = ss.job
    if job is None:
        return
    snap, big = job.snapshot()
    total = sum(a["to_copy"] for a in snap.values())
    copied = sum(a["copied"] for a in snap.values())
    failed = sum(a["failed"] for a in snap.values())
    total_bytes = sum(a["bytes_todo"] for a in snap.values())
    done_bytes = sum(a["bytes_done"] for a in snap.values())
    attachments = sum(a["attachments"] for a in snap.values())
    done = sum(a["status"] not in ("queued", "running") for a in snap.values())
    elapsed = (job.finished or time.time()) - job.started

    st.divider()
    head = "Dry run" if job.dry_run else "Migration"
    if job.running:
        st.subheader(f"{head} running... {'(stopping)' if job.stop.is_set() else ''}")
    else:
        st.subheader(f"{head} finished in {mm.fmt_duration(elapsed)}")

    if job.dry_run:
        m = st.columns(5)
        m[0].metric("Accounts done", f"{done}/{len(snap)}")
        m[1].metric("Emails to copy", f"{total:,}")
        m[2].metric("Size to copy", mm.fmt_size(total_bytes))
        m[3].metric("Attachments", f"{attachments:,}")
        m[4].metric(f"Emails over {job.big_mb} MB", f"{len(big):,}")
        if total:
            fast, slow = mm.estimate_seconds(
                [{"bytes": a["bytes_todo"], "count": a["to_copy"]} for a in snap.values()], job.workers)
            note = "so far, " if job.running else ""
            st.info(f"⏱ Estimated copy time ({note}{job.workers} mailbox(es) in parallel): "
                    f"**{mm.fmt_duration(fast)} to {mm.fmt_duration(slow)}**. "
                    "Rough guess; the real speed depends on both servers.")
    else:
        m = st.columns(5)
        m[0].metric("Accounts done", f"{done}/{len(snap)}")
        m[1].metric("Copied", f"{copied:,} / {total:,}")
        m[2].metric("Size copied", f"{mm.fmt_size(done_bytes)} / {mm.fmt_size(total_bytes)}")
        m[3].metric("Failed", f"{failed:,}")
        speed = done_bytes / elapsed if elapsed > 0 else 0
        if job.running and speed > 0 and elapsed > 20:
            m[4].metric("Time left (est.)", mm.fmt_duration((total_bytes - done_bytes) / speed),
                        f"{speed / mm.MB:.2f} MB/s", delta_color="off")
        else:
            m[4].metric("Speed", f"{speed / mm.MB:.2f} MB/s" if speed else "...")
        if total_bytes:
            st.progress(min(done_bytes / total_bytes, 1.0),
                        text="Totals grow as each folder is scanned, so the bar can step back briefly.")

    icons = {"queued": "⏳ queued", "running": "🔄 running", "ok": "✅ ok",
             "partial": "⚠️ partial", "error": "❌ error", "stopped": "⏹ stopped"}
    recs = []
    for k, v in snap.items():
        r = {"Account": k, "Status": icons.get(v["status"], v["status"]), "Current folder": v["folder"],
             "Emails to copy": v["to_copy"], "Size": mm.fmt_size(v["bytes_todo"])}
        if job.dry_run:
            r["Attachments"] = v["attachments"]
        else:
            r["Copied"] = v["copied"]
            r["Failed"] = v["failed"]
        r["Large emails"] = v["big"]
        r["Error"] = v["error"]
        recs.append(r)
    table = pd.DataFrame(recs)
    st.dataframe(table, width="stretch", hide_index=True)

    big_df = None
    if big:
        big_df = pd.DataFrame([
            {"Account": b["account"], "Folder": b["folder"], "Size (MB)": round(b["size"] / mm.MB, 1),
             "Date": b.get("date", ""), "From": b.get("from", ""), "Subject": b.get("subject", "")}
            for b in sorted(big, key=lambda b: -b["size"])
        ])
        with st.expander(f"⚠️ {len(big)} email(s) over {job.big_mb} MB", expanded=not job.running):
            st.caption("The new host may reject these if they exceed its max message size. "
                       "Check its limit, or ask users to save those attachments elsewhere.")
            st.dataframe(big_df, width="stretch", hide_index=True)

    with st.expander("Live log", expanded=job.running):
        st.code("\n".join(list(job.logs)[-200:]) or "Waiting...", language=None)

    if not job.running:
        csv_bytes = table.drop(columns=["Current folder"]).to_csv(index=False).encode()
        d1, d2, d3, _ = st.columns([1, 1, 1, 2])
        d1.download_button("⬇ Summary CSV", csv_bytes, "mailmove_summary.csv", "text/csv",
                           width="stretch")
        d2.download_button("⬇ Full log", "\n".join(job.logs).encode(), "mailmove_log.txt",
                           width="stretch")
        if big_df is not None:
            d3.download_button("⬇ Large emails", big_df.to_csv(index=False).encode(),
                               "mailmove_large_emails.csv", "text/csv", width="stretch")
        if job.dry_run:
            st.info("Dry run done. Turn off Dry run in Options and click Start migration to copy for real.")
        else:
            st.info("Done. After switching MX records, run the migration again to pick up late emails "
                    "(only new messages will be copied). Then clear the passwords below.")


@st.fragment(run_every=1.5)
def live_progress():
    render_progress()
    if ss.job is not None and not ss.job.running and ss.get("_was_running"):
        ss._was_running = False
        st.rerun()  # re-enable the inputs once the job ends
    if ss.job is not None and ss.job.running:
        ss._was_running = True


live_progress()

st.divider()
if st.button("🧹 Clear accounts and passwords", disabled=busy):
    ss.accounts_df = pd.DataFrame([{c: "" for c in COLUMNS}])
    ss.upload_id = f"cleared-{time.time()}"
    ss.up_n += 1
    st.rerun()
