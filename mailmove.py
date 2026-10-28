#!/usr/bin/env python3
"""
mailmove.py : copy all mail from one IMAP server to another (any hosting).

Usage:
  python mailmove.py --src-host mail.oldhost.com --dst-host mail.newhost.com --accounts accounts.csv
  python mailmove.py ... --dry-run            # count only, copy nothing
  python mailmove.py ... --workers 4          # migrate 4 mailboxes in parallel
  python mailmove.py ... --skip trash,junk    # skip folders by name or role
  python mailmove.py ... --big-mb 25          # flag emails over 25 MB (default)

The dry run also reports total size, attachment count, large emails and a
rough copy-time estimate.

accounts.csv columns (header required):
  src_user,src_pass,dst_user,dst_pass
  (dst_user / dst_pass may be blank to reuse the source values)

Safe to re-run: messages already on the destination are skipped, so run it
once before switching MX, then again after (delta sync).
Source mailbox is opened read-only and messages are fetched with PEEK, so
read/unread status on the old server is untouched.

Requires Python 3.9+. Standard library only.
"""
import argparse
import csv
import email
import email.policy
import imaplib
import logging
import re
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

imaplib._MAXLINE = 50_000_000  # big mailboxes return long FETCH lines

ROLE_NAMES = {
    "\\sent": {"sent", "sent items", "sent messages", "sent mail"},
    "\\drafts": {"drafts", "draft"},
    "\\trash": {"trash", "deleted items", "deleted messages", "bin"},
    "\\junk": {"junk", "spam", "junk e-mail", "junk email", "bulk mail"},
    "\\archive": {"archive", "archives"},
}

LIST_RE = re.compile(rb'\((?P<flags>[^)]*)\) (?P<delim>"(?:\\.|[^"])*"|NIL) (?P<name>.+)$')
TOKEN_RE = re.compile(rb'\s*(?:(\()|(\))|"((?:\\.|[^"\\])*)"|\{(\d+)\}\r\n|([^\s()"]+))')
MB = 1024 * 1024


# ---------------------------------------------------------------- helpers

def fmt_size(n):
    return f"{n / 1024 ** 3:.2f} GB" if n >= 1024 ** 3 else f"{n / MB:.1f} MB"


def fmt_duration(sec):
    sec = int(sec)
    if sec < 60:
        return "under 1 min"
    h, m = divmod(round(sec / 60), 60)
    return f"{h}h {m}m" if h else f"{m} min"


def estimate_seconds(accounts, workers):
    """Rough (fast, slow) copy time for [{"bytes": n, "count": n}, ...].

    Each message costs a round trip on top of its transfer time, and accounts
    run `workers` at a time, so the biggest account sets the minimum.
    """
    out = []
    for mbps, per_msg in ((4, 0.05), (1, 0.25)):
        times = [a["bytes"] / (mbps * MB) + a["count"] * per_msg for a in accounts]
        out.append(max(max(times, default=0), sum(times) / max(workers, 1)))
    return tuple(out)


def uid_sets(uids, chunk=500):
    """Yield compact UID sets ("1:5,9,12:14") of up to `chunk` UIDs each."""
    uids = sorted(uids)
    for i in range(0, len(uids), chunk):
        part, ranges = uids[i:i + chunk], []
        start = prev = part[0]
        for u in part[1:] + [None]:
            if u is not None and u == prev + 1:
                prev = u
                continue
            ranges.append(f"{start}:{prev}" if start != prev else str(start))
            start = prev = u
        yield ",".join(ranges)


def parse_sexp(buf):
    """Parse IMAP response data into nested lists of str / None."""
    stack, cur, pos = [], [], 0
    while pos < len(buf):
        m = TOKEN_RE.match(buf, pos)
        if not m:
            break
        pos = m.end()
        if m.group(1):
            stack.append(cur)
            cur = []
        elif m.group(2):
            if stack:
                done, cur = cur, stack.pop()
                cur.append(done)
        elif m.group(3) is not None:
            s = m.group(3).replace(b'\\"', b'"').replace(b"\\\\", b"\\")
            cur.append(s.decode("utf-8", "replace"))
        elif m.group(4):
            n = int(m.group(4))
            cur.append(buf[pos:pos + n].decode("utf-8", "replace"))
            pos += n
        else:
            atom = m.group(5).decode("utf-8", "replace")
            cur.append(None if atom.upper() == "NIL" else atom)
    return cur


def pairs(lst):
    """IMAP key/value list -> dict with lowercased keys."""
    if not isinstance(lst, list):
        return {}
    return {str(lst[i]).lower(): lst[i + 1] for i in range(0, len(lst) - 1, 2)}


def count_attachments(part):
    """Count attachments in a parsed BODYSTRUCTURE (inline images with a name count too)."""
    if not isinstance(part, list) or not part:
        return 0
    if isinstance(part[0], list):  # multipart: child parts come first, then subtype
        n = 0
        for p in part:
            if not isinstance(p, list):
                break
            n += count_attachments(p)
        return n
    ctype = f"{part[0]}/{part[1] if len(part) > 1 else ''}".lower()
    params = pairs(part[2]) if len(part) > 2 else {}
    # fixed fields: 7 basic, +1 lines for text, +3 envelope/body/lines for message/rfc822; then md5, disposition
    ext = 7 + (1 if ctype.startswith("text/") else 3 if ctype == "message/rfc822" else 0)
    disp = part[ext + 1] if len(part) > ext + 1 else None
    dtype = str(disp[0]).lower() if isinstance(disp, list) and disp else ""
    dparams = pairs(disp[1]) if isinstance(disp, list) and len(disp) > 1 else {}
    if dtype == "attachment" or "filename" in dparams or "name" in params:
        return 1
    return 1 if ctype == "message/rfc822" else 0  # forwarded email


def quote(name: str) -> str:
    return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'


def unquote(raw: bytes) -> str:
    s = raw.decode("utf-8", "replace").strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    return s


def parse_fetch(data):
    """Turn imaplib FETCH output into [(meta_bytes, literal_bytes), ...]."""
    out, cur = [], None
    for part in data:
        if isinstance(part, tuple):
            if cur:
                out.append(cur)
            cur = [part[0], part[1]]
        elif isinstance(part, bytes) and cur is not None:
            cur[0] += b" " + part  # trailing fields, e.g. " UID 12)"
    if cur:
        out.append(cur)
    return out


def message_key(header: bytes, size: int, idate: bytes) -> str:
    m = re.search(rb"Message-ID:\s*(<[^>]+>|\S+)", header or b"", re.I)
    if m:
        return m.group(1).decode("utf-8", "replace").strip().lower()
    try:
        t = imaplib.Internaldate2tuple(b'INTERNALDATE "' + idate + b'"')
        stamp = str(int(time.mktime(t)))
    except Exception:
        stamp = idate.decode(errors="replace")
    return f"nomid|{size}|{stamp}"


def folder_role(name, flags, delim, prefix):
    for role in ROLE_NAMES:
        if role in flags:
            return role
    rel = name[len(prefix):] if prefix and name.startswith(prefix) else name
    for role, names in ROLE_NAMES.items():
        if rel.lower() in names:
            return role
    return None


# ---------------------------------------------------------------- connection

class Mailbox:
    def __init__(self, host, port, user, password, timeout=180):
        self.host, self.port = host, port
        self.user, self.password = user, password
        self.timeout = timeout
        self.connect()

    def connect(self):
        if self.port == 143:
            self.conn = imaplib.IMAP4(self.host, self.port, timeout=self.timeout)
            try:
                self.conn.starttls()
            except Exception:
                pass
        else:
            self.conn = imaplib.IMAP4_SSL(self.host, self.port, timeout=self.timeout)
        self.conn.login(self.user, self.password)
        self.folders, self.delim = self._list()
        self.prefix = self._namespace_prefix()
        self.roles = {}
        for name, flags in self.folders.items():
            role = folder_role(name, flags, self.delim, self.prefix)
            if role and role not in self.roles:
                self.roles[role] = name

    def reconnect(self):
        try:
            self.conn.logout()
        except Exception:
            pass
        self.connect()

    def close(self):
        try:
            self.conn.logout()
        except Exception:
            pass

    def _list(self):
        typ, data = self.conn.list()
        folders, delims = {}, []
        for item in data or []:
            if item is None:
                continue
            if isinstance(item, tuple):  # folder name sent as literal
                m = LIST_RE.match(item[0])
                name = item[1].decode("utf-8", "replace")
            else:
                m = LIST_RE.match(item)
                name = unquote(m.group("name")) if m else None
            if not m or not name:
                continue
            flags = {f.lower() for f in m.group("flags").decode().split()}
            if "\\noselect" in flags or "\\nonexistent" in flags:
                continue
            d = m.group("delim")
            if d != b"NIL":
                delims.append(unquote(d))
            folders[name] = flags
        delim = max(set(delims), key=delims.count) if delims else "/"
        return folders, delim

    def _namespace_prefix(self):
        try:
            typ, data = self.conn.namespace()
            m = re.match(rb'\(\("((?:\\.|[^"])*)"', data[0] or b"")
            if m:
                return m.group(1).decode()
        except Exception:
            pass
        others = [f for f in self.folders if f.upper() != "INBOX"]
        p = "INBOX" + self.delim
        if others and all(f.upper().startswith(p.upper()) for f in others):
            return p
        return ""

    def ensure_folder(self, name):
        if name in self.folders or name.upper() == "INBOX":
            return
        typ, resp = self.conn.create(quote(name))
        if typ != "OK" and b"exist" not in b" ".join(r or b"" for r in resp).lower():
            raise imaplib.IMAP4.error(f"cannot create folder {name}: {resp}")
        try:
            self.conn.subscribe(quote(name))
        except Exception:
            pass
        self.folders[name] = set()


def map_folder(name, flags, src, dst):
    if name.upper() == "INBOX":
        return "INBOX"
    role = folder_role(name, flags, src.delim, src.prefix)
    if role and role in dst.roles:
        return dst.roles[role]
    rel = name[len(src.prefix):] if src.prefix and name.startswith(src.prefix) else name
    parts = rel.split(src.delim) if src.delim else [rel]
    parts = [p.replace(dst.delim, "_") for p in parts]
    return dst.prefix + dst.delim.join(parts)


def fetch_index(conn, folder):
    """Return {message_key: (uid, size)} for every message in folder."""
    typ, data = conn.select(quote(folder), readonly=True)
    if typ != "OK":
        raise imaplib.IMAP4.error(f"cannot open {folder}: {data}")
    if int(data[0] or 0) == 0:
        return {}
    typ, data = conn.uid(
        "FETCH", "1:*", "(RFC822.SIZE INTERNALDATE BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"
    )
    index = {}
    for meta, lit in parse_fetch(data):
        uid = re.search(rb"UID (\d+)", meta)
        size = re.search(rb"RFC822\.SIZE (\d+)", meta)
        idate = re.search(rb'INTERNALDATE "([^"]+)"', meta)
        if not uid:
            continue
        n = int(size.group(1)) if size else 0
        key = message_key(lit, n, idate.group(1) if idate else b"")
        index.setdefault(key, (int(uid.group(1)), n))
    return index


def scan_attachments(conn, uids):
    """Return total attachments in uids of the selected folder (structure only, no bodies)."""
    total = 0
    for uset in uid_sets(uids):
        typ, data = conn.uid("FETCH", uset, "(BODYSTRUCTURE)")
        if typ != "OK":
            continue
        buf = b" ".join(p[0] + b"\r\n" + p[1] if isinstance(p, tuple) else p for p in data if p)
        for item in parse_sexp(buf):
            if isinstance(item, list):
                total += count_attachments(pairs(item).get("bodystructure"))
    return total


def fetch_summaries(conn, uids):
    """Return {uid: {date, from, subject}} for uids of the selected folder."""
    out = {}
    for uset in uid_sets(uids):
        typ, data = conn.uid("FETCH", uset, "(BODY.PEEK[HEADER.FIELDS (DATE FROM SUBJECT)])")
        if typ != "OK":
            continue
        for meta, lit in parse_fetch(data):
            uid = re.search(rb"UID (\d+)", meta)
            if not uid:
                continue
            msg = email.message_from_bytes(lit or b"", policy=email.policy.default)
            rec = {}
            for h in ("date", "from", "subject"):
                try:
                    rec[h] = str(msg.get(h, "") or "")
                except Exception:
                    rec[h] = "?"
            out[int(uid.group(1))] = rec
    return out


# ---------------------------------------------------------------- copy logic

def copy_one(src, dst, uid, dname):
    typ, data = src.conn.uid("FETCH", str(uid), "(FLAGS INTERNALDATE BODY.PEEK[])")
    parts = parse_fetch(data)
    if typ != "OK" or not parts:
        raise imaplib.IMAP4.error(f"fetch failed: {data}")
    meta, raw = parts[0]
    fl = re.search(rb"FLAGS \(([^)]*)\)", meta)
    flags = [f for f in (fl.group(1).decode().split() if fl else []) if f.lower() != "\\recent"]
    idate = re.search(rb'INTERNALDATE "([^"]+)"', meta)
    date = f'"{idate.group(1).decode()}"' if idate else None
    typ, resp = dst.conn.append(quote(dname), " ".join(flags) or None, date, raw)
    if typ != "OK":
        raise imaplib.IMAP4.error(f"append rejected: {resp}")


def copy_folder(src, dst, sname, sflags, dname, log, dry, progress=None, stop=None, big_bytes=0):
    """Copy missing messages. Returns (copied, failed, stats) where stats holds
    bytes / attachments / big for the messages that needed copying."""
    src_index = fetch_index(src.conn, sname)
    if not dry:
        dst.ensure_folder(dname)
    dst_keys = set(fetch_index(dst.conn, dname)) if dname in dst.folders or dname == "INBOX" else set()
    todo = sorted(v for key, v in src_index.items() if key not in dst_keys)  # [(uid, size)]
    stats = {"bytes": sum(s for _, s in todo), "attachments": 0, "big": []}

    # src.conn is still on sname (read only) after fetch_index
    if dry and todo:
        stats["attachments"] = scan_attachments(src.conn, [u for u, _ in todo])
    big = [(u, s) for u, s in todo if big_bytes and s >= big_bytes]
    if big:
        info = fetch_summaries(src.conn, [u for u, _ in big])
        stats["big"] = [{"folder": sname, "uid": u, "size": s, **info.get(u, {})}
                        for u, s in sorted(big, key=lambda x: -x[1])]

    msg = f"{sname} -> {dname}: {len(src_index)} on source, {len(todo)} to copy ({fmt_size(stats['bytes'])}"
    if dry:
        msg += f", {stats['attachments']} attachments"
    log.info(msg + ")")
    for b in stats["big"]:
        log.warning(f"  large email {fmt_size(b['size'])} in {sname}: {b.get('subject', '')!r}")
    if progress:
        progress("folder", sname, len(todo), stats["bytes"], stats["attachments"], stats["big"])
    if dry or not todo:
        return len(todo), 0, stats

    src.conn.select(quote(sname), readonly=True)
    copied = failed = 0
    for i, (uid, size) in enumerate(todo, 1):
        if stop is not None and stop.is_set():
            log.warning(f"{sname}: stopped by user")
            break
        for attempt in (1, 2):
            try:
                copy_one(src, dst, uid, dname)
                copied += 1
                if progress:
                    progress("copied", sname, size)
                break
            except (imaplib.IMAP4.abort, OSError) as e:  # connection dropped
                if attempt == 2:
                    failed += 1
                    if progress:
                        progress("failed", sname, size)
                    log.warning(f"  uid {uid} failed after reconnect: {e}")
                    break
                log.warning(f"  connection lost ({e}), reconnecting")
                src.reconnect()
                dst.reconnect()
                src.conn.select(quote(sname), readonly=True)
            except imaplib.IMAP4.error as e:  # message rejected (too big etc.)
                failed += 1
                if progress:
                    progress("failed", sname, size)
                log.warning(f"  uid {uid} skipped: {e}")
                break
        if i % 100 == 0:
            log.info(f"  {sname}: {i}/{len(todo)}")
    return copied, failed, stats


def should_skip(name, flags, mbox, skip):
    if not skip:
        return False
    rel = name[len(mbox.prefix):] if mbox.prefix and name.startswith(mbox.prefix) else name
    role = folder_role(name, flags, mbox.delim, mbox.prefix)
    candidates = {name.lower(), rel.lower()}
    if role:
        candidates.add(role.lstrip("\\"))
    return bool(candidates & skip)


def make_logger(user, log_dir):
    log = logging.getLogger(user)
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    fmt = logging.Formatter(f"%(asctime)s [{user}] %(levelname)s %(message)s", "%H:%M:%S")
    safe = re.sub(r"[^\w.@-]", "_", user)
    fh = logging.FileHandler(Path(log_dir) / (safe + ".log"))
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return log


def migrate_account(row, args):
    su, sp = row["src_user"].strip(), row["src_pass"]
    du = (row.get("dst_user") or "").strip() or su
    dp = row.get("dst_pass") or sp
    log = make_logger(su, args.log_dir)
    result = {"account": su, "folders": 0, "copied": 0, "failed": 0, "bytes": 0,
              "attachments": 0, "big": [], "status": "ok", "error": ""}
    big_bytes = int(getattr(args, "big_mb", 25) * MB)
    src = dst = None
    t0 = time.time()
    hook = getattr(args, "progress", None)
    progress = (lambda *a: hook(su, *a)) if hook else None
    stop = getattr(args, "stop_event", None)
    try:
        src = Mailbox(args.src_host, args.src_port, su, sp)
        dst = Mailbox(args.dst_host, args.dst_port, du, dp)
        log.info(f"connected. source delim '{src.delim}' prefix '{src.prefix}', "
                 f"dest delim '{dst.delim}' prefix '{dst.prefix}'")
        if progress:
            progress("start")
        for sname, sflags in sorted(src.folders.items()):
            if stop is not None and stop.is_set():
                result["status"] = "stopped"
                break
            if should_skip(sname, sflags, src, args.skip):
                log.info(f"{sname}: skipped")
                continue
            dname = map_folder(sname, sflags, src, dst)
            try:
                c, f, st = copy_folder(src, dst, sname, sflags, dname, log, args.dry_run,
                                       progress, stop, big_bytes)
                result["folders"] += 1
                result["copied"] += c
                result["failed"] += f
                result["bytes"] += st["bytes"]
                result["attachments"] += st["attachments"]
                result["big"] += [dict(b, account=su) for b in st["big"]]
            except imaplib.IMAP4.error as e:
                log.error(f"{sname}: folder failed: {e}")
                result["status"] = "partial"
        if stop is not None and stop.is_set():
            result["status"] = "stopped"
        elif result["failed"]:
            result["status"] = "partial"
    except Exception as e:
        log.error(f"account failed: {e}")
        result.update(status="error", error=str(e))
    finally:
        for m in (src, dst):
            if m:
                m.close()
    verb = "would copy" if args.dry_run else "copied"
    extra = f", {result['attachments']} attachments" if args.dry_run else ""
    log.info(f"done in {time.time() - t0:.0f}s: {verb} {result['copied']} ({fmt_size(result['bytes'])}{extra}), "
             f"failed {result['failed']}, large emails {len(result['big'])}")
    return result


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Copy mailboxes between IMAP servers.")
    ap.add_argument("--src-host", required=True)
    ap.add_argument("--src-port", type=int, default=993)
    ap.add_argument("--dst-host", required=True)
    ap.add_argument("--dst-port", type=int, default=993)
    ap.add_argument("--accounts", required=True, help="CSV: src_user,src_pass,dst_user,dst_pass")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--skip", default="", help="comma list, e.g. trash,junk")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--big-mb", type=float, default=25, help="flag emails at least this big (MB)")
    ap.add_argument("--log-dir", default="logs")
    args = ap.parse_args()
    args.skip = {s.strip().lower() for s in args.skip.split(",") if s.strip()}
    Path(args.log_dir).mkdir(exist_ok=True)
    socket.setdefaulttimeout(180)

    with open(args.accounts, newline="", encoding="utf-8-sig") as f:
        rows = [r for r in csv.DictReader(f) if (r.get("src_user") or "").strip()]
    if not rows:
        sys.exit("No accounts found in CSV.")

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(migrate_account, r, args) for r in rows]
        for fut in as_completed(futures):
            results.append(fut.result())

    results.sort(key=lambda r: r["account"])
    summary = Path(args.log_dir) / "summary.csv"
    with open(summary, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["account", "folders", "emails", "size_mb", "attachments", "large_emails",
                    "failed", "status", "error"])
        for r in results:
            w.writerow([r["account"], r["folders"], r["copied"], round(r["bytes"] / MB, 1),
                        r["attachments"] if args.dry_run else "", len(r["big"]),
                        r["failed"], r["status"], r["error"]])

    big = [b for r in results for b in r["big"]]
    if big:
        big_csv = Path(args.log_dir) / "large_emails.csv"
        with open(big_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["account", "folder", "size_mb", "date", "from", "subject"])
            for b in big:
                w.writerow([b["account"], b["folder"], round(b["size"] / MB, 1),
                            b.get("date", ""), b.get("from", ""), b.get("subject", "")])

    verb = "to copy" if args.dry_run else "copied"
    print("\n=== SUMMARY ===")
    for r in results:
        att = f" attachments={r['attachments']}" if args.dry_run else ""
        print(f"{r['account']:40} {r['status']:8} {verb}={r['copied']:<7} size={fmt_size(r['bytes']):>10}"
              f"{att} large={len(r['big'])} failed={r['failed']} {r['error']}")
    total = sum(r["bytes"] for r in results)
    print(f"\nTotal {verb}: {sum(r['copied'] for r in results)} emails, {fmt_size(total)}", end="")
    print(f", {sum(r['attachments'] for r in results)} attachments" if args.dry_run else "")
    if big:
        print(f"{len(big)} email(s) over {args.big_mb:g} MB, listed in {big_csv}. "
              "The new host may reject these; check its max message size.")
    if args.dry_run:
        fast, slow = estimate_seconds([{"bytes": r["bytes"], "count": r["copied"]} for r in results],
                                      args.workers)
        print(f"Estimated copy time: {fmt_duration(fast)} to {fmt_duration(slow)} "
              f"with --workers {args.workers} (rough, depends on both servers).")
    print(f"\nSaved to {summary}. Delete accounts.csv when finished (it holds passwords).")


if __name__ == "__main__":
    main()
