"""
mailmove.py : IMAP primitives for MailMove v2.

Used by worker.py (the migration engine) and app.py (connection test). This
module never writes to the source: folders are opened with EXAMINE, bodies are
read with BODY.PEEK[], and no STORE / EXPUNGE / COPY / MOVE / DELETE is issued.

Connections are IMAPS on port 993 only (ssl.create_default_context): the
certificate chain, expiry and hostname are always verified, for both source and
destination. There is no STARTTLS and no plaintext fallback.
"""
import imaplib
import re
import socket
import ssl
import time
from collections import defaultdict

import config

imaplib._MAXLINE = 50_000_000  # big mailboxes return long FETCH lines

MB = 1024 * 1024

ROLE_NAMES = {
    "\\sent": {"sent", "sent items", "sent messages", "sent mail"},
    "\\drafts": {"drafts", "draft"},
    "\\trash": {"trash", "deleted items", "deleted messages", "bin"},
    "\\junk": {"junk", "spam", "junk e-mail", "junk email", "bulk mail"},
    "\\archive": {"archive", "archives"},
}

LIST_RE = re.compile(rb'\((?P<flags>[^)]*)\) (?P<delim>"(?:\\.|[^"])*"|NIL) (?P<name>.+)$')
TOKEN_RE = re.compile(rb'\s*(?:(\()|(\))|"((?:\\.|[^"\\])*)"|\{(\d+)\}\r\n|([^\s()"]+))')
APPENDUID_RE = re.compile(rb"\[APPENDUID (\d+) (\d+)\]", re.I)
MESSAGE_ID_RE = re.compile(rb"^Message-ID:\s*(<[^>\r\n]+>|\S+)", re.I | re.M)
META_ITEMS = "(UID RFC822.SIZE INTERNALDATE FLAGS BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"


# ---------------------------------------------------------------- formatting / estimates

def fmt_size(n):
    n = n or 0
    return f"{n / 1024 ** 3:.2f} GB" if n >= 1024 ** 3 else f"{n / MB:.1f} MB"


def fmt_duration(sec):
    sec = int(sec)
    if sec < 60:
        return "under 1 min"
    h, m = divmod(round(sec / 60), 60)
    return f"{h}h {m}m" if h else f"{m} min"


def estimate_seconds(accounts, workers):
    """Rough (fast, slow) copy time for [{"bytes": n, "count": n}, ...]. An ESTIMATE:
    assumes 1-4 MB/s plus a round trip per message; accounts run `workers` at a time."""
    out = []
    for mbps, per_msg in ((4, 0.05), (1, 0.25)):
        times = [a["bytes"] / (mbps * MB) + a["count"] * per_msg for a in accounts]
        out.append(max(max(times, default=0), sum(times) / max(workers, 1)))
    return tuple(out)


# ---------------------------------------------------------------- IMAP parsing helpers

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
    for part in data or []:
        if isinstance(part, tuple):
            if cur:
                out.append(cur)
            cur = [part[0], part[1]]
        elif isinstance(part, bytes) and re.match(rb"\d+ \(", part):
            if cur:  # a response without any literal (e.g. empty header sent as "")
                out.append(cur)
            cur = [part, b""]
        elif isinstance(part, bytes) and cur is not None:
            cur[0] += b" " + part  # trailing fields, e.g. " UID 12)"
    if cur:
        out.append(cur)
    return out


def internal_epoch(idate):
    """INTERNALDATE string -> epoch seconds (None if unparseable)."""
    if not idate:
        return None
    try:
        t = imaplib.Internaldate2tuple(b'INTERNALDATE "' + idate.encode() + b'"')
        return int(time.mktime(t)) if t else None
    except Exception:
        return None


def parse_meta(meta, literal):
    uid = re.search(rb"UID (\d+)", meta)
    if not uid:
        return None
    size = re.search(rb"RFC822\.SIZE (\d+)", meta)
    idate = re.search(rb'INTERNALDATE "([^"]+)"', meta)
    flags = re.search(rb"FLAGS \(([^)]*)\)", meta)
    mid = MESSAGE_ID_RE.search(literal or b"")
    idate_s = idate.group(1).decode() if idate else None
    return {
        "uid": int(uid.group(1)),
        "size": int(size.group(1)) if size else 0,
        "idate": idate_s,
        "epoch": internal_epoch(idate_s),
        "mid": mid.group(1).decode("utf-8", "replace").strip() if mid else None,
        "flags": flags.group(1).decode() if flags else "",
    }


def folder_role(name, flags, delim, prefix):
    for role in ROLE_NAMES:
        if role in flags:
            return role
    rel = name[len(prefix):] if prefix and name.startswith(prefix) else name
    for role, names in ROLE_NAMES.items():
        if rel.lower() in names:
            return role
    return None


# ---------------------------------------------------------------- errors

class ImapNo(imaplib.IMAP4.error):
    """Server answered NO to a command (the command did not take effect)."""


def classify(exc):
    """-> (category, retryable). Transient errors are retried with backoff;
    permanent ones need operator action."""
    if isinstance(exc, ssl.SSLCertVerificationError):
        return "tls_certificate", False
    if isinstance(exc, ssl.SSLError):
        return "tls", True  # handshake reset etc.; certificate problems are caught above
    if isinstance(exc, socket.gaierror):
        return "dns", True
    if isinstance(exc, ConnectionRefusedError):
        return "connection_refused", True
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return "timeout", True
    if isinstance(exc, (imaplib.IMAP4.abort, OSError, EOFError)):
        return "connection", True
    if isinstance(exc, imaplib.IMAP4.error):
        return classify_text(str(exc))
    return "internal", False


def classify_text(text):
    t = (text or "").lower()
    if any(k in t for k in ("authenticationfailed", "authentication failed", "login failed",
                            "invalid credentials", "authorizationfailed", "incorrect password",
                            "bad username", "username or password", "invalid login", "login incorrect")):
        return "auth", False
    if "overquota" in t or "quota" in t:
        return "quota", False
    if any(k in t for k in ("toobig", "too big", "too large", "message size", "size limit", "exceeds")):
        return "too_large", False
    if any(k in t for k in ("unavailable", "throttl", "too many", "try again", "temporar", "busy",
                            "[limit]", "inuse", "[serverbug]")):
        return "throttled", True
    if any(k in t for k in ("nonexistent", "doesn't exist", "does not exist", "no such mailbox", "unknown mailbox")):
        return "missing_mailbox", False
    return "imap_error", False


REMEDIATION = {
    "auth": "Login rejected. Check the email address and password for this account, then click Sync again.",
    "tls_certificate": "The server's TLS certificate is invalid, expired or issued to a different name. "
                       "Do not disable verification: use the secure incoming server hostname from cPanel > "
                       "Email Accounts > Connect Devices (often like server123.hostingprovider.com). Check it with: "
                       "openssl s_client -connect HOST:993 -servername HOST -verify_return_error </dev/null",
    "tls": "TLS handshake failed on port 993. Check the hostname and that the server offers IMAPS on 993.",
    "quota": "Destination mailbox is full. Increase its quota on the new host, then click Sync again.",
    "too_large": "The destination rejected this message as too large. Raise the host's limit or save the "
                 "attachment elsewhere.",
    "missing_mailbox": "Folder disappeared or cannot be created.",
}


def sanitize(text, secrets=()):
    """Short, single-line error text with any known secret removed."""
    s = str(text or "")
    for sec in secrets:
        if sec:
            s = s.replace(sec, "***")
    s = re.sub(r"\s+", " ", s).strip()
    return s[:300]


# ---------------------------------------------------------------- connection

IMAPS_PORT = 993


def tls_context():
    """Default context: CERT_REQUIRED, hostname checking, system CA store, modern TLS."""
    return ssl.create_default_context()


class Mailbox:
    def __init__(self, host, port, user, password, security="ssl", timeout=None):
        if int(port) != IMAPS_PORT or security != "ssl":
            raise ValueError("Only IMAPS on port 993 is supported (no STARTTLS, no plaintext).")
        self.host, self.port = host, IMAPS_PORT
        self.user, self.password = user, password
        self.security = "ssl"
        self.timeout = timeout or config.IMAP_TIMEOUT
        self.conn = None
        self.connect()

    def _open(self):
        """IMAPS with certificate + hostname verification and a socket timeout."""
        return imaplib.IMAP4_SSL(self.host, IMAPS_PORT, ssl_context=tls_context(), timeout=self.timeout)

    def connect(self):
        self.conn = self._open()
        self.conn.login(self.user, self.password)
        try:
            typ, data = self.conn.capability()
            caps = data[0].decode().upper().split() if typ == "OK" and data and data[0] else []
        except Exception:
            caps = [c.upper() for c in getattr(self.conn, "capabilities", ())]
        self.capabilities = set(caps)
        self.folders, self.delim = self._list()
        self.prefix = self._namespace_prefix()
        self.roles = {}
        for name, flags in sorted(self.folders.items()):
            role = folder_role(name, flags, self.delim, self.prefix)
            if role and role not in self.roles:
                self.roles[role] = name

    @property
    def uidplus(self):
        return "UIDPLUS" in self.capabilities

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

    def _resp_int(self, code):
        try:
            typ, data = self.conn.response(code)
            if data and data[-1] is not None:
                return int(data[-1].split()[0])
        except Exception:
            pass
        return None

    def examine(self, folder):
        """Open read-only (EXAMINE). -> {uidvalidity, uidnext, exists}"""
        typ, data = self.conn.select(quote(folder), readonly=True)
        if typ != "OK":
            raise ImapNo(f"cannot open folder {folder}: {data}")
        exists = int(data[0] or 0) if data and data[0] else 0
        uv, un = self._resp_int("UIDVALIDITY"), self._resp_int("UIDNEXT")
        if uv is None:
            typ, data = self.conn.status(quote(folder), "(UIDVALIDITY UIDNEXT)")
            if typ == "OK" and data and data[0]:
                m = re.search(rb"UIDVALIDITY (\d+)", data[0])
                n = re.search(rb"UIDNEXT (\d+)", data[0])
                uv = int(m.group(1)) if m else None
                un = int(n.group(1)) if n else un
        if uv is None:
            raise ImapNo(f"server did not report UIDVALIDITY for {folder}")
        return {"uidvalidity": uv, "uidnext": un, "exists": exists}

    def uid_search_all(self):
        typ, data = self.conn.uid("SEARCH", None, "ALL")
        if typ != "OK":
            raise ImapNo(f"UID SEARCH failed: {data}")
        return sorted({int(x) for part in data if part for x in part.split()})

    def fetch_meta(self, uids, chunk=500):
        """Yield metadata dicts (uid, size, idate, epoch, mid, flags) for the selected folder."""
        for uset in uid_sets(uids, chunk):
            typ, data = self.conn.uid("FETCH", uset, META_ITEMS)
            if typ != "OK":
                raise ImapNo(f"FETCH failed: {data}")
            for meta, lit in parse_fetch(data):
                m = parse_meta(meta, lit)
                if m:
                    yield m

    def fetch_message(self, uid):
        """-> (flags list, internaldate str, raw bytes), or None if the UID no longer exists."""
        typ, data = self.conn.uid("FETCH", str(uid), "(UID FLAGS INTERNALDATE BODY.PEEK[])")
        if typ != "OK":
            raise ImapNo(f"FETCH failed: {data}")
        for meta, raw in parse_fetch(data):
            m = re.search(rb"UID (\d+)", meta)
            if (m and int(m.group(1)) != uid) or (not m and not raw):
                continue  # unsolicited FETCH (e.g. a flag update for another message)
            fl = re.search(rb"FLAGS \(([^)]*)\)", meta)
            flags = [f for f in (fl.group(1).decode().split() if fl else []) if f.lower() != "\\recent"]
            idate = re.search(rb'INTERNALDATE "([^"]+)"', meta)
            return flags, (idate.group(1).decode() if idate else None), raw
        return None

    def scan_attachments(self, uids):
        """Total attachments in uids of the selected folder (structure only, no bodies)."""
        total = 0
        for uset in uid_sets(uids):
            typ, data = self.conn.uid("FETCH", uset, "(BODYSTRUCTURE)")
            if typ != "OK":
                continue
            buf = b" ".join(p[0] + b"\r\n" + p[1] if isinstance(p, tuple) else p for p in data if p)
            for item in parse_sexp(buf):
                if isinstance(item, list):
                    total += count_attachments(pairs(item).get("bodystructure"))
        return total

    def ensure_folder(self, name):
        if name in self.folders or name.upper() == "INBOX":
            return
        typ, resp = self.conn.create(quote(name))
        text = b" ".join(r or b"" for r in resp).decode("utf-8", "replace")
        if typ != "OK" and "exist" not in text.lower():
            raise ImapNo(f"cannot create folder {name}: {text}")
        try:
            self.conn.subscribe(quote(name))
        except Exception:
            pass
        self.folders[name] = set()

    def append(self, folder, flags, idate, raw):
        """APPEND one message. -> (ok, dest_uidvalidity, dest_uid, response_text).
        ok=False means the server answered NO, so the message was NOT stored.
        Connection errors are raised: the outcome is then unknown."""
        date = f'"{idate}"' if idate else None
        typ, resp = self.conn.append(quote(folder), " ".join(flags) or None, date, raw)
        text = b" ".join(r or b"" for r in (resp or [])).decode("utf-8", "replace")
        if typ != "OK":
            return False, None, None, text
        m = APPENDUID_RE.search(" ".join(text.split()).encode())
        return True, (int(m.group(1)) if m else None), (int(m.group(2)) if m else None), text


# ---------------------------------------------------------------- folder planning

def should_skip(name, flags, mbox, skip):
    if not skip:
        return False
    rel = name[len(mbox.prefix):] if mbox.prefix and name.startswith(mbox.prefix) else name
    role = folder_role(name, flags, mbox.delim, mbox.prefix)
    candidates = {name.lower(), rel.lower()}
    if role:
        candidates.add(role.lstrip("\\"))
    return bool(candidates & {s.lower() for s in skip})


def translate_name(name, src, dst):
    rel = name[len(src.prefix):] if src.prefix and name.startswith(src.prefix) else name
    parts = rel.split(src.delim) if src.delim else [rel]
    parts = [p.replace(dst.delim, "_") if dst.delim else p for p in parts]
    return dst.prefix + dst.delim.join(parts)


def plan_folders(src, dst, skip=(), existing=None):
    """-> [(source_name, flags, destination_name)], INBOX first.

    Every source folder gets its OWN destination folder: special folders map to
    the destination's Sent/Trash/Junk/... only once, and any name collision gets a
    " (2)" suffix instead of merging. `existing` ({source: destination}) keeps
    mappings chosen on an earlier pass stable."""
    existing = dict(existing or {})
    used = {d.lower() for d in existing.values()}
    plan = []
    names = sorted(src.folders, key=lambda n: (n.upper() != "INBOX", n))
    for sname in names:
        flags = src.folders[sname]
        if should_skip(sname, flags, src, skip):
            continue
        if sname in existing:
            plan.append((sname, flags, existing[sname]))
            continue
        dname = None
        if sname.upper() == "INBOX":
            dname = "INBOX"
        else:
            role = folder_role(sname, flags, src.delim, src.prefix)
            if role and role in dst.roles and dst.roles[role].lower() not in used:
                dname = dst.roles[role]
            if dname is None:
                dname = translate_name(sname, src, dst)
        base, n = dname, 2
        while dname.lower() in used:
            dname = f"{base} ({n})"
            n += 1
        used.add(dname.lower())
        plan.append((sname, flags, dname))
    return plan


# ---------------------------------------------------------------- destination index

class DestIndex:
    """In-memory view of ONE destination folder (uid -> Message-ID, size, date),
    used to match source messages to destination copies. Each destination
    message can be claimed by at most one source message, so duplicates on
    either side are counted correctly."""

    def __init__(self, metas=()):
        self.by_uid = {}
        self.by_key = defaultdict(list)
        self.by_mid = defaultdict(list)
        self.claimed = set()
        for m in metas:
            self.add(m)

    def add(self, m):
        self.by_uid[m["uid"]] = (m["mid"], m["size"], m["epoch"])
        self.by_key[(m["mid"], m["size"], m["epoch"])].append(m["uid"])
        if m["mid"]:
            self.by_mid[m["mid"]].append(m["uid"])

    def __len__(self):
        return len(self.by_uid)

    def claim(self, uid):
        self.claimed.add(uid)

    def take_exact(self, mid, size, epoch):
        for uid in self.by_key.get((mid, size, epoch), ()):
            if uid not in self.claimed:
                self.claimed.add(uid)
                return uid
        return None

    def unclaimed_by_mid(self, mid):
        return [u for u in self.by_mid.get(mid, ()) if u not in self.claimed] if mid else []

    def unclaimed_similar_no_mid(self, size, epoch):
        """Destination messages without a Message-ID sharing size OR date: weak evidence."""
        return [u for u, (mid, s, e) in self.by_uid.items()
                if u not in self.claimed and mid is None and (s == size or (epoch and e == epoch))]


def load_dest_index(mbox, folder):
    """EXAMINE folder on mbox and index it. -> (uidvalidity, DestIndex)"""
    info = mbox.examine(folder)
    uids = mbox.uid_search_all() if info["exists"] else []
    return info["uidvalidity"], DestIndex(mbox.fetch_meta(uids) if uids else ())
