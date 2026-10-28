"""
In-process fake IMAP server + connection that mimics the imaplib.IMAP4 API
surface used by mailmove.Mailbox, with fault injection for tests.

Responses are shaped exactly like imaplib returns them (tuples for literals,
untagged response codes via .response()), so the real parsing code runs.
"""
import imaplib
import re
import threading
from dataclasses import dataclass, field


class SimulatedCrash(BaseException):
    """Kills the worker thread like a SIGKILL would: not caught by `except Exception`."""


@dataclass
class Msg:
    raw: bytes
    flags: list = field(default_factory=list)
    idate: str = "01-Jan-2024 10:00:00 +0000"


class Folder:
    def __init__(self, uidvalidity, flags=()):
        self.uidvalidity = uidvalidity
        self.uidnext = 1
        self.msgs = {}  # uid -> Msg
        self.flags = set(flags)

    def add(self, msg):
        uid = self.uidnext
        self.msgs[uid] = msg
        self.uidnext += 1
        return uid


class FakeServer:
    """One IMAP host. users: {login: password}. Folders per user."""

    def __init__(self, users, delim=".", prefix="INBOX.", uidplus=True, folders=("Sent", "Drafts", "Trash", "Junk"),
                 special_use=True):
        self.users = dict(users)
        self.delim, self.prefix, self.uidplus = delim, prefix, uidplus
        self.lock = threading.RLock()
        self.mailboxes = {}
        self._uv = 1000
        self.down = 0              # refuse this many connection attempts (int) or forever (True)
        self.faults = []           # list of dicts: {"op":..., "mode":..., "count": n, "match": callable}
        self.max_size = None       # destination size limit
        self.quota_bytes = None
        self.alter_on_append = None  # callable(raw)->raw: simulate a server that rewrites messages
        self.commands = []         # (user, command, args) log for read-only assertions
        self.connections = 0
        for u in self.users:
            self.mailboxes[u] = {}
            self.create_folder(u, "INBOX")
            for f in folders:
                role = {"Sent": "\\Sent", "Drafts": "\\Drafts", "Trash": "\\Trash", "Junk": "\\Junk"}.get(f)
                self.create_folder(u, prefix + f if prefix else f, flags=[role] if (role and special_use) else [])

    def create_folder(self, user, name, flags=()):
        with self.lock:
            self._uv += 1
            self.mailboxes[user][name] = Folder(self._uv, flags)
            return self.mailboxes[user][name]

    def folder(self, user, name):
        return self.mailboxes[user][name]

    def add(self, user, folder, raw, flags=("\\Seen",), idate="01-Jan-2024 10:00:00 +0000"):
        if folder not in self.mailboxes[user]:
            self.create_folder(user, folder)
        return self.mailboxes[user][folder].add(Msg(raw, list(flags), idate))

    def reset_uidvalidity(self, user, folder):
        """Renumber a folder like a server rebuild would."""
        with self.lock:
            old = self.mailboxes[user][folder]
            self._uv += 1
            new = Folder(self._uv, old.flags)
            for m in old.msgs.values():
                new.add(m)
            self.mailboxes[user][folder] = new

    def fault(self, op, mode="abort", count=1, match=None):
        self.faults.append({"op": op, "mode": mode, "count": count, "match": match})

    def take_fault(self, op, **ctx):
        with self.lock:
            for f in self.faults:
                if f["op"] == op and f["count"] > 0 and (f["match"] is None or f["match"](ctx)):
                    f["count"] -= 1
                    return f["mode"]
        return None

    def count(self, user, folder):
        return len(self.mailboxes[user].get(folder, Folder(0)).msgs)

    def all_raw(self, user, folder):
        return [m.raw for m in self.mailboxes[user][folder].msgs.values()]


def make_msg(i, size=0, mid=True, subject=None, body=b""):
    hdr = ""
    if mid:
        hdr += f"Message-ID: <m{i}@test>\r\n" if mid is True else f"Message-ID: {mid}\r\n"
    hdr += f"Subject: {subject or f'Message {i}'}\r\nFrom: a@test\r\nDate: Mon, 1 Jan 2024 10:00:00 +0000\r\n\r\n"
    return hdr.encode() + (body or f"body {i}\r\n".encode()) + b"x" * size


class FakeConn:
    def __init__(self, server, host):
        self.server, self.host = server, host
        self.user = None
        self.sel = None
        self.readonly = None
        self.untagged = {}
        if server.down is True or (isinstance(server.down, int) and server.down > 0):
            if server.down is not True:
                server.down -= 1
            raise ConnectionRefusedError(111, "Connection refused")
        if server.take_fault("connect"):
            raise ConnectionRefusedError(111, "Connection refused")
        server.connections += 1
        self.capabilities = ("IMAP4REV1", "UIDPLUS") if server.uidplus else ("IMAP4REV1",)

    # --- helpers
    def _log(self, cmd, *args):
        self.server.commands.append((self.user, cmd, args))

    def _fault(self, op, **ctx):
        mode = self.server.take_fault(op, user=self.user, folder=self.sel, **ctx)
        if mode == "abort":
            raise imaplib.IMAP4.abort("socket error: EOF")
        if mode == "timeout":
            raise TimeoutError("timed out")
        if mode == "crash":
            raise SimulatedCrash(op)
        return mode

    def _box(self):
        return self.server.mailboxes[self.user]

    # --- imaplib API
    def login(self, user, password):
        self._log("LOGIN", user)
        if self.server.users.get(user) != password:
            raise imaplib.IMAP4.error("b'[AUTHENTICATIONFAILED] Authentication failed.'")
        self.user = user
        return "OK", [b"Logged in"]

    def capability(self):
        return "OK", [" ".join(self.capabilities).encode()]

    def list(self):
        self._log("LIST")
        out = []
        for name, f in self._box().items():
            fl = " ".join(sorted(f.flags | {"\\HasNoChildren"}))
            out.append(f'({fl}) "{self.server.delim}" "{name}"'.encode())
        return "OK", out

    def namespace(self):
        if self.server.prefix:
            return "OK", [f'(("{self.server.prefix}" "{self.server.delim}")) NIL NIL'.encode()]
        return "OK", [f'(("" "{self.server.delim}")) NIL NIL'.encode()]

    def select(self, mailbox, readonly=False):
        name = mailbox.strip('"')
        self._log("EXAMINE" if readonly else "SELECT", name)
        self._fault("select", name=name)
        f = self._box().get(name)
        if f is None:
            return "NO", [b"[NONEXISTENT] Mailbox doesn't exist"]
        self.sel, self.readonly = name, readonly
        self.untagged = {"UIDVALIDITY": [str(f.uidvalidity).encode()], "UIDNEXT": [str(f.uidnext).encode()]}
        return "OK", [str(len(f.msgs)).encode()]

    def response(self, code):
        data = self.untagged.pop(code.upper(), None)
        return code, data if data is not None else [None]

    def status(self, mailbox, items):
        f = self._box()[mailbox.strip('"')]
        return "OK", [f'{mailbox} (MESSAGES {len(f.msgs)} UIDNEXT {f.uidnext} UIDVALIDITY {f.uidvalidity})'.encode()]

    def create(self, mailbox):
        name = mailbox.strip('"')
        self._log("CREATE", name)
        if name in self._box():
            return "NO", [b"[ALREADYEXISTS] Mailbox already exists"]
        self.server.create_folder(self.user, name)
        return "OK", [b"Create completed"]

    def subscribe(self, mailbox):
        return "OK", [b""]

    def logout(self):
        return "BYE", [b""]

    def _uids(self, uset, f):
        if uset == "1:*":
            return sorted(f.msgs)
        out = []
        for r in uset.split(","):
            a, _, b = r.partition(":")
            out += [u for u in range(int(a), int(b or a) + 1) if u in f.msgs]
        return out

    def uid(self, cmd, *args):
        cmd = cmd.upper()
        self._log("UID " + cmd, *args)
        f = self._box()[self.sel]
        if cmd == "SEARCH":
            self._fault("search")
            return "OK", [" ".join(str(u) for u in sorted(f.msgs)).encode()]
        if cmd != "FETCH":
            raise AssertionError(f"unexpected UID {cmd}")
        uset, what = args
        if "BODY[" in what and ".PEEK" not in what:
            raise AssertionError("non-PEEK body fetch would set \\Seen on the source")
        out = []
        for seq, u in enumerate(self._uids(uset, f), 1):
            m = f.msgs[u]
            flags = " ".join(m.flags)
            if "BODYSTRUCTURE" in what:
                out.append(f'{seq} (UID {u} BODYSTRUCTURE ("TEXT" "PLAIN" NIL NIL NIL "7BIT" {len(m.raw)} 1 NIL NIL NIL NIL))'.encode())
            elif "HEADER.FIELDS (MESSAGE-ID)" in what:
                self._fault("fetch_meta")
                h = re.search(rb"^Message-ID:[^\r\n]*\r\n", m.raw, re.M | re.I)
                h = (h.group(0) if h else b"") + b"\r\n"
                line = (f'{seq} (UID {u} RFC822.SIZE {len(m.raw)} INTERNALDATE "{m.idate}" FLAGS ({flags}) '
                        f'BODY[HEADER.FIELDS (MESSAGE-ID)] {{{len(h)}}}')
                out += [(line.encode(), h), b")"]
            elif "BODY.PEEK[]" in what:
                self._fault("fetch_body", uid=u)
                line = f'{seq} (UID {u} FLAGS ({flags}) INTERNALDATE "{m.idate}" BODY[] {{{len(m.raw)}}}'
                out += [(line.encode(), m.raw), b")"]
        return "OK", out

    def append(self, mailbox, flags, date_time, message):
        name = mailbox.strip('"')
        self._log("APPEND", name)
        srv = self.server
        self._fault("append_before", name=name)
        if srv.max_size and len(message) > srv.max_size:
            return "NO", [b"[TOOBIG] Message too large"]
        if srv.quota_bytes is not None:
            used = sum(len(m.raw) for fo in self._box().values() for m in fo.msgs.values())
            if used + len(message) > srv.quota_bytes:
                return "NO", [b"[OVERQUOTA] Quota exceeded (mailbox for user is full)"]
        mode = srv.take_fault("append_no", user=self.user, folder=name)
        if mode:
            return "NO", [mode.encode()]
        f = self._box().get(name)
        if f is None:
            return "NO", [b"[TRYCREATE] Mailbox doesn't exist"]
        raw = srv.alter_on_append(message) if srv.alter_on_append else message
        fl = [x for x in (flags or "").strip("()").split() if x]
        idate = date_time.strip('"') if date_time else "01-Jan-2024 10:00:00 +0000"
        uid = f.add(Msg(raw, fl, idate))
        self._fault("append_after", name=name)   # stored, but the client never hears back
        if srv.uidplus:
            return "OK", [f"[APPENDUID {f.uidvalidity} {uid}] Append completed.".encode()]
        return "OK", [b"Append completed."]


SOURCE_MUTATIONS = {"STORE", "EXPUNGE", "MOVE", "COPY", "DELETE", "UID STORE", "UID EXPUNGE", "UID MOVE",
                    "UID COPY", "APPEND", "CREATE", "SELECT"}
