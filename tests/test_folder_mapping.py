import types

import mailmove as mm


def box(folders, delim=".", prefix="INBOX."):
    b = types.SimpleNamespace(folders=folders, delim=delim, prefix=prefix, roles={})
    for name, flags in sorted(folders.items()):
        role = mm.folder_role(name, flags, delim, prefix)
        if role and role not in b.roles:
            b.roles[role] = name
    return b


CPANEL = {"INBOX": set(), "INBOX.Sent": {"\\sent"}, "INBOX.Junk": {"\\junk"}, "INBOX.spam": set(),
          "INBOX.Trash": {"\\trash"}, "INBOX.Clients.2024": set(), "INBOX.Clients": set()}


def test_cpanel_to_cpanel_keeps_every_folder_separate():
    plan = mm.plan_folders(box(CPANEL), box({"INBOX": set(), "INBOX.Sent": {"\\sent"}, "INBOX.Junk": {"\\junk"},
                                             "INBOX.Trash": {"\\trash"}}))
    m = {s: d for s, _, d in plan}
    assert plan[0][0] == "INBOX"
    assert m["INBOX.Junk"] == "INBOX.Junk"
    assert m["INBOX.spam"] == "INBOX.spam"          # NOT merged into Junk
    assert m["INBOX.Clients.2024"] == "INBOX.Clients.2024"
    assert len(set(m.values())) == len(m)


def test_cpanel_to_slash_server_without_prefix():
    dst = box({"INBOX": set(), "Sent Items": {"\\sent"}, "Junk Email": {"\\junk"}, "Deleted Items": {"\\trash"}},
              delim="/", prefix="")
    m = {s: d for s, _, d in mm.plan_folders(box(CPANEL), dst)}
    assert m["INBOX.Sent"] == "Sent Items"
    assert m["INBOX.Trash"] == "Deleted Items"
    assert m["INBOX.Junk"] == "Junk Email"
    assert m["INBOX.spam"] == "spam"
    assert m["INBOX.Clients.2024"] == "Clients/2024"


def test_gmail_like_source_and_delimiter_inside_name():
    src = box({"INBOX": set(), "[Gmail]/Sent Mail": {"\\sent"}, "Work/a.b": set(), "Work/a_b": set()},
              delim="/", prefix="")
    dst = box({"INBOX": set()})
    m = {s: d for s, _, d in mm.plan_folders(src, dst)}
    assert m["Work/a.b"] == "INBOX.Work.a_b"
    assert m["Work/a_b"] == "INBOX.Work.a_b (2)"     # collision gets a suffix, never merged
    assert len(set(m.values())) == len(m)


def test_existing_mapping_is_stable_and_skip():
    src, dst = box(CPANEL), box({"INBOX": set(), "INBOX.Junk": {"\\junk"}})
    first = {s: d for s, _, d in mm.plan_folders(src, dst)}
    again = {s: d for s, _, d in mm.plan_folders(src, dst, existing=first)}
    assert first == again
    skipped = {s for s, _, _ in mm.plan_folders(src, dst, skip={"junk", "trash"})}
    assert "INBOX.Junk" not in skipped and "INBOX.spam" not in skipped and "INBOX.Trash" not in skipped
    assert "INBOX.Sent" in skipped


def test_non_ascii_modified_utf7_names_round_trip():
    # IMAP modified UTF-7 names are passed through byte-for-byte
    name = "INBOX.Caf&AOk-"
    m = {s: d for s, _, d in mm.plan_folders(box({"INBOX": set(), name: set()}), box({"INBOX": set()}))}
    assert m[name] == name


def test_default_skip_is_nothing():
    plan = mm.plan_folders(box(CPANEL), box({"INBOX": set()}))
    assert {s for s, _, _ in plan} == set(CPANEL)
