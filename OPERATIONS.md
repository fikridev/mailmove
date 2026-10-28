# MailMove runbook: 16 GB migration before 31 October 2026

The commands assume the droplet is deployed per `README.md` and you are connected with
`ssh -N -L 8501:127.0.0.1:8501 root@YOUR_DROPLET_IP` (dashboard at http://localhost:8501).
Shorthand used below:

```bash
mm() { sudo -u mailmove bash -c "set -a; . /etc/mailmove/mailmove.env; set +a; cd /opt/mailmove; .venv/bin/python $*"; }
```

## 0. Before anything (today)

- [ ] Ask the old host for a **full cPanel account backup** (or a short service extension). This is the safety net if their server disappears early.
- [ ] Confirm **every destination mailbox exists** on the new host, with enough **quota** (sum of each account's size plus 20%).
- [ ] Get both **IMAP hostnames** from each cPanel, under Email Accounts, then Connect Devices: the "secure incoming server", port 993. Don't use `mail.yourdomain`: it moves when DNS changes.
- [ ] Check both certificates from the droplet:
      `openssl s_client -connect HOST:993 -servername HOST -verify_return_error </dev/null | grep -E "Verify return|subject="`
      You want `Verify return code: 0 (ok)`.
- [ ] Note the destination's **maximum message size** (often 25 to 50 MB). Larger emails will be reported as failed.
- [ ] Lower the domain's **MX TTL** to 300 seconds now, so the cutover propagates quickly later.

## 1. Acceptance test on ONE small mailbox (28 Oct, about 1 hour)

Use a small or test account (for example `testmailtransfer@`). Do not continue until every step passes.

1. **Test connection**: New job tab, enter both hosts, add the test account, click **🔌 Test connection**. Both sides should say "TLS certificate valid" and list the folders.
2. **Dry run**: click **🔍 Start dry run**. Compare "Emails to copy" and the size with cPanel's disk usage for that account. Trash and Junk are included.
3. **Migrate**: **▶ Start migration from this**. When it's done, open the account on the new host (webmail) and check Inbox, Sent, any custom folder, read/unread state, dates, and a couple of attachments.
4. **Retry**: start another small job, then simulate an outage by blocking the old host for 2 minutes:
   `iptables -I OUTPUT -d OLD_IMAP_IP -p tcp --dport 993 -j REJECT` and later `iptables -D OUTPUT -d OLD_IMAP_IP -p tcp --dport 993 -j REJECT`.
   The job should show 🔁 retrying, then continue by itself.
5. **Dashboard restart**: `systemctl restart mailmove-dashboard`. The job keeps running (check `journalctl -u mailmove-worker -f`).
6. **Worker restart**: `systemctl restart mailmove-worker` mid-copy. It resumes, and the job ends with no duplicates (the folder count on the new host equals the source).
7. **Rerun**: click **🔁 Sync again** on the finished job. It should finish quickly with nothing new copied.
8. **Verification report**: `mm scripts/verify_migration.py --job N`. Every folder should be `OK`.

## 2. Bulk migration (29 Oct)

1. **Dry run for all accounts** (upload the CSV, then **🔍 Start dry run**). Note the total size, the large emails, and anything "already on destination". Delete the CSV from your Mac afterwards.
2. **▶ Start migration from this**, with **2 mailboxes in parallel** (the default).
3. Watch for the first 30 to 60 minutes:
   - "MB/s measured" and "Time left" on the dashboard (these are measured, not estimated).
   - Reconnects per account. A few is fine; constant reconnects mean the old host is struggling.
   - `journalctl -u mailmove-worker -f` for `throttled` or `timeout`.
4. Only if the old host stays healthy (no throttling, few reconnects): pause the job, then create a new job with 3 in parallel for the remaining accounts. Never jump straight to 8.
5. Back up the database every few hours: `sudo -u mailmove sqlite3 /var/lib/mailmove/data/mailmove.db ".backup /var/lib/mailmove/backup/mm-$(date +%F-%H%M).db"`

**If the old host goes offline:** do nothing. The job shows 🔁 retrying and resumes on its own when the host comes back, even hours later. **Priority accounts:** if time is short, pause, then start a job containing only the critical mailboxes (the CSV order is the processing order).

## 3. Verify and cut over (30 Oct)

1. Job status should be ✅ completed. If it's ⚠️ completed with errors, open **Problem and large emails**:
   - `failed / too_large`: ask the user to save the attachment, or raise the new host's limit and click Sync again.
   - `failed / quota`: raise the quota, then Sync again.
   - `needs_review`: look in that destination folder. If the email is there, **Accept as present**; if not, **Copy anyway**, then Sync again.
   - `skipped / deleted_on_source`: the email was deleted on the old server during the copy. Usually fine; tell the user.
2. Run the independent check: `mm scripts/verify_migration.py --job N --csv /var/lib/mailmove/verify-N.csv`
3. **Switch MX** (and SPF/DKIM/autodiscover as the new host requires) to the new host.
4. Wait for the old TTL to expire, then click **🔁 Sync again** to copy mail that was still delivered to the old server. It never deletes anything on the destination.
5. Send test emails from outside to a few addresses and confirm they arrive on the **new** host.

## 4. Final reconciliation (31 Oct, before the old host shuts down)

1. **🔁 Sync again** one last time, then `verify_migration.py` again. Keep the CSV report.
2. Download the **Account report** and **Problem emails** CSVs from the dashboard. They contain no passwords.
3. Keep the old host's full backup (step 0) somewhere independent of both hosts.
4. Recreate by hand anything IMAP cannot move: forwarders, aliases, filters, autoresponders, and calendars or contacts if used.
5. Users update their mail apps to the new server settings.

## 5. After the migration

```bash
mm scripts/purge_credentials.py --all          # wipes the stored encrypted passwords and VACUUMs the DB
systemctl disable --now mailmove-worker mailmove-dashboard
shred -u /var/lib/mailmove/backup/*.db           # old backups also contain encrypted passwords
```
Remove the `MAILMOVE_KEY` line from `/etc/mailmove/mailmove.env` once no job needs to run again.

## Remaining risks

| Risk | Mitigation |
|---|---|
| Old host disappears before the copy finishes | Get the provider's full backup now (step 0). The tool cannot recover mail from a server that is gone. |
| Destination rewrites messages, so sizes differ | Uploads with a confirmed UID are verified by UID plus Message-ID. Interrupted ones with only a partial match go to needs_review (no duplicates, but manual review). |
| Destination without UIDPLUS | Verification falls back to Message-ID + size + date matching. Tested. |
| Emails above the destination size limit | Reported as `failed / too_large`, never silently skipped. |
| Wrong password for an account | The account fails after 3 attempts. There's no in-place credential editing yet: create a new job for that account. |
| 16 GB will take longer than estimated | Throughput depends on the old host. The dashboard shows measured MB/s and time left after the first minutes. Start early and prioritise critical mailboxes. |
| Tests use a simulated IMAP server | The acceptance test in section 1 against real servers is mandatory before the bulk run. |

## Deferred (not needed for the deadline)

Editing credentials of an existing job · per-account priority controls · dashboard login (access is via SSH tunnel only) · STARTTLS / non-993 ports · automatic scheduled DB backups · byte-for-byte content verification (would download every message twice).
