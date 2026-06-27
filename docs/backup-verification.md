# Backup Verification

Soroban Pulse runs automated backup verification daily via `.github/workflows/backup-ci.yml`.

## What Is Tested

| Check | Description |
|-------|-------------|
| Encrypted backup creation | Confirms `scripts/backup.sh` produces a `.dump.gpg` file |
| GPG encryption verification | Validates the backup file is genuinely GPG-encrypted |
| Full restore | Runs `scripts/restore.sh` against a fresh Postgres instance |
| Row count match | Compares `COUNT(*)` between source and restored DB |
| Data integrity checksum | Compares `md5(string_agg(tx_hash ...))` across both DBs |
| Timing metrics | Records backup and restore durations for RTO tracking |

## Schedule

Runs daily at 03:00 UTC and on `workflow_dispatch` for manual triggers.

## Manual Trigger

```bash
gh workflow run backup-ci.yml
```

## Local Verification

```bash
# Set up two local Postgres instances, then:
DATABASE_URL="postgres://..." BACKUP_ENCRYPTION_KEY="..." BACKUP_DEST=/tmp/bk bash scripts/backup.sh
DATABASE_URL="postgres://..." BACKUP_ENCRYPTION_KEY="..." bash scripts/restore.sh /tmp/bk/<file>.dump.gpg
```

## Interpreting Failures

| Symptom | Likely cause |
|---------|-------------|
| No `.dump.gpg` file | `pg_dump` failed or GPG not installed |
| GPG verification fails | Wrong `BACKUP_ENCRYPTION_KEY` or corrupt file |
| Row count mismatch | Restore truncated; check `pg_restore` output |
| Checksum mismatch | Data corruption during encrypt/decrypt cycle |

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `BACKUP_ENCRYPTION_KEY` | Passphrase for GPG symmetric encryption |
| `BACKUP_DEST` | Directory where backup files are written |
| `DATABASE_URL` | Source database connection string |
