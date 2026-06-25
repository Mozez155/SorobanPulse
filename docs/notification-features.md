# Notification System — Advanced Features

## Maintenance Windows (#495)

Suppress notifications during planned maintenance to avoid false-positive alerts.

### API

**Create a maintenance window**
```
POST /v1/admin/maintenance-windows
Authorization: Bearer <ADMIN_API_KEY>

{
  "start_time": "2026-07-01T02:00:00Z",
  "end_time":   "2026-07-01T04:00:00Z",
  "contract_ids": ["CABC...", "CDEF..."],   // optional — empty means all contracts
  "description": "Contract upgrade"
}
```

**List active / upcoming windows**
```
GET /v1/admin/maintenance-windows
```

**Cancel a window**
```
DELETE /v1/admin/maintenance-windows/:id
```

### How it works

When a notification is about to be sent, the system checks `maintenance_windows` for any
window that overlaps `NOW()` and matches the event's `contract_id`.  If a match is found
the notification is suppressed and the
`soroban_pulse_notifications_maintenance_suppressed_total` counter is incremented.

### Metric

| Metric | Type | Description |
|--------|------|-------------|
| `soroban_pulse_notifications_maintenance_suppressed_total` | counter | Number of notifications suppressed by a maintenance window |

---

## Notification Audit Log (#496)

Every notification attempt is recorded in a `notification_audit_log` table for compliance auditing.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `channel_type` | TEXT | `webhook`, `email`, or `sms` |
| `recipient` | TEXT | URL / email address / phone number |
| `event_id` | TEXT | Triggering event ID (nullable) |
| `triggered_at` | TIMESTAMPTZ | When the notification was initiated |
| `delivered_at` | TIMESTAMPTZ | When the notification was confirmed delivered (nullable) |
| `status` | TEXT | `pending`, `delivered`, or `failed` |
| `error` | TEXT | Error message on failure (nullable) |

### API

**Query the audit log**
```
GET /v1/admin/notifications/audit-log
    ?from=2026-07-01T00:00:00Z
    &to=2026-07-02T00:00:00Z
    &channel_type=webhook
    &status=failed
    &limit=100
    &offset=0
```

### Retention

Set `NOTIFICATION_AUDIT_LOG_RETENTION_DAYS` (default: `90`) to control how long entries
are kept.  A background job runs once per day and deletes entries older than the threshold.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NOTIFICATION_AUDIT_LOG_RETENTION_DAYS` | `90` | Days to retain audit log entries |

---

## Template Versioning (#497)

Notification templates are stored in the database with version numbers.  Activating a
previous version takes effect immediately without a service restart.

### API

**Create a new template version**
```
POST /v1/admin/notification-templates
Authorization: Bearer <ADMIN_API_KEY>

{
  "name": "event_alert",
  "subject": "New event indexed: {{contract_id}}",
  "body": "Event {{id}} was indexed at ledger {{ledger}}."
}
```
The version number is assigned automatically (max existing version + 1).

**List versions for a template**
```
GET /v1/admin/notification-templates/:name/versions
```

**Activate a specific version**
```
POST /v1/admin/notification-templates/:name/activate/:version
```
All other versions for that template are deactivated atomically.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `name` | TEXT | Template name |
| `version` | INT | Version number (auto-incremented per name) |
| `subject` | TEXT | Email subject line |
| `body` | TEXT | Email body |
| `is_active` | BOOL | Whether this version is currently in use |
| `created_at` | TIMESTAMPTZ | Creation timestamp |

---

## Channel Health Checks (#498)

A background task periodically probes each notification channel and exposes a health gauge.

### How it works

- **Webhooks**: sends a `GET` request to the configured URL and checks for a 2xx response.
- **Email**: marks healthy when `NOTIFICATION_HEALTH_CHECK_EMAIL` is configured.

The check runs every `NOTIFICATION_HEALTH_CHECK_INTERVAL_SECS` seconds (default: 300).

### Metric

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `soroban_pulse_notification_channel_healthy` | gauge | `channel`, `type` | `1` = healthy, `0` = unhealthy |

### Alert

The `NotificationChannelUnhealthy` alert in `docs/alerts.yml` fires at **critical** severity
when a channel gauge stays at `0` for 5 minutes.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NOTIFICATION_HEALTH_CHECK_INTERVAL_SECS` | `300` | Seconds between health checks |
| `NOTIFICATION_HEALTH_CHECK_EMAIL` | — | Address used for email channel liveness checks |
