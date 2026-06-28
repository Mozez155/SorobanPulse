# Notification Delivery Receipts

_Issue #475_

The notification system records a **delivery receipt** for every notification
delivery attempt. This lets operators audit whether a notification was
successfully delivered — a common compliance requirement for critical alerts.

## What is recorded

Each delivery attempt is written to the `notification_deliveries` table:

| Column              | Description                                              |
| ------------------- | -------------------------------------------------------- |
| `id`                | Receipt identifier (UUID).                               |
| `channel_type`      | Channel the notification went through (`webhook`, `email`). |
| `channel_config_id` | Optional reference to a configured channel.              |
| `event_id`          | The originating event (`events.id`), when resolvable.    |
| `status`            | `success` or `failure`.                                  |
| `delivered_at`      | When the attempt completed.                              |
| `error`             | Error detail when `status = 'failure'`.                  |

Webhook deliveries record one receipt per event. Email deliveries are batched,
so one receipt is recorded per event included in the batch.

## Querying delivery history

```
GET /v1/admin/notifications/deliveries
```

Query parameters:

| Parameter      | Description                                        |
| -------------- | -------------------------------------------------- |
| `channel_type` | Filter by channel type (`webhook`, `email`).       |
| `status`       | Filter by status (`success` or `failure`).         |
| `limit`        | Max receipts to return (default 100, max 1000).    |

Example:

```bash
curl -H "X-API-Key: $ADMIN_API_KEY" \
  "https://your-host/v1/admin/notifications/deliveries?channel_type=webhook&status=failure&limit=50"
```

Response:

```json
{
  "count": 1,
  "deliveries": [
    {
      "id": "f0e1d2c3-...",
      "channel_type": "webhook",
      "channel_config_id": null,
      "event_id": "a1b2c3d4-...",
      "status": "failure",
      "delivered_at": "2026-06-25T12:00:00Z",
      "error": "HTTP 500: Internal Server Error"
    }
  ]
}
```

An invalid `status` value returns `400 Bad Request`.

## Metrics

Two counters are exported on `/metrics`:

- `soroban_pulse_notification_delivery_success_total` — successful deliveries.
- `soroban_pulse_notification_delivery_failure_total` — failed deliveries.
