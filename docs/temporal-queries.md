# Temporal Event Queries

The `/v1/events/temporal` endpoint lets you query events using **relative time expressions** or absolute timestamps, with optional time-bucket aggregation.

## Endpoint

```
GET /v1/events/temporal
```

## Query Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `since` | string | Relative start of the window. E.g. `"24h"`, `"1d"`, `"7d"`, `"30m"`, `"1w"`. Mutually exclusive with `from_timestamp`. |
| `before` | string | Relative end of the window (default: now). Same format as `since`. Mutually exclusive with `to_timestamp`. |
| `from_timestamp` | string | Absolute ISO 8601 start. Mutually exclusive with `since`. |
| `to_timestamp` | string | Absolute ISO 8601 end. |
| `contract_id` | string | Filter by contract ID. |
| `event_type` | string | Filter by event type: `contract`, `diagnostic`, `system`. |
| `aggregate` | bool | When `true`, return bucketed counts instead of raw events. |
| `window` | string | Aggregation bucket size: `"1m"`, `"5m"`, `"1h"` (default), `"1d"`. Only used when `aggregate=true`. |
| `limit` | int | Max results (default: 100, max: 1000). |
| `page` | int | Page number for offset pagination (default: 1). |

## Relative Time Syntax

Relative time expressions consist of a positive integer followed by a unit suffix:

| Suffix | Unit |
|--------|------|
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days |
| `w` | weeks |

**Examples:** `30s`, `5m`, `24h`, `1d`, `7d`, `2w`

## Examples

### All events in the last 24 hours

```
GET /v1/events/temporal?since=24h
```

### Events from a specific contract in the last 7 days

```
GET /v1/events/temporal?since=7d&contract_id=CABC...
```

### Hourly aggregated counts over the past week

```
GET /v1/events/temporal?since=1w&aggregate=true&window=1h
```

### Absolute window with aggregation

```
GET /v1/events/temporal?from_timestamp=2026-01-01T00:00:00Z&to_timestamp=2026-01-02T00:00:00Z&aggregate=true&window=1d
```

### Events in the last 30 minutes before 2 hours ago

```
GET /v1/events/temporal?since=2h30m... (use before=2h&since=2h30m)
GET /v1/events/temporal?since=150m&before=120m
```

## Response Shape

```json
{
  "from": "2026-06-26T03:00:00Z",
  "to": "2026-06-27T03:00:00Z",
  "events": [...],
  "buckets": [],
  "total": 42
}
```

When `aggregate=true`:

```json
{
  "from": "2026-06-20T03:00:00Z",
  "to": "2026-06-27T03:00:00Z",
  "events": [],
  "buckets": [
    {
      "bucket_start": "2026-06-20T03:00:00Z",
      "event_count": 120,
      "contract_count": 3
    }
  ],
  "total": 168
}
```

`events` and `buckets` are mutually exclusive — only one will be populated depending on whether `aggregate` is set.

## Error Cases

| Error | Cause |
|-------|-------|
| 400 – `since` and `from_timestamp` are mutually exclusive | Both provided |
| 400 – `before` and `to_timestamp` are mutually exclusive | Both provided |
| 400 – either `since` or `from_timestamp` is required | Neither provided |
| 400 – start of window must be before end of window | `from >= to` |
| 400 – invalid relative time expression | Unknown unit or non-positive value |
| 400 – invalid window | `window` not one of `1m`, `5m`, `1h`, `1d` |
