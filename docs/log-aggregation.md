# Log Aggregation Setup

Soroban Pulse emits structured JSON logs when `RUST_LOG_FORMAT=json`. This document covers integration with ELK Stack, Datadog, and AWS CloudWatch.

## Enabling JSON Logs

```bash
RUST_LOG_FORMAT=json RUST_LOG=info ./soroban-pulse
```

Example log line:

```json
{
  "timestamp": "2026-06-27T03:00:00.000Z",
  "level": "INFO",
  "message": "Event indexed",
  "contract_id": "CABC...XYZ",
  "ledger": 54321,
  "target": "soroban_pulse::indexer"
}
```

---

## ELK Stack Integration

### Filebeat Configuration

```yaml
# filebeat.yml
filebeat.inputs:
  - type: container
    paths:
      - /var/lib/docker/containers/*/*.log
    processors:
      - decode_json_fields:
          fields: ["message"]
          target: ""
          overwrite_keys: true
      - add_fields:
          target: service
          fields:
            name: soroban-pulse

output.elasticsearch:
  hosts: ["http://elasticsearch:9200"]
  index: "soroban-pulse-%{+yyyy.MM.dd}"

setup.template.settings:
  index.number_of_shards: 1
```

### Logstash Pipeline

```ruby
# logstash/pipeline/soroban-pulse.conf
input {
  beats { port => 5044 }
}

filter {
  if [service][name] == "soroban-pulse" {
    json { source => "message" }
    date { match => ["timestamp", "ISO8601"] target => "@timestamp" }
    mutate {
      rename => { "level" => "log.level" }
      rename => { "target" => "log.logger" }
    }
  }
}

output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "soroban-pulse-%{+YYYY.MM.dd}"
  }
}
```

### Key Fields for Kibana

| Field | Type | Description |
|-------|------|-------------|
| `log.level` | keyword | INFO, WARN, ERROR, DEBUG |
| `contract_id` | keyword | Soroban contract identifier |
| `ledger` | long | Ledger sequence number |
| `correlation_id` | keyword | Request trace ID |
| `error` | text | Error message (when present) |

---

## Datadog Log Forwarding

### datadog-agent.yaml

```yaml
# /etc/datadog-agent/conf.d/soroban_pulse.d/conf.yaml
logs:
  - type: docker
    service: soroban-pulse
    source: rust
    tags:
      - env:production
      - team:indexer
    processing_rules:
      - type: multi_line
        name: new_log_start
        pattern: '^\{"timestamp"'
```

### Datadog Log Pipelines

Create a pipeline in Datadog with these processors:

1. **JSON parser** — parse the full log line as JSON
2. **Date remapper** — map `timestamp` → `@timestamp`
3. **Status remapper** — map `level` → log status (`INFO`→info, `WARN`→warning, `ERROR`→error)
4. **Attribute remapper** — map `target` → `logger.name`

### Useful Datadog Queries

```
service:soroban-pulse level:ERROR                        # All errors
service:soroban-pulse @contract_id:CABC*                 # Contract-specific logs
service:soroban-pulse @ledger:[50000 TO 60000]           # Ledger range
service:soroban-pulse @correlation_id:<id>               # Request trace
```

---

## AWS CloudWatch Integration

### CloudWatch Agent Configuration

```json
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/soroban-pulse/*.log",
            "log_group_name": "/soroban-pulse/application",
            "log_stream_name": "{instance_id}",
            "timestamp_format": "%Y-%m-%dT%H:%M:%S",
            "timezone": "UTC",
            "multi_line_start_pattern": "^\\{\"timestamp\""
          }
        ]
      }
    }
  }
}
```

### Kubernetes: Fluent Bit → CloudWatch

```yaml
# fluent-bit-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluent-bit-config
data:
  fluent-bit.conf: |
    [SERVICE]
        Flush        5
        Log_Level    info

    [INPUT]
        Name             tail
        Path             /var/log/containers/soroban-pulse*.log
        Parser           docker
        Tag              soroban.pulse.*
        Refresh_Interval 5

    [FILTER]
        Name    parser
        Match   soroban.pulse.*
        Key_Name log
        Parser  json

    [OUTPUT]
        Name              cloudwatch_logs
        Match             soroban.pulse.*
        region            us-east-1
        log_group_name    /soroban-pulse/application
        log_stream_prefix soroban-pulse-
        auto_create_group true
```

### CloudWatch Insights Queries

```sql
-- Error rate over time
fields @timestamp, level, message, error
| filter level = "ERROR"
| stats count() as error_count by bin(5m)
| sort @timestamp desc

-- Top error messages
fields message, error
| filter level = "ERROR"
| stats count() as occurrences by message
| sort occurrences desc
| limit 20

-- Indexer lag events
fields @timestamp, ledger, message
| filter message like /indexer lag/
| sort @timestamp desc
```

---

## Structured Log Parsing Reference

All Soroban Pulse log fields follow the conventions in `docs/logging.md`.

| Field | Present When | Example |
|-------|-------------|---------|
| `timestamp` | Always | `2026-06-27T03:00:00Z` |
| `level` | Always | `INFO` |
| `message` | Always | `Event indexed` |
| `target` | Always | `soroban_pulse::indexer` |
| `contract_id` | Event context | `CABC...XYZ` |
| `tx_hash` | Event context | `a1b2c3...` |
| `ledger` | Event context | `54321` |
| `error` | On errors/warnings | `connection timeout` |
| `correlation_id` | HTTP requests | `550e8400-e29b-41d4-a716` |
| `attempt` | Retries | `2` |
