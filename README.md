# IoT event sink

## Sink

### Usage

```bash
# Start with a config file
go run ./cmd/sink -config config.yaml

# Enable journal encryption
JOURNAL__ENCRYPTION_KEY=$(openssl rand -base64 32) \
  go run ./cmd/sink -config config.yaml
```

**Flags:**
- `-config`: Path to the YAML configuration file.

### Configuration

```yaml
server:
  addr: ":8080"
  read_timeout: 10s
  write_timeout: 10s

sink:
  buffer_size: 128
  flush_interval: 1s

journal:
  dir: "./data/journal"
  max_size: 67108864  # 64MB
  encryption_key: ""  # optional, base64-encoded 32-byte key

dedup:
  enabled: true
  capacity: 100000

rate_limit:
  enabled: false
  bytes_per_sec: 1048576
```

Journal supports AES-256-GCM encryption at rest. 

```bash
# Generate a key
openssl rand -base64 32
```

### API

**Endpoints:**
- `POST /ingest`: Single event (supports `msgpack` or `json`)
- `POST /ingest/batch`: Batch upload (supports `ndjson` or `jsonl`)
- `GET /metrics`: Prometheus metrics

**Event format:**
```json
{
  "idempotency_id": "716e5e1f-123e-48c2-95b6-02d4cf83b7e0",
  "sensor": "temp-01",
  "val": 42,
  "ts": 12345678
}
```

### Simulation

A simple tool for load testing.

```bash
# Basic run
go run ./cmd/edge -rate 1000 -duration 60s -workers 16

# Run multiple sensors in background
go run ./cmd/edge -sensor temp-north -rate 50 &
go run ./cmd/edge -sensor temp-south -rate 50 &
```

**Flags:**
- `-addr`: Sink address (default: `http://localhost:8080`)
- `-sensor`: Sensor name (default: `edge-sensor-1`)
- `-rate`: Messages per second (default: `10`)
- `-duration`: Simulation duration (default: `10s`)
- `-workers`: Concurrent workers (default: `4`)
