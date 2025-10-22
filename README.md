# Propher

`Propher` is a replay-and-profile CLI for stream workloads. It rewrites timestamps in JSONL dumps, pushes messages to Redis or MQTT, then profiles end-to-end latency from result queues.

## What It Solves

- Reproduce production-like stream load from real dumps.
- Measure service and queue latency in microseconds.
- Compare source dump messages with observed result messages by `message_id` or other ID-field.
- Detect and export missing messages (`lost.json`); received messages latency and serve time (`latency.json`); stats, RPS, percentiles (`latency.stats.json`).

## Installation

### Requirements

- Go 1.25+
- Results queue (Only Redis is supported at the moment)
- Input queue (Supports MQTT and Redis)

### Build

```bash
git clone <your-repo-url>
cd propher
go build -o propher ./cmd/propher
```

### Run without build

```bash
go run ./cmd/propher/main.go <mode> [flags]
```

## Configuration

Configuration is loaded from environment and optional `.env`.

- Redis: `REDIS_URL` (preferred) or `REDIS_ADDR`, `REDIS_PASS`, `REDIS_DB`
- MQTT: `MQTT_BROKER`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `MQTT_CLIENT_ID`
- Common: `TIMEOUT`, `DEBUG`

## Modes

### `load-dump-and-rewrite`

Reads JSONL, rewrites `sent_epoch` or field with `-sent-field` name, writes a new dump, and pushes messages to input queue (currently Redis or MQTT).

Important flags:

- `-in-dump` (required) - `in-dump` is file with your profiling input data, one message per line (JSONL or other); must be prepared before running profiler;
- `-out-dump` (required) - `out-dump` is copy of input messages from `in-dump` with  updated `sent_epoch` or field with `-sent-field` name;
- `-sent-field`, `-epoch-unit` (`ms|s`), `-mode` (`same|increment`)
- `-step`, `-base-epoch`
- Redis target: `-redis-queue`, `-redis-push`, `-clear-queue`, `-batch`
- MQTT target: `-mqtt-topic`, `-mqtt-qos`, `-mqtt-retain`



### `measure-list-latency`

Consumes Redis result queue, matches records with source dump by `message_id`, calculates latency stats, and exports unmatched source records.

Important flags:

- `-obs-queue` (required) - output queue which is observed for result messages; currently only Redis queues supported;
- `-source-dump` (required) - dump of input messages for compating sent and received time; usually same as `-in-dump` 
- `-message-id-field`, `-source-sent-field`, `-source-sent-unit`
- `-t0-field`, `-t0-unit`
- `-duration-sec`, `-block-sec`, `-out-jsonl`
- `-restore`, `-restore-verify-empty`

Outputs:

- Per-record data: `latency.jsonl` (or `-out-jsonl`)
- Aggregate stats: `<out-jsonl>.stats.json`
- Missing messages from source dump: `lost.json`

## Example Usage

### 1) Rewrite and push to Redis list

```bash
go run ./cmd/propher/main.go load-dump-and-rewrite \
  -in-dump ./test.dump \
  -out-dump ./test_2.dump \
  -redis-queue profiling_queue_1 \
  -redis-push rpush
```

### 2) Rewrite and push dump to input queue and run measurement in one command

```bash
go run ./cmd/propher/main.go run \
  -in-dump ./test.dump \
  -out-dump ./test_2.dump \
  -mqtt-topic input_queue \
  -redis-push rpush \
  -obs-queue profiling_queue_1 \
  -source-dump ./test_2.dump \
  -out-jsonl ./latency.jsonl
```

### 3) Rewrite and publish to MQTT

```bash
go run ./cmd/propher/main.go load-dump-and-rewrite \
  -in-dump ./test.dump \
  -out-dump ./test_2.dump \
  -mqtt-topic smpp
```

### 4) Measure latency from Redis result queue

```bash
go run ./cmd/propher/main.go measure-list-latency \
  -obs-queue profiling_queue_1 \
  -source-dump ./test_2.dump \
  -out-jsonl ./latency.jsonl
```

### 5) Restore messages back after measurement

```bash
go run ./cmd/propher/main.go measure-list-latency \
  -obs-queue profiling_queue_1 \
  -source-dump ./test_2.dump \
  -restore \
  -restore-verify-empty
```

## Notes

- Use explicit modes (`load-dump-and-rewrite`, `measure-list-latency`) for predictable behavior.
- `source-dump` must contain unique `message_id` values for correct matching.
