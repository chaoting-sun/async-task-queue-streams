# Redis Streams Task Queue

A task queue built on Redis Streams with consumer groups, automatic stale-message recovery, bounded retries with a dead letter queue, graceful shutdown, and built-in observability.

## Features

- **Redis Streams** as the message broker (append-only log with retained history)
- **Consumer Groups** for distributed, load-balanced task processing (`XREADGROUP`)
- **Automatic recovery** of messages from crashed or slow workers (`XAUTOCLAIM`)
- **Bounded retries** using native `times_delivered` tracking (`XPENDING`)
- **Dead Letter Queue** stream for messages that exceed max retries
- **Graceful shutdown** on `SIGTERM`/`SIGINT` — in-flight tasks finish before exit
- **Health endpoint** exposing stream depth, consumer lag, pending messages, and alerts

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Docker Network                                │
│                                                                         │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────────────┐   │
│  │   FastAPI    │      │    Redis     │      │      Workers         │   │
│  │   Producer   │─────▶│   Streams    │◀─────│  (Consumer Group)    │   │
│  │   :8000      │      │   :6379      │      │                      │   │
│  └──────────────┘      └──────────────┘      │  ┌────┐ ┌────┐ ┌────┐│   │
│         │                     │              │  │ W1 │ │ W2 │ │ W3 ││   │
│         │                     │              │  └────┘ └────┘ └────┘│   │
│         ▼                     ▼              └──────────────────────┘   │
│     POST /process-image   XREADGROUP                                    │
│     GET  /status          XACK / XAUTOCLAIM                             │
│     GET  /health                                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

## How It Works

1. **Produce**: `POST /process-image` writes task metadata to `task:data:{task_id}` and appends a message to `task_stream` via `XADD`.
2. **Consume**: Each worker calls `XREADGROUP` with its consumer name, pulling one new message at a time. The message stays in the Pending Entries List (PEL) until acknowledged.
3. **Recover**: Before reading new work, each worker calls `XAUTOCLAIM` to take over any PEL entries idle longer than `STALE_MESSAGE_THRESHOLD_MS` — replacing the custom reaper pattern required with Redis Lists.
4. **Retry / DLQ**: On failure, the message is left in the PEL so `XAUTOCLAIM` retries it. Once `times_delivered >= MAX_RETRIES`, the worker re-publishes it to `dead_letter_stream` with error context and acknowledges the original.
5. **Shutdown**: On `SIGTERM`/`SIGINT` the main loop exits after the current task finishes. Docker Compose gives each worker `stop_grace_period: 30s`.

## Project Structure

```
async-task-queue-streams/
├── docker-compose.yml
├── app/
│   ├── main.py                  # FastAPI producer + /health endpoint
│   ├── Dockerfile
│   └── requirements.txt
├── worker/
│   ├── worker.py                # Main loop: recovery → claim → process → ack
│   ├── stream_manager.py        # XADD / XREADGROUP / XACK / XAUTOCLAIM / XPENDING / XINFO
│   ├── task_processor.py        # Task execution + status updates
│   ├── config.py                # Env-driven configuration
│   ├── redis_client.py
│   ├── __main__.py
│   └── Dockerfile
└── spec/                        # Design notes (architecture, streams concepts, comparison)
```

## Quick Start

```bash
# Start Redis, API, and 2 workers
docker-compose up --build

# Submit a task
curl -X POST "http://localhost:8000/process-image?image_url=test.jpg"

# Check task status
curl "http://localhost:8000/status/<task_id>"

# Scale workers
docker-compose up --scale worker=3

# Health + observability
curl "http://localhost:8000/health"

# Inspect the stream directly
redis-cli XINFO STREAM task_stream
redis-cli XINFO GROUPS task_stream
redis-cli XPENDING task_stream workers
redis-cli XLEN dead_letter_stream
```

## Configuration

All settings are env vars; defaults live in `worker/config.py` and `app/main.py`.

| Variable                       | Default               | Purpose                                              |
| ------------------------------ | --------------------- | ---------------------------------------------------- |
| `REDIS_HOST` / `REDIS_PORT`    | `localhost` / `6379`  | Redis connection                                     |
| `STREAM_NAME`                  | `task_stream`         | Main task stream                                     |
| `CONSUMER_GROUP`               | `workers`             | Consumer group name                                  |
| `CONSUMER_NAME`                | `$HOSTNAME`           | Per-worker identity                                  |
| `BLOCK_MS`                     | `5000`                | `XREADGROUP` block timeout                           |
| `STALE_MESSAGE_THRESHOLD_MS`   | `30000`               | Idle time before `XAUTOCLAIM` reassigns a message    |
| `MAX_RETRIES`                  | `1`                   | Delivery attempts before moving to DLQ               |
| `DLQ_STREAM_NAME`              | `dead_letter_stream`  | DLQ stream name                                      |
| `ALERT_HIGH_PENDING_THRESHOLD` | `100`                 | Health alert: pending count                          |
| `ALERT_STALE_MESSAGE_MS`       | `60000`               | Health alert: message idle time                      |
| `ALERT_IDLE_CONSUMER_MS`       | `300000`              | Health alert: consumer idle time                     |

> Note: `task_processor.py` simulates a 30% failure rate and 1–3s processing time to exercise the retry/DLQ path.

## Lists vs Streams

This project is the counterpart to a Redis Lists implementation. Streams remove most of the custom coordination code:

| Aspect             | Lists                                   | Streams (this project)             |
| ------------------ | --------------------------------------- | ---------------------------------- |
| Message delivery   | `BLMOVE` (+ ZADD for processing set)    | `XREADGROUP` (single op)           |
| Consumer tracking  | Custom processing ZSET + lease tokens   | Built-in consumer groups           |
| Visibility timeout | Sorted-set scores + Lua                 | Built-in via `XPENDING` idle time  |
| Stale recovery     | Custom reaper loop                      | `XAUTOCLAIM`                       |
| Retry counting     | Manual, stored in task data             | Native `times_delivered`           |
| Message history    | Lost after pop                          | Retained until trimmed             |
| Observability      | Hand-rolled                             | `XINFO STREAM` / `GROUPS` / `CONSUMERS` |

See `spec/COMPARISON.md` for a deeper dive.

## License

MIT
