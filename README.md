# Redis Streams Task Queue

A production-grade task queue implementation using Redis Streams, demonstrating consumer groups, automatic recovery, and exactly-once delivery semantics.

## Project Goals

This project demonstrates:

1. **Redis Streams** as a message broker (vs Lists in `async-task-queue`)
2. **Consumer Groups** for distributed task processing
3. **Built-in recovery** using `XAUTOCLAIM` (replacing custom reaper)
4. **Exactly-once delivery** with atomic acknowledgment
5. **Observability** with native Stream introspection

## Architecture Overview

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
│     POST /task            XREADGROUP                                    │
│     GET /status           XACK / XAUTOCLAIM                             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Lists vs Streams Comparison

| Aspect                 | Lists (`async-task-queue`)              | Streams (This Project)             |
| ---------------------- | --------------------------------------- | ---------------------------------- |
| **Message Delivery**   | `BLMOVE` (atomic but 2-step with ZADD)  | `XREADGROUP` (single operation)    |
| **Consumer Tracking**  | Custom `processing_zset` + lease tokens | Built-in consumer groups           |
| **Visibility Timeout** | Custom sorted set + Lua scripts         | Built-in with `XPENDING` idle time |
| **Stale Recovery**     | Custom `reaper.py` (~135 lines)         | `XAUTOCLAIM` (one command)         |
| **Retry Counting**     | Manual in task data                     | Automatic `times_delivered`        |
| **Atomicity**          | 5 custom Lua scripts                    | Native commands                    |
| **Message History**    | Lost after pop                          | Retained until trimmed             |
| **Observability**      | Manual implementation                   | `XINFO` commands                   |

## Project Structure

```
async-task-queue-streams/
├── docker-compose.yml
├── README.md
├── spec/
│   ├── ARCHITECTURE.md          # System design diagrams
│   ├── STREAMS-CONCEPTS.md      # Redis Streams fundamentals
│   ├── CONSUMER-GROUPS.md       # Consumer group patterns
│   └── COMPARISON.md            # Lists vs Streams deep dive
├── app/
│   ├── Dockerfile
│   ├── main.py                  # FastAPI producer (XADD)
│   └── requirements.txt
└── worker/
    ├── Dockerfile
    ├── __init__.py
    ├── __main__.py
    ├── worker.py                # Main worker loop
    ├── stream_manager.py        # Stream operations abstraction
    ├── task_processor.py        # Task execution logic
    ├── config.py                # Configuration
    ├── redis_client.py          # Redis connection
    └── requirements.txt
```

## Implementation Phases

### Phase 1: Core Stream Implementation

**Goal**: Working task queue with Redis Streams basics

**Components**:

- [ ] Producer API with `XADD`
- [ ] Consumer group initialization
- [ ] Worker loop with `XREADGROUP`
- [ ] Basic `XACK` acknowledgment
- [ ] Task status storage

**Key Concepts**:

- Stream as append-only log
- Consumer group membership
- Message ID structure (`<timestamp>-<sequence>`)
- Pending Entries List (PEL)

**Intentionally Deferred**:

- Stale message recovery
- Retry logic
- Dead letter queue
- Graceful shutdown

```
┌──────────┐     XADD      ┌─────────────┐    XREADGROUP    ┌──────────┐
│ Producer │──────────────▶│ task_stream │◀─────────────────│  Worker  │
└──────────┘               └─────────────┘                  └──────────┘
                                                                  │
                                                             XACK │
                                                                  ▼
                                                           [Completed]
```

---

### Phase 2: Stale Message Recovery

**Goal**: Automatic recovery of messages from crashed workers

**Components**:

- [ ] `XAUTOCLAIM` implementation
- [ ] Recovery loop in worker
- [ ] Idle time threshold configuration

**Key Concepts**:

- Pending Entries List (PEL) tracking
- Message idle time
- Ownership transfer between consumers
- Compare to custom reaper elimination

```
Worker 1 crashes after XREADGROUP, before XACK
                │
                ▼
        ┌───────────────┐
        │     PEL       │  Message stuck, idle time increasing
        │  msg_id: 123  │
        │  idle: 45000  │
        └───────────────┘
                │
                │  XAUTOCLAIM (idle > 30000ms)
                ▼
        ┌───────────────┐
        │   Worker 2    │  Claims ownership, reprocesses
        └───────────────┘
```

---

### Phase 3: Retry Logic & Dead Letter Queue

**Goal**: Bounded retries with failed message preservation

**Components**:

- [ ] Delivery count tracking via `XPENDING`
- [ ] Max retry threshold
- [ ] Dead letter stream (`dead_letter_stream`)
- [ ] Failed task status updates

**Key Concepts**:

- `times_delivered` automatic tracking
- Separating recoverable vs permanent failures
- DLQ as a stream (not list) for consistency
- Retry backoff strategies (optional)

```
                    ┌─────────────────────────────────────┐
                    │          task_stream                │
                    └─────────────────────────────────────┘
                                     │
                                     │ XREADGROUP
                                     ▼
                              ┌─────────────┐
                              │   Process   │
                              └─────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
                    ▼                ▼                ▼
               [Success]        [Failure]        [Failure]
                  │           (retry < MAX)     (retry >= MAX)
                  │                │                │
                XACK          Don't ACK           XADD
                  │           (stay in PEL)         │
                  ▼                │                ▼
             [Complete]            │      ┌─────────────────┐
                                   │      │ dead_letter_    │
                                   │      │ stream          │
                                   │      └─────────────────┘
                                   │                │
                                   └──── XAUTOCLAIM ┘
                                         (later)
```

---

### Phase 4: Graceful Shutdown

**Goal**: Clean worker termination without message loss

**Components**:

- [ ] Signal handling (SIGTERM, SIGINT)
- [ ] Current task completion before exit
- [ ] Consumer deregistration (optional)

**Key Concepts**:

- Docker `stop_grace_period`
- In-flight message handling
- Consumer group rebalancing

```
                    SIGTERM received
                           │
                           ▼
               ┌───────────────────────┐
               │  Set running = False  │
               └───────────────────────┘
                           │
                           ▼
               ┌───────────────────────┐
               │ Complete current task │
               │ (if any in progress)  │
               └───────────────────────┘
                           │
                           ▼
               ┌───────────────────────┐
               │  ACK if completed     │
               │  Leave in PEL if not  │
               └───────────────────────┘
                           │
                           ▼
               ┌───────────────────────┐
               │   Exit gracefully     │
               └───────────────────────┘
```

---

### Phase 5: Observability & Monitoring

**Goal**: Production-ready visibility into queue health

**Components**:

- [ ] Stream info endpoint (`XINFO STREAM`)
- [ ] Consumer group stats (`XINFO GROUPS`)
- [ ] Per-consumer metrics (`XINFO CONSUMERS`)
- [ ] Pending message inspection (`XPENDING`)
- [ ] Health check endpoint

**Key Concepts**:

- Queue depth monitoring
- Consumer lag detection
- Stale message alerts
- Integration with monitoring systems

```
GET /health
{
  "stream": {
    "length": 150,
    "first_entry_id": "1708000000000-0",
    "last_entry_id": "1708001234567-0"
  },
  "consumer_group": {
    "name": "workers",
    "pending": 5,
    "consumers": 3,
    "last_delivered_id": "1708001234560-0"
  },
  "consumers": [
    {"name": "worker-abc123", "pending": 2, "idle": 1500},
    {"name": "worker-def456", "pending": 2, "idle": 800},
    {"name": "worker-ghi789", "pending": 1, "idle": 200}
  ]
}
```

---

### Phase 6: Production Hardening (Optional)

**Goal**: Scale and reliability optimizations

**Components**:

- [ ] Stream trimming (`MAXLEN` / `MINID`)
- [ ] Batch message processing
- [ ] Redis persistence configuration
- [ ] Connection pooling
- [ ] Metrics export (Prometheus format)

**Key Concepts**:

- Memory management
- Throughput optimization
- Data durability trade-offs

---

## Feature Summary by Phase

| Phase | Features                    | Complexity | Lines of Code |
| ----- | --------------------------- | ---------- | ------------- |
| **1** | Basic produce/consume, XACK | Low        | ~200          |
| **2** | XAUTOCLAIM recovery         | Low        | +40           |
| **3** | Retry counting, DLQ         | Medium     | +60           |
| **4** | Graceful shutdown           | Low        | +30           |
| **5** | XINFO observability         | Medium     | +50           |
| **6** | Trimming, batching, metrics | Medium     | +50           |

**Total estimated**: ~430 lines (vs ~600 in Lists implementation)

---

## Key Interview Discussion Points

### Phase 1

- How does `XREADGROUP` differ from `BLMOVE`?
- What is the Pending Entries List (PEL)?
- Why consumer groups over competing consumers?

### Phase 2

- How does `XAUTOCLAIM` replace the custom reaper?
- What determines "stale" in Streams vs sorted set scores?
- Race condition comparison between approaches

### Phase 3

- How does `times_delivered` work automatically?
- Why use a stream for DLQ instead of a list?
- Idempotency considerations for retried tasks

### Phase 4

- What happens to pending messages on crash vs graceful shutdown?
- How does Docker orchestrate the shutdown sequence?

### Phase 5

- What metrics indicate unhealthy queue state?
- How to detect slow consumers?
- Consumer lag vs queue depth

---

## Quick Start (After Implementation)

```bash
# Start all services
docker-compose up --build

# Submit a task
curl -X POST "http://localhost:8000/process-image?image_url=test.jpg"

# Check status
curl "http://localhost:8000/status/{task_id}"

# Scale workers
docker-compose up --scale worker=3

# Monitor health
curl "http://localhost:8000/health"

# Inspect stream directly
redis-cli XINFO STREAM task_stream
redis-cli XINFO GROUPS task_stream
redis-cli XPENDING task_stream workers
```

## License

MIT License
