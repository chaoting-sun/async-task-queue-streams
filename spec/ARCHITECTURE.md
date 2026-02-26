# System Architecture

## Overview

```mermaid
graph LR
    subgraph Clients
        U[User]
    end
    
    subgraph Docker Network
        API[FastAPI :8000]
        Redis[(Redis Streams :6379)]
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker N]
    end
    
    U -->|POST /process-image| API
    U -->|GET /status/:id| API
    
    API -->|XADD| Redis
    API -->|GET task:data:*| Redis
    
    Redis -->|XREADGROUP| W1
    Redis -->|XREADGROUP| W2
    Redis -->|XREADGROUP| W3
    
    W1 -->|XACK| Redis
    W2 -->|XACK| Redis
    W3 -->|XACK| Redis
```

## Component Responsibilities

| Component | Role | Key Operations |
|-----------|------|----------------|
| **FastAPI** | Producer | `XADD` to stream, serve status queries |
| **Redis Streams** | Message Broker | Stream storage, consumer group management, PEL tracking |
| **Workers** | Consumers | `XREADGROUP`, process tasks, `XACK`, `XAUTOCLAIM` |

## Redis Data Structures

```mermaid
graph TB
    subgraph "Streams (New)"
        TS[task_stream]
        DLS[dead_letter_stream]
    end
    
    subgraph "Consumer Group State (Automatic)"
        CG[Consumer Group: workers]
        PEL[Pending Entries List]
    end
    
    subgraph "Task Storage"
        TD[task:data:*]
    end
    
    TS --> CG
    CG --> PEL
```

| Key | Type | Purpose |
|-----|------|---------|
| `task_stream` | Stream | Main task queue |
| `dead_letter_stream` | Stream | Failed tasks after max retries |
| `task:data:{id}` | String (JSON) | Task status and payload |

### Comparison: Lists vs Streams Data Structures

```mermaid
graph TB
    subgraph "Lists Implementation (Old)"
        IQ[image_queue<br/>List]
        PQ[processing_queue<br/>List]
        PZ[processing_zset<br/>Sorted Set]
        TL[task:lease:*<br/>Hash]
        DLQ[dead_letter_queue<br/>List]
        TD1[task:data:*<br/>String]
    end
    
    subgraph "Streams Implementation (New)"
        TS2[task_stream<br/>Stream]
        DLS2[dead_letter_stream<br/>Stream]
        TD2[task:data:*<br/>String]
    end
```

**Eliminated by Streams**:
- `processing_queue` → Replaced by PEL (Pending Entries List)
- `processing_zset` → Replaced by automatic idle time tracking
- `task:lease:*` → Replaced by consumer ownership in consumer group

## Consumer Group Architecture

```mermaid
graph TB
    subgraph task_stream
        M1[msg-1]
        M2[msg-2]
        M3[msg-3]
        M4[msg-4]
        M5[msg-5]
    end
    
    subgraph "Consumer Group: workers"
        direction TB
        LDI[last_delivered_id: msg-3]
        
        subgraph PEL[Pending Entries List]
            P1[msg-2 → worker-1, idle: 5000ms]
            P2[msg-3 → worker-2, idle: 1000ms]
        end
        
        subgraph Consumers
            C1[worker-1]
            C2[worker-2]
            C3[worker-3]
        end
    end
    
    M1 -.->|delivered & acked| ACK[Acknowledged]
    M2 --> P1
    M3 --> P2
    M4 -.->|not yet delivered| LDI
    M5 -.->|not yet delivered| LDI
```

## Message Flow

### Happy Path

```mermaid
sequenceDiagram
    participant User
    participant API
    participant Stream as task_stream
    participant PEL
    participant Worker
    
    User->>API: POST /process-image
    API->>Stream: XADD {task_id, image_url}
    API->>API: SET task:data:{id}
    API-->>User: {task_id, message_id}
    
    Worker->>Stream: XREADGROUP (">")
    Stream->>PEL: Add to pending
    Stream-->>Worker: {message_id, fields}
    
    Worker->>Worker: Process task
    Worker->>API: SET task:data:{id} (completed)
    Worker->>Stream: XACK
    Stream->>PEL: Remove from pending
```

### Failure Recovery Path

```mermaid
sequenceDiagram
    participant Worker1 as Worker 1
    participant Stream as task_stream
    participant PEL
    participant Worker2 as Worker 2
    
    Worker1->>Stream: XREADGROUP
    Stream->>PEL: Add (owner: worker-1)
    Stream-->>Worker1: message
    
    Note over Worker1: CRASH!
    
    Note over PEL: Idle time increasing...
    
    Worker2->>Stream: XAUTOCLAIM (idle > 30s)
    Stream->>PEL: Transfer ownership to worker-2
    Stream-->>Worker2: message
    
    Worker2->>Worker2: Reprocess
    Worker2->>Stream: XACK
    Stream->>PEL: Remove
```

## Docker Compose Structure

```mermaid
graph TB
    subgraph docker-compose
        API[api :8000]
        Redis[redis :6379]
        W1[worker replica 1]
        W2[worker replica 2]
        W3[worker replica N]
    end
    
    API --> Redis
    W1 --> Redis
    W2 --> Redis
    W3 --> Redis
    
    Host[Host Machine] -->|localhost:8000| API
    Host -->|localhost:6379| Redis
```

```yaml
services:
  redis:
    image: redis:alpine
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    
  api:
    build: ./app
    depends_on: [redis]
    environment:
      - REDIS_HOST=redis
    
  worker:
    build: ./worker
    depends_on: [redis]
    stop_grace_period: 30s
    deploy:
      replicas: 3  # or use --scale worker=3
```

## Scaling Model

```mermaid
graph LR
    subgraph "Single Stream"
        S[task_stream]
    end
    
    subgraph "Consumer Group"
        S --> CG[workers]
    end
    
    subgraph "Workers (scale horizontally)"
        CG --> W1[worker-1]
        CG --> W2[worker-2]
        CG --> W3[worker-3]
        CG --> WN[worker-N]
    end
```

**Key Properties**:
- Each message delivered to exactly ONE consumer in the group
- Automatic load distribution
- No coordination needed between workers
- Workers can join/leave dynamically
