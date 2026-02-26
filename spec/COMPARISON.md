# Lists vs Streams: Deep Comparison

## Problem Statement

Building a reliable task queue requires:
1. **Exactly-once delivery** - Each task processed by one worker
2. **Visibility timeout** - Recover tasks from crashed workers
3. **Retry mechanism** - Handle transient failures
4. **Dead letter queue** - Preserve permanently failed tasks
5. **Observability** - Monitor queue health

Both Lists and Streams can solve this, but with very different complexity.

## Architecture Comparison

### Lists Implementation (async-task-queue)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ image_queue │────▶│ processing_ │────▶│ dead_letter │
│   (List)    │     │   queue     │     │   _queue    │
└─────────────┘     │   (List)    │     │   (List)    │
                    └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐     ┌─────────────┐
                    │ processing_ │     │ task:lease: │
                    │   zset      │     │   {id}      │
                    │ (Sorted Set)│     │   (Hash)    │
                    └─────────────┘     └─────────────┘
```

**5 Redis data structures** to track task state.

### Streams Implementation (This Project)

```
┌─────────────┐                         ┌─────────────┐
│ task_stream │                         │dead_letter_ │
│  (Stream)   │                         │  stream     │
└─────────────┘                         └─────────────┘
       │
       └──── Consumer Group (automatic PEL tracking)
```

**2 Redis data structures** - consumer group handles the rest.

## Operation-by-Operation Comparison

### 1. Claim a Task

**Lists**:
```python
# Step 1: Atomic move to processing queue
task_id = r.blmove(IMAGE_QUEUE, PROCESSING_QUEUE, timeout, "RIGHT", "LEFT")

# Step 2: Register in sorted set for expiry tracking (GAP HERE!)
lease_token = str(uuid.uuid4())
expiry_time = time.time() + VISIBILITY_TIMEOUT
pipe = r.pipeline(transaction=True)
pipe.zadd(PROCESSING_ZSET, {task_id: expiry_time})
pipe.hset(f"task:lease:{task_id}", mapping={"token": lease_token, "expiry": expiry_time})
pipe.execute()
```

**Problem**: Gap between `BLMOVE` and `ZADD` - if crash here, task is orphaned.

**Streams**:
```python
messages = r.xreadgroup(
    GROUP, CONSUMER,
    {STREAM: ">"},
    count=1, block=BLOCK_MS
)
```

**No gap** - single atomic operation adds to PEL automatically.

### 2. Acknowledge Completion

**Lists**:
```python
# Must verify lease token to prevent race with reaper
# Requires Lua script for atomicity
ACK_COMPLETE_LUA = """
local stored_token = redis.call('HGET', lease_key, 'token')
if stored_token ~= lease_token then
    return 0  -- Reaper took over
end
redis.call('LREM', processing_queue, 1, task_id)
redis.call('ZREM', processing_zset, task_id)
redis.call('DEL', lease_key)
redis.call('SET', task_data_key, new_task_json)
return 1
"""
```

**Streams**:
```python
r.xack(STREAM, GROUP, message_id)
```

One command. No race conditions possible.

### 3. Recover Stale Tasks

**Lists** (`reaper.py` - 135 lines):
```python
def reap_expired_tasks():
    now = time.time()
    expired_task_ids = r.zrangebyscore(PROCESSING_ZSET, "-inf", now)
    
    for task_id in expired_task_ids:
        # Atomic claim with Lua script
        reaper_token = str(uuid.uuid4())
        new_expiry = now + REAPER_GRACE_PERIOD
        claimed = REAPER_CLAIM_LUA(...)
        
        if claimed:
            task = json.loads(r.get(f"task:data:{task_id}"))
            retry_count = task.get('retry_count', 0)
            
            if retry_count < MAX_RETRIES:
                task['retry_count'] = retry_count + 1
                ACK_REQUEUE_LUA(task_id, reaper_token, json.dumps(task), IMAGE_QUEUE)
            else:
                task['status'] = 'failed'
                ACK_REQUEUE_LUA(task_id, reaper_token, json.dumps(task), DEAD_LETTER_QUEUE)
```

**Streams**:
```python
def recover_stale_messages():
    result = r.xautoclaim(STREAM, GROUP, CONSUMER, min_idle_time=30000, start_id="0-0")
    return result[1]  # Returns list of claimed messages
```

**One command** replaces 135 lines + 2 Lua scripts.

### 4. Track Retry Count

**Lists**:
```python
# Manual tracking in task data
task['retry_count'] = task.get('retry_count', 0) + 1
r.set(f"task:data:{task_id}", json.dumps(task))
```

**Streams**:
```python
# Automatic! Query with XPENDING
pending = r.xpending_range(STREAM, GROUP, min=msg_id, max=msg_id, count=1)
times_delivered = pending[0]['times_delivered']  # Automatic counter
```

### 5. Handle Orphan Tasks

**Lists**:
```python
def recover_orphan_tasks():
    """
    Handle crash between BLMOVE and ZADD.
    Task in processing_queue but NOT in processing_zset.
    """
    processing_tasks = r.lrange(PROCESSING_QUEUE, 0, -1)
    for task_id in processing_tasks:
        score = r.zscore(PROCESSING_ZSET, task_id)
        if score is None:
            r.lrem(PROCESSING_QUEUE, 1, task_id)
            r.lpush(IMAGE_QUEUE, task_id)
```

**Streams**:
```python
# Not needed! No gap exists.
# XREADGROUP atomically adds to PEL.
```

## Code Complexity Comparison

| Component | Lists | Streams |
|-----------|-------|---------|
| Claim task | 15 lines + pipeline | 5 lines |
| Acknowledge | 30-line Lua script | 1 line |
| Recover stale | 80+ lines + Lua script | 5 lines |
| Recover orphan | 15 lines | 0 (not needed) |
| Heartbeat | 25-line Lua script | 0 (not needed) |
| Retry tracking | Manual in every path | Automatic |
| **Total custom code** | ~200 lines | ~30 lines |
| **Lua scripts** | 5 scripts | 0 scripts |

## Race Condition Comparison

### Lists Race Conditions (Solved with Lua)

1. **BLMOVE-ZADD Gap**
   - Worker crashes between operations
   - Task stuck in processing_queue without expiry tracking
   - **Solution**: Orphan recovery routine

2. **Ack-vs-Reaper Race**
   - Worker completing task while reaper reclaiming
   - Both try to "own" the task
   - **Solution**: Lease tokens + Lua atomic scripts

3. **Heartbeat Race**
   - Worker extending lease while reaper claiming
   - **Solution**: Lua script to verify token

### Streams Race Conditions

**None** - by design:
- `XREADGROUP` atomically delivers AND registers in PEL
- `XACK` atomically removes from PEL
- `XAUTOCLAIM` atomically transfers ownership
- No gap between operations

## When to Use Each

### Use Lists When:
- Learning distributed systems fundamentals
- Need to understand visibility timeout patterns
- Want to demonstrate problem-solving skills
- System is simple enough (single worker)

### Use Streams When:
- Building production systems
- Need reliable exactly-once processing
- Want built-in consumer groups
- Need message history/replay capability
- Want native observability

## Migration Path

```
Phase 1: Lists Implementation
├── Understand the problems
├── Implement custom solutions
└── Feel the pain

Phase 2: Streams Implementation
├── Replace custom code with native commands
├── Remove Lua scripts
├── Simplify architecture
└── Appreciate the abstraction
```

## Summary Table

| Aspect | Lists | Streams |
|--------|-------|---------|
| **Complexity** | High (custom solutions) | Low (native features) |
| **Race conditions** | Many (need Lua) | None (by design) |
| **Data structures** | 5 | 2 |
| **Lua scripts** | 5 | 0 |
| **Lines of code** | ~600 | ~400 |
| **Orphan recovery** | Custom routine | Not needed |
| **Retry counting** | Manual | Automatic |
| **Observability** | Manual | `XINFO` commands |
| **Learning value** | High (understand problems) | Medium (use solutions) |
| **Production ready** | With care | Yes |
