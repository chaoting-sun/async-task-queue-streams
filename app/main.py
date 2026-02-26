import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
STREAM_NAME = os.getenv("STREAM_NAME", "task_stream")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "workers")

# Observability settings for health monitoring
ALERT_HIGH_PENDING_THRESHOLD = int(os.getenv("ALERT_HIGH_PENDING_THRESHOLD", "100"))
ALERT_STALE_MESSAGE_MS = int(os.getenv("ALERT_STALE_MESSAGE_MS", "60000"))
ALERT_IDLE_CONSUMER_MS = int(os.getenv("ALERT_IDLE_CONSUMER_MS", "300000"))
PENDING_DETAILS_COUNT = int(os.getenv("PENDING_DETAILS_COUNT", "100"))


redis_client: redis.Redis = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
    )
    yield
    redis_client.close()


app = FastAPI(
    title="Redis Streams Task Queue",
    description="Task queue producer using Redis Streams",
    lifespan=lifespan,
)


class TaskSubmission(BaseModel):
    image_url: str


class TaskResponse(BaseModel):
    task_id: str
    message_id: str
    status: str


class TaskStatus(BaseModel):
    task_id: str
    status: str
    image_url: str | None = None
    created_at: float | None = None
    updated_at: float | None = None
    result: dict | None = None
    error: str | None = None


# Health endpoint response models
class StreamInfo(BaseModel):
    length: int
    first_entry: Any | None = None
    last_entry: Any | None = None
    groups: int
    radix_tree_keys: int = 0
    radix_tree_nodes: int = 0


class GroupInfo(BaseModel):
    name: str
    pending: int
    consumers: int
    last_delivered_id: str | None = None
    entries_read: int | None = None
    lag: int | None = None


class ConsumerInfo(BaseModel):
    name: str
    pending: int
    idle: int
    inactive: int | None = None


class PendingDetail(BaseModel):
    message_id: str
    consumer: str
    idle_ms: int
    times_delivered: int


class HealthResponse(BaseModel):
    stream: StreamInfo | None = None
    group: GroupInfo | None = None
    consumers: list[ConsumerInfo] = []
    pending_details: list[PendingDetail] = []
    alerts: list[str] = []
    status: str = "healthy"


@app.post("/process-image", response_model=TaskResponse)
def process_image(image_url: str):
    """
    Submit an image processing task.
    Uses XADD to add the task to the Redis Stream.
    """
    task_id = str(uuid.uuid4())

    task_data = {
        "task_id": task_id,
        "status": "pending",
        "image_url": image_url,
        "created_at": time.time(),
    }
    redis_client.set(f"task:data:{task_id}", json.dumps(task_data))

    message_id = redis_client.xadd(
        STREAM_NAME,
        {
            "task_id": task_id,
            "image_url": image_url,
        },
    )

    return TaskResponse(
        task_id=task_id,
        message_id=message_id,
        status="pending",
    )


@app.get("/status/{task_id}", response_model=TaskStatus)
def get_status(task_id: str):
    """
    Get the status of a task.
    Reads from task:data:{task_id} key.
    """
    task_key = f"task:data:{task_id}"
    data = redis_client.get(task_key)

    if not data:
        raise HTTPException(status_code=404, detail="Task not found")

    task_data = json.loads(data)

    return TaskStatus(
        task_id=task_id,
        status=task_data.get("status", "unknown"),
        image_url=task_data.get("image_url"),
        created_at=task_data.get("created_at"),
        updated_at=task_data.get("updated_at"),
        result=task_data.get("result"),
        error=task_data.get("error"),
    )


@app.get("/")
def root():
    """Basic health check endpoint."""
    return {"status": "ok", "service": "task-queue-producer"}


# Observability & Monitoring

def get_stream_info() -> dict | None:
    """Get stream-level information using XINFO STREAM."""
    try:
        info = redis_client.xinfo_stream(STREAM_NAME)
        return {
            "length": info.get("length", 0),
            "first_entry": info.get("first-entry"),
            "last_entry": info.get("last-entry"),
            "groups": info.get("groups", 0),
            "radix_tree_keys": info.get("radix-tree-keys", 0),
            "radix_tree_nodes": info.get("radix-tree-nodes", 0),
        }
    except redis.ResponseError as e:
        if "no such key" in str(e).lower():
            return {
                "length": 0,
                "first_entry": None,
                "last_entry": None,
                "groups": 0,
                "radix_tree_keys": 0,
                "radix_tree_nodes": 0,
            }
        raise


def get_group_info() -> dict | None:
    """Get consumer group information using XINFO GROUPS."""
    try:
        groups = redis_client.xinfo_groups(STREAM_NAME)
        for group in groups:
            if group.get("name") == CONSUMER_GROUP:
                return {
                    "name": group.get("name"),
                    "pending": group.get("pending", 0),
                    "consumers": group.get("consumers", 0),
                    "last_delivered_id": group.get("last-delivered-id"),
                    "entries_read": group.get("entries-read"),
                    "lag": group.get("lag"),
                }
        return None
    except redis.ResponseError as e:
        if "no such key" in str(e).lower():
            return None
        raise


def get_consumer_info() -> list[dict]:
    """Get per-consumer information using XINFO CONSUMERS."""
    try:
        consumers = redis_client.xinfo_consumers(STREAM_NAME, CONSUMER_GROUP)
        return [
            {
                "name": c.get("name"),
                "pending": c.get("pending", 0),
                "idle": c.get("idle", 0),
                "inactive": c.get("inactive"),
            }
            for c in consumers
        ]
    except redis.ResponseError as e:
        if "no such key" in str(e).lower() or "NOGROUP" in str(e):
            return []
        raise


def get_pending_details(count: int = PENDING_DETAILS_COUNT) -> list[dict]:
    """Get detailed pending entries using XPENDING."""
    try:
        pending = redis_client.xpending_range(
            name=STREAM_NAME,
            groupname=CONSUMER_GROUP,
            min="-",
            max="+",
            count=count,
        )
        return [
            {
                "message_id": p.get("message_id"),
                "consumer": p.get("consumer"),
                "idle_ms": p.get("time_since_delivered", 0),
                "times_delivered": p.get("times_delivered", 1),
            }
            for p in pending
        ]
    except redis.ResponseError as e:
        if "no such key" in str(e).lower() or "NOGROUP" in str(e):
            return []
        raise


def check_alerts(
    group_info: dict | None,
    pending_details: list[dict],
    consumer_info: list[dict],
) -> list[str]:
    """Check for health alerts based on configured thresholds."""
    alerts = []

    if group_info and group_info["pending"] > ALERT_HIGH_PENDING_THRESHOLD:
        alerts.append(
            f"HIGH_PENDING_COUNT: {group_info['pending']} pending messages "
            f"(threshold: {ALERT_HIGH_PENDING_THRESHOLD})"
        )

    for p in pending_details:
        if p["idle_ms"] > ALERT_STALE_MESSAGE_MS:
            alerts.append(
                f"STALE_MESSAGE: {p['message_id']} idle for {p['idle_ms']}ms "
                f"(threshold: {ALERT_STALE_MESSAGE_MS}ms)"
            )

    for c in consumer_info:
        if c["idle"] > ALERT_IDLE_CONSUMER_MS:
            alerts.append(
                f"IDLE_CONSUMER: {c['name']} idle for {c['idle']}ms "
                f"(threshold: {ALERT_IDLE_CONSUMER_MS}ms)"
            )

    return alerts


@app.get("/health", response_model=HealthResponse)
def health():
    """
    Comprehensive health endpoint for observability.

    Returns:
        - Stream info: queue depth, first/last entry, groups count
        - Group info: pending count, consumers, last delivered ID
        - Consumer info: per-worker pending and idle time
        - Pending details: message-level pending information
        - Alerts: detected issues based on configured thresholds
        - Status: overall health status (healthy/unhealthy)
    """
    stream_info = get_stream_info()
    group_info = get_group_info()
    consumer_info = get_consumer_info()
    pending_details = get_pending_details()
    alerts = check_alerts(group_info, pending_details, consumer_info)

    status = "unhealthy" if alerts else "healthy"

    return HealthResponse(
        stream=StreamInfo(**stream_info) if stream_info else None,
        group=GroupInfo(**group_info) if group_info else None,
        consumers=[ConsumerInfo(**c) for c in consumer_info],
        pending_details=[PendingDetail(**p) for p in pending_details],
        alerts=alerts,
        status=status,
    )
