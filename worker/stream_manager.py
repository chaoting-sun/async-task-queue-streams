import redis
import time
from typing import Optional
from config import (
    STREAM_NAME,
    CONSUMER_GROUP,
    CONSUMER_NAME,
    BLOCK_MS,
    STALE_MESSAGE_THRESHOLD_MS,
    STALE_RECOVERY_COUNT,
    MAX_RETRIES,
    DLQ_STREAM_NAME,
    ALERT_HIGH_PENDING_THRESHOLD,
    ALERT_STALE_MESSAGE_MS,
    ALERT_IDLE_CONSUMER_MS,
    PENDING_DETAILS_COUNT,
)


class StreamManager:
    """
    Manages Redis Stream operations:
    - Consumer group initialization
    - Task claiming via XREADGROUP
    - Task acknowledgment via XACK
    - Stale message recovery via XAUTOCLAIM
    - Delivery count tracking via XPENDING
    - Dead Letter Queue management
    - Observability via XINFO commands
    """

    def __init__(self, redis_client: redis.Redis):
        self.r = redis_client
        self.stream = STREAM_NAME
        self.group = CONSUMER_GROUP
        self.consumer = CONSUMER_NAME

    def ensure_consumer_group(self) -> None:
        """
        Create consumer group if it doesn't exist.
        Uses MKSTREAM to create the stream if needed.
        """
        try:
            self.r.xgroup_create(
                name=self.stream,
                groupname=self.group,
                id="0",
                mkstream=True,
            )
            print(f"Created consumer group '{self.group}' on stream '{self.stream}'")
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                print(f"Consumer group '{self.group}' already exists")
            else:
                raise

    def claim_task(self, block_ms: int = BLOCK_MS) -> tuple[Optional[str], Optional[dict]]:
        """
        Claim a new task from the stream using XREADGROUP.

        Uses ">" as the ID to get only new messages that have never
        been delivered to any consumer in this group.

        Returns:
            tuple: (message_id, fields) or (None, None) if no message available
        """
        result = self.r.xreadgroup(
            groupname=self.group,
            consumername=self.consumer,
            streams={self.stream: ">"},
            count=1,
            block=block_ms,
        )

        if not result:
            return None, None

        stream_name, messages = result[0]
        if not messages:
            return None, None

        message_id, fields = messages[0]
        return message_id, fields

    def ack(self, message_id: str) -> int:
        """
        Acknowledge a message, removing it from the Pending Entries List (PEL).

        Args:
            message_id: The ID of the message to acknowledge

        Returns:
            int: Number of messages acknowledged (1 if successful, 0 if not found)
        """
        return self.r.xack(self.stream, self.group, message_id)

    def recover_stale_messages(
        self,
        min_idle_ms: int = STALE_MESSAGE_THRESHOLD_MS,
        count: int = STALE_RECOVERY_COUNT,
    ) -> list[tuple[str, dict]]:
        """
        Recover stale messages from crashed or slow workers using XAUTOCLAIM.

        XAUTOCLAIM atomically finds messages in the PEL that have been idle
        longer than min_idle_ms and transfers ownership to this consumer.
        This replaces the complex reaper logic needed with Redis Lists.

        Args:
            min_idle_ms: Minimum idle time in milliseconds for a message to be
                         considered stale (default from config)
            count: Maximum number of messages to claim per call

        Returns:
            list: List of (message_id, fields) tuples for recovered messages
        """
        result = self.r.xautoclaim(
            name=self.stream,
            groupname=self.group,
            consumername=self.consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )

        if not result:
            return []

        # XAUTOCLAIM returns: [next_start_id, [(msg_id, fields), ...], [deleted_ids]]
        # We need the second element which contains the claimed messages
        claimed_messages = result[1] if len(result) > 1 else []
        return claimed_messages

    def get_delivery_count(self, message_id: str) -> int:
        """
        Get the delivery count for a specific message using XPENDING.

        Redis automatically tracks how many times a message has been delivered
        via the `times_delivered` field in the pending entry.

        Args:
            message_id: The ID of the message to check

        Returns:
            int: Number of times this message has been delivered (1 = first attempt)
        """
        pending = self.r.xpending_range(
            name=self.stream,
            groupname=self.group,
            min=message_id,
            max=message_id,
            count=1,
        )

        if pending:
            return pending[0].get("times_delivered", 1)
        return 1

    def move_to_dlq(self, message_id: str, fields: dict, error: str) -> str:
        """
        Move a failed message to the Dead Letter Queue.

        Adds the original message to the DLQ stream with additional context
        (error message, original message ID, failure timestamp), then
        acknowledges the original message to remove it from the PEL.

        Args:
            message_id: The original message ID
            fields: The original message fields
            error: The error message describing why the task failed

        Returns:
            str: The new message ID in the DLQ stream
        """
        dlq_fields = {
            **fields,
            "error": error,
            "original_msg_id": message_id,
            "failed_at": str(time.time()),
            "original_consumer": self.consumer,
        }

        dlq_msg_id = self.r.xadd(DLQ_STREAM_NAME, dlq_fields)

        self.ack(message_id)

        return dlq_msg_id

    def should_move_to_dlq(self, message_id: str) -> bool:
        """
        Check if a message has exceeded the maximum retry limit.

        Args:
            message_id: The ID of the message to check

        Returns:
            bool: True if delivery count >= MAX_RETRIES
        """
        delivery_count = self.get_delivery_count(message_id)
        return delivery_count >= MAX_RETRIES

    def get_max_retries(self) -> int:
        """Return the configured maximum retry count."""
        return MAX_RETRIES

    # Observability & Monitoring methods

    def get_stream_info(self) -> dict:
        """
        Get stream-level information using XINFO STREAM.

        Returns:
            dict: Stream info including length, first/last entry, groups count
        """
        try:
            info = self.r.xinfo_stream(self.stream)
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

    def get_group_info(self) -> dict | None:
        """
        Get consumer group information using XINFO GROUPS.

        Returns:
            dict: Group info including pending count, consumers, last delivered ID,
                  or None if group doesn't exist
        """
        try:
            groups = self.r.xinfo_groups(self.stream)
            for group in groups:
                if group.get("name") == self.group:
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

    def get_consumer_info(self) -> list[dict]:
        """
        Get per-consumer information using XINFO CONSUMERS.

        Returns:
            list: List of consumer info dicts with name, pending count, idle time
        """
        try:
            consumers = self.r.xinfo_consumers(self.stream, self.group)
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

    def get_pending_details(self, count: int = PENDING_DETAILS_COUNT) -> list[dict]:
        """
        Get detailed pending entries using XPENDING.

        Returns:
            list: List of pending entry details with message_id, consumer,
                  idle_ms, and times_delivered
        """
        try:
            pending = self.r.xpending_range(
                name=self.stream,
                groupname=self.group,
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

    def check_alerts(self) -> list[str]:
        """
        Check for health alerts based on configured thresholds.

        Returns:
            list: List of alert strings indicating potential issues
        """
        alerts = []

        group_info = self.get_group_info()
        if group_info and group_info["pending"] > ALERT_HIGH_PENDING_THRESHOLD:
            alerts.append(
                f"HIGH_PENDING_COUNT: {group_info['pending']} pending messages "
                f"(threshold: {ALERT_HIGH_PENDING_THRESHOLD})"
            )

        pending_details = self.get_pending_details()
        for p in pending_details:
            if p["idle_ms"] > ALERT_STALE_MESSAGE_MS:
                alerts.append(
                    f"STALE_MESSAGE: {p['message_id']} idle for {p['idle_ms']}ms "
                    f"(threshold: {ALERT_STALE_MESSAGE_MS}ms)"
                )

        consumer_info = self.get_consumer_info()
        for c in consumer_info:
            if c["idle"] > ALERT_IDLE_CONSUMER_MS:
                alerts.append(
                    f"IDLE_CONSUMER: {c['name']} idle for {c['idle']}ms "
                    f"(threshold: {ALERT_IDLE_CONSUMER_MS}ms)"
                )

        return alerts

    def get_health_metrics(self) -> dict:
        """
        Get comprehensive health metrics for the task queue.

        Combines stream info, group info, consumer info, pending details,
        and alerts into a single response suitable for a health endpoint.

        Returns:
            dict: Complete health metrics including stream, group, consumers,
                  pending, and alerts
        """
        return {
            "stream": self.get_stream_info(),
            "group": self.get_group_info(),
            "consumers": self.get_consumer_info(),
            "pending_details": self.get_pending_details(),
            "alerts": self.check_alerts(),
        }
