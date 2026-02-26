import json
import time
import random
import redis


class TaskProcessor:
    """
    Processes tasks from the stream.
    Updates task status in Redis during processing.
    Supports DLQ status updates for failed tasks.
    """

    def __init__(self, redis_client: redis.Redis):
        self.r = redis_client

    def process(self, task_id: str, fields: dict) -> tuple[bool, str | None]:
        """
        Process a task and update its status.

        Args:
            task_id: The task ID (from task:data:{task_id})
            fields: The message fields from the stream

        Returns:
            tuple: (success: bool, error: str | None)
        """
        task_key = f"task:data:{task_id}"

        self._update_status(task_key, "processing")

        try:
            image_url = fields.get("image_url", "unknown")
            print(f"Processing task {task_id}: {image_url}")

            # Simulate real-world flakiness (Chaos Monkey) - 30% failure rate
            if random.random() < 0.3:
                raise Exception("Simulated Network Error!")

            processing_time = random.uniform(1.0, 3.0)
            time.sleep(processing_time)

            result = {
                "processed_url": f"processed_{image_url}",
                "processing_time": round(processing_time, 2),
            }

            self._update_status(task_key, "completed", result=result)
            print(f"Completed task {task_id} in {processing_time:.2f}s")
            return True, None

        except Exception as e:
            error_msg = str(e)
            self._update_status(task_key, "failed", error=error_msg)
            print(f"Failed task {task_id}: {e}")
            return False, error_msg

    def update_status_dlq(self, task_id: str, error: str, attempts: int) -> None:
        """
        Update task status to indicate it was moved to the Dead Letter Queue.

        Args:
            task_id: The task ID
            error: The error message that caused the final failure
            attempts: Total number of delivery attempts
        """
        task_key = f"task:data:{task_id}"
        self._update_status(
            task_key,
            "dead_letter",
            error=error,
            extra={"total_attempts": attempts, "moved_to_dlq_at": time.time()},
        )

    def _update_status(
        self,
        task_key: str,
        status: str,
        result: dict | None = None,
        error: str | None = None,
        extra: dict | None = None,
    ) -> None:
        """Update task status in Redis."""
        existing = self.r.get(task_key)
        if existing:
            data = json.loads(existing)
        else:
            data = {}

        data["status"] = status
        data["updated_at"] = time.time()

        if result:
            data["result"] = result
        if error:
            data["error"] = error
        if extra:
            data.update(extra)

        self.r.set(task_key, json.dumps(data))
