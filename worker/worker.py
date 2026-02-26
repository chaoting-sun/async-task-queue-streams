import signal
from redis_client import get_redis_client
from stream_manager import StreamManager
from task_processor import TaskProcessor
from config import CONSUMER_NAME

# Graceful shutdown flag
# This flag controls the main loop - when set to False, the worker
# will complete its current task and exit cleanly
running = True


def signal_handler(signum: int, frame) -> None:
    """
    Handle shutdown signals (SIGTERM, SIGINT) for graceful termination.

    When a signal is received, we set running=False which allows the
    current task to complete before the worker exits. This prevents
    messages from being left in an indeterminate state.

    Args:
        signum: The signal number received
        frame: The current stack frame (unused)
    """
    global running
    signal_name = signal.Signals(signum).name
    print(f"Received {signal_name} (signal {signum}), initiating graceful shutdown...")
    running = False


def process_message(
    manager: StreamManager,
    processor: TaskProcessor,
    message_id: str,
    fields: dict,
    is_recovered: bool = False,
) -> None:
    """
    Process a single message from the stream.

    Checks delivery count and routes to DLQ after max retries.

    Args:
        manager: StreamManager instance for acknowledgment
        processor: TaskProcessor instance for task execution
        message_id: The Redis stream message ID
        fields: The message fields containing task data
        is_recovered: True if this message was recovered via XAUTOCLAIM
    """
    task_id = fields.get("task_id")
    if not task_id:
        print(f"Message {message_id} missing task_id, acknowledging anyway")
        manager.ack(message_id)
        return

    source = "Recovered" if is_recovered else "Claimed"
    delivery_count = manager.get_delivery_count(message_id)
    print(f"{source} message {message_id} for task {task_id} (attempt {delivery_count}/{manager.get_max_retries()})")

    success, error = processor.process(task_id, fields)

    if success:
        ack_count = manager.ack(message_id)
        print(f"Acknowledged message {message_id} (ack_count={ack_count})")
    else:
        if manager.should_move_to_dlq(message_id):
            dlq_msg_id = manager.move_to_dlq(message_id, fields, error)
            processor.update_status_dlq(task_id, error, delivery_count)
            print(
                f"Task {task_id} exceeded max retries ({delivery_count}), "
                f"moved to DLQ as {dlq_msg_id}"
            )
        else:
            print(
                f"Task {task_id} failed (attempt {delivery_count}/{manager.get_max_retries()}), "
                f"leaving in PEL for retry"
            )


def start_worker() -> None:
    """
    Main worker entry point.

    Implementation:
    - Register signal handlers for graceful shutdown
    - First recover stale messages from crashed/slow workers (XAUTOCLAIM)
    - Then claim new messages (XREADGROUP)
    - Check delivery count and route to DLQ if max retries exceeded
    - Acknowledge on completion (XACK)
    - On shutdown signal: complete current task, then exit cleanly
    """
    global running

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"Starting worker: {CONSUMER_NAME}")

    redis_client = get_redis_client()
    manager = StreamManager(redis_client)
    processor = TaskProcessor(redis_client)

    manager.ensure_consumer_group()

    print(f"Worker {CONSUMER_NAME} ready, waiting for tasks...")

    while running:
        # First, check for stale messages from crashed/slow workers
        stale_messages = manager.recover_stale_messages()
        for message_id, fields in stale_messages:
            process_message(manager, processor, message_id, fields, is_recovered=True)
            # Check shutdown flag between processing recovered messages
            if not running:
                break

        # Exit loop if shutdown was requested during recovery processing
        if not running:
            break

        # Then, claim new messages
        message_id, fields = manager.claim_task()

        if message_id is None:
            continue

        process_message(manager, processor, message_id, fields, is_recovered=False)

    print(f"Worker {CONSUMER_NAME} stopped gracefully")


if __name__ == "__main__":
    start_worker()
