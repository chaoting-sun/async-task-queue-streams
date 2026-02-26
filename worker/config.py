import os

# Redis connection settings
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Stream settings
STREAM_NAME = os.getenv("STREAM_NAME", "task_stream")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "workers")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", os.getenv("HOSTNAME", f"worker-{os.getpid()}"))

# Timing settings
BLOCK_MS = int(os.getenv("BLOCK_MS", "5000"))

# Stale message recovery settings
# Messages pending longer than this threshold can be claimed by other workers
STALE_MESSAGE_THRESHOLD_MS = int(os.getenv("STALE_MESSAGE_THRESHOLD_MS", "30000"))
# Maximum number of stale messages to recover per iteration
STALE_RECOVERY_COUNT = int(os.getenv("STALE_RECOVERY_COUNT", "10"))

# Retry and Dead Letter Queue settings
# Maximum number of delivery attempts before moving to DLQ
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "1"))
# Dead Letter Queue stream name for failed messages
DLQ_STREAM_NAME = os.getenv("DLQ_STREAM_NAME", "dead_letter_stream")

# Observability & Monitoring settings
# Alert threshold for high pending message count
ALERT_HIGH_PENDING_THRESHOLD = int(os.getenv("ALERT_HIGH_PENDING_THRESHOLD", "100"))
# Alert threshold for stale messages (idle time in milliseconds)
ALERT_STALE_MESSAGE_MS = int(os.getenv("ALERT_STALE_MESSAGE_MS", "60000"))
# Alert threshold for idle consumers (idle time in milliseconds)
ALERT_IDLE_CONSUMER_MS = int(os.getenv("ALERT_IDLE_CONSUMER_MS", "300000"))
# Maximum number of pending entries to fetch for detailed inspection
PENDING_DETAILS_COUNT = int(os.getenv("PENDING_DETAILS_COUNT", "100"))
