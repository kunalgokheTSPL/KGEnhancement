import logging
import os
from dataclasses import dataclass, field
from typing import Dict, Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

# =====================================================================
# 1. CUSTOM EXCEPTIONS
# =====================================================================

class LoggingFrameworkError(Exception):
    """Base exception for all logging framework errors."""
    pass


class QueueOverflowError(LoggingFrameworkError):
    """Raised when the in-memory queue is full and cannot accept more logs."""
    pass


class StorageBackendError(LoggingFrameworkError):
    """Raised when upload, exist-check, or delete operations fail in the storage backend."""
    pass


class SpoolError(LoggingFrameworkError):
    """Raised when writing to or reading from the local spool directory fails."""
    pass


# =====================================================================
# 2. DATA MODELS
# =====================================================================

@dataclass
class ApiLogRecord:
    """Lightweight in-memory model representing an individual API log record."""
    request_timestamp: int  # Milliseconds since epoch
    api_endpoint: str
    http_method: str
    status_code: int
    success: bool
    error_message: str = ""
    latency_ms: float = 0.0
    context: Dict[str, str] = field(default_factory=dict)


# =====================================================================
# 3. SETTINGS & CONFIGURATION
# =====================================================================

class LoggingSettings(BaseSettings):
    """Configuration settings for the API Logging Framework."""
    model_config = SettingsConfigDict(
        env_prefix="APILOG_", 
        case_sensitive=False,
        extra="ignore"
    )

    # General / Instance Configuration
    instance_id: str = "scheduler01"
    log_level: str = "INFO"

    # Batching Configuration
    batch_duration_seconds: int = 60
    queue_max_size: int = 100000

    # Storage Backend Configuration
    rustfs_bucket: str = "logging"
    rustfs_endpoint: Optional[str] = None
    rustfs_access_key: Optional[str] = None
    rustfs_secret_key: Optional[str] = None

    # Spool / Retry Configuration
    spool_path: str = "./.spool/apilog"
    retry_interval_seconds: int = 60

    def model_post_init(self, __context) -> None:
        self.spool_path = os.path.abspath(os.path.expanduser(self.spool_path))
