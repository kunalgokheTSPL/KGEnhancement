import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any

from utility.logging.config import (
    LoggingSettings, ApiLogRecord
)
from utility.logging.storage import get_log_storage
from utility.logging.workers import (
    LogQueue, SpoolManager, BatchWorker, RetryWorker, get_s3_prefix_and_filename
)
from utility.logging.protobuf.apilog_pb2 import ApiBatch

logger = logging.getLogger(__name__)


# =====================================================================
# 1. LIFECYCLE SIGNAL REGISTRATION HELPER
# =====================================================================

def register_signal_handlers(manager: Any) -> None:
    """Registers SIGTERM and SIGINT handler to gracefully shutdown LoggingManager."""
    def shutdown_handler(signum: int, frame: Any) -> None:
        logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        try:
            manager.stop()
        except Exception as e:
            logger.critical(f"Error during shutdown sequence: {e}", exc_info=True)
        finally:
            sys.exit(0)

    try:
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
        logger.info("Signal handlers for SIGTERM and SIGINT registered successfully.")
    except ValueError:
        logger.warning(
            "Could not register signal handlers (must be in main thread). "
            "Ensure the FastAPI app lifespans trigger the LoggingManager.stop() call."
        )


# =====================================================================
# 2. LOGGING MANAGER
# =====================================================================

class LoggingManager:
    """Core coordinator of the API Logging Framework."""

    def __init__(self, config: LoggingSettings):
        self.config = config
        
        self.queue = None
        self.spool = None
        self.storage = None
        self.metrics = None
        self.batch_worker = None
        self.retry_worker = None
        
        self._is_active = False
        self._is_stopping = False

    def start(self, register_signals: bool = False) -> None:
        """Starts all components of the logging subsystem."""
        if self._is_active:
            logger.warning("LoggingManager is already active.")
            return

        logger.info("Initializing API Logging Framework...")
        
        self.queue = LogQueue(max_size=self.config.queue_max_size)
        self.spool = SpoolManager(spool_path=self.config.spool_path)
        
        from utility.logging.workers import MetricsCollector
        self.metrics = MetricsCollector()
        self.metrics.register_resources(self.queue, self.spool)
        
        # S3 Storage Backend connects using our wrapped S3 client.
        # Credentials and host details are resolved internally from p2.utils.database_driver.RustfsDriver.
        self.storage = get_log_storage(
            os.getenv("DEPLOYMENT_MODE", "on_prem"),
            bucket=self.config.rustfs_bucket,
            endpoint=self.config.rustfs_endpoint,
            access_key=self.config.rustfs_access_key,
            secret_key=self.config.rustfs_secret_key,
        )
        
        try:
            self.storage.connect()
        except Exception as e:
            logger.error(
                f"Failed to connect to primary storage backend during startup "
                f"(endpoint='{self.storage.driver.config['endpoint']}', "
                f"bucket='{self.config.rustfs_bucket}'): {e}"
            )
            logger.warning(
                f"Framework will start but ALL logs will spool locally to "
                f"'{self.config.spool_path}' and never leave this machine until "
                f"the RustFS endpoint above becomes reachable. Verify RUSTFS_HOST/"
                f"RUSTFS_PORT/RUSTFS_ENDPOINT (or APILOG_RUSTFS_ENDPOINT) are correct "
                f"for this environment."
            )

        self.batch_worker = BatchWorker(
            config=self.config,
            queue=self.queue,
            storage=self.storage,
            spool=self.spool,
            metrics=self.metrics
        )
        self.retry_worker = RetryWorker(
            config=self.config,
            storage=self.storage,
            spool=self.spool,
            metrics=self.metrics
        )
        
        self.batch_worker.start()
        self.retry_worker.start()
        
        # Register standard shutdown OS signal handlers
        if register_signals:
            register_signal_handlers(self)
        
        self._is_active = True
        self._is_stopping = False
        logger.info("API Logging Framework is ready and workers are running.")

    def stop(self) -> None:
        """Gracefully shuts down workers and flushes any pending queue data."""
        if not self._is_active:
            logger.warning("LoggingManager is not active.")
            return

        logger.info("Initiating graceful shutdown of API Logging Framework...")
        self._is_stopping = True

        if self.batch_worker:
            self.batch_worker.stop()
            self.batch_worker.join(timeout=10.0)
            
        if self.retry_worker:
            self.retry_worker.stop()
            self.retry_worker.join(timeout=5.0)

        self._flush_remaining_logs()

        self._is_active = False
        self._is_stopping = False
        logger.info("API Logging Framework shutdown complete.")

    def enqueue_log(self, record: ApiLogRecord) -> bool:
        if self._is_stopping or not self._is_active:
            logger.warning("Failed to enqueue log: logging framework is stopping or inactive.")
            return False
            
        return self.queue.push(record)

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.get_metrics() if self.metrics else {}

    def _flush_remaining_logs(self) -> None:
        if not self.queue or self.queue.size == 0:
            logger.info("Queue is empty. No final flush required.")
            return

        records = self.queue.drain()
        logger.info(f"Flushing {len(records)} remaining log records during shutdown...")

        batch_start = min(r.request_timestamp for r in records)
        batch_end = max(r.request_timestamp for r in records)
        
        filename = f"{batch_start // 1000}.pb"
        
        dt = datetime.fromtimestamp(batch_start / 1000.0, tz=timezone.utc)
        s3_key = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{filename}"

        try:
            batch_proto = ApiBatch()
            batch_proto.batch_start_timestamp = batch_start
            batch_proto.batch_end_timestamp = batch_end
            batch_proto.instance_id = self.config.instance_id
            batch_proto.batch_sequence = 9999
            
            for r in records:
                log_proto = batch_proto.logs.add()
                log_proto.request_timestamp = r.request_timestamp
                log_proto.api_endpoint = r.api_endpoint
                log_proto.http_method = r.http_method
                log_proto.status_code = r.status_code
                log_proto.success = r.success
                log_proto.error_message = r.error_message or ""
                log_proto.latency_ms = r.latency_ms
                if r.context:
                    for k, v in r.context.items():
                        log_proto.context[k] = str(v)

            serialized_data = batch_proto.SerializeToString()
            
            try:
                self.storage.upload(s3_key, serialized_data)
                logger.info("Final shutdown flush successfully uploaded to S3.")
            except Exception as upload_err:
                logger.error(f"Final shutdown flush failed to upload, writing to spool: {upload_err}")
                self.spool.save(filename, serialized_data)
                
        except Exception as e:
            logger.error(f"Critical failure during final flush serialization: {e}")
