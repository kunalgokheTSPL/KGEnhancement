import os
import queue
import time
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

from utility.logging.config import (
    LoggingSettings, ApiLogRecord, SpoolError, QueueOverflowError
)
from utility.logging.storage import StorageBackend
from utility.logging.protobuf.apilog_pb2 import ApiLog, ApiBatch

logger = logging.getLogger(__name__)


# =====================================================================
# 1. THREAD-SAFE QUEUE
# =====================================================================

class LogQueue:
    """An abstraction over a thread-safe, bounded, in-memory queue."""

    def __init__(self, max_size: int = 100000):
        self._queue: queue.Queue = queue.Queue(maxsize=max_size)
        self._dropped_count: int = 0

    def push(self, record: ApiLogRecord) -> bool:
        try:
            self._queue.put_nowait(record)
            return True
        except queue.Full:
            self._dropped_count += 1
            logger.warning(
                f"Log Queue is full (size={self._queue.qsize()}). "
                f"Dropped log record to prevent request blockage. Total dropped: {self._dropped_count}"
            )
            return False

    def drain(self, limit: Optional[int] = None) -> List[ApiLogRecord]:
        items: List[ApiLogRecord] = []
        try:
            while not self._queue.empty():
                if limit is not None and len(items) >= limit:
                    break
                items.append(self._queue.get_nowait())
                self._queue.task_done()
        except queue.Empty:
            pass
        return items

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def max_size(self) -> int:
        return self._queue.maxsize

    @property
    def dropped_count(self) -> int:
        return self._dropped_count


# =====================================================================
# 2. SPOOL MANAGER
# =====================================================================

class SpoolManager:
    """Manages temporary disk storage for failed uploads."""

    def __init__(self, spool_path: str):
        self.spool_path = os.path.abspath(spool_path)
        self.pending_dir = os.path.join(self.spool_path, "pending")
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        try:
            os.makedirs(self.pending_dir, exist_ok=True)
        except Exception as e:
            raise SpoolError(f"Failed to create spool directories under {self.spool_path}: {e}") from e

    def save(self, filename: str, data: bytes) -> str:
        file_path = os.path.join(self.pending_dir, filename)
        try:
            if os.path.exists(file_path):
                logger.warning(f"Spool file already exists: {file_path}. Overwriting.")
            
            with open(file_path, "wb") as f:
                f.write(data)
            
            logger.info(f"Saved failed upload to spool: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to write to spool file {file_path}: {e}")
            raise SpoolError(f"Failed to write to spool: {e}") from e

    def list_pending(self) -> List[Tuple[str, str]]:
        if not os.path.exists(self.pending_dir):
            return []
        try:
            files_with_time = []
            for file_name in os.listdir(self.pending_dir):
                full_path = os.path.join(self.pending_dir, file_name)
                if os.path.isfile(full_path):
                    mtime = os.path.getmtime(full_path)
                    files_with_time.append((mtime, full_path, file_name))
            
            files_with_time.sort(key=lambda x: x[0])
            return [(path, name) for _, path, name in files_with_time]
        except Exception as e:
            logger.error(f"Failed to scan pending spool directory: {e}")
            raise SpoolError(f"Failed to scan spool: {e}") from e

    def delete(self, filename: str) -> None:
        file_path = os.path.join(self.pending_dir, filename)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted successfully spooled file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to delete spooled file {file_path}: {e}")
                raise SpoolError(f"Failed to delete spool file: {e}") from e

    def get_metrics(self) -> dict:
        pending_files = self.list_pending()
        count = len(pending_files)
        total_size = 0
        oldest_time = 0.0

        if count > 0:
            oldest_path = pending_files[0][0]
            try:
                oldest_time = os.path.getmtime(oldest_path)
            except Exception:
                pass

        for path, _ in pending_files:
            try:
                total_size += os.path.getsize(path)
            except Exception:
                pass

        return {
            "pending_count": count,
            "spool_size_bytes": total_size,
            "oldest_file_timestamp": oldest_time
        }


# =====================================================================
# 3. METRICS COLLECTOR
# =====================================================================

class MetricsCollector:
    """Thread-safe collector for logging framework telemetry and operations metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        
        self._upload_success_count = 0
        self._upload_failure_count = 0
        self._retry_success_count = 0
        self._retry_failure_count = 0
        
        self._last_batch_size = 0
        self._last_batch_duration_ms = 0.0
        self._last_raw_bytes = 0
        self._last_upload_time_ms = 0.0

        self._queue: Optional[LogQueue] = None
        self._spool: Optional[SpoolManager] = None

    def register_resources(self, queue: LogQueue, spool: SpoolManager) -> None:
        self._queue = queue
        self._spool = spool

    def record_upload_success(self) -> None:
        with self._lock:
            self._upload_success_count += 1

    def record_upload_failure(self) -> None:
        with self._lock:
            self._upload_failure_count += 1

    def record_retry_attempt(self, success: bool) -> None:
        with self._lock:
            if success:
                self._retry_success_count += 1
            else:
                self._retry_failure_count += 1

    def record_batch_processed(
        self, 
        batch_size: int, 
        batch_duration_ms: float, 
        raw_bytes_size: int
    ) -> None:
        with self._lock:
            self._last_batch_size = batch_size
            self._last_batch_duration_ms = batch_duration_ms
            self._last_raw_bytes = raw_bytes_size

    def record_upload_timing(self, upload_time_ms: float) -> None:
        with self._lock:
            self._last_upload_time_ms = upload_time_ms

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            queue_len = self._queue.size if self._queue else 0
            queue_dropped = self._queue.dropped_count if self._queue else 0
            
            spool_metrics = self._spool.get_metrics() if self._spool else {
                "pending_count": 0,
                "spool_size_bytes": 0,
                "oldest_file_timestamp": 0.0
            }

            oldest_age_sec = 0.0
            if spool_metrics["oldest_file_timestamp"] > 0:
                oldest_age_sec = max(0.0, time.time() - spool_metrics["oldest_file_timestamp"])

            return {
                "queue": {
                    "length": queue_len,
                    "max_size": self._queue.max_size if self._queue else 0,
                    "dropped_records": queue_dropped
                },
                "spool": {
                    "pending_files": spool_metrics["pending_count"],
                    "spool_size_bytes": spool_metrics["spool_size_bytes"],
                    "oldest_file_age_seconds": oldest_age_sec
                },
                "uploads": {
                    "success_count": self._upload_success_count,
                    "failure_count": self._upload_failure_count
                },
                "retries": {
                    "success_count": self._retry_success_count,
                    "failure_count": self._retry_failure_count,
                    "total_attempts": self._retry_success_count + self._retry_failure_count
                },
                "last_batch": {
                    "size": self._last_batch_size,
                    "duration_ms": self._last_batch_duration_ms,
                    "raw_bytes": self._last_raw_bytes
                }
            }


# =====================================================================
# 4. S3 KEY HELPER
# =====================================================================

def get_s3_prefix_and_filename(
    timestamp_ms: int
) -> Tuple[str, str]:
    timestamp_seconds = timestamp_ms // 1000
    file_name = f"{timestamp_seconds}.pb"
    
    # Corrected timestamp conversion (requires division by 1000.0)
    dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
    s3_key = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{file_name}"
    
    return s3_key, file_name


# =====================================================================
# 5. BACKGROUND BATCH WORKER
# =====================================================================

class BatchWorker(threading.Thread):
    """Background worker that pulls log records, batches them, serializes, and uploads."""

    def __init__(
        self,
        config: LoggingSettings,
        queue: LogQueue,
        storage: StorageBackend,
        spool: SpoolManager,
        metrics: MetricsCollector
    ):
        super().__init__(name="ApiLogBatchWorker", daemon=True)
        self.config = config
        self.queue = queue
        self.storage = storage
        self.spool = spool
        self.metrics = metrics
        
        self._sequence = 1
        self._stop_event = threading.Event()
        self._cond = threading.Condition()

    def run(self) -> None:
        logger.info("Batch Worker started.")
        interval = self.config.batch_duration_seconds
        
        while not self._stop_event.is_set():
            with self._cond:
                self._cond.wait(timeout=float(interval))
            
            if self._stop_event.is_set():
                break
                
            self._process_batch()
            
        logger.info("Batch Worker stopped.")

    def stop(self) -> None:
        self._stop_event.set()
        with self._cond:
            self._cond.notify_all()

    def _process_batch(self) -> None:
        records = self.queue.drain()
        if not records:
            return

        batch_start = min(r.request_timestamp for r in records)
        batch_end = max(r.request_timestamp for r in records)
        s3_key, filename = get_s3_prefix_and_filename(batch_start)

        start_time = time.perf_counter()
        try:
            batch_proto = ApiBatch()
            batch_proto.batch_start_timestamp = batch_start
            batch_proto.batch_end_timestamp = batch_end
            batch_proto.instance_id = self.config.instance_id
            batch_proto.batch_sequence = self._sequence
            
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
            
            upload_success = False
            try:
                self.storage.upload(s3_key, serialized_data)
                upload_success = True
                self.metrics.record_upload_success()
            except Exception as upload_err:
                logger.error(f"Batch upload failed, fallback to local spool: {upload_err}")
                self.metrics.record_upload_failure()
                
            if not upload_success:
                self.spool.save(filename, serialized_data)

            duration_ms = (time.perf_counter() - start_time) * 1000
            self.metrics.record_batch_processed(
                batch_size=len(records),
                batch_duration_ms=duration_ms,
                raw_bytes_size=len(serialized_data)
            )

            self._sequence += 1

        except Exception as e:
            logger.error(f"Critical error during batch creation/upload: {e}", exc_info=True)
            self.metrics.record_upload_failure()


# =====================================================================
# 6. BACKGROUND RETRY WORKER
# =====================================================================

class RetryWorker(threading.Thread):
    """Background worker that scans the local spool directory and retries uploading failed files."""

    def __init__(
        self,
        config: LoggingSettings,
        storage: StorageBackend,
        spool: SpoolManager,
        metrics: MetricsCollector
    ):
        super().__init__(name="ApiLogRetryWorker", daemon=True)
        self.config = config
        self.storage = storage
        self.spool = spool
        self.metrics = metrics
        
        self._stop_event = threading.Event()
        self._cond = threading.Condition()
        
        self._failures: Dict[str, Tuple[int, float]] = {}
        self._max_backoff_multiplier = 32

    def run(self) -> None:
        logger.info("Retry Worker started.")
        interval = self.config.retry_interval_seconds
        
        while not self._stop_event.is_set():
            with self._cond:
                self._cond.wait(timeout=float(interval))
                
            if self._stop_event.is_set():
                break
                
            self._process_retries()
            
        logger.info("Retry Worker stopped.")

    def stop(self) -> None:
        self._stop_event.set()
        with self._cond:
            self._cond.notify_all()

    def _process_retries(self) -> None:
        try:
            pending_files = self.spool.list_pending()
        except Exception as e:
            logger.error(f"Retry worker failed to scan spool: {e}")
            return

        if not pending_files:
            return

        current_time = time.time()
        logger.info(f"Retry worker found {len(pending_files)} pending files in spool.")

        for file_path, filename in pending_files:
            if self._stop_event.is_set():
                break

            if filename in self._failures:
                fail_count, next_retry = self._failures[filename]
                if current_time < next_retry:
                    continue

            try:
                ts_str = filename.replace("_final.pb", "").replace(".pb", "")
                batch_start_ts = int(ts_str)
                dt = datetime.fromtimestamp(batch_start_ts, tz=timezone.utc)
                s3_key = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{filename}"
            except Exception:
                dt = datetime.now(timezone.utc)
                s3_key = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{filename}"

            logger.info(f"Attempting retry upload for {filename} -> {s3_key}")
            
            try:
                with open(file_path, "rb") as f:
                    data = f.read()

                self.storage.upload(s3_key, data)
                
                self.spool.delete(filename)
                self._failures.pop(filename, None)
                self.metrics.record_retry_attempt(success=True)
                
            except Exception as e:
                fail_count, _ = self._failures.get(filename, (0, 0.0))
                new_fail_count = fail_count + 1
                
                backoff_multiplier = min(2 ** (new_fail_count - 1), self._max_backoff_multiplier)
                backoff_duration = self.config.retry_interval_seconds * backoff_multiplier
                next_retry_time = current_time + backoff_duration
                
                self._failures[filename] = (new_fail_count, next_retry_time)
                
                logger.error(
                    f"Retry upload failed for {filename} (Attempt #{new_fail_count}). "
                    f"Backing off for {backoff_duration} seconds: {e}"
                )
                self.metrics.record_retry_attempt(success=False)
                break
