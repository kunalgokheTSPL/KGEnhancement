"""
In-place "progress" logging for pipeline log files.

`pip install ...` style: one line shows the current step (e.g. "extracting
chunk 25/61"), and the next progress update overwrites it instead of adding a
new line. The next non-progress record commits the line in place and resumes
normal appending.

Why this exists
---------------
Each LLM chunk in the docs pipeline writes one INFO line. A 200-page PDF =
~60 chunks = 60 near-identical lines. Tailing the log via /api/logs/{key}
becomes hard to read and the file balloons.

How to use
----------
1.  Build the logger with :func:`setup_pipeline_logger` from this module
    (or attach :class:`InPlaceProgressHandler` to your existing logger).
2.  Tag any "this is a progress tick" record with `extra={"progress": True}`::

        log.info("LLM extracting chunk %d/%d", i + 1, n, extra={"progress": True})

    The first such record reserves a slot. Each subsequent progress record
    overwrites that slot. The next *non-progress* record (e.g. "done") seals
    the slot and writes after it.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path


class InPlaceProgressHandler(logging.FileHandler):
    """File handler with terminal-style in-place updates for progress records."""

    def __init__(self, filename: str, encoding: str = "utf-8") -> None:
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        super().__init__(filename, mode="r+", encoding=encoding, delay=False)
        self.stream.seek(0, os.SEEK_END)
        self._progress_pos: int | None = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            is_progress = bool(getattr(record, "progress", False))
            stream = self.stream
            if is_progress:
                if self._progress_pos is None:
                    self._progress_pos = stream.tell()
                else:
                    stream.seek(self._progress_pos)
                stream.write(msg + "\n")
                stream.truncate()
                stream.flush()
            else:
                if self._progress_pos is not None:
                    stream.seek(0, os.SEEK_END)
                    self._progress_pos = None
                stream.write(msg + "\n")
                stream.flush()
        except Exception:
            self.handleError(record)


def setup_pipeline_logger(
    logger_name: str,
    log_file: str,
    *,
    fmt: str = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
    file_level: int = logging.DEBUG,
    stream_level: int = logging.INFO,
) -> logging.Logger:
    """Configure a pipeline logger that writes to *log_file* with in-place"""
    formatter = logging.Formatter(fmt, datefmt=datefmt)

    fh: logging.Handler = InPlaceProgressHandler(log_file)
    fh.setLevel(file_level)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(stream_level)
    ch.setFormatter(formatter)

    log = logging.getLogger(logger_name)
    log.setLevel(min(file_level, stream_level))
    if not log.handlers:
        log.addHandler(fh)
        log.addHandler(ch)
    return log
