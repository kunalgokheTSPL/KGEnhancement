"""
pipeline_runner.py
==================
Two modes:

  TEST mode  — runs once on a fixed historical time range (for testing)
  LIVE mode  — watches IoTDB, triggers analysis on every new data arrival

Set MODE = "test" or MODE = "live" below, then:
    python pipeline_runner.py
"""

from __future__ import annotations

from p0.driver import (
    get_iotdb_connection,
    list_timeseries_paths,
    fetch_iot_data,
)

import logging
import sys
import time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional


_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("PipelineRunner")


# ──────────────────────────────────────────────────────────────────────────────
# ✏️  Config — edit these
# ──────────────────────────────────────────────────────────────────────────────

# "test" → run once on historical data
# "live" → run forever, trigger on new data
MODE = "test"

# TEST: fixed historical range from your IoTDB
TEST_START = datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
TEST_END = datetime(2024, 3, 4, 18, 38, 0, tzinfo=timezone.utc)

# LIVE: how often to poll IoTDB for new data (seconds)
POLL_SECONDS = 10

# LIVE: how much history to load on each trigger (hours)
WINDOW_HOURS = 1

# API endpoint
API_URL = "http://localhost:8000/analyze"

# Analyzer settings
ANALYZER_CONFIG = {
    "frozen_window": 5,
    "zero_window": 5,
    "spike_std_multiplier": 5.0,
    "duplicate_signal_percentile": 99.5,
}


# ──────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────────────


def get_all_tags(conn) -> List[str]:
    """Get all tag names from IoTDB."""
    paths = list_timeseries_paths(conn)
    # Extract just the tag name (last part of path e.g. root.plant.CP_PH001 → CP_PH001)
    return [p.split(".")[-1] for p in paths if p]


def fmt(dt: datetime) -> str:
    """Format datetime to the string format fetch_iot_data expects."""
    return dt.strftime("%d-%m-%Y %H:%M:%S")


def fetch_window(conn, tags: List[str], start: datetime, end: datetime) -> list:
    """Fetch data for given time window, return as list of row dicts for API."""
    df = fetch_iot_data(conn, tags, fmt(start), fmt(end))
    if df.empty:
        return []
    df["timestamp"] = df["timestamp"].astype(str)
    return df.to_dict(orient="records")


def post_to_api(data: list) -> bool:
    """POST data to quality API. Returns True on success."""
    try:
        response = requests.post(
            API_URL,
            json={"data": data, "config": ANALYZER_CONFIG},
            timeout=60,
        )
        response.raise_for_status()
        result = response.json()
        logger.info(
            "Analysis done | score=%.2f | frozen=%d | spikes=%d | missing=%d | null_bursts=%d",
            result.get("quality_score", 0),
            len(result.get("frozen_signals", [])),
            len(result.get("spikes", [])),
            len(result.get("missing_values", [])),
            len(result.get("null_bursts", [])),
        )
        return True
    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to API at %s — is it running?", API_URL)
        return False
    except requests.exceptions.HTTPError as e:
        logger.error("API error: %s | %s", e, e.response.text if e.response else "")
        return False
    except Exception:
        logger.exception("Unexpected error posting to API")
        return False


# ──────────────────────────────────────────────────────────────────────────────
# TEST mode
# ──────────────────────────────────────────────────────────────────────────────


def run_test() -> None:
    logger.info("TEST MODE | %s → %s", TEST_START.isoformat(), TEST_END.isoformat())

    conn = get_iotdb_connection()
    tags = get_all_tags(conn)

    if not tags:
        logger.error("No tags found in IoTDB.")
        return

    logger.info("Found %d tags — fetching data…", len(tags))
    data = fetch_window(conn, tags, TEST_START, TEST_END)

    if not data:
        logger.error("No data returned for the given time range.")
        return

    logger.info("Fetched %d rows — posting to API…", len(data))
    post_to_api(data)
    logger.info("Done. Fetch result: GET %s/latest", API_URL.replace("/analyze", ""))


# ──────────────────────────────────────────────────────────────────────────────
# LIVE mode
# ──────────────────────────────────────────────────────────────────────────────

_last_seen_ts: Optional[datetime] = None


def get_latest_timestamp(conn, tags: List[str]) -> Optional[datetime]:
    """Cheap check — only last 5 min to detect if new data arrived."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=5)
    try:
        df = fetch_iot_data(conn, tags[:10], fmt(start), fmt(end))
        if df.empty or "timestamp" not in df.columns:
            return None
        latest = df["timestamp"].max()
        if hasattr(latest, "to_pydatetime"):
            latest = latest.to_pydatetime()
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        return latest
    except Exception as e:
        logger.warning("Could not fetch latest timestamp: %s", e)
        return None


def run_live() -> None:
    global _last_seen_ts

    logger.info(
        "LIVE MODE | polling every %ds | window=%dh", POLL_SECONDS, WINDOW_HOURS
    )

    while True:
        try:
            conn = get_iotdb_connection()
            tags = get_all_tags(conn)

            if not tags:
                logger.warning("No tags found — retrying in %ds", POLL_SECONDS)
                time.sleep(POLL_SECONDS)
                continue

            latest_ts = get_latest_timestamp(conn, tags)

            if latest_ts is None:
                time.sleep(POLL_SECONDS)
                continue

            if _last_seen_ts is None or latest_ts > _last_seen_ts:
                logger.info("New data | ts=%s | prev=%s", latest_ts, _last_seen_ts)
                _last_seen_ts = latest_ts

                end = datetime.now(timezone.utc)
                start = end - timedelta(hours=WINDOW_HOURS)
                data = fetch_window(conn, tags, start, end)

                if data:
                    logger.info("Fetched %d rows — posting to API…", len(data))
                    post_to_api(data)
                else:
                    logger.warning("Window fetch returned empty — skipping")
            else:
                logger.debug("No new data since %s", _last_seen_ts)

        except Exception:
            logger.exception("Error in poll cycle — retrying in %ds", POLL_SECONDS)

        time.sleep(POLL_SECONDS)


# ──────────────────────────────────────────────────────────────────────────────
# Entry
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if MODE == "test":
        run_test()
    elif MODE == "live":
        run_live()
    else:
        logger.error("Invalid MODE '%s' — set MODE to 'test' or 'live'", MODE)
