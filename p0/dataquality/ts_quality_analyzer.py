"""
ts_quality_analyzer.py
=======================
IoT Time-Series Data Quality Analyzer — Live / Real-Time Window
----------------------------------------------------------------
Designed for live data: load last 1 hour, run on every new data arrival.
All checks are simple, direct, and fast on small windows.

Checks:
  1.  Dataset Overview
  2.  Sensor Count
  3.  Timestamp Validation
  4.  Missing Timestamps
  5.  Missing Values          — any null = missing
  6.  Duplicate Rows
  7.  Frozen Signal           — same value 5+ consecutive rows
  8.  Stuck-at-Zero           — any zero in last 5 rows
  9.  Dead Sensor             — near-zero unique values
  10. Spike Detection         — every value vs 5×std of window
  11. Duplicate Signals
  12. Null Burst
  13. Overall Quality Score

Usage:
    from ts_quality_analyzer import TimeSeriesQualityAnalyzer, PipelineConfig
    result = TimeSeriesQualityAnalyzer(df).analyze()
"""

from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TSQualityAnalyzer")


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class PipelineConfig:
    """
    frozen_window               : consecutive rows with same value = frozen (default 5)
    zero_window                 : look back N rows for zero check (default 5)
    spike_std_multiplier        : value is spike if > N × std of window (default 5)
    duplicate_signal_percentile : correlation percentile for duplicate check (default 99.5)
    """

    frozen_window: int = 5
    zero_window: int = 5
    spike_std_multiplier: float = 5.0
    duplicate_signal_percentile: float = 99.5


# ──────────────────────────────────────────────────────────────────────────────
# Result container
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class AnalysisResult:
    run_at: str = field(default_factory=lambda: datetime.now().isoformat())
    dataset_overview: Dict = field(default_factory=dict)
    sensor_count: Dict = field(default_factory=dict)
    timestamp_validation: Dict = field(default_factory=dict)
    missing_timestamps: Dict = field(default_factory=dict)
    missing_values: List[Dict] = field(default_factory=list)
    duplicate_rows: Dict = field(default_factory=dict)
    frozen_signals: List[Dict] = field(default_factory=list)
    stuck_at_zero: List[Dict] = field(default_factory=list)
    dead_sensors: List[Dict] = field(default_factory=list)
    spikes: List[Dict] = field(default_factory=list)
    duplicate_signals: List[Dict] = field(default_factory=list)
    null_bursts: List[Dict] = field(default_factory=list)
    quality_score: float = 100.0
    checks_summary: Dict = field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────────
# Analyzer
# ──────────────────────────────────────────────────────────────────────────────


class TimeSeriesQualityAnalyzer:
    def __init__(
        self, df: pd.DataFrame, config: Optional[PipelineConfig] = None
    ) -> None:
        self.cfg = config or PipelineConfig()
        self.df, self.ts_col = self._prepare(df)
        self.numeric_cols = self._detect_cols(numeric=True)
        self.categorical_cols = self._detect_cols(numeric=False)
        self.n = len(self.df)
        # Derive actual interval from data (in minutes) — used instead of hardcoded * 2
        self.interval_minutes = self._detect_interval_minutes()
        logger.info(
            "Analyzer ready | rows=%d | numeric=%d | categorical=%d | interval=%.1fmin",
            self.n,
            len(self.numeric_cols),
            len(self.categorical_cols),
            self.interval_minutes,
        )

    # ── Preparation ──────────────────────────────────────────────────

    def _prepare(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
        ts_col = self._find_timestamp_col(df)
        out = df.copy()
        out[ts_col] = pd.to_datetime(out[ts_col])
        out = out.sort_values(ts_col).reset_index(drop=True)
        return out, ts_col

    @staticmethod
    def _find_timestamp_col(df: pd.DataFrame) -> str:
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return col
        keywords = (
            "timestamp",
            "time",
            "date",
            "ts",
            "datetime",
            "created_at",
            "eventtime",
        )
        for col in df.columns:
            if any(kw in col.lower() for kw in keywords):
                return col
        for col in df.columns:
            try:
                pd.to_datetime(df[col])
                return str(col)
            except Exception:
                pass
        raise ValueError("No timestamp column found.")

    def _detect_cols(self, numeric: bool) -> List[str]:
        return [
            c
            for c in self.df.columns
            if c != self.ts_col
            and (pd.api.types.is_numeric_dtype(self.df[c]) == numeric)
        ]

    def _detect_interval_minutes(self) -> float:
        """Detect actual data interval from timestamps — no hardcoding."""
        ts = self.df[self.ts_col]
        diff = ts.diff().dropna()
        if diff.empty:
            return 1.0
        mode = diff.mode()[0]
        return round(mode.total_seconds() / 60, 2)

    # ── Check 1: Dataset Overview ─────────────────────────────────────

    def _check_dataset_overview(self) -> Dict:
        ts = self.df[self.ts_col]
        s, e = ts.min(), ts.max()
        return {
            "rows": int(self.n),
            "columns": int(len(self.df.columns)),
            "numeric_sensors": len(self.numeric_cols),
            "categorical_sensors": len(self.categorical_cols),
            "start_time": str(s),
            "end_time": str(e),
            "duration_minutes": round((e - s).total_seconds() / 60, 2),
            "data_interval_minutes": self.interval_minutes,
        }

    # ── Check 2: Sensor Count ─────────────────────────────────────────

    def _check_sensor_count(self) -> Dict:
        return {
            "total_signal_columns": int(len(self.df.columns) - 1),
            "numeric": len(self.numeric_cols),
            "categorical": len(self.categorical_cols),
        }

    # ── Check 3: Timestamp Validation ────────────────────────────────

    def _check_timestamp_validation(self) -> Dict:
        ts = self.df[self.ts_col]
        diff = ts.diff().dropna()
        mode = diff.mode()[0]
        dups = int(ts.duplicated().sum())
        irreg = int((diff != mode).sum())
        return {
            "expected_frequency": str(mode),
            "duplicate_timestamps": dups,
            "irregular_intervals": irreg,
            "monotonic_increasing": bool(ts.is_monotonic_increasing),
            "null_timestamps": int(ts.isna().sum()),
            "min_gap": str(diff.min()),
            "max_gap": str(diff.max()),
            "status": "PASS" if (dups == 0 and irreg == 0) else "WARN",
        }

    # ── Check 4: Missing Timestamps ───────────────────────────────────

    def _check_missing_timestamps(self) -> Dict:
        ts = self.df[self.ts_col]
        diff = ts.diff().dropna()
        mode = diff.mode()[0]
        large = diff[diff > mode]
        gap_list = [
            {"after_time": str(ts.iloc[i - 1]), "gap": str(diff.iloc[i])}
            for i in large.index
        ]
        total_missing = sum(
            max(int(g.total_seconds() / mode.total_seconds()) - 1, 0) for g in large
        )
        return {
            "expected_interval": str(mode),
            "gap_count": len(gap_list),
            "total_missing_slots_est": int(total_missing),
            "gaps": gap_list[:50],
            "status": "PASS" if len(gap_list) == 0 else "WARN",
        }

    # ── Check 5: Missing Values ───────────────────────────────────────

    def _check_missing_values(self) -> List[Dict]:
        out = []
        for col in self.numeric_cols + self.categorical_cols:
            missing = int(self.df[col].isna().sum())
            if missing > 0:
                null_idx = self.df[col][self.df[col].isna()].index.tolist()
                null_times = [str(self.df[self.ts_col].iloc[i]) for i in null_idx[:5]]
                out.append(
                    {
                        "affected_pct": round(missing / self.n * 100, 2),
                        "unit": "%",
                        "sensor": col,
                        "missing_count": missing,
                        "missing_pct": round(missing / self.n * 100, 2),
                        "first_null_at": null_times[0] if null_times else "",
                        "null_times": null_times,
                        "status": "FAIL",
                    }
                )
        out.sort(key=lambda x: x["missing_count"], reverse=True)
        return out

    # ── Check 6: Duplicate Rows ───────────────────────────────────────

    def _check_duplicate_rows(self) -> Dict:
        full_dups = int(self.df.duplicated().sum())
        ts_dups = int(self.df.duplicated(subset=[self.ts_col]).sum())
        return {
            "full_duplicate_rows": full_dups,
            "timestamp_duplicate_rows": ts_dups,
            "status": "PASS" if full_dups == 0 else "FAIL",
        }

    # ── Check 7: Frozen Signal ────────────────────────────────────────

    def _check_frozen_signals(self) -> List[Dict]:
        w = self.cfg.frozen_window
        out = []
        for col in self.numeric_cols:
            s = self.df[col].dropna()
            if len(s) < w:
                continue
            tail = s.iloc[-w:]
            if tail.nunique() == 1:
                frozen_val = tail.iloc[0]
                run = w
                vals = s.values
                for i in range(len(vals) - w - 1, -1, -1):
                    if vals[i] == frozen_val:
                        run += 1
                    else:
                        break
                out.append(
                    {
                        "affected_pct": round(run / self.n * 100, 2),
                        "unit": "%",
                        "sensor": col,
                        "frozen_value": round(float(frozen_val), 4)
                        if not pd.isna(frozen_val)
                        else None,
                        "frozen_rows": run,
                        "frozen_minutes": round(run * self.interval_minutes, 2),
                        "since": str(self.df[self.ts_col].iloc[-run]),
                        "status": "FAIL",
                    }
                )
        out.sort(key=lambda x: x["frozen_rows"], reverse=True)
        return out

    # ── Check 8: Stuck-at-Zero ────────────────────────────────────────

    def _check_stuck_at_zero(self) -> List[Dict]:
        w = self.cfg.zero_window
        out = []
        for col in self.numeric_cols:
            s = self.df[col].dropna()
            if len(s) == 0:
                continue
            tail = s.iloc[-w:]
            zero_rows = int((tail == 0).sum())
            if zero_rows > 0:
                zero_times = [
                    str(self.df[self.ts_col].iloc[i])
                    for i in self.df[col].iloc[-w:][self.df[col].iloc[-w:] == 0].index
                ]
                out.append(
                    {
                        "affected_pct": round(zero_rows / self.n * 100, 2),
                        "unit": "%",
                        "sensor": col,
                        "zero_rows": zero_rows,
                        "zero_pct": round(zero_rows / len(tail) * 100, 2),
                        "window_rows": w,
                        "zero_at_times": zero_times[:5],
                        "status": "FAIL" if zero_rows >= w else "WARN",
                    }
                )
        out.sort(key=lambda x: x["zero_rows"], reverse=True)
        return out

    # ── Check 9: Dead Sensor ─────────────────────────────────────────

    def _check_dead_sensors(self) -> List[Dict]:
        out = []
        for col in self.numeric_cols:
            s = self.df[col].dropna()
            if len(s) == 0:
                out.append(
                    {
                        "affected_pct": 0.0,
                        "unit": "%",
                        "sensor": col,
                        "unique_values": 0,
                        "unique_ratio": 0.0,
                        "status": "FAIL",
                    }
                )
                continue
            ratio = s.nunique() / len(s)
            if ratio <= 0.001:
                out.append(
                    {
                        "affected_pct": round(int(s.nunique()) / self.n * 100, 2),
                        "unit": "%",
                        "sensor": col,
                        "unique_values": int(s.nunique()),
                        "unique_ratio": round(ratio, 6),
                        "status": "FAIL",
                    }
                )
        return out

    # ── Check 10: Spike Detection ─────────────────────────────────────

    def _check_spikes(self) -> List[Dict]:
        k = self.cfg.spike_std_multiplier
        out = []
        for col in self.numeric_cols:
            s = self.df[col].dropna()
            if len(s) < 4:
                continue
            mean = float(s.mean())
            std = float(s.std())
            if std < 1e-9:
                continue
            threshold = k * std
            spike_mask = (s - mean).abs() > threshold
            spike_idx = spike_mask[spike_mask].index.tolist()
            if spike_idx:
                spikes = []
                for i in spike_idx:
                    val = float(self.df[col].iloc[i])
                    spikes.append(
                        {
                            "row": int(i),
                            "timestamp": str(self.df[self.ts_col].iloc[i]),
                            "value": round(val, 4),
                            "deviation": round(abs(val - mean) / std, 2),
                        }
                    )
                out.append(
                    {
                        "affected_pct": round(len(spikes) / self.n * 100, 2),
                        "unit": "%",
                        "sensor": col,
                        "spike_count": len(spikes),
                        "window_mean": round(mean, 4),
                        "window_std": round(std, 4),
                        "threshold": round(threshold, 4),
                        "threshold_rule": f"{k}×std({std:.4f})",
                        "spikes": spikes[:10],
                        "status": "FAIL",
                    }
                )
        out.sort(key=lambda x: x["spike_count"], reverse=True)
        return out

    # ── Check 11: Duplicate Signal ────────────────────────────────────

    def _check_duplicate_signals(self) -> List[Dict]:
        if len(self.numeric_cols) < 2:
            return []
        corr = self.df[self.numeric_cols].corr().abs()
        tri = np.triu_indices(len(self.numeric_cols), k=1)
        vals = corr.values[tri]
        vals = vals[~np.isnan(vals)]
        if len(vals) == 0:
            return []
        thresh = float(np.percentile(vals, self.cfg.duplicate_signal_percentile))
        method = f"P{self.cfg.duplicate_signal_percentile} of {len(vals)} pairs = {thresh:.6f}"
        pairs, seen = [], set()
        for i, a in enumerate(self.numeric_cols):
            for j, b in enumerate(self.numeric_cols):
                if j <= i or (a, b) in seen:
                    continue
                seen.add((a, b))
                v = corr.loc[a, b]
                if pd.notna(v) and v >= thresh:
                    pairs.append(
                        {
                            "affected_pct": round(float(v) * 100, 2),
                            "unit": "%",
                            "sensor_a": a,
                            "sensor_b": b,
                            "correlation": round(float(v), 6),
                            "threshold": round(thresh, 6),
                            "threshold_derivation": method,
                        }
                    )
        pairs.sort(key=lambda x: x["correlation"], reverse=True)
        return pairs

    # ── Check 12: Null Burst ─────────────────────────────────────────

    def _check_null_bursts(self) -> List[Dict]:
        out = []
        for col in self.numeric_cols:
            s = self.df[col]
            is_null = s.isna().astype(int)
            groups = is_null.ne(is_null.shift()).cumsum()
            sizes = is_null.groupby(groups).transform("sum")
            max_run = int(sizes.max())
            if max_run >= 3:
                start_idx = int((sizes == max_run).idxmax())
                out.append(
                    {
                        "affected_pct": round(max_run / self.n * 100, 2),
                        "unit": "%",
                        "sensor": col,
                        "max_null_burst": max_run,
                        "burst_start_time": str(self.df[self.ts_col].iloc[start_idx]),
                        "burst_minutes": round(max_run * self.interval_minutes, 2),
                        "status": "FAIL",
                    }
                )
        out.sort(key=lambda x: x["max_null_burst"], reverse=True)
        return out

    # ── Check 13: Quality Score ───────────────────────────────────────

    def _compute_quality_score(self, r: AnalysisResult) -> float:
        score = 100.0
        n_s = max(len(self.numeric_cols), 1)
        n_r = max(self.n, 1)

        score -= min((len(r.frozen_signals) / n_s) * 25, 25)
        score -= min((len(r.missing_values) / n_s) * 20, 20)
        total_spikes = sum(x["spike_count"] for x in r.spikes)
        score -= min((total_spikes / n_r) * 100 * 0.15, 15)
        score -= min((len(r.stuck_at_zero) / n_s) * 10, 10)
        score -= min((len(r.dead_sensors) / n_s) * 10, 10)
        score -= min((len(r.null_bursts) / n_s) * 10, 10)
        dup_pct = r.duplicate_rows.get("full_duplicate_rows", 0) / n_r * 100
        score -= min(dup_pct * 0.5, 5)

        return round(max(score, 0.0), 2)

    def _build_checks_summary(self, r: AnalysisResult) -> Dict:
        """
        Percentage of sensors affected by each check.
        affected_pct = (affected_sensors / total_numeric_sensors) * 100
        """
        n_s = max(len(self.numeric_cols), 1)
        n_r = max(self.n, 1)

        def pct(count: int) -> float:
            return round(count / n_s * 100, 2)

        total_spikes = sum(x["spike_count"] for x in r.spikes)

        return {
            "missing_values": {
                "affected_sensors": len(r.missing_values),
                "total_sensors": n_s,
                "affected_pct": pct(len(r.missing_values)),
                "unit": "%",
            },
            "frozen_signals": {
                "affected_sensors": len(r.frozen_signals),
                "total_sensors": n_s,
                "affected_pct": pct(len(r.frozen_signals)),
                "unit": "%",
            },
            "stuck_at_zero": {
                "affected_sensors": len(r.stuck_at_zero),
                "total_sensors": n_s,
                "affected_pct": pct(len(r.stuck_at_zero)),
                "unit": "%",
            },
            "dead_sensors": {
                "affected_sensors": len(r.dead_sensors),
                "total_sensors": n_s,
                "affected_pct": pct(len(r.dead_sensors)),
                "unit": "%",
            },
            "spikes": {
                "affected_sensors": len(r.spikes),
                "total_sensors": n_s,
                "affected_pct": pct(len(r.spikes)),
                "unit": "%",
                "total_spike_count": total_spikes,
            },
            "null_bursts": {
                "affected_sensors": len(r.null_bursts),
                "total_sensors": n_s,
                "affected_pct": pct(len(r.null_bursts)),
                "unit": "%",
            },
            "duplicate_signals": {
                "duplicate_pairs": len(r.duplicate_signals),
                "total_sensors": n_s,
                "affected_pct": pct(len(r.duplicate_signals) * 2),
                "unit": "%",
            },
            "duplicate_rows": {
                "duplicate_rows": r.duplicate_rows.get("full_duplicate_rows", 0),
                "total_rows": n_r,
                "affected_pct": round(
                    r.duplicate_rows.get("full_duplicate_rows", 0) / n_r * 100, 2
                ),
                "unit": "%",
            },
            "timestamp_gaps": {
                "gap_count": r.missing_timestamps.get("gap_count", 0),
                "missing_slots_est": r.missing_timestamps.get(
                    "total_missing_slots_est", 0
                ),
                "status": r.missing_timestamps.get("status", "PASS"),
            },
        }

    # ── Main entry ────────────────────────────────────────────────────

    def analyze(self) -> AnalysisResult:
        logger.info("Starting analysis…")
        r = AnalysisResult()

        r.dataset_overview = self._check_dataset_overview()
        r.sensor_count = self._check_sensor_count()
        r.timestamp_validation = self._check_timestamp_validation()
        r.missing_timestamps = self._check_missing_timestamps()
        r.missing_values = self._check_missing_values()
        r.duplicate_rows = self._check_duplicate_rows()
        r.frozen_signals = self._check_frozen_signals()
        r.stuck_at_zero = self._check_stuck_at_zero()
        r.dead_sensors = self._check_dead_sensors()
        r.spikes = self._check_spikes()
        r.duplicate_signals = self._check_duplicate_signals()
        r.null_bursts = self._check_null_bursts()
        r.quality_score = self._compute_quality_score(r)
        r.checks_summary = self._build_checks_summary(r)

        logger.info(
            "Done | score=%.2f | frozen=%d | spikes=%d | missing=%d | dup_pairs=%d",
            r.quality_score,
            len(r.frozen_signals),
            len(r.spikes),
            len(r.missing_values),
            len(r.duplicate_signals),
        )
        return r

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self.analyze()), indent=2, default=str)
