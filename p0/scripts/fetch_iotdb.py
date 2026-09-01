#!/usr/bin/env python3
"""
DecisionOps — IoTDB Data Fetcher
=================================
One-click script to fetch sensor / quality-lab timeseries data from IoTDB.
Automatically creates an SSH tunnel to the EC2 — no manual setup needed.

First-time setup (once):
    pip install pandas requests
    python fetch_iotdb.py --setup        # saves your SSH key path

Usage:
    python fetch_iotdb.py                          # interactive menu
    python fetch_iotdb.py --list                   # list all devices & tags
    python fetch_iotdb.py --device Kiln            # fetch all Kiln sensor data
    python fetch_iotdb.py --device Kiln --tags CP_KN001_TI_017_PV,CP_KN001_PI_001_SV
    python fetch_iotdb.py --device Kiln --last 7d  # last 7 days only
    python fetch_iotdb.py --all                    # fetch everything into CSVs

Requirements:  pip install pandas requests
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import socket
import subprocess
import sys
import time
import datetime as dt
from pathlib import Path

try:
    import requests
    import pandas as pd
except ImportError:
    sys.exit("Missing dependencies.  Run:  pip install pandas requests")

_p0_env = Path(__file__).resolve().parents[2] / ".env"
if _p0_env.exists():
    try:
        from dotenv import load_dotenv as _load_dotenv

        _load_dotenv(str(_p0_env), override=False)
    except ImportError:
        pass

_EC2_HOST = os.getenv("_EC2_HOST")
_EC2_USER = os.getenv("_EC2_USER")
_REMOTE_PORT = int(os.getenv("_REMOTE_PORT"))
_LOCAL_PORT = int(os.getenv("_LOCAL_PORT"))
_BASE_URL = f"http://localhost:{_LOCAL_PORT}/rest/v2/query"
_AUTH = (os.getenv("IOTDB_USER", "root"), os.getenv("IOTDB_PASSWORD", "root"))
_HEADERS = {"Content-Type": "application/json"}
_PLANT = os.getenv("_PLANT")
_ROOT = os.getenv("_ROOT")

_CONFIG_PATH = Path(__file__).parent / ".fetch_iotdb.json"

_DEVICES = {
    "Crusher": f"{_ROOT}.sensors.{_PLANT}.CP_CR_0001_Crusher",
    "Preheater": f"{_ROOT}.sensors.{_PLANT}.CP_PH_0001_Preheater",
    "Cooler": f"{_ROOT}.sensors.{_PLANT}.CP_CC_0001_Cooler",
    "CementMill": f"{_ROOT}.sensors.{_PLANT}.CP_CM_0001_CementMill1",
    "CoalMill": f"{_ROOT}.sensors.{_PLANT}.CP_CO_0001_CoalMill",
    "Kiln": f"{_ROOT}.sensors.{_PLANT}.CP_KN_0001_Kiln",
    "RawMill": f"{_ROOT}.sensors.{_PLANT}.CP_RM_0001_RawMill",
    "RawMealQuality": f"{_ROOT}.quality.{_PLANT}.Raw_Meal_Quality_2024",
    "CementQuality": f"{_ROOT}.quality.{_PLANT}.Cement_Quality_2021_2024",
    "CoalQuality": f"{_ROOT}.quality.{_PLANT}.Coal_Quality_2024",
    "ClinkerQuality": f"{_ROOT}.quality.{_PLANT}.Clinker_Quality_2021_2024",
}

_tunnel_proc: subprocess.Popen | None = None


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        return json.loads(_CONFIG_PATH.read_text())
    return {}


def _save_config(cfg: dict):
    _CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


def _find_ssh_key() -> str | None:
    """Auto-detect SSH key from saved config or common locations."""
    cfg = _load_config()
    if cfg.get("ssh_key") and Path(cfg["ssh_key"]).expanduser().exists():
        return str(Path(cfg["ssh_key"]).expanduser())

    ssh_dir = Path.home() / ".ssh"
    if not ssh_dir.exists():
        return None

    for pattern in ["*.pem", "id_ed25519", "id_rsa"]:
        matches = sorted(ssh_dir.glob(pattern))
        for key_path in matches:
            if key_path.suffix in (".pub",) or key_path.name in (
                "known_hosts",
                "config",
                "authorized_keys",
            ):
                continue
            return str(key_path)
    return None


def _port_is_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _cleanup_tunnel():
    global _tunnel_proc
    if _tunnel_proc and _tunnel_proc.poll() is None:
        _tunnel_proc.terminate()
        _tunnel_proc.wait(timeout=5)
        _tunnel_proc = None


def _ensure_tunnel(ssh_key: str | None = None):
    """Start an SSH tunnel if localhost:18080 is not already reachable."""
    global _tunnel_proc

    if _port_is_open(_LOCAL_PORT):
        return

    key_path = ssh_key or _find_ssh_key()
    if not key_path:
        print(
            "\n  Could not find an SSH key to connect to the server.\n"
            "  Run:  python fetch_iotdb.py --setup\n"
        )
        sys.exit(1)

    print("  Connecting to server (SSH tunnel)...", end="", flush=True)

    cmd = [
        "ssh",
        "-N",
        "-i",
        key_path,
        "-L",
        f"{_LOCAL_PORT}:localhost:{_REMOTE_PORT}",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ExitOnForwardFailure=yes",
        f"{_EC2_USER}@{_EC2_HOST}",
    ]

    try:
        _tunnel_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        sys.exit("\n  'ssh' command not found. Install OpenSSH client.")

    atexit.register(_cleanup_tunnel)

    for i in range(30):
        if _tunnel_proc.poll() is not None:
            stderr = _tunnel_proc.stderr.read().decode() if _tunnel_proc.stderr else ""
            sys.exit(f"\n  SSH tunnel failed to start.\n  {stderr.strip()}")
        if _port_is_open(_LOCAL_PORT):
            print(" connected.")
            return
        time.sleep(0.5)

    _tunnel_proc.terminate()
    sys.exit("\n  SSH tunnel timed out. Check your key and network.")


def _run_setup():
    """Interactive first-time setup — saves SSH key path."""
    print("\n" + "=" * 60)
    print("  DecisionOps — First-Time Setup")
    print("=" * 60)

    key = _find_ssh_key()
    if key:
        print(f"\n  Auto-detected SSH key: {key}")
        use_it = input("  Use this key? [Y/n]: ").strip().lower()
        if use_it in ("", "y", "yes"):
            _save_config({"ssh_key": key})
            print("  Saved. You're all set!\n")
            return

    key = input("\n  Path to your SSH key (e.g. ~/.ssh/my-key.pem): ").strip()
    expanded = str(Path(key).expanduser())
    if not Path(expanded).exists():
        sys.exit(f"  File not found: {expanded}")

    _save_config({"ssh_key": expanded})
    print("  Saved. You're all set!\n")


def _query(sql: str) -> dict:
    resp = requests.post(
        _BASE_URL, json={"sql": sql}, auth=_AUTH, headers=_HEADERS, timeout=60
    )
    resp.raise_for_status()
    return resp.json()


def _result_to_df(result: dict, device_path: str) -> pd.DataFrame:
    """Convert IoTDB REST v2 JSON result into a tidy pandas DataFrame."""
    cols = result.get("column_names") or result.get("expressions") or []
    timestamps = result.get("timestamps")
    values = result.get("values") or []

    if not cols or not values:
        return pd.DataFrame()

    prefix = device_path + "."
    clean_cols = []
    for c in cols:
        c_clean = c.replace("count(", "").rstrip(")")
        if c_clean.startswith(prefix):
            c_clean = c_clean[len(prefix) :]
        clean_cols.append(c_clean)

    data = {}
    if timestamps:
        data["timestamp"] = [dt.datetime.fromtimestamp(t / 1000) for t in timestamps]
    for i, col_name in enumerate(clean_cols):
        if i < len(values):
            data[col_name] = values[i]

    df = pd.DataFrame(data)
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def list_devices() -> dict[str, str]:
    """Return {friendly_name: full_iotdb_path}."""
    return dict(_DEVICES)


def list_tags(device_name: str) -> list[str]:
    """Return measurement tag names for a device."""
    path = _DEVICES.get(device_name)
    if not path:
        raise ValueError(
            f"Unknown device '{device_name}'. Choose from: {list(_DEVICES)}"
        )
    result = _query(f"SHOW TIMESERIES {path}.**")
    full_names = (result.get("values") or [[]])[0]
    prefix = path + "."
    return [n[len(prefix) :] if n.startswith(prefix) else n for n in full_names]


def fetch(
    device_name: str,
    tags: list[str] | None = None,
    last: str | None = None,
    limit: int = 100_000,
) -> pd.DataFrame:
    """Fetch timeseries data as a pandas DataFrame."""
    path = _DEVICES.get(device_name)
    if not path:
        raise ValueError(
            f"Unknown device '{device_name}'. Choose from: {list(_DEVICES)}"
        )

    select_clause = "*" if not tags else ", ".join(tags)
    sql = f"SELECT {select_clause} FROM {path}"

    if last:
        sql += f" WHERE time > now() - {last}"
    sql += f" LIMIT {limit}"

    result = _query(sql)
    return _result_to_df(result, path)


def _interactive():
    print("\n" + "=" * 60)
    print("  DecisionOps — IoTDB Data Fetcher")
    print("=" * 60)

    devices = list(_DEVICES.keys())
    print("\nAvailable devices:\n")
    for i, name in enumerate(devices, 1):
        category = "quality" if "Quality" in name else "sensor"
        print(f"  [{i:2d}] {name:<25s}  ({category})")

    print("\n  [ 0] Fetch ALL devices -> separate CSVs")
    print("  [ q] Quit\n")

    choice = input("Pick a device number: ").strip()

    if choice.lower() == "q":
        return

    if choice == "0":
        _fetch_all()
        return

    try:
        idx = int(choice) - 1
        device_name = devices[idx]
    except (ValueError, IndexError):
        print(f"Invalid choice: {choice}")
        return

    print(f"\nFetching tag list for {device_name}...")
    tags = list_tags(device_name)
    print(f"  {len(tags)} tags available:")
    for t in tags:
        print(f"    - {t}")

    last = (
        input("\nTime window (e.g. 7d, 24h, 30m) or Enter for all data: ").strip()
        or None
    )

    print(f"\nFetching data from {device_name}...")
    df = fetch(device_name, last=last)
    print(f"  Got {len(df)} rows x {len(df.columns)} columns")

    if df.empty:
        print("  No data found.")
        return

    print(f"\n  Columns: {df.columns.tolist()}")
    print("\n  Preview (first 5 rows):")
    print(df.head().to_string(index=False))

    out_dir = Path("iotdb_exports")
    out_dir.mkdir(exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{device_name}_{ts}.csv"
    df.to_csv(out_path, index=False)
    print(f"\n  Saved -> {out_path}  ({len(df)} rows)")


def _fetch_all():
    out_dir = Path("iotdb_exports")
    out_dir.mkdir(exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    for name in _DEVICES:
        print(f"  Fetching {name}...", end="", flush=True)
        try:
            df = fetch(name)
            if df.empty:
                print(" (empty)")
                continue
            out_path = out_dir / f"{name}_{ts}.csv"
            df.to_csv(out_path, index=False)
            print(f" {len(df)} rows -> {out_path}")
        except Exception as e:
            print(f" ERROR: {e}")

    print(f"\n  All exports saved to ./{out_dir}/")


def main():
    ap = argparse.ArgumentParser(
        description="Fetch sensor & quality data from IoTDB (no config needed)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
First-time:
  python fetch_iotdb.py --setup                   # save your SSH key path

Examples:
  python fetch_iotdb.py                           # interactive menu
  python fetch_iotdb.py --list                    # show devices & tags
  python fetch_iotdb.py --device Kiln             # all Kiln data -> CSV
  python fetch_iotdb.py --device Kiln --last 7d   # last 7 days
  python fetch_iotdb.py --device Kiln --tags CP_KN001_TI_017_PV
  python fetch_iotdb.py --all                     # all devices -> CSVs
""",
    )
    ap.add_argument(
        "--setup", action="store_true", help="First-time setup: save SSH key path"
    )
    ap.add_argument(
        "--list", action="store_true", help="List all devices and their tags"
    )
    ap.add_argument(
        "--device", type=str, help="Device name (e.g. Kiln, Crusher, CementQuality)"
    )
    ap.add_argument("--tags", type=str, help="Comma-separated tag names (default: all)")
    ap.add_argument("--last", type=str, help="Time window (e.g. 7d, 24h, 30m)")
    ap.add_argument(
        "--all", action="store_true", help="Fetch all devices into separate CSVs"
    )
    ap.add_argument(
        "--limit", type=int, default=100_000, help="Max rows (default: 100000)"
    )
    ap.add_argument(
        "--out",
        type=str,
        help="Output CSV path (default: iotdb_exports/<device>_<ts>.csv)",
    )
    ap.add_argument("--key", type=str, help="SSH key path (overrides saved config)")
    args = ap.parse_args()

    if args.setup:
        _run_setup()
        return

    _ensure_tunnel(ssh_key=args.key)

    if not any([args.list, args.device, args.all]):
        _interactive()
        return

    if args.list:
        print("\nDevices and tags:\n")
        for name, path in _DEVICES.items():
            category = "quality" if "Quality" in name else "sensor"
            tags = list_tags(name)
            print(f"  {name} ({category}) -- {len(tags)} tags")
            for t in tags:
                print(f"    - {t}")
            print()
        return

    if args.all:
        _fetch_all()
        return

    if args.device:
        tag_list = [t.strip() for t in args.tags.split(",")] if args.tags else None
        df = fetch(args.device, tags=tag_list, last=args.last, limit=args.limit)

        if df.empty:
            print("No data found.")
            return

        print(f"Fetched {len(df)} rows x {len(df.columns)} columns")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nPreview:\n{df.head().to_string(index=False)}")

        out_dir = Path("iotdb_exports")
        out_dir.mkdir(exist_ok=True)
        if args.out:
            out_path = Path(args.out)
        else:
            ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = out_dir / f"{args.device}_{ts}.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved -> {out_path}")


if __name__ == "__main__":
    main()
