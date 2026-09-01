#!/usr/bin/env python3
"""
DecisionOps — PostgreSQL CDM Data Fetcher
==========================================
One-click script to fetch CDM (Canonical Data Model) tables from PostgreSQL.
Automatically creates an SSH tunnel to the EC2 — no manual setup needed.

First-time setup (once):
    pip install pandas psycopg2-binary
    python fetch_postgres.py --setup        # saves your SSH key path

Usage:
    python fetch_postgres.py                          # interactive menu
    python fetch_postgres.py --list                   # list tables + row counts
    python fetch_postgres.py --table failure_mode     # fetch full table -> CSV
    python fetch_postgres.py --table equipment --columns "equipment_id,equipment_type,status"
    python fetch_postgres.py --table failure_mode --where "risk_category='HIGH - Immediate Action'"
    python fetch_postgres.py --all                    # all tables -> CSVs
    python fetch_postgres.py --sql "SELECT * FROM equipment WHERE criticality='A'"

Requirements:  pip install pandas psycopg2-binary
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
    import pandas as pd
    import psycopg2
except ImportError:
    sys.exit("Missing dependencies.  Run:  pip install pandas psycopg2-binary")

_p0_env = Path(__file__).resolve().parents[2] / ".env"
if _p0_env.exists():
    try:
        from dotenv import load_dotenv as _load_dotenv

        _load_dotenv(str(_p0_env), override=False)
    except ImportError:
        pass

_EC2_HOST = os.getenv("EC2_HOST")
_EC2_USER = os.getenv("EC2_USER")
_REMOTE_PORT = int(os.getenv("POSTGRES_REMOTE_PORT"))
_LOCAL_PORT = int(os.getenv("POSTGRES_LOCAL_PORT"))

_DB_HOST = os.getenv("IOTDB_HOST")
_DB_PORT = int(os.getenv("IOTDB_PORT"))
_DB_NAME = os.getenv("CDM_DB_NAME")
_DB_USER = os.getenv("IOTDB_USER")
_DB_PASS = os.getenv("IOTDB_PASSWORD")

_CONFIG_PATH = Path(__file__).parent / ".fetch_postgres.json"

_TABLES = [
    ("equipment", "Equipment & Assets", "Master equipment register"),
    ("equipment_pid", "Equipment & Assets", "P&ID source equipment"),
    ("equipment_sap", "Equipment & Assets", "SAP source equipment"),
    (
        "equipment_connectivity",
        "Equipment & Assets",
        "Equipment-to-equipment connections",
    ),
    ("functional_location", "Equipment & Assets", "Functional locations hierarchy"),
    ("process_unit", "Equipment & Assets", "Process units / areas"),
    ("asset_cross_reference", "Equipment & Assets", "PID <-> SAP tag mapping"),
    ("asset_relationship", "Equipment & Assets", "Parent-child / MONITORS / FLOW"),
    ("work_order", "Maintenance & FMEA", "Maintenance work orders"),
    ("notification", "Maintenance & FMEA", "Maintenance notifications"),
    ("task_list", "Maintenance & FMEA", "Task lists / operations"),
    ("maintenance_plan", "Maintenance & FMEA", "Preventive maintenance plans"),
    ("work_center", "Maintenance & FMEA", "Work centers"),
    ("failure_mode", "Maintenance & FMEA", "FMEA failure modes + RPN scores"),
    ("failure_cause", "Maintenance & FMEA", "FMEA failure causes"),
    ("failure_effect", "Maintenance & FMEA", "FMEA failure effects"),
    ("material", "Materials & Procurement", "Material master"),
    ("bom_item", "Materials & Procurement", "Bills of material"),
    ("vendor", "Materials & Procurement", "Vendor master"),
    ("purchase_order", "Materials & Procurement", "Purchase orders"),
    ("goods_receipt", "Materials & Procurement", "Goods receipts"),
    ("invoice", "Materials & Procurement", "Invoices"),
    ("timeseries_metadata", "Timeseries & Documents", "Sensor tag register + limits"),
    (
        "document_metadata",
        "Timeseries & Documents",
        "Document metadata + extracted entities",
    ),
    ("standards_field_registry", "Governance", "CDM field standards"),
]

_TABLE_NAMES = [t[0] for t in _TABLES]


_tunnel_proc: subprocess.Popen | None = None


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        return json.loads(_CONFIG_PATH.read_text())
    return {}


def _save_config(cfg: dict):
    _CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


def _find_ssh_key() -> str | None:
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
    global _tunnel_proc

    if _port_is_open(_LOCAL_PORT):
        return

    key_path = ssh_key or _find_ssh_key()
    if not key_path:
        print(
            "\n  Could not find an SSH key to connect to the server.\n"
            "  Run:  python fetch_postgres.py --setup\n"
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

    for _ in range(30):
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
    print("\n" + "=" * 60)
    print("  DecisionOps — PostgreSQL Fetcher Setup")
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


def _get_conn():
    return psycopg2.connect(
        host=_DB_HOST,
        port=_DB_PORT,
        dbname=_DB_NAME,
        user=_DB_USER,
        password=_DB_PASS,
    )


def _query_df(sql: str) -> pd.DataFrame:
    import warnings

    conn = _get_conn()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return pd.read_sql(sql, conn)
    finally:
        conn.close()


def list_tables() -> pd.DataFrame:
    """Return table names with row counts."""
    conn = _get_conn()
    try:
        rows = []
        cur = conn.cursor()
        for tname, category, desc in _TABLES:
            try:
                cur.execute(f"SELECT count(*) FROM {tname}")
                count = cur.fetchone()[0]
            except Exception:
                conn.rollback()
                count = "-"
            rows.append((tname, category, desc, count))
        return pd.DataFrame(rows, columns=["table", "category", "description", "rows"])
    finally:
        conn.close()


def fetch_table(
    table_name: str,
    columns: list[str] | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    if table_name not in _TABLE_NAMES:
        raise ValueError(f"Unknown table '{table_name}'. Choose from: {_TABLE_NAMES}")

    col_clause = ", ".join(columns) if columns else "*"
    sql = f"SELECT {col_clause} FROM {table_name}"
    if where:
        sql += f" WHERE {where}"
    if limit:
        sql += f" LIMIT {limit}"

    return _query_df(sql)


def run_sql(sql: str) -> pd.DataFrame:
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT"):
        sys.exit("  Only SELECT queries are allowed.")
    return _query_df(sql)


def _interactive():
    print("\n" + "=" * 60)
    print("  DecisionOps — PostgreSQL CDM Data Fetcher")
    print("=" * 60)

    current_cat = ""
    table_list = []
    for i, (tname, category, desc) in enumerate(_TABLES):
        if category != current_cat:
            current_cat = category
            print(f"\n  {category.upper()}:")
        table_list.append(tname)
        print(f"    [{i + 1:2d}] {tname:<30s} {desc}")

    print("\n    [ 0] Fetch ALL tables -> separate CSVs")
    print("    [ q] Quit\n")

    choice = input("Pick a table number: ").strip()

    if choice.lower() == "q":
        return

    if choice == "0":
        _fetch_all()
        return

    try:
        idx = int(choice) - 1
        table_name = table_list[idx]
    except (ValueError, IndexError):
        print(f"Invalid choice: {choice}")
        return

    print(f"\nFetching {table_name}...")
    df = fetch_table(table_name)
    print(f"  Got {len(df)} rows x {len(df.columns)} columns")

    if df.empty:
        print("  Table is empty.")
        return

    print(f"\n  Columns: {df.columns.tolist()}")
    print("\n  Preview (first 5 rows):")
    print(df.head().to_string(index=False))

    out_dir = Path("postgres_exports")
    out_dir.mkdir(exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{table_name}_{ts}.csv"
    df.to_csv(out_path, index=False)
    print(f"\n  Saved -> {out_path}  ({len(df)} rows)")


def _fetch_all():
    out_dir = Path("postgres_exports")
    out_dir.mkdir(exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    for tname, _, _ in _TABLES:
        print(f"  Fetching {tname}...", end="", flush=True)
        try:
            df = fetch_table(tname)
            if df.empty:
                print(" (empty)")
                continue
            out_path = out_dir / f"{tname}_{ts}.csv"
            df.to_csv(out_path, index=False)
            print(f" {len(df)} rows -> {out_path}")
        except Exception as e:
            print(f" ERROR: {e}")

    print(f"\n  All exports saved to ./{out_dir}/")


def main():
    ap = argparse.ArgumentParser(
        description="Fetch CDM data from PostgreSQL (no config needed)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
First-time:
  python fetch_postgres.py --setup                    # save your SSH key path

Examples:
  python fetch_postgres.py                            # interactive menu
  python fetch_postgres.py --list                     # show tables + row counts
  python fetch_postgres.py --table failure_mode       # full table -> CSV
  python fetch_postgres.py --table equipment --columns "equipment_id,equipment_type,status"
  python fetch_postgres.py --table failure_mode --where "risk_category='HIGH - Immediate Action'"
  python fetch_postgres.py --all                      # all tables -> CSVs
  python fetch_postgres.py --sql "SELECT * FROM equipment WHERE criticality='A'"
""",
    )
    ap.add_argument(
        "--setup", action="store_true", help="First-time setup: save SSH key path"
    )
    ap.add_argument(
        "--list", action="store_true", help="List all CDM tables with row counts"
    )
    ap.add_argument(
        "--table", type=str, help="Table name to fetch (e.g. failure_mode, equipment)"
    )
    ap.add_argument(
        "--columns", type=str, help="Comma-separated column names (default: all)"
    )
    ap.add_argument(
        "--where",
        type=str,
        help="WHERE clause filter (e.g. \"plant_code_id='CEMENT_PLANT_1'\")",
    )
    ap.add_argument("--limit", type=int, help="Max rows to fetch")
    ap.add_argument(
        "--all", action="store_true", help="Fetch all tables into separate CSVs"
    )
    ap.add_argument("--sql", type=str, help="Run a custom SELECT query")
    ap.add_argument(
        "--out",
        type=str,
        help="Output CSV path (default: postgres_exports/<table>_<ts>.csv)",
    )
    ap.add_argument("--key", type=str, help="SSH key path (overrides saved config)")
    args = ap.parse_args()

    if args.setup:
        _run_setup()
        return

    _ensure_tunnel(ssh_key=args.key)

    if not any([args.list, args.table, args.all, args.sql]):
        _interactive()
        return

    if args.list:
        print("\n  CDM Tables:\n")
        df = list_tables()
        for _, row in df.iterrows():
            print(
                f"    {row['table']:<30s}  {str(row['rows']):>6s} rows    ({row['description']})"
            )
        total = sum(r for r in df["rows"] if isinstance(r, int))
        print(f"\n    {'TOTAL':<30s}  {total:>6d} rows")
        return

    if args.sql:
        df = run_sql(args.sql)
        print(f"Fetched {len(df)} rows x {len(df.columns)} columns")
        print(f"\nPreview:\n{df.head(10).to_string(index=False)}")

        out_dir = Path("postgres_exports")
        out_dir.mkdir(exist_ok=True)
        if args.out:
            out_path = Path(args.out)
        else:
            ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = out_dir / f"query_{ts}.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved -> {out_path}")
        return

    if args.all:
        _fetch_all()
        return

    if args.table:
        col_list = (
            [c.strip() for c in args.columns.split(",")] if args.columns else None
        )
        df = fetch_table(
            args.table, columns=col_list, where=args.where, limit=args.limit
        )

        if df.empty:
            print("Table is empty.")
            return

        print(f"Fetched {len(df)} rows x {len(df.columns)} columns")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nPreview:\n{df.head().to_string(index=False)}")

        out_dir = Path("postgres_exports")
        out_dir.mkdir(exist_ok=True)
        if args.out:
            out_path = Path(args.out)
        else:
            ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = out_dir / f"{args.table}_{ts}.csv"
        df.to_csv(out_path, index=False)
        print(f"\nSaved -> {out_path}")


if __name__ == "__main__":
    main()
