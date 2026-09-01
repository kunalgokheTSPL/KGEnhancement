import sys
import os
import socket
from dotenv import load_dotenv

# Ensure the root directory is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

# Enable ANSI escape sequences for color on Windows
if os.name == "nt":
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        # Enable ENABLE_VIRTUAL_TERMINAL_PROCESSING (0x0004)
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

# Color codes
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Load environment variables and decrypted secrets
load_dotenv()
from utility.secret_manager import load_secrets

load_secrets()

# Import the actual drivers to ensure we test the active runtime configuration
try:
    from p2.utility.database_driver import PostgresDriver, IoTDBDriver, RustfsDriver
except ImportError as imp_err:
    print(
        f"{RED}[FAIL] Could not import drivers from p2.utility.database_driver: {imp_err}{RESET}"
    )
    sys.exit(1)


def mask_value(val):
    if not val:
        return "<Not Set>"
    val = str(val)
    if len(val) <= 4:
        return "*" * len(val)
    return val[:2] + "*" * (len(val) - 4) + val[-2:]


def print_env_diagnostics():
    print("\n" + "=" * 70)
    print("ENVIRONMENT DIAGNOSTICS".center(70))
    print("=" * 70)

    # Instantiate drivers temporarily to inspect their resolved configurations
    try:
        pg_cdm = PostgresDriver()
        pg_mf = PostgresDriver()
        pg_db = PostgresDriver()
    except Exception:
        pg_cdm = pg_mf = pg_db = None

    try:
        iotdb = IoTDBDriver()
    except Exception:
        iotdb = None

    try:
        rustfs = RustfsDriver()
    except Exception:
        rustfs = None

    print("\n[PostgreSQL Environment Variables]")
    if pg_cdm:
        print(f"  POSTGRES_HOST           : {pg_cdm.config.get('host')}")
        print(f"  POSTGRES_PORT           : {pg_cdm.config.get('port')}")
        print(f"  POSTGRES_USER           : {pg_cdm.config.get('user')}")
        print(
            f"  POSTGRES_PASSWORD       : {mask_value(pg_cdm.config.get('password'))}"
        )
        print(f"  POSTGRES_DB_CDM         : {pg_cdm.config.get('database')}")
    else:
        print("  <Postgres Driver Init Failed / Config Unreachable>")

    if pg_mf:
        print(f"  POSTGRES_DB_MODELFACTORY: {pg_mf.config.get('database')}")
    if pg_db:
        print(f"  POSTGRES_DB_DASHBOARD   : {pg_db.config.get('database')}")

    print("\n[IoTDB Environment Variables]")
    if iotdb:
        print(f"  IOTDB_HOST              : {iotdb.config.get('host')}")
        print(f"  IOTDB_PORT              : {iotdb.config.get('port')}")
        print(f"  IOTDB_USER              : {iotdb.config.get('user')}")
        print(f"  IOTDB_PASSWORD          : {mask_value(iotdb.config.get('password'))}")
    else:
        print("  <IoTDB Driver Init Failed / Config Unreachable>")

    print("\n[RustFS Environment Variables]")
    if rustfs:
        print(f"  RUSTFS_HOST             : {rustfs.config.get('host')}")
        print(f"  RUSTFS_PORT             : {rustfs.config.get('port')}")
        print(f"  RUSTFS_ACCESS_ID        : {rustfs.config.get('access_id')}")
        print(
            f"  RUSTFS_SECRET_KEY       : {mask_value(rustfs.config.get('secret_key'))}"
        )
    else:
        print("  <RustFS Driver Init Failed / Config Unreachable>")

    print("=" * 70 + "\n")


def test_tcp_port(host, port, timeout=2):
    """Checks if host:port is reachable at the TCP socket level."""
    if not host or not port:
        return False, "Host or port not configured."
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        return False, f"DNS resolution failed for '{host}': {e}"

    try:
        with socket.create_connection((ip, int(port)), timeout=timeout):
            return True, f"Port {port} is open on {ip}."
    except ConnectionRefusedError:
        return False, f"Connection refused. Port {port} is closed on {ip}."
    except socket.timeout:
        return False, f"Connection timed out. Port {port} on {ip} is unreachable."
    except Exception as e:
        return False, f"Network error: {e}"


def test_postgres(results):
    print(f"\n{BOLD}{CYAN}1. POSTGRESQL CONNECTIONS{RESET}")
    print(
        f"{CYAN}----------------------------------------------------------------------{RESET}"
    )

    databases = ["cdm", "modelfactory", "decisionops_dashboard"]
    all_ok = True

    for db_key in databases:
        try:
            driver = PostgresDriver()
            host = driver.config.get("host")
            port = driver.config.get("port", 5432)
            db_name = driver.config.get("database")
            user = driver.config.get("user")

            print(
                f"  * Database: {BOLD}{db_key}{RESET} (Name: '{db_name}', Host: {host}:{port})"
            )

            # Step A: TCP reachability
            is_up, net_msg = test_tcp_port(host, port)
            if is_up:
                print(f"    {GREEN}[PASS]{RESET} Network port is open.")
            else:
                print(f"    {RED}[FAIL]{RESET} Network port check: {net_msg}")
                all_ok = False
                results.append(
                    (
                        f"Postgres ({db_key})",
                        f"{host}:{port}",
                        "FAIL",
                        "Port closed/unreachable",
                    )
                )
                continue

            # Step B: Connect & Query
            try:
                driver.connect()
                with driver.conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    version_info = cursor.fetchone()[0]
                    # Extract a shorter version description
                    short_version = (
                        version_info.split(",")[0]
                        if version_info
                        else "Unknown Version"
                    )
                print(
                    f"    {GREEN}[PASS]{RESET} Auth successful. DB Version: {short_version}"
                )
                driver.close()
                results.append(
                    (
                        f"Postgres ({db_key})",
                        f"{host}:{port}",
                        "PASS",
                        f"Connected ({short_version})",
                    )
                )
            except Exception as conn_err:
                print(f"    {RED}[FAIL]{RESET} Connection or authentication failed.")
                print(f"           Details: {conn_err}")
                all_ok = False
                results.append(
                    (
                        f"Postgres ({db_key})",
                        f"{host}:{port}",
                        "FAIL",
                        "Auth/Connection Error",
                    )
                )
        except Exception as init_err:
            print(f"  * Database: {BOLD}{db_key}{RESET}")
            print(f"    {RED}[FAIL]{RESET} Driver initialization failed: {init_err}")
            all_ok = False
            results.append((f"Postgres ({db_key})", "Unknown", "FAIL", "Init Error"))

    return all_ok


def test_iotdb(results):
    print(f"\n{BOLD}{CYAN}2. IOTDB CONNECTION{RESET}")
    print(
        f"{CYAN}----------------------------------------------------------------------{RESET}"
    )

    try:
        driver = IoTDBDriver()
        host = driver.config.get("host", "localhost")
        port = driver.config.get("port", 18080)

        print(f"  * Endpoint: {BOLD}{driver.base_url}{RESET}")

        # Step A: TCP reachability
        is_up, net_msg = test_tcp_port(host, port)
        if is_up:
            print(f"    {GREEN}[PASS]{RESET} Network port is open.")
        else:
            print(f"    {RED}[FAIL]{RESET} Network port check: {net_msg}")
            results.append(
                ("IoTDB", f"{host}:{port}", "FAIL", "Port closed/unreachable")
            )
            return False

        # Step B: Query REST v2 API
        try:
            driver.connect()
            # Try metadata query and get actual timeseries
            res = driver.query("SHOW TIMESERIES LIMIT 5")
            timeseries_list = res.get("values", [[]])[0] if res else []
            count = len(timeseries_list)

            print(f"    {GREEN}[PASS]{RESET} REST API connection and query successful.")
            if count > 0:
                print("    [INFO] Timeseries sample (up to 5):")
                for ts in timeseries_list:
                    print(f"           - {ts}")
            else:
                print("    [INFO] No timeseries registered in database.")

            results.append(
                (
                    "IoTDB",
                    f"{host}:{port}",
                    "PASS",
                    f"Connected ({count} timeseries sample fetched)",
                )
            )
            return True
        except Exception as err:
            print(f"    {RED}[FAIL]{RESET} API query failed.")
            print(f"           Details: {err}")
            results.append(
                ("IoTDB", f"{host}:{port}", "FAIL", "Query/REST call failure")
            )
            return False
    except Exception as init_err:
        print(f"    {RED}[FAIL]{RESET} Driver initialization failed: {init_err}")
        results.append(("IoTDB", "Unknown", "FAIL", "Init Error"))
        return False


def test_rustfs(results):
    print(f"\n{BOLD}{CYAN}3. RUSTFS (S3-COMPATIBLE) CONNECTION{RESET}")
    print(
        f"{CYAN}----------------------------------------------------------------------{RESET}"
    )

    try:
        driver = RustfsDriver()
        host = driver.config.get("host")
        port = driver.config.get("port")
        endpoint = f"http://{host}:{port}"

        print(
            f"  * Endpoint: {BOLD}{endpoint}{RESET} (Access ID: {driver.config.get('access_id')})"
        )

        # Step A: TCP reachability
        is_up, net_msg = test_tcp_port(host, port)
        if is_up:
            print(f"    {GREEN}[PASS]{RESET} Network port is open.")
        else:
            print(f"    {RED}[FAIL]{RESET} Network port check: {net_msg}")

            # Check alternative port if configured one failed
            if port != 50580:
                print("    [INFO] Checking alternative port 50580...")
                alt_up, _ = test_tcp_port(host, 50580)
                if alt_up:
                    print(f"    {GREEN}[FOUND]{RESET} Service is active on port 50580!")
                    print(
                        "            --> Action: Update your configuration/driver port to 50580."
                    )
            results.append(
                ("RustFS (S3)", f"{host}:{port}", "FAIL", "Port closed/unreachable")
            )
            return False

        # Step B: Boto3 List Buckets API
        try:
            driver.connect()
            resp = driver.client.list_buckets()
            buckets = [b["Name"] for b in resp.get("Buckets", [])]
            print(f"    {GREEN}[PASS]{RESET} Boto3 list_buckets query successful.")
            print(f"    [INFO] Found {len(buckets)} bucket(s): {buckets}")
            driver.close()
            results.append(
                (
                    "RustFS (S3)",
                    f"{host}:{port}",
                    "PASS",
                    f"Connected ({len(buckets)} buckets: {', '.join(buckets[:3])}...)"
                    if buckets
                    else "Connected (0 buckets)",
                )
            )
            return True
        except Exception as err:
            print(f"    {RED}[FAIL]{RESET} S3 API connection failed.")
            print(f"           Details: {err}")
            results.append(
                ("RustFS (S3)", f"{host}:{port}", "FAIL", "S3 Protocol Error")
            )
            return False
    except Exception as init_err:
        print(f"    {RED}[FAIL]{RESET} Driver initialization failed: {init_err}")
        results.append(("RustFS (S3)", "Unknown", "FAIL", "Init Error"))
        return False


def print_summary_table(results):
    print("\n" + "=" * 80)
    print(f"{BOLD}CONNECTION STATUS SUMMARY{RESET}".center(80))
    print("=" * 80)

    # Header
    col_widths = [32, 18, 10, 32]
    header = f" {'Connection':<31} | {'Endpoint':<17} | {'Status':<9} | {'Details':<31}"
    print(header)
    print("-" * 105)

    # Rows
    all_ok = True
    for conn, endpoint, status, details in results:
        status_color = GREEN if status == "PASS" else RED
        status_str = f"{status_color}{status:<9}{RESET}"
        if status == "FAIL":
            all_ok = False
        print(f" {conn:<31} | {endpoint:<17} | {status_str} | {details:<31}")

    print("-" * 105)
    if all_ok:
        print(
            f"\n{GREEN}{BOLD}Result: SUCCESS - All connections are verified and healthy!{RESET}".center(
                105
            )
        )
    else:
        print(
            f"\n{RED}{BOLD}Result: FAILURE - One or more connection checks failed.{RESET}".center(
                105
            )
        )
    print("=" * 105)


def main():
    print("=" * 105)
    print(
        f"{BOLD}DecisionOps Connection Integration Diagnostic Suite{RESET}".center(105)
    )
    print("=" * 105)

    print_env_diagnostics()

    results = []
    test_postgres(results)
    test_iotdb(results)
    test_rustfs(results)

    print_summary_table(results)


if __name__ == "__main__":
    main()
