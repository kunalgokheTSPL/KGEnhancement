import sys
import os
import traceback
import warnings
from starlette.routing import Mount, Route

# Silence all deprecation and user warnings
warnings.filterwarnings("ignore")

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


def log_detailed_exception(e):
    """
    Parses the traceback structure to isolate the exact line and file in the project
    responsible for the error and prints it in a prominent debug format.
    """
    tb = e.__traceback__
    extracted = traceback.extract_tb(tb)

    # Trace backwards to find the frame within the user's workspace files
    project_frame = None
    for frame in reversed(extracted):
        if "DecisionOps" in frame.filename and "site-packages" not in frame.filename:
            project_frame = frame
            break

    # Fallback to the leaf frame if no workspace frame is matched
    if not project_frame and extracted:
        project_frame = extracted[-1]

    print(f"\n{RED}{BOLD}[CRITICAL FAILURE DIAGNOSTICS]{RESET}")
    print(f"  {RED}Exception Class{RESET} : {BOLD}{type(e).__name__}{RESET}")
    print(f"  {RED}Error Message  {RESET} : {e}")

    if project_frame:
        print(f"  {RED}Offending File {RESET} : {BOLD}{project_frame.filename}{RESET}")
        print(f"  {RED}Line Number    {RESET} : {BOLD}{project_frame.lineno}{RESET}")
        print(f"  {RED}Function Name  {RESET} : {project_frame.name}()")
        print(f"  {RED}Statement Code {RESET} : `{project_frame.line}`")

    print(f"\n{YELLOW}{BOLD}Full Exception Traceback:{RESET}")
    traceback.print_exc(file=sys.stdout)


print("=" * 85)
print(f"{BOLD}DecisionOps Application Dynamic Routing Test Suite{RESET}".center(85))
print("=" * 85)

print("\n[+] Loading secrets and initializing app components...")
try:
    from fastapi.testclient import TestClient
    from app import app as gateway_app

    client = TestClient(gateway_app)
    print(
        f"    {GREEN}[PASS]{RESET} Gateway application imported and TestClient initialized successfully.\n"
    )
except Exception as e:
    print(f"    {RED}[FAIL]{RESET} Failed to load app or initialize TestClient.")
    log_detailed_exception(e)
    sys.exit(1)

# Dynamically discover all routes and mounted sub-apps
endpoints = []

print(f"{BOLD}{CYAN}Discovering active routes in Gateway...{RESET}")
for route in gateway_app.routes:
    if isinstance(route, Route):
        # Standard route. Test simple GET routes that do not require path parameters.
        if "GET" in route.methods and "{" not in route.path:
            endpoints.append(
                {
                    "method": "GET",
                    "path": route.path,
                    "desc": f"Root GET endpoint '{route.name or route.path}'",
                    "expected_status": 200,
                }
            )
    elif isinstance(route, Mount):
        # Mounted sub-application. Test its OpenAPI documentation endpoint.
        mount_path = route.path
        endpoints.append(
            {
                "method": "GET",
                "path": f"{mount_path.rstrip('/')}/openapi.json",
                "desc": f"Mounted app '{mount_path}' OpenAPI configuration",
                "expected_status": 200,
            }
        )

print(f"    {GREEN}[OK]{RESET} Discovered {len(endpoints)} endpoints to test.\n")

results = []
all_ok = True

print(f"{BOLD}{CYAN}Testing Discovered Routes:{RESET}")
print(
    f"{CYAN}-------------------------------------------------------------------------------------{RESET}"
)

for ep in endpoints:
    method = ep["method"]
    path = ep["path"]
    desc = ep["desc"]
    expected_status = ep["expected_status"]

    print(f"  * Routing to {BOLD}{path}{RESET} ({desc})...")
    try:
        if method == "GET":
            response = client.get(path, timeout=15)
        else:
            response = client.post(path, timeout=15)

        status = response.status_code

        # Mounted apps return 200 for openapi.json. Check for correct status.
        status_ok = status == expected_status

        if status_ok:
            print(f"    {GREEN}[PASS]{RESET} Status: {status} OK.")
            results.append((path, desc, "PASS", f"Status: {status}"))
        else:
            all_ok = False
            err_msg = f"Expected status {expected_status}, got {status}"
            print(f"    {RED}[FAIL]{RESET} {err_msg}")
            print(f"           Response snippet: {response.text[:200]}")
            results.append((path, desc, "FAIL", err_msg))
    except Exception as e:
        all_ok = False
        print(f"    {RED}[ERROR]{RESET} Exception occurred during request to {path}.")
        log_detailed_exception(e)
        results.append((path, desc, "FAIL", f"Exception: {type(e).__name__}"))

print("\n" + "=" * 90)
print(f"{BOLD}ROUTING TEST SUMMARY{RESET}".center(90))
print("=" * 90)

# Print tabular summary
header = f" {'Route Path':<35} | {'Description':<38} | {'Status':<9}"
print(header)
print("-" * 90)

for path, desc, status, msg in results:
    status_color = GREEN if status == "PASS" else RED
    status_str = f"{status_color}{status:<9}{RESET}"
    print(f" {path:<35} | {desc:<38} | {status_str}")

print("-" * 90)
if all_ok:
    print(
        f"\n{GREEN}{BOLD}Result: SUCCESS - All discovered routes and sub-apps are mounted and routing correctly!{RESET}".center(
            90
        )
    )
else:
    print(
        f"\n{RED}{BOLD}Result: FAILURE - One or more application routes failed.{RESET}".center(
            90
        )
    )
print("=" * 90)
sys.exit(0 if all_ok else 1)
