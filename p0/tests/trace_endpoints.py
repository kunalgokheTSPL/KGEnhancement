"""Drive every p0 endpoint over HTTP and record which p0 functions each one executes.

Runs a real journey (create plant -> uploads with real files -> reads -> patches ->
delete plant) against a throwaway ``*_testcase`` plant, with a profiler capturing
every p0-namespace call per request. Output: a per-endpoint call-chain reference.

Dev tool, not a test (pytest ignores it — no test_ prefix). Lives with the test
harness it depends on; it creates and drops a real throwaway database per run.

    MASTER_KEY=... PYTHONPATH=$PWD AUTH_ENABLED=false \
      .venv/bin/python p0/tests/trace_endpoints.py > /tmp/trace.md
"""

from __future__ import annotations

import io
import sys
import threading
import uuid
from collections import defaultdict
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND))

from utility.secret_manager import load_secrets

load_secrets()

from p0.tests._p0_test_base import cdm_app, DEV_TOKEN, MountedTestClient

PREFIX = str(BACKEND / "p0")
SKIP_DIRS = ("/tests/", "/Login/", "/dataquality/", "/scripts/", "/_attic/")

_calls: list[tuple[int, str, str]] = []
_depth = threading.local()


def _profiler(frame, event, arg):
    """Record ('depth', module, qualname) for every p0-namespace python call."""
    if event not in ("call", "return"):
        return
    fn = frame.f_code.co_filename
    if not fn.startswith(PREFIX) or any(s in fn for s in SKIP_DIRS):
        return
    d = getattr(_depth, "v", 0)
    if event == "return":
        _depth.v = max(0, d - 1)
        return
    _depth.v = d + 1
    mod = fn[len(PREFIX) + 1 :].replace(".py", "").replace("/", ".")
    _calls.append((d, mod, frame.f_code.co_qualname))


def traced(fn, *a, **kw):
    """Run fn with the profiler armed on this and any new thread."""
    _calls.clear()
    _depth.v = 0
    threading.setprofile(_profiler)
    sys.setprofile(_profiler)
    try:
        return fn(*a, **kw)
    finally:
        sys.setprofile(None)
        threading.setprofile(None)


def chain() -> list[str]:
    """The recorded calls as indented 'module.qualname' lines, first-hit order."""
    seen, out = set(), []
    for d, mod, qn in _calls:
        key = f"{mod}.{qn}"
        if key in seen or qn.startswith(("<", "_profiler")):
            continue
        seen.add(key)
        out.append("  " * min(d, 6) + key)
    return out


def _pdf() -> bytes:
    import fitz

    d = fitz.open()
    d.new_page().insert_text((72, 72), "TRACE KILN 01")
    b = d.tobytes()
    d.close()
    return b


def _xlsx() -> bytes:
    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame([{"EQUNR": "EQ-9001", "EQKTX": "Trace Pump", "TPLNR": "A1"}]).to_excel(
        buf, index=False, engine="openpyxl"
    )
    return buf.getvalue()


def main() -> None:
    plant = f"trace_{uuid.uuid4().hex[:6]}_testcase"
    c = MountedTestClient(cdm_app, cookies={"access_token": DEV_TOKEN})
    report: list[dict] = []

    def hit(label, verb, path, *, params=None, jsonb=None, data=None, files=None):
        r = traced(
            c.request, verb, path, params=params, json=jsonb, data=data, files=files
        )
        body_keys = "-"
        if r.headers.get("content-type", "").startswith("application/json"):
            b = r.json()
            body_keys = ",".join(sorted(b)) if isinstance(b, dict) else type(b).__name__
        report.append(
            {
                "label": label,
                "req": f"{verb} {path}",
                "params": params or data or (list(jsonb) if isinstance(jsonb, dict) else None),
                "status": r.status_code,
                "body": body_keys,
                "chain": chain(),
            }
        )
        return r

    P = {"plant_code_id": plant}
    csvb = b"timestamp,TAG-1\n2026-06-01T00:00:00Z,1.5\n"

    from p0.api import plants as _pl

    def _teardown():
        try:
            _pl._drop_plant_databases(plant)
        finally:
            _pl._unregister_plant(plant)

    import atexit

    atexit.register(_teardown)

    hit("health", "GET", "/health")
    hit("create plant", "POST", "/configRouter/plants", jsonb={"plant_code_id": plant, "label": "trace", "industry": "cement"})
    hit("list plants", "GET", "/configRouter/plants", params=P)
    hit("purge preview", "GET", f"/configRouter/plants/{plant}/purgePreview")
    for label, path in [
        ("sap tables", "/configRouter/sapTables"),
        ("column renames", "/configRouter/columnRenames"),
        ("column renames (sap)", "/configRouter/columnRenames/sap"),
        ("user config", "/configRouter/userConfig"),
        ("cdm config", "/configRouter/cdm"),
        ("entities", "/configRouter/entities"),
        ("schema", "/configRouter/schema"),
        ("relationships", "/configRouter/relationships"),
        ("validation contracts", "/configRouter/validation"),
        ("industries", "/configRouter/industries"),
        ("industry (cement)", "/configRouter/industries/cement"),
        ("document types", "/configRouter/documentTypes"),
    ]:
        hit(label, "GET", path, params=P)

    hit("upload pnid pdf", "POST", "/connectors/pnid/upload", data=P, files={"files": ("t.pdf", _pdf(), "application/pdf")})
    hit("upload sap xlsx", "POST", "/connectors/sap/upload", data=P, files={"files": ("t.xlsx", _xlsx(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
    hit("upload ts csv", "POST", "/connectors/timeseries/upload", data=P, files={"files": ("t.csv", csvb, "text/csv")})
    hit("upload docs pdf", "POST", "/connectors/documents/upload", data={**P, "document_type": "equipment_manuals"}, files={"files": ("m.pdf", _pdf(), "application/pdf")})
    r = hit("upload jobs", "GET", "/connectors/jobs", params=P)
    jobs_data = r.json().get("data") if r.status_code == 200 else None
    jobs = jobs_data if isinstance(jobs_data, list) else (jobs_data or {}).get("jobs", [])
    job_id = next((j.get("job_id") for j in jobs if isinstance(j, dict)), None)
    if job_id:
        hit("upload job by id", "GET", f"/connectors/jobs/{job_id}")
    hit("pnid existing", "GET", "/connectors/pnid/existing", params=P)
    hit("connectors file (404 path)", "GET", "/connectors/file", params={**P, "path": "raw/pnid/t.pdf"})

    hit("flow list", "GET", "/flow/state", params=P)
    hit("flow upsert", "POST", "/flow/upsert", params=P, jsonb={**P, "flow": "ts", "content_hash": "t" * 64, "file_name": "t.csv", "stage": "staged"})

    for label, path in [
        ("pnid context", "/context/getPnidContext"),
        ("sap context", "/context/getSapContext"),
        ("ts context", "/context/getTsContext"),
        ("docs context", "/context/getDocsContext"),
        ("sap joins", "/context/getSapJoins"),
        ("commit history", "/context/getCommitHistory"),
    ]:
        hit(label, "GET", path, params=P)
    hit("ts commit", "POST", "/context/commitTsContext", jsonb={**P, "rows": [{"tag_name": "TAG-1", "op_limit_h": "10"}]})
    hit("pnid patch", "PATCH", "/context/patchPnidContext", jsonb={**P, "rows": []})

    for label, path in [
        ("data tables", "/data/tables"),
        ("data equipment", "/data/equipment"),
        ("data work orders", "/data/workOrders"),
        ("data relationships", "/data/relationships"),
        ("data query ts_metadata", "/data/query/ts_metadata"),
    ]:
        hit(label, "GET", path, params=P)

    hit("pipeline jobs", "GET", "/pipeline/jobs", params=P)
    hit("pipeline run (validation path)", "POST", "/pipeline/run", jsonb={"stage": "nope", **P})
    for label, path in [
        ("logs pipeline", "/logs/pipeline"),
        ("logs api", "/logs/api"),
    ]:
        hit(label, "GET", path, params=P)

    hit("delete plant (cascade)", "DELETE", f"/configRouter/plants/{plant}")

    print("# p0 endpoint trace — what each request actually executes\n")
    print(f"Traced against a live throwaway plant (`{plant}`), real PDF/XLSX/CSV uploads,")
    print("real postgres + rustfs. Chains show p0 functions in first-call order,")
    print("indented by call depth. Regenerate: `python p0/scripts/trace_endpoints.py`.\n")
    groups: dict[str, list[dict]] = defaultdict(list)
    for e in report:
        groups[e["req"].split("/")[1].split("?")[0] if "/" in e["req"] else "?"]
    for e in report:
        seg = e["req"].split(" ")[1].split("/")[1]
        groups[seg].append(e)
    for seg in sorted(groups):
        print(f"\n## /{seg}\n")
        for e in groups[seg]:
            print(f"### {e['req']}  — {e['label']}")
            print(f"- sent: `{e['params']}`")
            print(f"- answered: **{e['status']}**, body keys: `{e['body']}`")
            if e["chain"]:
                print("```")
                print("\n".join(e["chain"][:40]))
                if len(e["chain"]) > 40:
                    print(f"... (+{len(e['chain']) - 40} more)")
                print("```")
            else:
                print("- (no p0-frame calls recorded)")
            print()


if __name__ == "__main__":
    main()
