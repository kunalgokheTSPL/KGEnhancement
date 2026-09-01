# DecisionOps CDM — Python SDK

A thin Python client over the CDM Admin REST API (p0). It wraps the **existing**
endpoints so the platform can be driven programmatically — ingest, process,
review, commit, and query the Canonical Data Model from code.

> Single dependency: `requests`. Adds no server behaviour — it's a typed-ish
> convenience layer over `/api/*`.

## Install

```bash
pip install requests
# then import cdm_client.py from this folder (or copy it into your project)
```

## Auth

Every call except `health()` needs a Keycloak bearer token. Pass it explicitly or
set the `CDM_TOKEN` env var.

```python
from cdm_client import CdmClient
cdm = CdmClient("http://localhost:4400/p0_cdm", token="<jwt>")
```

## End-to-end example (ingest → process → review → commit)

```python
# 1. register / pick a plant
cdm.create_plant("HYDRO", label="Hydrogen Plant")

# 2. upload tag metadata
cdm.upload_timeseries("HYDRO", ["timeseries_tag_metadata.csv"], file_type="metadata")

# 3. process it (one pipeline run per file)
job = cdm.run_pipeline("ts", "HYDRO", file_names=["timeseries_tag_metadata.csv"])
cdm.wait_for_job(job["job_id"])          # blocks until completed/failed

# 4. review + (optionally) edit, then commit
ctx = cdm.get_ts_context("HYDRO", source_file="timeseries_tag_metadata.csv")
cdm.commit_ts("HYDRO", ctx["rows"], batch_id=job.get("batch_id"))

# 5. query the committed CDM + lineage
cdm.equipment("HYDRO")                    # canonical equipment
cdm.query_table("timeseries_metadata", "HYDRO", limit=50)
cdm.activity("HYDRO")                     # who-did-what feed (incl. data-quality)
cdm.summary("HYDRO")                      # error/warn/run/commit counts
```

## Method map (→ endpoint)

| Method | Endpoint |
|---|---|
| `health()` | `GET /api/health` |
| `plants()` / `create_plant()` / `delete_plant()` | `…/config/plants` |
| `upload_timeseries()` / `upload_documents()` | `…/connectors/*/upload` |
| `run_pipeline()` / `get_job()` / `list_jobs()` / `wait_for_job()` | `…/pipeline/*` |
| `get_ts_context()` / `commit_ts()` / `get_docs_context()` / `commit_docs()` | `…/context/*` |
| `flow_state()` | `…/flow/state` |
| `activity()` / `log_events()` / `summary()` | `…/logs/*` |
| `tables()` / `equipment()` / `get_equipment()` / `relationships()` / `query_table()` | `…/data/*` |
| `knowledge_graph()` | `…/context/docs/kg` |

## Errors

Non-2xx responses raise `CdmError` carrying `.status`, `.body`, `.url`:

```python
from cdm_client import CdmError
try:
    cdm.commit_ts("HYDRO", rows)
except CdmError as e:
    print(e.status, e.body)   # e.g. 400 {"detail": "plant_code is required"}
```

## Notes

- All data calls are **plant-scoped** — pass `plant_code`.
- Commits are **idempotent** server-side (per-file UPSERT / replace-by-source_file),
  so re-running this flow won't duplicate.
- The full interactive API reference is the live Swagger UI at **`/docs`** (or
  ReDoc at `/redoc`); this SDK mirrors that surface.
