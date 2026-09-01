# p0 — CDM Admin
Developed by ALFA Product Team

p0 is a **single service**, not a collection of them: the gateway mounts it as one
router tree. So it is organised **by layer** (routers / services / database), the
same way a single p1 feature is organised internally.

## Layout

```
p0/
  api/                    the served application
    main.py               router tree; this is what the gateway imports
    routers/              HTTP surface — request in, response out, no logic
    services/             logic: flow, audit, transforms, doc types, pipeline stages, jobs
    database/             storage adapters: connection (postgres), cdm_writer,
                          timeseries (iotdb), rustfs_store (s3)
    schemas/              pydantic request/response models
    plants.py             plant registry + per-plant DB provisioning
    config.py             every path and setting, resolved from p0_DIR
    deps.py deps_auth.py responses.py validation.py audit_context.py
  pipelines/              stage runner scripts                  <- RUN AS SUBPROCESSES
  source_processing/      per-source extraction (pnid, sap, ts, documents)
  utils/                  shared helpers for pipelines + source processing
  connectors/             connector workers                     <- RUN AS SUBPROCESSES
  config/                 yaml templates (schema, entities, industry configs)
  sdk/  scripts/  docs/   client sdk, helper scripts, design docs
  Login/                  auth (owned separately)
  dataquality/            data-quality service (owned separately)
  tests/
  _attic/                 dead prototype scripts, kept for reference
  Implementation/         DEPRECATED shim — delete once p2 stops importing it (see below)
```

## The thing that will bite you

`p0/connectors/`, `p0/pipelines/` and `p0/source_processing/` look unused — nothing imports them.
They are **launched as subprocesses by path**:

```python
subprocess.run([sys.executable, str(CONNECTORS_DIR / "run_connector.py"), ...])   # services/connector_jobs.py
p0_DIR / "source_processing" / "pnid" / "complete_pnid_extraction_without_ui.py"  # services/pipeline_stages.py
```

An import-graph or dead-code tool will report them unreachable. They are not.
Moving or renaming them breaks the app **at runtime, not at import** — nothing fails
until someone triggers an upload or a pipeline run. The paths come from `config.py`
(`CONNECTORS_DIR`, `p0_DIR`); change them together or not at all.

## Import contracts other code depends on

Do not rename or relocate these — they are imported from outside p0:

| Path | Imported by |
|---|---|
| `p0.api.main:router` | `app.py` (gateway), `deployment/application/app_registry.py` |
| `p0.api.plants:validate_plant_code` | `p2/expression/api/v1/{expression,overview}_api.py` |

`p0/Implementation/` is now a **deprecation shim** and nothing else: three files that
re-export `p0.api.plants`, because p2 still imports the old path. Once p2 changes these
two lines, delete the whole `p0/Implementation/` folder:

```python
# p2/expression/api/v1/expression_api.py:12
# p2/expression/api/v1/overview_api.py:5
- from p0.Implementation.api.plants import validate_plant_code as _validate_plant_code
+ from p0.api.plants import validate_plant_code as _validate_plant_code
```

## Layering rules

- A **router** may import services, database, schemas. It owns HTTP status codes.
- A **service** may import database. It must not import a router, and should not
  raise `HTTPException` (a few still do — see below).
- **database/** talks to postgres / iotdb / rustfs and nothing else.
- Nothing outside `routers/` imports `schemas/`.

Convention, not tooling. Two checks worth running before a PR:

```bash
grep -rn "from \.\.routers\." p0/api/*.py        # logic -> router: must be empty
grep -rln "schemas" p0/api/services/           # services -> schemas: must be empty
```

## Response contract

Success: `{"success": true, "message": ..., "data": ...}`.
Error: `{"success": false, "message": ..., "errors": [{"field": ..., "message": ...}]}`.

Handler-level validation (missing/unknown plant, bad body fields) answers **400**;
framework schema validation answers **422** via the gateway handler. Routers that
`raise HTTPException` render through `EnvelopeRoute` (responses.py), so the envelope
holds on every router. Enforced by `tests/api/test_endpoint_contract.py`.

## Known rough edges

- `database/timeseries.py` (752 LOC) is an IoTDB client *and* CSV parsing *and*
  retention *and* ingest orchestration. It wants splitting.
- `services/{transforms,doc_types,connector_jobs}.py` still raise `HTTPException`
  in a few places — the logic layer should not be picking HTTP status codes.
- `routers/context.py` (2.4k) and `routers/connectors.py` (1.9k) are still large.
- `GET /context/getCommitHistory` and `getSapJoins` answer 200 + empty for a blank
  plant instead of 400 (xfail-documented in the contract test).

## Deployment mode — one switch

`DEPLOYMENT_MODE` in env.enc is the **single** variable that targets a whole
deployment. Set it once; every backend follows:

| DEPLOYMENT_MODE | object store | Postgres |
|---|---|---|
| `onprem` (default) | RustFS, `s3://<RUSTFS_BUCKET>/…` | POSTGRES_* host, no SSL |
| `azure` | ADLS Gen2, `abfs://<ADLS_CONTAINER>/…` | AZURE_CDM_DB_* / AZURE_PG_* host, `sslmode=require` |

`utility.database_driver.deployment_mode()` is the source of truth;
`storage_backend()` and `PostgresDriver("cdm")` both derive from it. The **CDM
database name is fixed in code** in both modes, so routing/naming never drift
across clouds.

Azure env keys (mirroring `azure_deployment_test/azure_driver.py`):
`ADLS_ACCOUNT_NAME`, `ADLS_CONTAINER`, `ADLS_ACCOUNT_KEY` (or Managed Identity via
`DefaultAzureCredential`), `AZURE_CDM_DB_HOST/USER/PASS/PORT` (fallback
`AZURE_PG_HOST/USER/PASSWORD/PORT`).

Escape hatch: `STORAGE_BACKEND` can override just the object store for the rare
split case, but the norm is to set only `DEPLOYMENT_MODE`. It is an
**infrastructure decision, not a runtime toggle** — credentials live in
env.enc/KeyVault, and a UI switch could not supply them.
Contract-tested in `tests/api/test_storage_backend.py`.

## Running the tests

```bash
export MASTER_KEY=... PYTHONPATH=$PWD AUTH_ENABLED=false
.venv/bin/python -m pytest p0/tests/ -q
```

Some tests are marked `real_provisioning` — they create and drop **real** databases.
They always clean up, and refuse to touch a plant whose name lacks `_testcase`.
