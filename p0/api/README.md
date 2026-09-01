# DecisionOps CDM Admin API

Unified FastAPI server for managing the p0 Canonical Data Model (CDM).

## Quick Start

```bash
cd p0/Implementation
source /path/to/venv/bin/activate
uvicorn api.main:app --host 127.0.0.1 --port 4400 --reload
```

**Interactive docs:** http://localhost:4400/docs (Swagger UI) | http://localhost:4400/redoc (ReDoc)

## Architecture

```
┌──────────────────────────────┐
│  Next.js Frontend (:3000)    │     decision_ops_frontend-cement-dev/
│  AdminPanel, Dashboard, etc. │
└──────────────┬───────────────┘
               │  next.config.ts rewrites
               │  /api/* → http://127.0.0.1:4400/api/*
               ▼
┌──────────────────────────────┐
│  FastAPI Backend (:4400)     │     p0/Implementation/api/
│  CDM Admin API               │
├──────────────────────────────┤
│  Routers:                    │
│  ├─ /api/health              │  Health probes
│  ├─ /api/config/*            │  SAP, renames, user config, schema
│  ├─ /api/pipeline/*          │  Trigger & monitor CDM pipelines
│  ├─ /api/connectors/*        │  P&ID / Document upload & cloud sync
│  ├─ /api/data/*              │  Query CDM output (equipment, WOs, etc.)
│  └─ /api/ontology/*          │  KG export from PostgreSQL
└──────────────┬───────────────┘
               │
      ┌────────┼────────┐
      ▼        ▼        ▼
  YAML Configs  CSV/Out   PostgreSQL
  (p0 config/)  (p0 data/) (optional)
```

## API Endpoints

### Health
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Liveness probe — config status & DB connectivity |

### Configuration (`/api/config/`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/config/sap-tables` | List all SAP extraction tables |
| POST | `/api/config/sap-tables` | Register a new SAP table |
| DELETE | `/api/config/sap-tables/{group}/{entry}` | Remove a SAP table |
| GET | `/api/config/column-renames` | List all source→rename mappings |
| GET | `/api/config/column-renames/{source_key}` | Get renames for one source |
| PUT | `/api/config/column-renames/{source_key}` | Replace renames for one source |
| GET | `/api/config/user-config` | Get user_config.yaml |
| PUT | `/api/config/user-config` | Replace user_config.yaml entirely |
| PATCH | `/api/config/user-config` | Deep-merge into user_config.yaml |
| GET | `/api/config/cdm` | Get CDM master config (read-only) |
| GET | `/api/config/entities` | Get entity definitions (read-only) |
| GET | `/api/config/schema` | Get SQL schema definitions (read-only) |
| GET | `/api/config/relationships` | Get relationship definitions (read-only) |
| GET | `/api/config/validation` | Get validation contracts (read-only) |

### Pipeline (`/api/pipeline/`)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/pipeline/run` | Trigger a pipeline stage (sap/pnid/ts/docs/full) |
| GET | `/api/pipeline/jobs/{job_id}` | Poll pipeline job status |
| GET | `/api/pipeline/jobs` | List all pipeline jobs |
| GET | `/api/pipeline/outputs` | List CDM output tables by stage |
| GET | `/api/pipeline/outputs/{stage}/{table}` | Preview a CDM output table (paginated) |

### Connectors (`/api/connectors/`)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/connectors/pnid/upload` | Upload P&ID files from device |
| POST | `/api/connectors/pnid/connect/cloud` | Connect P&ID from S3/ADLS/GCS |
| POST | `/api/connectors/pnid/connect/onedrive` | Connect P&ID from OneDrive |
| POST | `/api/connectors/documents/upload` | Upload document files from device |
| POST | `/api/connectors/documents/connect/cloud` | Connect documents from cloud |
| POST | `/api/connectors/documents/connect/onedrive` | Connect documents from OneDrive |
| GET | `/api/connectors/jobs/{job_id}` | Poll connector job status |
| GET | `/api/connectors/jobs` | List all connector jobs |

### CDM Data (`/api/data/`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/data/tables` | List available CDM tables |
| GET | `/api/data/equipment` | Query equipment records (search, paginate) |
| GET | `/api/data/equipment/{uid}` | Get one equipment + relationships |
| GET | `/api/data/work-orders` | Query work order records |
| GET | `/api/data/relationships` | Query relationship edges |
| GET | `/api/data/query/{table_name}` | Generic query for any CDM table |

### Ontology (`/api/ontology/`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/ontology/schema` | Get ontology structure (no DB needed) |
| GET | `/api/ontology/use-cases` | List use-case profiles |
| POST | `/api/ontology/export` | Export KG from PostgreSQL |

## Frontend Integration

The Next.js frontend (`decision_ops_frontend-cement-dev`) proxies API calls via `next.config.ts` rewrites:

```typescript
// next.config.ts
const CDM_API_URL = process.env.CDM_API_URL || "http://127.0.0.1:4400";

// All /api/config/*, /api/pipeline/*, /api/connectors/*,
// /api/data/*, /api/ontology/*, /api/health are proxied to FastAPI
```

**Legacy paths** (`/api/pnid/*`, `/api/documents/*`) are supported via 307 redirects to the new `/api/connectors/*` paths.

The existing `/api/user-config` Next.js route (direct YAML file I/O) remains as a fallback.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CDM_DB_HOST` | `localhost` | PostgreSQL host |
| `CDM_DB_PORT` | `5433` | PostgreSQL port |
| `CDM_DB_NAME` | `decisionops_cdm` | PostgreSQL database |
| `CDM_DB_USER` | `decisionops` | PostgreSQL user |
| `CDM_DB_PASS` | (none) | PostgreSQL password — if empty, falls back to CSV |
| `p0_DATA_DIR` | `p0/Implementation/data` | Data directory override |
| `CDM_API_URL` | `http://127.0.0.1:4400` | (Frontend) FastAPI backend URL |

## Project Structure

```
p0/Implementation/api/
├── __init__.py
├── main.py              # FastAPI app entry point + backward-compat redirects
├── config.py            # Paths, CORS, metadata constants
├── deps.py              # YAML helpers, validation, deep merge
├── schemas/
│   ├── config.py        # Pydantic models: SAP tables, column renames, user config
│   ├── connectors.py    # Pydantic models: cloud/OneDrive connectors, job status
│   ├── data.py          # Pydantic models: equipment, work orders, relationships
│   └── pipeline.py      # Pydantic models: pipeline run, job status, validation
└── routers/
    ├── config_router.py # /api/config/* — SAP tables, renames, user config, schema
    ├── connectors.py    # /api/connectors/* — P&ID & document upload/cloud
    ├── pipeline.py      # /api/pipeline/* — trigger & monitor CDM pipelines
    ├── data.py          # /api/data/* — query CDM output tables
    ├── ontology.py      # /api/ontology/* — KG schema & export
    └── health.py        # /api/health — liveness probe
```
