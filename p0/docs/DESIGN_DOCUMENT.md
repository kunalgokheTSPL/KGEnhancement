# CDM Pipeline — Staff-Level Technical Design Document

**Project:** Industrial Asset CDM (Canonical Data Model) Pipeline  
**Implementation:** Implementation 3  
**Last Updated:** February 2026  
**Purpose:** Reference document for developers and coding agents. Covers architecture, design decisions, data flow, identity resolution strategy, config contracts, and known limitations. Read this before touching any code.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Solution Overview](#2-solution-overview)
3. [Architecture Diagram](#3-architecture-diagram)
4. [Core Design Principles](#4-core-design-principles)
5. [Pipeline Stages — Detailed](#5-pipeline-stages--detailed)
6. [Asset Identity Resolution — The Central Problem](#6-asset-identity-resolution--the-central-problem)
7. [Config File Contracts](#7-config-file-contracts)
8. [Source Systems & Their Quirks](#8-source-systems--their-quirks)
9. [Entity & Relationship Model](#9-entity--relationship-model)
10. [UID Generation Strategy](#10-uid-generation-strategy)
11. [File & Directory Layout](#11-file--directory-layout)
12. [Module Reference](#12-module-reference)
13. [How to Add a New Source System](#13-how-to-add-a-new-source-system)
14. [Known Issues & Design Debt](#14-known-issues--design-debt)
15. [Coding Agent Quick Reference](#15-coding-agent-quick-reference)

---

## 1. Problem Statement

An industrial plant has equipment data spread across four disconnected source systems:

| Source | Type | Equipment Identifier | Format Example |
|---|---|---|---|
| **SAP PM/MM** | ERP — structured | `EQUNR` (numeric) | `10001234` |
| **P&ID drawings** | Engineering docs | Tag code | `351HG1`, `441KN1` |
| **Timeseries** | Sensor/historian | Tag prefix in tag name | `351HG1_VZ1` |
| **Documents** | PDFs (SOP, OEM, Maintenance) | Text or inline tag | `"Hot Air Generator"`, `311CD-1` |

The goal is **asset identity resolution**: establish that `EQUNR 10001234` in SAP, P&ID tag `351HG1`, sensor tag `351HG1_VZ1`, and PDF section "Hot Air Generator 351HG1" all refer to the same **physical piece of equipment**, and link all related data (work orders, tag readings, maintenance docs, BOM) to that single physical asset.

This is equivalent to what a **Knowledge Graph** would do (nodes = entities, edges = relationships), but implemented as a **config-driven ETL pipeline** producing CSV/Parquet files instead of a graph database. The output is directly importable into a relational DB, graph DB, or analytics platform.

---

## 2. Solution Overview

The pipeline is a **multi-source ETL with a shared canonical schema**. Each source system runs its own independent pipeline (Load → Rename → Asset Identity → Derive → Entity Build → Relationships), then all outputs are merged in a final consolidation step.

**The lingua franca across all sources is the P&ID tag format** (e.g., `351HG1`). This becomes the universal `equipment_id` / `normalized_asset` that joins all entities together.

```
SAP PM/MM  ─────┐
P&ID PDFs  ─────┤──► [Source Pipelines] ──► [Final CDM Consolidation] ──► DB-ready CSVs
Timeseries ─────┤         (per source)
Documents  ─────┘
```

**Output is a flat CDM** (Canonical Data Model) — a set of normalised tables with deterministic UIDs and a separate relationships table acting as the graph edge store.

---

## 3. Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│                        RAW STAGING INPUTS                                  │
│  data/staging/sap/*.csv   data/staging/pnid/*.pdf                          │
│  data/staging/ts/*.csv    data/staging/docs/ (or data/work/docs/)          │
└──────────┬────────────────────┬──────────────────┬──────────────────┬──────┘
           │                    │                  │                  │
           ▼                    ▼                  ▼                  ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
│ SAP Pipeline     │ │ P&ID Pipeline    │ │ TS Pipeline  │ │ Docs Pipeline    │
│                  │ │                  │ │              │ │                  │
│ sap_merging_impl │ │ Gemini Vision    │ │ ts_processing│ │ doc_processing   │
│   ↓              │ │   ↓ (PDF→XLSX)   │ │   ↓          │ │   ↓              │
│ sap_processing   │ │ pnid_from_pdfs   │ │ ts_end_to_end│ │ docs_end_to_end  │
│   ↓              │ │   ↓              │ │   ↓          │ │   ↓              │
│ col_rename (SAP) │ │ col_rename (PID) │ │ col_rename   │ │ col_rename       │
│   ↓              │ │   ↓              │ │   ↓          │ │   ↓              │
│ derived_fields   │ │ derived_fields   │ │ ts_asset_id  │ │ doc_asset_id     │
│   ↓              │ │   ↓              │ │   ↓          │ │   ↓              │
│ identity_rules   │ │ identity_rules   │ │ derived_flds │ │ derived_fields   │
│   ↓              │ │   ↓              │ │   ↓          │ │   ↓              │
│ canonical_build  │ │ canonical_build  │ │ identity_rls │ │ identity_rules   │
│   ↓              │ │   ↓              │ │   ↓          │ │   ↓              │
│ xref (SAP↔PID)   │ │ uid_tables       │ │ canonical_bld│ │ canonical_build  │
│   ↓              │ │   ↓              │ │   ↓          │ │   ↓              │
│ uid_tables       │ │ relationships    │ │ uid_tables   │ │ uid_tables       │
│   ↓              │ │   ↓ validation   │ │   ↓ rels     │ │   ↓ rels         │
│ relationships    │ │                  │ │   ↓ validation│ │   ↓ validation   │
│   ↓ validation   │ │                  │ │              │ │                  │
└────────┬─────────┘ └────────┬─────────┘ └──────┬───────┘ └────────┬─────────┘
         │                    │                   │                  │
         ▼                    ▼                   ▼                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│           data/out/sap/   data/out/pnid/   data/out/ts/   data/out/docs/   │
│           entities/       entities/        entities/      entities/         │
│           relationships/  relationships/   relationships/ relationships/    │
└───────────────────────────────────────┬────────────────────────────────────┘
                                        │
                                        ▼
                          ┌─────────────────────────┐
                          │   build_final_cdm.py     │
                          │  (config-driven merge)   │
                          │  schema_frozen.yaml       │
                          │  cdm_config_frozen.yaml   │
                          └─────────────┬────────────┘
                                        │
                                        ▼
                          data/out/final_validation/
                          ├── entities/
                          │   ├── equipment_sap.csv
                          │   ├── equipment_pid.csv
                          │   ├── equipment_connectivity.csv
                          │   ├── functional_location.csv
                          │   ├── work_order.csv
                          │   ├── task_list.csv
                          │   ├── material.csv
                          │   ├── timeseries_metadata.csv (→ documents.csv)
                          │   └── documents.csv
                          ├── relationships/
                          └── cdm_summary.json
```

---

## 4. Core Design Principles

### p1 — Config Drives Everything
There is **no hardcoded table name, column name, or pipeline path** in the production code. All structural decisions are externalised into YAML config files (see §7). Adding a new entity, renaming a column, or changing a relationship requires only config changes — no code changes.

### p2 — Deterministic, Reproducible UIDs
Every entity UID is a `sha1` hash of its identity keys. Same input = same UID, always. This means pipelines are **idempotent** — re-running produces identical UIDs, enabling safe incremental updates and cross-pipeline UID lookups.

```
uid = "<prefix>:sha1(<plant_code>|<identity_key_1>|...|<identity_key_n>)"
```

### P3 — Pipeline Isolation + Merge-On-Write
Each source pipeline writes its own slice of every entity table. Writing uses `merge_entity_file_on_disk` — new rows from this run **overwrite** existing rows with the same primary key, but rows from other source systems already on disk are **preserved**. This prevents the SAP pipeline from wiping P&ID equipment rows, and vice versa.

### P4 — P&ID Tag Format is the Universal Asset Identity Key
The P&ID tag code (e.g., `351HG1`) is the **single shared namespace** across all four source systems. Every other identifier (SAP EQUNR, TS tag prefix, document equipment mention) is ultimately resolved to a P&ID-format tag code in the `equipment_id` column. This is what enables cross-source joins.

### P5 — Confidence Scoring on Every Resolution
Every asset identity resolution step assigns a `confidence` score. Deterministic exact matches = 1.0. Heuristic/fuzzy matches = 0.50–0.95. Downstream consumers can filter by confidence threshold.

### P6 — Fail Gracefully, Never Silently Corrupt
If a source file is missing, the pipeline continues with an empty DataFrame and logs a warning. No silent data corruption. Missing cross-references produce empty values in `equipment_id`, not wrong values.

---

## 5. Pipeline Stages — Detailed

Each source pipeline follows this exact sequence:

### Stage 1: Source Processing (Load + Merge)
**Purpose:** Read raw files from `data/staging/` and merge multi-table sources into unified DataFrames.

- **SAP:** ~14 tables (AUFK, AFKO, AFVC, EQUI, IFLOT, PLKO, PLPO, MARA, etc.) merged by primary key joins → 4 datasets: `workorder`, `floc`, `tasklist`, `material`
- **P&ID:** PDFs converted to images → Gemini Vision API → JSON extraction → consolidated XLSX (`Components` sheet = equipment, `Connections` sheet = connectivity)
- **Timeseries:** Single CSV (`timeseries_tag_metadata.csv`) loaded as-is
- **Documents:** Multiple LLM-extracted XLSXs (Maintenance, SOP, OEM types) merged into a single DataFrame

**Key rule:** Source processing produces ONLY raw merged data. No column renaming, no business logic yet.

**Implementation:** `src/source_processing/{sap,pnid,ts}/`

---

### Stage 2: Column Rename
**Purpose:** Map source-specific column names to canonical CDM column names using `{source}_column_rename.yaml`.

- Operates per `source_name` key (e.g., `sap_wo`, `sap_assets`, `pid`, `timeseries`, `documents`)
- Non-mapped columns are preserved (not dropped) — this is intentional, extra columns carry debug value
- Implementation: `src/utils/renamer.py` → `apply_column_rename(df, rename_cfg, source_name=...)`

**Config:** `config/sap_column_rename.yaml`, `config/pnid_column_rename.yaml`, `config/timeseries_column_rename.yaml`, `config/docs_column_rename.yaml`

---

### Stage 3: Asset Identity Enrichment
**Purpose:** Resolve `normalized_asset` and `equipment_id` for each row. This is the most critical stage. Behaviour differs by source:

#### SAP
- `normalized_asset` = EQUNR (SAP equipment number, e.g., `"10001234"`)
- `equipment_id` = resolved to P&ID tag via `asset_cross_reference` table (built later, back-resolved post-canonicalisation)
- The SAP ↔ P&ID bridge is built in `build_asset_cross_reference()` after both entity outputs exist

#### P&ID
- `normalized_asset` = P&ID equipment tag (e.g., `"351HG1"`) — self-contained, already in the right format
- `equipment_id` = same as equipment tag
- No external reference needed

#### Timeseries
- `normalized_asset` = extracted from tag name prefix using regex: `"351HG1_VZ1"` → `"351HG1"`
- Extraction rules (in `_extract_tag_asset_key`):
  1. Split on `_`, `-`, space — take first token
  2. Strip instrument suffixes (IZ, SZ, VZ, etc.)
  3. Find pattern `[digits+letters]` or `[letters+digits]`
- `equipment_id` = enriched by layered matching against P&ID `equipment.csv`
- **P&ID pipeline must run before TS** for `equipment_id` enrichment to work

#### Documents
- `normalized_asset` uses a 3-strategy cascade:
  1. **Strategy 1 (inline_tag):** If `equipment_tag` column already contains a P&ID-format code (matches `^[0-9]{3}[A-Z]`) — use it directly
  2. **Strategy 2 (name_map):** Curated domain `EQUIPMENT_TAG_MAP` dict maps equipment names → P&ID codes (e.g., `"hot air generator" → "351HG1"`)
  3. **Strategy 3 (name_fallback):** Use whatever label/tag is available
- `equipment_id` = enriched from P&ID reference (same layered matching as TS)

**Layered matching used by TS and Docs for `equipment_id`:**
```
1. Exact match on asset_key                    → confidence 1.00
2. Substring match (either direction)          → confidence 0.92
3. 3-digit numeric family prefix (e.g., "351") → confidence 0.75
4. Fuzzy (SequenceMatcher ≥ 0.84 / 0.80)      → confidence = score
```

**Implementation:** `src/utils/asset_identity.py`, `src/utils/ts_asset_identity.py`, `src/utils/doc_asset_identity.py`

---

### Stage 4: Derived Fields
**Purpose:** Inject pipeline-generated fields that don't exist in the raw source:

| Field | Source | Value |
|---|---|---|
| `plant_code` | Runtime arg `--plant_code` | e.g., `"PLANT01"` — forced on every row |
| `source_system` | Constant per source key | `"SAP"`, `"PID"`, `"TS"`, `"DOCS"` |
| `source_record_id` | SHA1 of natural key fields | Stable across pipeline re-runs |
| `created_at` | Pipeline run timestamp (UTC) | ISO 8601 |
| `updated_at` | Source timestamp if available, else `created_at` | |
| `confidence` | 1.0 for deterministic, <1.0 for fuzzy | |
| `is_active` | `1` unless explicitly retired | |

**Config:** `config/derived_fields.yaml`  
**Implementation:** `src/utils/derived.py` → `apply_derived_fields()`

---

### Stage 5: Identity Rules
**Purpose:** Final normalisation pass to guarantee the two fields `normalized_asset` and `floc` exist and are clean before canonical entity building.

- If `normalized_asset` is missing, `apply_identity_rules` fills it using this priority: `equipment_id` → `EQUNR` → `functional_location` → `TPLNR` → `floc` → `tag_name`
- Normalises `floc` from `functional_location` or `TPLNR`
- Forces `plant_code` to uppercase + stripped

**Config:** `config/identity_frozen.yaml` (defines dedup strategy and key fields per entity)  
**Implementation:** `src/utils/identity.py` → `apply_identity_rules()`

---

### Stage 6: Canonical Entity Build
**Purpose:** Transform `post_sources` DataFrames into canonical entity tables as defined by `entities_frozen.yaml`.

The builder (`build_canonical_entities_from_sap_template`) iterates every entity defined in `entities_frozen.yaml` and:
1. Finds matching source DataFrames from `post_sources` dict
2. Materialises each schema column by following `attributes.<col>.from` fallback list
3. Drops rows where any identity key is empty
4. Deduplicates on identity keys
5. Generates deterministic UID for the primary key column
6. Returns `{canonical_table_name: DataFrame}`

**Config:** `config/entities_frozen.yaml`, `config/schema_frozen.yaml`  
**Implementation:** `src/pipelines/canonical_builder_sap.py` → `build_canonical_entities_from_sap_template()`

> **Note:** This same builder is reused by ALL four pipelines (SAP, P&ID, TS, Docs). It is not SAP-specific despite the filename.

---

### Stage 7: UID Lookup Tables
**Purpose:** Build in-memory lookup tables `{entity_name: DataFrame[uid_col, ...identity_keys]}` used by the relationship builder to look up UIDs by natural key.

**Implementation:** `src/utils/canonical_entities.py` → `build_uid_tables_from_canonical_entities()`

---

### Stage 8: Relationship Builder
**Purpose:** Build the relationship/edge table driven entirely by `relationships_frozen.yaml`.

For each relationship definition:
1. Reads the source DataFrame from `post_sources[rel.source]`
2. Resolves `from_entity` and `to_entity` natural keys → UIDs via lookup tables
3. Emits `(relationship_uid, parent_ref, child_ref, relationship_type, plant_code, confidence, ...)`
4. Deduplicates

If the source key is not in `post_sources`, the relationship is silently skipped (correct behaviour for standalone pipeline runs).

**Config:** `config/relationships_frozen.yaml`  
**Implementation:** `src/utils/canonical_relationships.py` → `build_asset_relationships()`

---

### Stage 9: Validation
**Purpose:** Run contractual validation checks on `post` DataFrames and emit a JSON report.

**Config:** `config/validation_contracts.yaml`  
**Implementation:** `src/utils/validate.py`  
**Output:** `data/out/{source}/validation_reports/`

---

### Stage 10: Final Consolidation
**Purpose:** Merge all pipeline outputs into a single `data/out/final_validation/` folder.

- For the `equipment` table: source-priority merge (PID > SAP > TS > Docs) — P&ID wins on column conflicts
- For all other tables: simple concat + dedup by unique constraint keys
- Schema is enforced from `schema_frozen.yaml`
- All decisions are driven by `cdm_config_frozen.yaml` — no hardcoded table names

**Implementation:** `src/pipelines/build_final_cdm.py`

---

## 6. Asset Identity Resolution — The Central Problem

This section is the most important for understanding the whole system.

### 6.1 The Two-Field Model

Every entity in the CDM carries two identity-related fields:

| Field | Purpose | Value Example |
|---|---|---|
| `normalized_asset` | **The match key** — used for UID hashing and cross-source joins | `"351HG1"` |
| `equipment_id` | **P&ID canonical tag** — the resolved physical tag code | `"351HG1"` |

For P&ID, TS, and Docs: `normalized_asset` ≈ `equipment_id` (both converge to P&ID format).  
For SAP: `normalized_asset` = EQUNR (numeric), `equipment_id` = P&ID tag (only when xref succeeds).

### 6.2 The SAP ↔ P&ID Bridge

Because SAP uses EQUNR (numeric) and P&ID uses tag codes, an explicit bridge is needed.

**`asset_cross_reference.csv`** is built by `build_asset_cross_reference()` in `canonical_builder_sap.py`:

```
Match Strategy (in priority order):
  1. Exact:    sap.normalized_asset == pid.normalized_asset  → confidence 1.0
  2. Exact:    sap.normalized_asset == pid.equipment_tag     → confidence 1.0
  3. Contains: any pid_tag token is a substring of sap.normalized_asset → confidence 0.8
```

After the xref is built, it is used to back-resolve `equipment_id` in SAP entity tables (`work_order.equipment_id`, `task_list.equipment_id`, `functional_location` references).

**Limitation:** If SAP EQUNR and P&ID tags have no overlapping token in any SAP field, the bridge produces zero rows. SAP records then have an empty `equipment_id`. This is a data quality issue, not a pipeline bug.

### 6.3 UID Hashing

```python
# Same function, same result, regardless of which pipeline produces the row
uid = f"eq:{sha1(f'{plant_code}|{normalized_asset}')}"
```

**Critical implication:** If SAP `normalized_asset = "10001234"` and P&ID `normalized_asset = "351HG1"`, they will have **different** `equipment_uid` values. They refer to the same physical asset but will be stored as two rows in the final `equipment` table. The `asset_cross_reference` table is the bridge between them.

**Future target:** Once `asset_cross_reference` coverage is high enough, SAP `normalized_asset` should be overwritten with the P&ID tag before UID generation, producing a **single unified row** per physical asset. The code supports this but it has not been activated to avoid data loss during the bridge-building phase.

### 6.4 Confidence Thresholds

| Match Method | Confidence | Threshold to Accept |
|---|---|---|
| Deterministic exact | 1.00 | Always accepted |
| P&ID exact (TS/Docs `equipment_id`) | 1.00 | Always accepted |
| Substring contains | 0.92 | Always accepted |
| Family prefix (3-digit) | 0.75 | Always accepted |
| Fuzzy SequenceMatcher (TS) | variable | ≥ 0.84 |
| Fuzzy SequenceMatcher (Docs) | variable | ≥ 0.80 |
| Name map (Docs) | 0.85 | Always accepted |
| Name fallback | 0.50 | Accepted but flagged |

---

## 7. Config File Contracts

All config files live in `config/`. **Never hardcode what belongs in config.**

### `schema_frozen.yaml`
**What it defines:** The complete relational schema — every table, every column name, data type, primary key, unique constraints, indexes, foreign keys.

**How it's used:**
- `apply_schema()` enforces column presence and order on every output DataFrame
- `build_uid_tables_from_canonical_entities()` reads `primary_key` per table
- `build_final_cdm.py` reads column lists and dedup keys for consolidation
- **Frozen** means: do not add/remove columns without also updating all pipelines and downstream consumers

**Key tables:** `equipment`, `equipment_sap`, `equipment_pid`, `equipment_connectivity`, `functional_location`, `work_order`, `task_list`, `material`, `timeseries_metadata`, `document_metadata`, `asset_cross_reference`, `relationships`

---

### `entities_frozen.yaml`
**What it defines:** For each logical entity — canonical table name, identity keys, source systems it pulls from, and for every column, the ordered fallback list of `source_name.column_name` references.

```yaml
equipment:
  canonical_table: equipment
  identity_keys: [plant_code, normalized_asset]   # used for UID hash + dedup
  sources: [sap_assets, pid, documents, timeseries]
  attributes:
    description:
      from: [sap_assets.description, pid.description, documents.description]
      # First non-empty value wins. sap_assets is tried first.
```

**How it's used:**
- `build_canonical_entities_from_sap_template()` reads this to know which columns to build and from where
- `build_asset_relationships()` reads `identity_keys` and `canonical_table` to resolve UIDs

**Key rule:** `identity_keys` must match columns that will actually be populated in `post_sources`. If they are empty for a row, that row is dropped.

---

### `relationships_frozen.yaml`
**What it defines:** Every relationship type — source DataFrame key, from/to entity names, relationship type string, dedup columns.

```yaml
wo_to_equipment:
  from_entity: work_order
  to_entity: equipment
  source: sap_wo          # must exist as a key in post_sources
  type: EXECUTED_ON
  directed: true
  dedupe_keys: [plant_code, wo_number, normalized_asset]
```

**How it's used:** `build_asset_relationships()` processes only relationships where `source` exists in the current pipeline's `post_sources` dict. Others are skipped silently.

---

### `identity_frozen.yaml`
**What it defines:** Per-entity identity resolution rules — match type, key fields, case sensitivity, null handling, dedup strategy.

**How it's used:** `apply_identity_rules()` — mainly enforces `normalized_asset` and `floc` existence. The full dedup logic is currently advisory (actual dedup happens in `build_canonical_entities_from_sap_template`).

---

### `derived_fields.yaml`
**What it defines:** How pipeline-injected fields (`plant_code`, `source_system`, `source_record_id`, `confidence`, timestamps) are computed per source.

**How it's used:** `apply_derived_fields()` reads this and applies rules per `dataset_name` / `source_name`.

---

### `{source}_column_rename.yaml`
**What it defines:** Mapping from raw source column names to CDM canonical column names, per `source_name` key.

```yaml
sources:
  sap_assets:
    EQUNR:  equipment_id
    TPLNR:  functional_location
    EQKTX:  description
```

**Key rule:** Only explicitly mapped columns are renamed. Unmapped columns are kept with their original name. There is no error if a mapped column is absent in the source — it is silently ignored.

---

### `cdm_config_frozen.yaml`
**What it defines:** Final consolidation behaviour — which tables to build, file paths relative to each pipeline's output dir, source priority for the equipment merge, output subdirectory names.

**How it's used:** `build_final_cdm.py` — the entire consolidation is driven by this file. No table names or file paths are hardcoded in the Python.

---

### `validation_contracts.yaml`
**What it defines:** Per-dataset validation rules — required columns, allowed null rates, value constraints.

**How it's used:** `run_validations()` produces a JSON report per pipeline run.

---

## 8. Source Systems & Their Quirks

### SAP PM/MM
- **Raw files:** 14+ SAP table extracts (CSV or Parquet) in `data/staging/sap/`
- **Merge logic:** `sap_merging_impl.py` joins AUFK+AFKO+AFVC+AFVV → `workorder`; EQUI+IFLOT+ILOA → `floc`; PLKO+PLPO+MAPL → `tasklist`; MARA+MARC+MARD → `material`
- **Source names in templates:** `sap_wo`, `sap_assets`, `sap_task_list`, `sap_material_master`
- **Identity challenge:** EQUNR is purely numeric, no overlap with P&ID tag format unless embedded in FLOC or text fields
- **Debug artifacts:** `data/out/sap/debug/sap_before.csv`, `sap_after.csv`, `dropped_rows.csv`

### P&ID (Piping & Instrumentation Diagrams)
- **Raw files:** PDF files in `data/staging/pnid/`
- **Extraction:** Gemini Vision API (`gemini-3.6-flash-flash`) converts each PDF page to an image, extracts equipment tags and connectivity into JSON → consolidated XLSX
- **Re-extraction skip:** If a `*_consolidated_*.xlsx` already exists in `data/work/pnid_extract/`, the extractor is skipped. Delete the XLSX to force re-extraction.
- **Output sheets:** `Components` (equipment), `Connections` (connectivity/FLOW edges)
- **Source name in templates:** `pid`
- **Identity:** Equipment tags are already in canonical P&ID format — no resolution needed

### Timeseries
- **Raw files:** Single CSV `data/staging/ts/timeseries_tag_metadata.csv`
- **Source name in templates:** `timeseries`
- **Identity:** Tag names embed the equipment code as prefix (`351HG1_VZ1` → `351HG1`). Regex extraction in `_extract_tag_asset_key()`. The tag format IS the P&ID format, so TS and P&ID self-align without a bridge.
- **Dependency:** Must have P&ID `equipment.csv` available for `equipment_id` enrichment. P&ID pipeline must run first.

### Documents (PDFs → LLM Extraction)
- **Types:** Maintenance, SOP, OEM manuals
- **Extraction:** Separate LLM extraction step (Titan or similar) produces `sop_sections_*.xlsx`, `maintenance_tasks_*.xlsx`, `oem_equipment_*.xlsx`
- **Post-extraction consolidation:** `doc_processing.py` merges all xlsx types using `cdm_config_frozen.yaml` document_processing config
- **Fallback:** If no xlsx files are present, `documents_merged.csv` from `data/work/docs/` is used
- **Source name in templates:** `documents`
- **Identity:** 3-strategy cascade (inline tag → name map → fallback). The `EQUIPMENT_TAG_MAP` in `doc_asset_identity.py` is **plant-specific** and must be updated per plant.

---

## 9. Entity & Relationship Model

### Entities (Nodes)

```
equipment_sap          ← SAP view (keyed by EQUNR / sap_equipment_id)
equipment_pid          ← P&ID view (keyed by pid_tag / P&ID tag code)
equipment              ← Unified view (keyed by normalized_asset = P&ID format)
equipment_connectivity ← P&ID flow connections (FROM → TO with connection_type)
functional_location    ← SAP FLOC hierarchy
process_unit           ← P&ID process area grouping
work_order             ← SAP PM work orders (PM01, PM02, etc.)
notification           ← SAP PM quality notifications
task_list              ← SAP PM task list operations
maintenance_plan       ← SAP PM maintenance plans
work_center            ← SAP PM work centers
material               ← SAP MM material master
bom_item               ← SAP MM bill of materials items
vendor                 ← SAP MM vendor master
purchase_order         ← SAP MM purchase orders
goods_receipt          ← SAP MM goods receipts
invoice                ← SAP MM invoices
timeseries_metadata    ← Tag metadata (unit, limits, description)
document_metadata      ← Document index (type, title, equipment link)
asset_cross_reference  ← SAP EQUNR ↔ P&ID tag mapping table
```

### Relationships (Edges)

```
FLOW              ← equipment → equipment  (P&ID connectivity)
LOCATED_AT        ← equipment → functional_location
PART_OF_PROCESS_UNIT ← equipment → process_unit
EXECUTED_ON       ← work_order → equipment
REPORTED_ON       ← notification → equipment
APPLIES_TO        ← task_list → equipment
SCHEDULES         ← maintenance_plan → equipment
HAS_BOM_ITEM      ← equipment → bom_item
IS_MATERIAL       ← bom_item → material
PURCHASED_FROM    ← purchase_order → vendor
ORDERS_MATERIAL   ← purchase_order → material
RECEIPT_AGAINST   ← goods_receipt → purchase_order
INVOICED_AGAINST  ← invoice → purchase_order
HAS_TAG           ← equipment → timeseries_metadata
HAS_DOCUMENT      ← equipment → document_metadata
```

The relationships table schema:
```
relationship_uid  | plant_code | parent_ref | child_ref | relationship_type
source_system     | source_record_id | confidence | updated_at | is_active
```
`parent_ref` and `child_ref` are the UID values of the from/to entities.

---

## 10. UID Generation Strategy

### Formula
```python
uid = f"{prefix}:{sha1(|.join(str(key) for key in identity_keys))}"
```

### Prefix Map
| Entity | Prefix | Identity Keys |
|---|---|---|
| equipment | `eq` | `[plant_code, normalized_asset]` |
| functional_location | `fl` | `[plant_code, floc]` |
| work_order | `wo` | `[plant_code, wo_number]` |
| task_list | `tl` | `[plant_code, task_list_id, task_list_counter, operation_no]` |
| material | `mat` | `[material_code]` |
| timeseries | `ts` | `[plant_code, tag_name]` |
| document | `doc` | `[plant_code, document_id]` |

### Cross-Source UID Equivalence
For TS and Docs: `eq:sha1("PLANT01|351HG1")` → matches the P&ID equipment row  
For SAP (before xref): `eq:sha1("PLANT01|10001234")` → DIFFERENT row  
For SAP (after xref + future overwrite): `eq:sha1("PLANT01|351HG1")` → SAME row as P&ID ✓

The `asset_cross_reference` table is what enables the mapping between the two SAP and P&ID UID spaces today.

---

## 11. File & Directory Layout

```
Implementation 3/
├── config/                          ← All pipeline config (do not hardcode what goes here)
│   ├── schema_frozen.yaml           ← Relational schema for all CDM tables
│   ├── entities_frozen.yaml         ← Entity definitions, identity keys, attribute mappings
│   ├── relationships_frozen.yaml    ← Relationship type definitions
│   ├── identity_frozen.yaml         ← Identity resolution rules per entity
│   ├── derived_fields.yaml          ← Derived/injected field rules
│   ├── cdm_config_frozen.yaml       ← Final consolidation config + document type defs
│   ├── validation_contracts.yaml    ← Per-dataset validation rules
│   ├── sap_column_rename.yaml       ← SAP raw → CDM column mapping
│   ├── pnid_column_rename.yaml      ← P&ID raw → CDM column mapping
│   ├── timeseries_column_rename.yaml← TS raw → CDM column mapping
│   ├── docs_column_rename.yaml      ← Docs raw → CDM column mapping
│   └── fmea_column_rename.yaml      ← FMEA raw → CDM column mapping
│
├── data/
│   ├── staging/                     ← RAW INPUT FILES — put source data here
│   │   ├── sap/                     ← SAP table CSVs/Parquets
│   │   ├── pnid/                    ← P&ID PDF files
│   │   ├── ts/                      ← timeseries_tag_metadata.csv
│   │   └── docs/                    ← Document source files (optional)
│   │
│   ├── work/                        ← Intermediate/debug artifacts
│   │   ├── sap/                     ← *_merged.csv, *_post.csv
│   │   ├── pnid_extract/            ← *_consolidated_*.xlsx (Gemini output), temp_images/
│   │   ├── ts/                      ← timeseries_post.csv
│   │   └── docs/                    ← documents_merged.csv, documents_post.csv
│   │
│   └── out/                         ← PIPELINE OUTPUTS
│       ├── sap/entities/            ← SAP canonical entities
│       ├── sap/relationships/
│       ├── pnid/entities/           ← P&ID equipment, connectivity
│       ├── pnid/relationships/
│       ├── ts/entities/             ← Timeseries metadata
│       ├── ts/relationships/
│       ├── docs/entities/           ← Document metadata
│       ├── docs/relationships/
│       └── final_validation/        ← FINAL DB-READY OUTPUT
│           ├── entities/
│           └── cdm_summary.json
│
├── src/
│   ├── pipelines/                   ← Top-level pipeline orchestrators
│   │   ├── run_sap_end_to_end.py    ← SAP pipeline entry point
│   │   ├── run_pnid_from_pdfs.py   ← P&ID pipeline entry point
│   │   ├── run_ts_end_to_end.py    ← TS pipeline entry point
│   │   ├── run_docs_end_to_end.py  ← Docs pipeline entry point
│   │   ├── run_sap_plus_pnid.py    ← Optional SAP+P&ID fusion
│   │   ├── build_final_cdm.py      ← Final consolidation
│   │   ├── canonical_builder_sap.py ← Canonical entity builder (used by ALL pipelines)
│   │   └── doc_processing.py       ← Document source processing
│   │
│   ├── source_processing/           ← Raw data loaders
│   │   ├── sap/sap_merging_impl.py  ← SAP table joins
│   │   ├── sap/sap_processing.py    ← SAP processing coordinator
│   │   ├── pnid/complete_pnid_extraction_without_ui.py  ← Gemini Vision extractor
│   │   ├── ts/ts_processing.py      ← TS CSV loader
│   │   └── ts/build_ts_tag_metadata.py
│   │
│   └── utils/                       ← Shared utilities (used by all pipelines)
│       ├── renamer.py               ← apply_column_rename()
│       ├── derived.py               ← apply_derived_fields()
│       ├── identity.py              ← apply_identity_rules()
│       ├── asset_identity.py        ← assign_equipment_uid(), normalize_asset_id()
│       ├── ts_asset_identity.py     ← enrich_timeseries_asset_identity()
│       ├── doc_asset_identity.py    ← enrich_document_asset_identity()
│       ├── pid_sap_bridge.py        ← build_pid_to_sap_bridge()
│       ├── canonical_entities.py    ← build_uid_tables_from_canonical_entities()
│       ├── canonical_relationships.py ← build_asset_relationships()
│       ├── schema_apply.py          ← apply_schema(), apply_schema_keep_extra()
│       ├── validate.py              ← run_validations(), report_to_json()
│       └── io.py                    ← File I/O helpers
│
└── scripts/
    ├── run_all.ps1                  ← Master pipeline runner (PowerShell)
    ├── run_sap.ps1 / run_sap.cmd   ← SAP-only runner
    ├── run_pnid.cmd                ← P&ID-only runner
    ├── validate_final.py           ← Output validation helper
    ├── _audit_outputs.py           ← Audit all output files
    ├── _preview_entities.py        ← Quick sanity check on entities
    └── check_cols.py               ← Column mapping coverage check
```

---

## 12. Module Reference

### `src/pipelines/canonical_builder_sap.py`
The core entity builder. Despite the name it is used by ALL four pipelines.

**Key functions:**
- `build_canonical_entities_from_sap_template(post_sources, entities_cfg, schema_cfg, plant_code)` → `dict[table_name: DataFrame]`
- `build_asset_cross_reference(sap_equipment_df, pid_equipment_df, plant_code)` → `DataFrame` (SAP↔P&ID bridge)
- `_ensure_primary_key(df, entity_name, identity_keys, pk_col)` → fills missing UID values
- `_parse_ref("sap_assets.equipment_id")` → `("sap_assets", "equipment_id")`

### `src/utils/asset_identity.py`
The single, shared UID generator. All four pipelines use this for `equipment_uid`.

**Key functions:**
- `make_equipment_uid(plant_code, normalized_asset)` → `"eq:<sha1>"`
- `assign_equipment_uid(df, plant_code, uid_col, asset_col)` → `DataFrame`
- `normalize_asset_id(value)` → uppercase, stripped, no extra whitespace

### `src/utils/canonical_entities.py`
Builds UID lookup tables (used by relationship builder) and handles merge-on-write.

**Key functions:**
- `build_uid_tables_from_canonical_entities(canonical_entities, entities_cfg, schema_cfg)` → `dict[entity_name: DataFrame]`
- `merge_entity_file_on_disk(df_new, filepath, pk_col)` → merged DataFrame (preserves other sources' rows)

### `src/utils/canonical_relationships.py`
Builds the relationship/edge table from config.

**Key functions:**
- `build_asset_relationships(post_sources, relationships_cfg, entities_cfg, schema_cfg, entity_uid_tables)` → `DataFrame`

### `src/utils/ts_asset_identity.py`
TS-specific asset identity enrichment.

**Key functions:**
- `enrich_timeseries_asset_identity(df, reference_candidates)` → `DataFrame`
- `_extract_tag_asset_key(tag_name)` → asset key string (e.g., `"351HG1"`)
- `_pick_best_reference(tag_key, ref_df)` → `(normalized_asset, equipment_id, confidence, method)`

### `src/utils/doc_asset_identity.py`
Document-specific asset identity enrichment.

**Key functions:**
- `enrich_document_asset_identity(df, reference_candidates)` → `DataFrame`
- `EQUIPMENT_TAG_MAP` — **plant-specific** dict, must be updated per plant deployment

### `src/utils/pid_sap_bridge.py`
Alternative SAP↔P&ID bridge using regex pattern extraction from SAP text fields.

**Key functions:**
- `build_pid_to_sap_bridge(sap_assets_df, pid_ref_df)` → bridge DataFrame
- `apply_pid_to_sap_bridge(df, bridge_df)` → enriched DataFrame

---

## 13. How to Add a New Source System

Follow these steps in order:

### Step 1: Create Source Processor
Create `src/source_processing/<source>/` with a function that returns `{"<dataset_name>": DataFrame}`.

### Step 2: Add Column Rename Config
Create `config/<source>_column_rename.yaml`:
```yaml
sources:
  <source_name>:
    RAW_COLUMN_NAME: canonical_column_name
    ...
```

### Step 3: Register Entity in `entities_frozen.yaml`
Add or extend an entity block:
```yaml
my_new_entity:
  canonical_table: my_table
  identity_keys: [plant_code, my_key]
  sources: [my_source_name]
  attributes:
    plant_code:
      from: [my_source_name.plant_code]
      required: true
    my_key:
      from: [my_source_name.raw_key_col]
      required: true
```

### Step 4: Add to `schema_frozen.yaml`
Define the table with all columns, primary key, unique constraints, indexes:
```yaml
my_table:
  primary_key: my_table_uid
  columns:
    my_table_uid: BIGSERIAL
    plant_code: VARCHAR(50)
    my_key: VARCHAR(120)
    ...
```

### Step 5: Add Relationships in `relationships_frozen.yaml`
```yaml
my_entity_to_equipment:
  from_entity: my_new_entity
  to_entity: equipment
  source: my_source_name
  type: MY_RELATIONSHIP_TYPE
  directed: true
  dedupe_keys: [plant_code, my_key, normalized_asset]
```

### Step 6: Update `cdm_config_frozen.yaml`
Register the new source under `sources` and add file paths to `consolidation.source_files`.

### Step 7: Add Asset Identity Logic (if needed)
If the new source has its own identifier format, add an enrichment function in `src/utils/` following the same pattern as `ts_asset_identity.py`.

### Step 8: Write the Pipeline Runner
Create `src/pipelines/run_<source>_end_to_end.py` following the same 10-stage pattern. The canonical builder and relationship builder are reused as-is.

---

## 14. Known Issues & Design Debt

### D1 — Dual UID for Same Physical Asset (Critical)
SAP and P&ID produce different `equipment_uid` values for the same physical asset because their `normalized_asset` values differ (EQUNR vs. P&ID tag). `asset_cross_reference` bridges them but they remain as two rows in the final `equipment` table. **Resolution:** Once xref coverage is validated, overwrite SAP `normalized_asset` with the P&ID tag before UID generation.

### D2 — `EQUIPMENT_TAG_MAP` is Hardcoded and Plant-Specific
The dict in `doc_asset_identity.py` maps equipment names to P&ID tags for a specific cement plant. Deploying to a new plant will silently produce wrong tags. **Resolution:** Move this map to a YAML config file (`config/equipment_name_map.yaml`) and load it at runtime.

### D3 — `build_canonical_entities_from_sap_template` Only Uses First Source's Rows
The multi-source union declared in `entities_frozen.yaml` (e.g., `equipment` drawing from `[sap_assets, pid, documents, timeseries]`) is not fully realised. Only rows from the first available source are indexed; columns from other sources fill those same rows. True multi-source union requires iterating all sources and concatenating before dedup.

### D4 — Missing P&ID Reference Fails Silently
If P&ID hasn't run when TS or Docs pipeline runs, `equipment_id` enrichment is silently skipped. No error, no warning beyond a file-not-found log. **Resolution:** Add an explicit check and warning when `--pid_out` is provided but the file is absent.

### D5 — Fuzzy Match Thresholds are Hardcoded
TS uses 0.84, Docs uses 0.80. These should be in `identity_frozen.yaml` or `derived_fields.yaml` for tuning without code changes.

### D6 — `run_sap_plus_pnid.py` Not Wired into Main Pipeline
The 3-tier SAP+P&ID equipment fusion script exists and is correct but is not called by `run_all.ps1`. Its output goes to `data/out/integrated/` which is disconnected from `build_final_cdm`. **Resolution:** Wire it as Step 6.5 in `run_all.ps1` and update `cdm_config_frozen.yaml` to consume from `data/out/integrated/` instead of separate SAP and P&ID dirs.

### D7 — TS `asset_match_confidence` = 0.90 for All Tag Pattern Matches
A tag whose prefix matches P&ID exactly gets the same confidence (0.90) as one that extracted cleanly but matched nothing. The P&ID match score should be added on top of the base tag extraction confidence.

---

## 15. Coding Agent Quick Reference

> **Read this section first** if you are a coding agent picking up a task in this codebase.

### Golden Rules

1. **Never hardcode table names, column names, or file paths in Python.** They belong in YAML config files.
2. **Never change schema_frozen.yaml column names** without also updating all four `*_column_rename.yaml` files and all `entities_frozen.yaml` attribute `from:` references.
3. **The canonical builder is shared.** `canonical_builder_sap.py` is used by ALL pipelines, not just SAP. Any change to it affects all four pipelines.
4. **identity_keys are sacred.** If you add or remove a key from `identity_frozen.yaml` or `entities_frozen.yaml`, the UID for ALL existing rows of that entity will change. This breaks cross-pipeline joins.
5. **P&ID must run before TS and Docs.** The TS and Docs pipelines read `data/out/pnid/entities/equipment.csv`. If P&ID hasn't run, `equipment_id` will be blank in TS and Docs outputs.
6. **plant_code is forced at every stage.** The runtime arg `--plant_code` overwrites any `plant_code` value that came from the source data. Never trust `plant_code` from a raw CSV.

### Key Data Structures

```python
# post_sources: the main dict passed through all stages
# Keys are template source names (must match entities_frozen.yaml source keys)
post_sources: dict[str, pd.DataFrame] = {
    "sap_wo": df_workorder,
    "sap_assets": df_floc,
    "sap_task_list": df_tasklist,
    "sap_material_master": df_material,
}

# canonical_entities: output of build_canonical_entities_from_sap_template
# Keys are canonical_table names (must match schema_frozen.yaml table names)
canonical_entities: dict[str, pd.DataFrame] = {
    "work_order": df_wo,
    "equipment": df_eq,
    "functional_location": df_floc,
    ...
}

# uid_tables: output of build_uid_tables_from_canonical_entities
# Keys are ENTITY names (from entities_frozen.yaml), not table names
uid_tables: dict[str, pd.DataFrame] = {
    "work_order": df_wo_uid,   # columns: wo_uid, plant_code, wo_number
    "equipment":  df_eq_uid,   # columns: equipment_uid, plant_code, normalized_asset
    ...
}
```

### Common Task Patterns

**Adding a new column to an existing entity:**
1. Add to `schema_frozen.yaml` under the table's `columns` block
2. Add to `entities_frozen.yaml` under the entity's `attributes` block with `from:` references
3. Add to the relevant `*_column_rename.yaml` if the raw source calls it something different
4. No Python changes needed if the column follows the standard from-reference pattern

**Changing an identity key:**
1. Update `identity_frozen.yaml` under the entity
2. Update `entities_frozen.yaml` under the entity's `identity_keys` list
3. All existing UIDs for that entity will change — downstream joins must be re-run
4. Update `schema_frozen.yaml` unique constraint if needed

**Adding a new relationship:**
1. Add a new block to `relationships_frozen.yaml`
2. Ensure both `from_entity` and `to_entity` have entries in `entities_frozen.yaml`
3. Ensure `source` key will exist in `post_sources` when the relevant pipeline runs
4. No Python changes needed

**Debugging why a relationship table is empty:**
1. Check that `source` key in `relationships_frozen.yaml` exists in `post_sources` (print `post_sources.keys()`)
2. Check that `from_entity` and `to_entity` have entries in `uid_tables` (print `uid_tables.keys()`)
3. Check that identity keys for both entities are populated in their respective post DataFrames
4. Look for "dropped rows with blank identity keys" in pipeline stdout

**Debugging why `equipment_id` is blank in TS/Docs output:**
1. Check that P&ID pipeline has run and `data/out/pnid/entities/equipment.csv` exists
2. Check that `--pid_out` arg was passed to the TS/Docs pipeline
3. Print `ref_assets` DataFrame inside `enrich_timeseries_asset_identity` to verify P&ID reference loaded
4. Check `asset_match_method` column in output — if it's `"tag_pattern"` or `"name_map"` but `equipment_id` is still blank, the P&ID reference lookup found no match above the confidence threshold

---

*End of Design Document*
