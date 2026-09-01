# p0 test suite

How the p0 tests are organised, what each part covers, and how to run them. Baseline
to keep green: the infra-free tier is **141 passed / 1 xfailed** (142 collected) with no
cluster and no secrets; the full suite (with a live cluster) adds the api tier on top.

## Tiers

Tests split by what they need to run — this is the line CI cares about.

| Tier | Location | Needs | Run |
|---|---|---|---|
| **unit** | `p0/tests/unit/` | nothing (no DB, no object store, no IoTDB, no `MASTER_KEY`) | `pytest p0/tests/unit/` |
| **connectors** | `p0/connectors/tests/` | nothing (local connectors + hand-built fixtures) | `pytest p0/connectors/tests/` |
| **api / full** | `p0/tests/api/` | a live cluster (postgres, IoTDB, RustFS) + `MASTER_KEY`; some create/drop real DBs | `pytest p0/tests/ p0/connectors/tests/` |

**CI gate:** `p0/scripts/ci_unit_tests.sh` runs the unit + connectors tiers with a
coverage floor (55% on the gate/transform core). It needs no cluster and no secrets,
so it can gate every PR. The api tier runs separately against real infra.

> Collection note: the unit tier is collected **by directory** on purpose. Passing an
> individual `p0/tests/*.py` file *alongside* the `p0/connectors/tests/` directory trips
> a pytest `pythonpath`+rootdir quirk (0 collected). Run directories, not mixed
> file+dir arg lists.

### Running

```bash
# infra-free (what CI runs) — no MASTER_KEY, no cluster
PYTHONPATH=$PWD pytest p0/tests/unit/ p0/connectors/tests/

# CI gate with coverage floor
PYTHON=.venv/bin/python bash p0/scripts/ci_unit_tests.sh

# full suite (needs a live cluster + MASTER_KEY)
MASTER_KEY=... PYTHONPATH=$PWD AUTH_ENABLED=false P0_DATA_DIR=... \
  .venv/bin/python -m pytest p0/tests/ p0/connectors/tests/ -q
```

The cluster the api tier points at is selected by `.env.enc.local` (swap between the
office `data.local` profile and the EC2/home-tunnel profile; the `*.bak` files next to
it are the saved profiles).

## What each area covers

**unit/** (infra-free, 90 tests)
- `test_gate_aggregators` — the staging + CDM quality-gate roll-up: a BLOCK leaf must
  produce `gate_status=BLOCKED`, and `run_staging_gate_or_raise` fails loudly on a
  missing config. This is the loud-vs-silent decision path.
- `test_staging_quality_gate` / `test_cdm_post_validation` — the pure leaf checks
  (format validators, natural keys, type/schema conformance, PK/referential integrity).
- `test_sap_canonical_builder` / `test_ts_tag_metadata` — golden tests of the
  deterministic SAP crosswalk and TS tag parsers (exact output rows, not just "it ran").
- `test_request_context` — plant extraction from query / JSON / multipart, without
  eating the upload.
- `test_security_regressions` — wrong `MASTER_KEY` can't decrypt (passing); hardcoded
  `MASTER_KEY` default is flagged (xfail, flips green when removed + key rotated).
- `test_connector_registry_conformance` — every `(data_type, source_type)` maps to a
  concrete `BaseConnector`, and the registry imports without the enterprise SDKs.

**connectors/tests/** (infra-free, 52 tests) — the document / pnid / destination
connectors against local + fake-backend fixtures.

**api/** (needs cluster, 28 files) — the HTTP surface end-to-end: plant provisioning +
**cross-tenant isolation**, commit flows (sap/pnid/docs/ts), flow state, the generated
envelope contract (`test_endpoint_contract`), and lifecycle E2E. The provisioning /
lifecycle files carry `@pytest.mark.real_provisioning` (they create + drop real DBs).

**Root** — `test_subprocess_cli_contract` launches every pipeline / source_processing
entry script with `--help` (import + CLI tripwire for the otherwise-untested subprocess
half). Needs `MASTER_KEY` because the scripts import config.

`trace_endpoints.py` is a dev tool, not a test (no `test_` prefix — pytest ignores it).

## Audit decisions (keep / fix / merge / delete)

- **Delete — dead code:** removed the 7 superseded per-doc-type extractors
  (`user_doc_extract` is the single generic one) and the `p0.Implementation` shim.
- **Delete — removed features:** none found — no KG/ontology test remnants remain.
- **xfail sweep:** the two live xfails are both valid, not stale — the
  `getCommitHistory`/`getSapJoins` blank-plant contract wart (documented, `strict=False`)
  and the `MASTER_KEY`-default security gap. No fixed-but-still-marked xfails.
- **Duplicate coverage:** kept both `test_endpoint_contract` (asserts the envelope shape
  across every route, generated) and the per-endpoint behaviour tests — they're
  complementary, not duplicative.
- **Fix — wiring:** the connector suite (1,384 LOC) was orphaned and un-importable
  (hard-imported cloud/SAP SDKs); made those imports optional and pulled the suite into
  the run.

## Known gaps (not yet covered)

- **pnid / docs pipeline stages** — LLM-based (Gemini/Bedrock); need a fake-LLM harness
  before they can be golden-tested. Deterministic SAP/TS cores are done.
- **Full subprocess stage runs** — the `--help` tripwire proves launch, not correctness.
- **Auth** — `AUTH_ENABLED=true` unsigned-cookie rejection lives in `Login/` (owned
  separately) and needs Keycloak/JWKS to test.
- **Coverage** — overall p0 sits at ~46%; the subprocess half (`pipelines/`,
  `source_processing/`) is the largest remaining hole.
