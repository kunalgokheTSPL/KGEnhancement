#!/usr/bin/env bash
#
# Infra-free p0 CI gate: the unit + connector tests with a coverage floor.
#
# Runs WITHOUT a database, object store, IoTDB, or MASTER_KEY — safe as a PR gate on
# any runner (Jenkins, GitHub Actions, local). The full suite, which depends on a live
# postgres / IoTDB / RustFS cluster, runs separately against real infra.
#
# Wire into CI as a single step, e.g.:
#   Jenkins:  sh 'pip install -r p0/requirements.txt coverage pytest && bash p0/scripts/ci_unit_tests.sh'
#   Actions:  run: pip install -r p0/requirements.txt coverage pytest && bash p0/scripts/ci_unit_tests.sh
#
# Override the floor with COVERAGE_FLOOR=NN; pass extra pytest args after the script.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export PYTHONPATH="$ROOT"

PYTHON="${PYTHON:-python}"
FLOOR="${COVERAGE_FLOOR:-55}"

# Coverage floor is scoped to the modules these tests own (the gate/transform core).
# The connector tests run and gate on pass/fail, but import as `connectors.*` so they
# are not part of this coverage measurement.
COV_MODULES="p0.utils.staging_quality_gate,p0.utils.cdm_post_validation,p0.pipelines.canonical_builder_sap,p0.source_processing.ts.build_ts_tag_metadata"

"$PYTHON" -m coverage run --source="$COV_MODULES" \
    -m pytest p0/tests/unit/ p0/connectors/tests/ "$@"

"$PYTHON" -m coverage report
echo "Enforcing coverage floor: ${FLOOR}% on the gate/transform core"
"$PYTHON" -m coverage report --fail-under="$FLOOR"
