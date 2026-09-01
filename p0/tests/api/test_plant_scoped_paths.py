"""
Plant-scoped RustFS path helpers.

These cover the 8 helpers in ``config.py`` that build the plant-coded
storage layout. The behaviour they protect:

  • Plant is the highest segregation level under each top-level prefix:

        s3://bucket/staging-pt/<plant_code_id>/<type>/...
        s3://bucket/processed_data/<plant_code_id>/<type>/...

  • Different plant_codes resolve to disjoint subtrees — proves different
    plants' data can't accidentally land at the same key.

  • An invalid plant_code_id is rejected up-front — the helpers refuse to
    build a path that would let a stray slash escape the plant boundary.
"""

from __future__ import annotations

import pytest

from p0.api.config import (
    RUSTFS_BUCKET,
    rustfs_processed_docs,
    rustfs_processed_pnid,
    rustfs_processed_sap,
    rustfs_processed_ts,
    rustfs_staging_docs,
    rustfs_staging_pnid,
    rustfs_staging_sap,
    rustfs_staging_ts,
)




_HELPERS = [
    (rustfs_staging_pnid, "staging-pt", "pnid"),
    (rustfs_staging_sap, "staging-pt", "sap"),
    (rustfs_staging_docs, "staging-pt", "documents"),
    (rustfs_staging_ts, "staging-pt", "timeseries"),
    (rustfs_processed_pnid, "processed_data", "pnid"),
    (rustfs_processed_sap, "processed_data", "sap"),
    (rustfs_processed_docs, "processed_data", "documents"),
    (rustfs_processed_ts, "processed_data", "timeseries"),
]


@pytest.mark.parametrize(
    "helper,root,type_seg", _HELPERS, ids=lambda x: getattr(x, "__name__", str(x))
)
def test_helper_layout_is_plant_then_type(helper, root, type_seg):
    """Layout contract: ``s3://<bucket>/p0/<root>/<plant>/<type>``.

    The ``p0/`` segment is a top-level namespace separating p0's keys
    from p1's inside the shared bucket. Within p0's namespace, plant
    comes BEFORE type — so wiping one plant deletes one subtree.
    """
    from p0.api.config import OBJECT_SCHEME, RUSTFS_P0_PREFIX

    # OBJECT_SCHEME (s3 on-prem / abfs on azure) so the contract holds in both modes
    assert (
        helper("plant_1_testcase")
        == f"{OBJECT_SCHEME}://{RUSTFS_BUCKET}/{RUSTFS_P0_PREFIX}/{root}/plant_1_testcase/{type_seg}"
    )


@pytest.mark.parametrize(
    "helper,root,type_seg", _HELPERS, ids=lambda x: getattr(x, "__name__", str(x))
)
def test_helper_keeps_plant_codes_distinct(helper, root, type_seg):
    """Different plant_codes always resolve to different paths."""
    a = helper("PlantA")
    b = helper("PlantB")
    assert a != b
    assert "/PlantA/" in a
    assert "/PlantB/" in b




@pytest.mark.parametrize(
    "bad",
    [
        "",
        None,
        "a/b",
        "../other",
        "Plant 1",
        "Plant_!",
        "P" * 51,
    ],
)
def test_helpers_reject_invalid_plant_code(bad):
    with pytest.raises(ValueError):
        rustfs_staging_pnid(bad)
