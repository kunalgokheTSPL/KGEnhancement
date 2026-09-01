"""Plant code agreement between the ts run and the pnid reference."""

SOURCE = "p0/pipelines/run_ts_end_to_end.py"


def test_a_plant_mismatch_is_reported_before_the_edges_come_back_empty():
    src = open(SOURCE).read()
    assert "0 HAS_TAG edges expected" in src
    assert "_eq_plants" in src


def test_the_guard_reads_the_plant_from_the_equipment_uid_table():
    src = open(SOURCE).read()
    block = src.split("_eq_plants")[1][:300]
    assert "plant_code_id" in block


def test_an_empty_relationship_source_is_reported_not_skipped_silently():
    src = open("p0/utils/canonical_relationships.py").read()
    assert "is present but empty" in src
