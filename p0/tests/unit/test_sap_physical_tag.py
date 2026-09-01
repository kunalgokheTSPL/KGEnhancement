"""A dotted functional location yields the real leaf tag, not a mangled one."""

from p0.pipelines.run_sap_end_to_end import _extract_physical_tag_from_floc as tag


def test_a_dotted_floc_gives_back_the_leaf_tag():
    assert tag("BOCP.GCO.BC2.A2410B-K2410B", "M014") == "K2410B"
    assert tag("BOKA.GEX.GC3.K7800-P7806", "M014") == "P7806"


def test_a_dotted_floc_with_no_hyphen_in_the_leaf_still_gives_the_leaf():
    assert tag("BODB.LIQ.CRN.X5420", "M014") == "X5420"


def test_the_hyphen_dialect_keeps_its_existing_reformatting():
    assert tag("B32-K0001", "B32") == "B32-K-0001"
    assert tag("B32-P12A", "B32") == "B32-P-0012A"


def test_an_unrecognised_shape_is_returned_untouched():
    assert tag("B32-0001", "B32") == "B32-0001"
    assert tag("SINGLE", "B32") == "SINGLE"
    assert tag("", "B32") == ""
