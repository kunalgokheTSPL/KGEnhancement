"""Functional locations parse into site, levels and joinable tag keys."""

from p0.utils.asset_identity import (
    NO_SITE,
    floc_keys,
    floc_site,
    normalize_tag,
    parse_floc,
    split_site_prefix,
)


def test_a_dotted_floc_splits_into_levels_and_a_parent_item_pair():
    f = parse_floc("BOCP.GCO.BC2.A2410B-K2410B")
    assert f.site == "BOCP"
    assert f.levels == ("BOCP", "GCO", "BC2", "A2410B-K2410B")
    assert f.parent == "A2410B"
    assert f.item == "K2410B"


def test_a_leaf_without_a_hyphen_is_the_item_and_has_no_parent():
    f = parse_floc("BNQB.LIQ.CRN.X5420")
    assert f.item == "X5420"
    assert f.parent == ""
    assert floc_keys("BNQB.LIQ.CRN.X5420") == ("X5420",)


def test_the_site_is_the_first_level_across_every_platform():
    for floc, site in (
        ("BOCP.GCO.BC2.A2410B-K2410B", "BOCP"),
        ("BOKA.GEX.GC3.K7800-P7806", "BOKA"),
        ("BODB.LIQ.CRN.X5420", "BODB"),
        ("TKPB-S3-V", "TKPB"),
    ):
        assert floc_site(floc) == site


def test_an_unparseable_floc_yields_no_site_rather_than_a_wrong_one():
    assert floc_site("") == NO_SITE
    assert floc_site(None) == NO_SITE
    assert floc_keys("") == ()


def test_item_comes_before_parent_so_the_closest_match_wins():
    assert floc_keys("BOCP.GCO.BC2.A2410B-K2410B") == ("K2410B", "A2410B")


def test_a_platform_prefixed_tag_loses_only_the_platform():
    assert split_site_prefix("BO-CPP K2410B") == ("BO-CPP", "K2410B")
    assert split_site_prefix("BOCPP P-1310A") == ("BOCPP", "P-1310A")
    assert split_site_prefix("WH BO-106L") == ("WH", "BO-106L")


def test_a_two_word_tag_with_no_digits_is_left_whole():
    assert split_site_prefix("PUMP HOUSE") == ("", "PUMP HOUSE")


def test_the_three_spellings_of_one_machine_normalize_together():
    assert normalize_tag("BO-CPP K2410B") == normalize_tag("K2410B") == "K2410B"
    assert normalize_tag("K-2410-B") == "K2410B"


def test_a_document_tag_matches_the_floc_it_belongs_to():
    assert normalize_tag("BO-CPP K2410B") in floc_keys("BOCP.GCO.BC2.A2410B-K2410B")
    assert normalize_tag("K7800") in floc_keys("BOKA.GEX.GC3.K7800-P7806")
