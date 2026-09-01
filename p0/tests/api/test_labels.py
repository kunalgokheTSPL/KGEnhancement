"""The four use-case labels: fixed vocabulary, mandatory, multi-select."""

from __future__ import annotations

import json

from p0.api.services import labels


def test_the_vocabulary_is_exactly_four_labels():
    assert {entry["key"] for entry in labels.catalogue()} == {
        "integrity", "reliability", "maintenance", "production"
    }


def test_the_catalogue_is_a_copy_callers_cannot_mutate():
    labels.catalogue()[0]["label"] = "tampered"
    assert labels.catalogue()[0]["label"] == "Integrity"


def test_pnid_is_the_one_connector_without_labels():
    assert labels.requires_labels("pnid") is False
    for connector in ("documents", "timeseries", "sap", "aif", "gloc", "lopc"):
        assert labels.requires_labels(connector) is True


def test_at_least_one_label_is_required():
    values, error = labels.validate("", connector="aif")
    assert values == []
    assert "at least one" in error.lower()


def test_pnid_uploads_pass_with_no_labels():
    values, error = labels.validate(None, connector="pnid")
    assert (values, error) == ([], None)


def test_an_unknown_label_is_rejected_with_the_allowed_set():
    _, error = labels.validate("integrity,safety", connector="aif")
    assert "safety" in error
    assert "reliability" in error


def test_multiple_labels_are_allowed():
    values, error = labels.validate("integrity,reliability", connector="aif")
    assert error is None
    assert values == ["integrity", "reliability"]


def test_a_json_array_is_accepted():
    values, error = labels.validate('["integrity","maintenance"]', connector="sap")
    assert error is None
    assert set(values) == {"integrity", "maintenance"}


def test_a_python_list_is_accepted():
    assert labels.parse(["Integrity", "PRODUCTION"]) == ["integrity", "production"]


def test_spelling_variants_resolve_to_the_canonical_key():
    assert labels.parse("Reliablity") == ["reliability"]
    assert labels.parse("Maintainance") == ["maintenance"]
    assert labels.parse("Asset Integrity") == ["integrity"]


def test_duplicates_collapse():
    assert labels.parse("integrity, Integrity ,INTEGRITY") == ["integrity"]


def test_storage_order_is_stable_regardless_of_tick_order():
    assert labels.normalise(["production", "integrity"]) == labels.normalise(
        ["integrity", "production"]
    )


def test_every_labelled_connector_has_a_suggestion_except_documents():
    assert labels.suggested_for("aif") == ["integrity"]
    assert labels.suggested_for("sap") == ["maintenance"]
    assert labels.suggested_for("timeseries") == ["reliability"]
    assert labels.suggested_for("documents") == []


def test_one_selection_applies_to_every_file_in_the_batch():
    mapping = labels.per_file("integrity", ["a.xlsx", "b.xlsx"], connector="aif")
    assert mapping == {"a.xlsx": ["integrity"], "b.xlsx": ["integrity"]}


def test_per_file_selection_is_honoured_when_supplied_as_an_object():
    raw = json.dumps({"a.xlsx": ["integrity"], "b.xlsx": ["production", "maintenance"]})
    mapping = labels.per_file(raw, ["a.xlsx", "b.xlsx"], connector="aif")
    assert mapping["a.xlsx"] == ["integrity"]
    assert mapping["b.xlsx"] == ["production", "maintenance"]


def test_a_file_missing_from_the_object_gets_no_labels():
    raw = json.dumps({"a.xlsx": ["integrity"]})
    mapping = labels.per_file(raw, ["a.xlsx", "b.xlsx"], connector="aif")
    assert mapping["b.xlsx"] == []


def test_pnid_files_are_stripped_of_labels_even_if_sent():
    mapping = labels.per_file("integrity", ["x.pdf"], connector="pnid")
    assert mapping["x.pdf"] == []


def test_every_upload_response_echoes_the_labels_it_stored():
    """flow_state had them but the response did not — the UI could not show the chips."""
    import ast
    import pathlib

    router = pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
    tree = ast.parse(router.read_text())
    labelled = {
        "uploadDocumentFiles", "uploadSapFiles", "uploadTimeseriesFiles",
        "_upload_findings_files",
    }
    seen = set()
    for fn in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
        if fn.name not in labelled:
            continue
        seen.add(fn.name)
        keys = [
            k.value
            for node in ast.walk(fn)
            if isinstance(node, ast.Dict)
            for k in node.keys
            if isinstance(k, ast.Constant) and isinstance(k.value, str)
        ]
        assert "labels" in keys, f"{fn.name} never puts labels on the file status"
    assert seen == labelled, f"missing upload handlers: {labelled - seen}"


def test_no_upload_handler_reads_a_name_it_never_assigned():
    """uploadPnidFiles has no labels at all — referencing chosen_labels there is a 500."""
    import ast
    import pathlib

    router = pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
    tree = ast.parse(router.read_text())
    functions = [
        n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]

    def assigns(fn):
        return any(
            isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store) and n.id == "chosen_labels"
            for n in ast.walk(fn)
        )

    for fn in functions:
        reads = any(
            isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id == "chosen_labels"
            for n in ast.walk(fn)
        )
        if not reads or assigns(fn):
            continue
        enclosing = [
            other for other in functions
            if other is not fn
            and any(c is fn for c in ast.walk(other))
            and assigns(other)
        ]
        assert enclosing, f"{fn.name} reads chosen_labels without assigning it"


def test_a_repeated_form_field_is_parsed_as_multiple_labels():
    """The UI sends labels=integrity&labels=reliability — a list, not a string."""
    assert labels.parse(["integrity", "reliability"]) == ["integrity", "reliability"]


def test_a_list_entry_holding_commas_is_still_split():
    """Some clients send one field with commas inside; both shapes must work."""
    assert labels.parse(["integrity,reliability"]) == ["integrity", "reliability"]
    assert labels.parse(["integrity", "production,maintenance"]) == [
        "integrity", "production", "maintenance"
    ]


def test_an_empty_list_is_still_a_missing_selection():
    values, error = labels.validate([], connector="aif")
    assert values == []
    assert "at least one" in error.lower()


def test_the_upload_field_is_an_array_so_swagger_shows_multi_select():
    """A plain string made the frontend think only one label could be sent."""
    import ast
    import pathlib

    router = pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
    body = router.read_text()
    assert 'labels: str = Form(' not in body

    tree = ast.parse(body)
    upload_functions = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name.lower().startswith("upload")
    ]
    assert upload_functions, "no upload endpoints found to check"

    def _labels_annotation(node):
        for arg in node.args.args:
            if arg.arg == "labels" and arg.annotation is not None:
                return ast.unparse(arg.annotation)
        return None

    with_labels = [(n.name, _labels_annotation(n)) for n in upload_functions]
    with_labels = [(name, ann) for name, ann in with_labels if ann is not None]
    assert with_labels, "no upload endpoint declares a labels parameter to check"

    not_array = [name for name, ann in with_labels if ann != "list[str]"]
    assert not not_array, f"labels not typed list[str] in: {not_array}"


def test_the_field_help_says_what_a_label_is():
    """The frontend asked 'what is this label?' — the description must answer it."""
    import pathlib

    body = (
        pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "connectors.py"
    ).read_text()
    help_text = body.split("_FINDINGS_LABEL_HELP = (")[1].split("\n)")[0]
    for expected in ("integrity", "reliability", "maintenance", "production", "NOT document"):
        assert expected in help_text


def test_a_json_array_sent_as_one_form_value_still_works():
    """As list[str] the JSON form arrives as a single element, not a bare string."""
    assert labels.parse(['["integrity","maintenance"]']) == ["integrity", "maintenance"]
    assert labels.parse('["integrity","maintenance"]') == ["integrity", "maintenance"]


def test_every_shape_the_ui_might_send_lands_on_the_same_answer():
    expected = ["integrity", "reliability"]
    for shape in (
        ["integrity", "reliability"],
        ["integrity,reliability"],
        ['["integrity","reliability"]'],
        "integrity,reliability",
        '["integrity","reliability"]',
    ):
        assert labels.parse(shape) == expected, shape
