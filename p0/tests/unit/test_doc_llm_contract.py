"""The model contract for document extraction: tool schema, parsing, and truncation."""

from __future__ import annotations

import json

import pytest

from p0.source_processing.documents.user_doc_extract import (
    _DEFAULT_MODEL,
    _DEFAULT_REGION,
    _EXTRACT_TOOL,
    _ExtractionFailed,
    _bedrock_region,
    _build_prompt,
    _call_llm,
    _extract_tool,
    _parse_text_entries,
)

COLS = ["Equipment_Tag", "Failure Mode", "Tag No."]


class _FakeBody:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode()


class _FakeClient:
    def __init__(self, payload):
        self._payload = payload
        self.last_body = None

    def invoke_model(self, modelId, body, contentType, accept):
        self.last_body = json.loads(body)
        return {"body": _FakeBody(self._payload)}


def _tool_use_payload(entries, stop_reason="tool_use"):
    return {
        "stop_reason": stop_reason,
        "content": [
            {"type": "tool_use", "name": _EXTRACT_TOOL, "input": {"entries": entries}}
        ],
    }


def _text_payload(text, stop_reason="end_turn"):
    return {"stop_reason": stop_reason, "content": [{"type": "text", "text": text}]}


def _call(payload):
    client = _FakeClient(payload)
    rows = _call_llm(client, "excerpt", "prompt {text}", "model", _extract_tool(COLS))
    return client, rows


def test_tool_schema_offers_exactly_the_requested_fields():
    schema = _extract_tool(COLS)["input_schema"]
    item = schema["properties"]["entries"]["items"]
    assert list(item["properties"]) == COLS
    assert item["additionalProperties"] is False
    assert schema["required"] == ["entries"]


def test_the_request_forces_the_tool_rather_than_hoping_for_json():
    client, _ = _call(_tool_use_payload([]))
    assert client.last_body["tool_choice"] == {"type": "tool", "name": _EXTRACT_TOOL}
    assert [t["name"] for t in client.last_body["tools"]] == [_EXTRACT_TOOL]


def test_temperature_is_not_sent_because_current_models_reject_it():
    client, _ = _call(_tool_use_payload([]))
    assert "temperature" not in client.last_body


def test_the_excerpt_is_injected_into_the_prompt_that_is_sent():
    client = _FakeClient(_tool_use_payload([]))
    _call_llm(
        client, "REACTOR TEMP 500C", "body: {text}", "m", _extract_tool(COLS)
    )
    assert client.last_body["messages"][0]["content"] == "body: REACTOR TEMP 500C"


def test_entries_come_back_from_a_tool_call():
    _, rows = _call(_tool_use_payload([{"Equipment_Tag": "P-101A"}]))
    assert rows == [{"Equipment_Tag": "P-101A"}]


def test_a_truncated_reply_fails_instead_of_returning_partial_entries():
    with pytest.raises(_ExtractionFailed):
        _call(_tool_use_payload([{"Equipment_Tag": "P-101A"}], stop_reason="max_tokens"))


def test_a_tool_call_without_an_entries_list_fails():
    payload = {
        "stop_reason": "tool_use",
        "content": [{"type": "tool_use", "name": _EXTRACT_TOOL, "input": {}}],
    }
    with pytest.raises(_ExtractionFailed):
        _call(payload)


def test_a_model_that_ignores_the_tool_still_parses_as_json():
    _, rows = _call(_text_payload('{"entries": [{"Tag No.": "K-200"}]}'))
    assert rows == [{"Tag No.": "K-200"}]


def test_unreadable_output_fails_loudly_rather_than_silently_dropping_the_chunk():
    with pytest.raises(_ExtractionFailed):
        _call(_text_payload("I could not find anything useful in this excerpt."))


def test_an_empty_result_is_not_a_failure():
    _, rows = _call(_tool_use_payload([]))
    assert rows == []


def test_parse_text_entries_reads_fenced_and_bare_shapes():
    assert _parse_text_entries('```json\n{"entries": [{"a": "1"}]}\n```') == [{"a": "1"}]
    assert _parse_text_entries('[{"a": "1"}]') == [{"a": "1"}]
    assert _parse_text_entries('{"a": "1"}') == [{"a": "1"}]
    assert _parse_text_entries("not json") is None
    assert _parse_text_entries("") is None


def test_the_prompt_names_the_tool_the_doc_type_and_every_field():
    prompt = _build_prompt(COLS, "FMEA")
    assert _EXTRACT_TOOL in prompt
    assert "FMEA" in prompt
    for c in COLS:
        assert c in prompt
    assert "{text}" in prompt


def test_an_arn_model_id_still_picks_its_own_region(monkeypatch):
    monkeypatch.setenv("BEDROCK_REGION", "ap-south-1")
    arn = "arn:aws:bedrock:eu-west-1:1:inference-profile/eu.anthropic.claude-sonnet-5"
    assert _bedrock_region(arn) == "eu-west-1"


def test_a_bare_profile_id_lands_in_a_region_that_serves_it(monkeypatch):
    monkeypatch.delenv("BEDROCK_REGION", raising=False)
    assert _bedrock_region(_DEFAULT_MODEL) == _DEFAULT_REGION
    assert _DEFAULT_MODEL.split(".")[0] == _DEFAULT_REGION.split("-")[0]


def test_the_region_env_var_still_wins_for_a_bare_profile_id(monkeypatch):
    monkeypatch.setenv("BEDROCK_REGION", "eu-central-1")
    assert _bedrock_region("some.model-id") == "eu-central-1"


def test_the_prompt_forbids_inventing_values():
    prompt = _build_prompt(COLS, "RCA").lower()
    assert "verbatim" in prompt
    assert "never infer" in prompt
    assert "<extracted value>" not in prompt
