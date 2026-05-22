"""Unit tests for the AG-UI RunAgentInput parser.

The parser extracts Langflow run parameters from a strict AG-UI ``RunAgentInput``.
No mocks: it is a pure function over real ``RunAgentInput`` instances.
"""

from __future__ import annotations

from ag_ui.core import RunAgentInput, UserMessage
from langflow.api.v2.converters import parse_run_agent_input


def _run_input(**overrides) -> RunAgentInput:
    base = {
        "thread_id": "sess-1",
        "run_id": "run-1",
        "state": {},
        "messages": [],
        "tools": [],
        "context": [],
        "forwarded_props": {},
    }
    base.update(overrides)
    return RunAgentInput(**base)


def test_parse_extracts_langflow_fields_from_forwarded_props():
    run_input = _run_input(
        forwarded_props={
            "flow_id": "flow-abc",
            "tweaks": {"X": {"p": 1}},
            "mode": "background",
            "start_component_id": "c1",
            "stop_component_id": "c2",
        }
    )

    parsed = parse_run_agent_input(run_input)

    assert parsed.flow_id == "flow-abc"
    assert parsed.tweaks == {"X": {"p": 1}}
    assert parsed.mode == "background"
    assert parsed.start_component_id == "c1"
    assert parsed.stop_component_id == "c2"


def test_parse_takes_input_value_from_last_user_message():
    run_input = _run_input(messages=[UserMessage(id="u1", content="first"), UserMessage(id="u2", content="latest")])

    parsed = parse_run_agent_input(run_input)

    assert parsed.input_value == "latest"


def test_parse_maps_thread_id_to_session_and_run_id():
    parsed = parse_run_agent_input(_run_input(thread_id="sess-9", run_id="run-9"))

    assert parsed.session_id == "sess-9"
    assert parsed.run_id == "run-9"


def test_parse_defaults_mode_to_stream_and_tweaks_to_empty():
    parsed = parse_run_agent_input(_run_input(forwarded_props={"flow_id": "f"}))

    assert parsed.mode == "stream"
    assert parsed.tweaks == {}
    assert parsed.start_component_id is None


def test_parse_handles_empty_messages_and_props():
    parsed = parse_run_agent_input(_run_input())

    assert parsed.input_value == ""
    assert parsed.flow_id is None
