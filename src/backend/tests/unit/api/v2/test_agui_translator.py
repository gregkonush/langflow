"""Unit tests for the AG-UI translator.

The translator consumes Langflow ``EventManager`` events and emits AG-UI
protocol events. These tests feed real ``EventManager`` event payloads and
assert on the emitted AG-UI event objects. No mocks: the translator is a pure,
stateful transformation with no I/O.
"""

from __future__ import annotations

from ag_ui.core import (
    RunErrorEvent,
    RunFinishedEvent,
    RunStartedEvent,
    StateDeltaEvent,
    StateSnapshotEvent,
    StepFinishedEvent,
    StepStartedEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
)
from langflow.api.v2.agui_translator import AGUITranslator


def test_run_lifecycle_emits_started_and_finished():
    t = AGUITranslator(run_id="r1", thread_id="t1")

    started = t.start()
    ended = t.translate("end", {})

    assert isinstance(started[0], RunStartedEvent)
    assert started[0].run_id == "r1"
    assert started[0].thread_id == "t1"
    assert isinstance(ended[0], RunFinishedEvent)
    assert ended[0].run_id == "r1"
    assert ended[0].thread_id == "t1"


def test_error_emits_run_error():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    out = t.translate("error", {"error": "boom"})

    assert isinstance(out[0], RunErrorEvent)
    assert "boom" in out[0].message


def test_error_reads_text_from_error_message_payload():
    """``error`` can carry a full ErrorMessage dump whose reason is in ``text``."""
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    out = t.translate("error", {"text": "component blew up", "category": "error"})

    assert isinstance(out[0], RunErrorEvent)
    assert "component blew up" in out[0].message


def test_token_sequence_emits_start_contents_then_end_on_boundary():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    first = t.translate("token", {"chunk": "Hel", "id": "m1"})
    second = t.translate("token", {"chunk": "lo", "id": "m1"})
    ended = t.translate("end", {})

    # First token opens the message and carries its delta.
    assert isinstance(first[0], TextMessageStartEvent)
    assert first[0].message_id == "m1"
    assert isinstance(first[1], TextMessageContentEvent)
    assert first[1].message_id == "m1"
    assert first[1].delta == "Hel"

    # Subsequent tokens are content only, same message id.
    assert len(second) == 1
    assert isinstance(second[0], TextMessageContentEvent)
    assert second[0].message_id == "m1"
    assert second[0].delta == "lo"

    # The end boundary closes the open message before finishing the run.
    assert isinstance(ended[0], TextMessageEndEvent)
    assert ended[0].message_id == "m1"
    assert isinstance(ended[1], RunFinishedEvent)


def test_new_message_id_closes_previous_message_and_opens_new():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    t.translate("token", {"chunk": "a", "id": "m1"})
    out = t.translate("token", {"chunk": "b", "id": "m2"})

    assert isinstance(out[0], TextMessageEndEvent)
    assert out[0].message_id == "m1"
    assert isinstance(out[1], TextMessageStartEvent)
    assert out[1].message_id == "m2"
    assert isinstance(out[2], TextMessageContentEvent)
    assert out[2].message_id == "m2"
    assert out[2].delta == "b"


def test_error_boundary_closes_open_text_message():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    t.translate("token", {"chunk": "partial", "id": "m1"})
    out = t.translate("error", {"error": "boom"})

    assert isinstance(out[0], TextMessageEndEvent)
    assert out[0].message_id == "m1"
    assert isinstance(out[1], RunErrorEvent)


def test_interleaved_non_terminal_event_does_not_split_open_message():
    """A non-terminal event between tokens must not close the streamed message.

    Langflow's agent streams one message id for its whole response while other
    events (build_start, add_message for other messages, log) interleave. Closing
    the message on those would split it into two START/END pairs reusing an
    already-ended id, which is malformed AG-UI.
    """
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    t.translate("token", {"chunk": "Hel", "id": "m1"})
    interleaved = t.translate("build_start", {"id": "node-x"})
    more = t.translate("token", {"chunk": "lo", "id": "m1"})

    # The interleaved non-terminal event must not close the open message.
    assert all(not isinstance(e, TextMessageEndEvent) for e in interleaved)
    # The continuing token must not re-open an already-open message.
    assert all(not isinstance(e, TextMessageStartEvent) for e in more)
    assert isinstance(more[0], TextMessageContentEvent)
    assert more[0].message_id == "m1"
    assert more[0].delta == "lo"


def test_vertices_sorted_emits_state_snapshot_of_all_nodes():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    out = t.translate("vertices_sorted", {"ids": ["a"], "to_run": ["a", "b", "c"]})

    assert len(out) == 1
    assert isinstance(out[0], StateSnapshotEvent)
    nodes = out[0].snapshot["nodes"]
    # The snapshot covers the full run set (to_run), not just the first layer.
    assert set(nodes) == {"a", "b", "c"}
    for node in nodes.values():
        assert node["status"] == "pending"
        assert node["output"] is None


def test_vertices_sorted_falls_back_to_ids_when_to_run_absent():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    out = t.translate("vertices_sorted", {"ids": ["a", "b"]})

    assert isinstance(out[0], StateSnapshotEvent)
    assert set(out[0].snapshot["nodes"]) == {"a", "b"}


def test_start_establishes_empty_node_state():
    """start() must seed the state container so later node deltas always apply."""
    t = AGUITranslator(run_id="r1", thread_id="t1")

    started = t.start()

    assert isinstance(started[0], RunStartedEvent)
    snapshots = [e for e in started if isinstance(e, StateSnapshotEvent)]
    assert snapshots
    assert snapshots[0].snapshot == {"nodes": {}}


def test_build_start_for_node_emits_step_started_and_running_delta():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    out = t.translate("build_start", {"id": "node-x"})

    assert isinstance(out[0], StepStartedEvent)
    assert out[0].step_name == "node-x"
    assert isinstance(out[1], StateDeltaEvent)
    op = out[1].delta[0]
    assert op["op"] == "add"
    assert op["path"] == "/nodes/node-x"
    assert op["value"] == {"status": "running", "output": None}


def test_graph_level_build_start_with_no_id_is_noop():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    # The graph-level build_start (the /build path) carries no node id;
    # RUN_STARTED already signals the run beginning.
    assert t.translate("build_start", {}) == []


def test_end_vertex_success_emits_step_finished_and_state_delta():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    out = t.translate(
        "end_vertex",
        {"build_data": {"id": "node-x", "valid": True, "data": {"outputs": {"out": "hello"}}}},
    )

    assert isinstance(out[0], StepFinishedEvent)
    assert out[0].step_name == "node-x"
    assert isinstance(out[1], StateDeltaEvent)
    op = out[1].delta[0]
    assert op["op"] == "add"
    assert op["path"] == "/nodes/node-x"
    assert op["value"] == {"status": "success", "output": {"outputs": {"out": "hello"}}}


def test_end_vertex_failure_sets_error_status():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    out = t.translate("end_vertex", {"build_data": {"id": "node-x", "valid": False, "data": None}})

    op = out[1].delta[0]
    assert op["value"]["status"] == "error"


def test_node_state_deltas_use_add_so_they_apply_without_vertices_sorted():
    """build_start/end_vertex must not depend on vertices_sorted seeding the node.

    Per-node build events and vertices_sorted come from different Langflow
    execution paths that do not co-occur. start() establishes ``/nodes`` and node
    deltas use ``add`` on ``/nodes/{id}`` (create-or-replace), so the JSON Patch
    applies cleanly whether or not a STATE_SNAPSHOT seeded the node first.
    """
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()  # no vertices_sorted in this path

    build = t.translate("build_start", {"id": "node-x"})
    end = t.translate("end_vertex", {"build_data": {"id": "node-x", "valid": True, "data": "ok"}})

    for events in (build, end):
        delta = next(e for e in events if isinstance(e, StateDeltaEvent))
        op = delta.delta[0]
        assert op["op"] == "add"
        assert op["path"] == "/nodes/node-x"


def test_end_vertex_does_not_close_open_text_message():
    t = AGUITranslator(run_id="r1", thread_id="t1")
    t.start()

    t.translate("token", {"chunk": "Hel", "id": "m1"})
    t.translate("end_vertex", {"build_data": {"id": "node-x", "valid": True, "data": {}}})
    more = t.translate("token", {"chunk": "lo", "id": "m1"})

    assert all(not isinstance(e, TextMessageStartEvent) for e in more)
    assert isinstance(more[0], TextMessageContentEvent)
    assert more[0].message_id == "m1"
