"""Unit tests for the AG-UI translator.

The translator consumes Langflow ``EventManager`` events and emits AG-UI
protocol events. These tests feed real ``EventManager`` event payloads and
assert on the emitted AG-UI event objects. No mocks: the translator is a pure,
stateful transformation with no I/O.
"""

from __future__ import annotations

from ag_ui.core import RunErrorEvent, RunFinishedEvent, RunStartedEvent
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
