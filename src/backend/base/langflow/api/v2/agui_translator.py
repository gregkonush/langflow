"""AG-UI translator.

Converts Langflow ``EventManager`` events into AG-UI protocol events. The
``EventManager`` queue is Langflow's internal event seam; this translator is the
one place that maps that vocabulary onto AG-UI, so the v2 workflows endpoint can
stream a standard AG-UI event stream.

One Langflow event may map to several AG-UI events, so ``translate`` returns a
list. The translator is stateful: one instance per run.
"""

from __future__ import annotations

from ag_ui.core import (
    BaseEvent,
    RunErrorEvent,
    RunFinishedEvent,
    RunStartedEvent,
    StateSnapshotEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
)


class AGUITranslator:
    """Translates Langflow ``EventManager`` events into AG-UI protocol events.

    Use one instance per run. Call :meth:`start` once to open the run, then
    :meth:`translate` for each ``EventManager`` event.
    """

    def __init__(self, run_id: str, thread_id: str) -> None:
        self.run_id = run_id
        self.thread_id = thread_id
        # Id of the text message currently being streamed by ``token`` events,
        # or ``None`` when no message is open.
        self._open_message_id: str | None = None

    def start(self) -> list[BaseEvent]:
        """Open the run. Emits ``RUN_STARTED``."""
        return [RunStartedEvent(run_id=self.run_id, thread_id=self.thread_id)]

    def translate(self, event_type: str, data: dict) -> list[BaseEvent]:
        """Map one ``EventManager`` event to zero or more AG-UI events."""
        if event_type == "token":
            return self._translate_token(data)
        if event_type == "vertices_sorted":
            return self._translate_vertices_sorted(data)

        # Only terminal events close an open text message. Non-terminal events
        # (build_start, end_vertex, log, ...) interleave with tokens of the same
        # streamed message and must stay transparent, or the message would be
        # split into multiple START/END pairs reusing an already-ended id.
        if event_type == "end":
            events = self._close_open_message()
            events.append(RunFinishedEvent(run_id=self.run_id, thread_id=self.thread_id))
            return events
        if event_type == "error":
            events = self._close_open_message()
            # The ``error`` payload varies by emission path: a full ErrorMessage
            # dump carries the reason in ``text``; the minimal path sends
            # ``{"error": str}``.
            message = data.get("text") or data.get("error") or "Unknown error"
            events.append(RunErrorEvent(message=str(message)))
            return events
        return []

    def _translate_token(self, data: dict) -> list[BaseEvent]:
        """Map a ``token`` event to text-message events.

        The first token of a message opens it with ``TEXT_MESSAGE_START``; a
        token for a different message id closes the previous one first.
        """
        message_id = str(data.get("id", ""))
        chunk = data.get("chunk", "")
        events: list[BaseEvent] = []
        if self._open_message_id != message_id:
            events.extend(self._close_open_message())
            events.append(TextMessageStartEvent(message_id=message_id, role="assistant"))
            self._open_message_id = message_id
        events.append(TextMessageContentEvent(message_id=message_id, delta=chunk))
        return events

    def _translate_vertices_sorted(self, data: dict) -> list[BaseEvent]:
        """Map ``vertices_sorted`` to a ``STATE_SNAPSHOT`` of the node graph.

        Seeds every node that will run with ``pending`` status so the canvas can
        render the graph before execution begins. ``to_run`` is the full run set;
        ``ids`` (the first layer only) is the fallback.
        """
        node_ids = data.get("to_run") or data.get("ids") or []
        snapshot = {"nodes": {node_id: {"status": "pending", "output": None} for node_id in node_ids}}
        return [StateSnapshotEvent(snapshot=snapshot)]

    def _close_open_message(self) -> list[BaseEvent]:
        """Emit ``TEXT_MESSAGE_END`` for the open message, if any."""
        if self._open_message_id is None:
            return []
        end = TextMessageEndEvent(message_id=self._open_message_id)
        self._open_message_id = None
        return [end]
