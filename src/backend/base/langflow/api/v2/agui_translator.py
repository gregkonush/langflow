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
    StateDeltaEvent,
    StateSnapshotEvent,
    StepFinishedEvent,
    StepStartedEvent,
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
        """Open the run.

        Emits ``RUN_STARTED`` and an empty node-graph ``STATE_SNAPSHOT``. The
        snapshot establishes ``/nodes`` so every later node ``STATE_DELTA`` has a
        parent to patch, regardless of which execution path drives the run.
        """
        return [
            RunStartedEvent(run_id=self.run_id, thread_id=self.thread_id),
            StateSnapshotEvent(snapshot={"nodes": {}}),
        ]

    def translate(self, event_type: str, data: dict) -> list[BaseEvent]:
        """Map one ``EventManager`` event to zero or more AG-UI events."""
        if event_type == "token":
            return self._translate_token(data)
        if event_type == "vertices_sorted":
            return self._translate_vertices_sorted(data)
        if event_type == "build_start":
            return self._translate_build_start(data)
        if event_type == "end_vertex":
            return self._translate_end_vertex(data)

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

    def _translate_build_start(self, data: dict) -> list[BaseEvent]:
        """Map a per-node ``build_start`` to a ``STEP_STARTED`` + a running ``STATE_DELTA``.

        The graph-level ``build_start`` (the ``/build`` path) carries no ``id`` and
        is a no-op here: ``RUN_STARTED`` already signals the run beginning.
        """
        node_id = data.get("id")
        if not node_id:
            return []
        return [
            StepStartedEvent(step_name=node_id),
            StateDeltaEvent(delta=[self._set_node(node_id, "running", None)]),
        ]

    def _translate_end_vertex(self, data: dict) -> list[BaseEvent]:
        """Map ``end_vertex`` to a ``STEP_FINISHED`` + a ``STATE_DELTA`` for status and output."""
        build_data = data.get("build_data") or {}
        node_id = build_data.get("id")
        if not node_id:
            return []
        status = "success" if build_data.get("valid") else "error"
        return [
            StepFinishedEvent(step_name=node_id),
            StateDeltaEvent(delta=[self._set_node(node_id, status, build_data.get("data"))]),
        ]

    @staticmethod
    def _set_node(node_id: str, status: str, output: object) -> dict:
        """Build the RFC 6902 op that writes a node's state.

        ``add`` on ``/nodes/{id}`` is create-or-replace: it applies whether or not
        the node was pre-seeded by a ``vertices_sorted`` snapshot, so the
        translator does not depend on event ordering across execution paths.
        """
        return {"op": "add", "path": f"/nodes/{node_id}", "value": {"status": status, "output": output}}

    def _close_open_message(self) -> list[BaseEvent]:
        """Emit ``TEXT_MESSAGE_END`` for the open message, if any."""
        if self._open_message_id is None:
            return []
        end = TextMessageEndEvent(message_id=self._open_message_id)
        self._open_message_id = None
        return [end]
