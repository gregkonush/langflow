"""AG-UI translator.

Converts Langflow ``EventManager`` events into AG-UI protocol events. The
``EventManager`` queue is Langflow's internal event seam; this translator is the
one place that maps that vocabulary onto AG-UI, so the v2 workflows endpoint can
stream a standard AG-UI event stream.

One Langflow event may map to several AG-UI events, so ``translate`` returns a
list. The translator is stateful: one instance per run.
"""

from __future__ import annotations

from ag_ui.core import BaseEvent, RunErrorEvent, RunFinishedEvent, RunStartedEvent


class AGUITranslator:
    """Translates Langflow ``EventManager`` events into AG-UI protocol events.

    Use one instance per run. Call :meth:`start` once to open the run, then
    :meth:`translate` for each ``EventManager`` event.
    """

    def __init__(self, run_id: str, thread_id: str) -> None:
        self.run_id = run_id
        self.thread_id = thread_id

    def start(self) -> list[BaseEvent]:
        """Open the run. Emits ``RUN_STARTED``."""
        return [RunStartedEvent(run_id=self.run_id, thread_id=self.thread_id)]

    def translate(self, event_type: str, data: dict) -> list[BaseEvent]:
        """Map one ``EventManager`` event to zero or more AG-UI events."""
        if event_type == "end":
            return [RunFinishedEvent(run_id=self.run_id, thread_id=self.thread_id)]
        if event_type == "error":
            # The ``error`` payload varies by emission path: a full ErrorMessage
            # dump carries the reason in ``text``; the minimal path sends
            # ``{"error": str}``.
            message = data.get("text") or data.get("error") or "Unknown error"
            return [RunErrorEvent(message=str(message))]
        return []
