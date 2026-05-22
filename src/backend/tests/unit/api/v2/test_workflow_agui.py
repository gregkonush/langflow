"""V2 Workflow endpoint tests for the AG-UI request contract.

The v2 ``POST /workflows`` endpoint accepts a strict AG-UI ``RunAgentInput`` body.
These tests exercise the real endpoint with real flows and a real database. No
mocks: the request body, the auth, and the graph run are all genuine.

Execution mode is carried in ``forwardedProps.mode``:
    - ``sync``       -> run inline, return the aggregated WorkflowExecutionResponse
    - ``background`` -> queue a job, return a WorkflowJobResponse
    - ``stream``     -> SSE (added in a later task)
"""

import json
from uuid import uuid4

import pytest
from ag_ui.core import RunAgentInput, UserMessage
from httpx import AsyncClient
from langflow.services.database.models.flow.model import Flow
from lfx.services.deps import session_scope


def _agui_body(flow_id, *, message: str = "hello", mode: str = "sync", tweaks: dict | None = None) -> dict:
    """Build an AG-UI RunAgentInput JSON body (camelCase wire shape)."""
    forwarded: dict = {"flow_id": str(flow_id), "mode": mode}
    if tweaks:
        forwarded["tweaks"] = tweaks
    return RunAgentInput(
        thread_id="thread-1",
        run_id="run-1",
        state={},
        messages=[UserMessage(id="u1", content=message)] if message else [],
        tools=[],
        context=[],
        forwarded_props=forwarded,
    ).model_dump(by_alias=True)


@pytest.fixture
async def empty_flow(created_api_key):
    """Create a real empty flow owned by the API-key user; clean it up after."""
    flow_id = uuid4()
    async with session_scope() as session:
        flow = Flow(
            id=flow_id,
            name="AG-UI Test Flow",
            description="Empty flow for AG-UI endpoint tests",
            data={"nodes": [], "edges": []},
            user_id=created_api_key.user_id,
        )
        session.add(flow)
        await session.flush()
        await session.refresh(flow)
    yield flow_id
    async with session_scope() as session:
        flow = await session.get(Flow, flow_id)
        if flow:
            await session.delete(flow)


@pytest.fixture
async def chatbot_flow(created_api_key, json_memory_chatbot_no_llm):
    """Create a real no-LLM chatbot flow (ChatInput -> Prompt/Memory -> ChatOutput)."""
    raw = json.loads(json_memory_chatbot_no_llm)
    flow_id = uuid4()
    async with session_scope() as session:
        flow = Flow(
            id=flow_id,
            name="AG-UI Chatbot Flow",
            description="No-LLM chatbot flow for AG-UI endpoint tests",
            data=raw.get("data", raw),
            user_id=created_api_key.user_id,
        )
        session.add(flow)
        await session.flush()
    yield flow_id
    async with session_scope() as session:
        flow = await session.get(Flow, flow_id)
        if flow:
            await session.delete(flow)


class TestAGUIRequestContract:
    """The endpoint accepts the AG-UI RunAgentInput body shape."""

    async def test_sync_mode_with_real_flow_returns_200(
        self,
        client: AsyncClient,
        created_api_key,
        empty_flow,
    ):
        """A RunAgentInput body with mode=sync runs the flow inline and returns 200."""
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body(empty_flow, mode="sync"),
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["flow_id"] == str(empty_flow)
        assert "job_id" in result
        assert isinstance(result["outputs"], dict)

    async def test_unknown_flow_returns_404(
        self,
        client: AsyncClient,
        created_api_key,
    ):
        """An AG-UI body whose forwardedProps.flow_id does not exist returns 404."""
        missing = "550e8400-e29b-41d4-a716-446655440000"
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body(missing, mode="sync"),
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "FLOW_NOT_FOUND"

    async def test_missing_flow_id_in_forwarded_props_returns_404(
        self,
        client: AsyncClient,
        created_api_key,
    ):
        """A RunAgentInput with no flow_id in forwardedProps cannot resolve a flow."""
        body = RunAgentInput(
            thread_id="t",
            run_id="r",
            state={},
            messages=[],
            tools=[],
            context=[],
            forwarded_props={"mode": "sync"},
        ).model_dump(by_alias=True)
        response = await client.post(
            "api/v2/workflows",
            json=body,
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 404

    async def test_requires_authentication(
        self,
        client: AsyncClient,
        empty_flow,
    ):
        """An AG-UI request with no API key and no session token is rejected."""
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body(empty_flow, mode="sync"),
        )

        assert response.status_code == 403

    async def test_accepts_session_token_auth(
        self,
        client: AsyncClient,
        logged_in_headers,
    ):
        """The endpoint accepts a session token, not only an API key."""
        missing = "550e8400-e29b-41d4-a716-446655440000"
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body(missing, mode="sync"),
            headers=logged_in_headers,
        )

        # Auth passes via the session token; 404 only because the flow does not exist.
        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "FLOW_NOT_FOUND"

    async def test_rejects_non_agui_body(
        self,
        client: AsyncClient,
        created_api_key,
    ):
        """The old flat {flow_id, background, stream, inputs} body is no longer valid."""
        response = await client.post(
            "api/v2/workflows",
            json={"flow_id": str(uuid4()), "background": False, "stream": False, "inputs": None},
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 422


class TestAGUIModeDispatch:
    """forwardedProps.mode selects the execution path."""

    async def test_background_mode_returns_job_response(
        self,
        client: AsyncClient,
        created_api_key,
        empty_flow,
    ):
        """mode=background queues a job and returns a job id."""
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body(empty_flow, mode="background"),
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["flow_id"] == str(empty_flow)
        assert result["job_id"]
        assert result["status"] in {"queued", "in_progress", "completed"}

    async def test_stream_mode_is_default(
        self,
        client: AsyncClient,
        created_api_key,
        empty_flow,
    ):
        """Omitting mode defaults to stream: a text/event-stream response."""
        body = _agui_body(empty_flow)
        body["forwardedProps"].pop("mode")
        response = await client.post(
            "api/v2/workflows",
            json=body,
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]


class TestAGUIStreaming:
    """mode=stream returns an AG-UI server-sent event stream."""

    async def test_stream_emits_run_lifecycle_events(
        self,
        client: AsyncClient,
        created_api_key,
        empty_flow,
    ):
        """A streamed run brackets its events with RUN_STARTED and RUN_FINISHED."""
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body(empty_flow, mode="stream"),
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        body = response.text
        assert "RUN_STARTED" in body
        assert "RUN_FINISHED" in body

    async def test_stream_unknown_flow_returns_404(
        self,
        client: AsyncClient,
        created_api_key,
    ):
        """A stream-mode request for a missing flow fails before streaming starts."""
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body("550e8400-e29b-41d4-a716-446655440000", mode="stream"),
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "FLOW_NOT_FOUND"

    async def test_stream_real_flow_runs_without_error(
        self,
        client: AsyncClient,
        created_api_key,
        json_memory_chatbot_no_llm,
    ):
        """Streaming a real no-LLM chatbot flow runs the graph end-to-end with no RUN_ERROR."""
        raw = json.loads(json_memory_chatbot_no_llm)
        flow_data = raw.get("data", raw)
        flow_id = uuid4()
        async with session_scope() as session:
            flow = Flow(
                id=flow_id,
                name="AG-UI Memory Chatbot Flow",
                data=flow_data,
                user_id=created_api_key.user_id,
            )
            session.add(flow)
            await session.flush()

        try:
            response = await client.post(
                "api/v2/workflows",
                json=_agui_body(flow_id, message="hello from agui", mode="stream"),
                headers={"x-api-key": created_api_key.api_key},
            )

            assert response.status_code == 200
            body = response.text
            assert "RUN_STARTED" in body
            assert "RUN_FINISHED" in body
            assert "RUN_ERROR" not in body
            # The ChatOutput component's message reached the stream as AG-UI
            # text-message events, proving the real event pipeline works.
            assert "TEXT_MESSAGE_START" in body
            assert "TEXT_MESSAGE_CONTENT" in body
        finally:
            async with session_scope() as session:
                flow = await session.get(Flow, flow_id)
                if flow:
                    await session.delete(flow)


class TestAGUISyncExecution:
    """mode=sync runs the flow inline and folds outputs into the response."""

    async def test_sync_real_flow_returns_completed_with_outputs(
        self,
        client: AsyncClient,
        created_api_key,
        chatbot_flow,
    ):
        """A sync run of a real chatbot flow completes with the terminal outputs."""
        response = await client.post(
            "api/v2/workflows",
            json=_agui_body(chatbot_flow, message="hello from agui", mode="sync"),
            headers={"x-api-key": created_api_key.api_key},
        )

        assert response.status_code == 200
        result = response.json()
        assert result["status"] == "completed"
        assert result["errors"] == []
        assert isinstance(result["outputs"], dict)
        # The chatbot flow produced at least one terminal output.
        assert result["outputs"]
