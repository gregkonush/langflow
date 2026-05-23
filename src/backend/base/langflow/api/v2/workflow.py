"""V2 Workflow execution endpoints.

This module implements the V2 Workflow API endpoints for executing flows with
enhanced error handling, timeout protection, and structured responses.

Endpoints:
    POST /workflow: Execute a workflow (sync, stream, or background modes)
    GET /workflow: Get workflow job status by job_id
    POST /workflow/stop: Stop a running workflow execution

Features:
    - Comprehensive error handling with structured error responses
    - Timeout protection for long-running executions
    - Support for multiple execution modes (sync, stream, background)
    - Session-cookie or API-key authentication

Configuration:
    EXECUTION_TIMEOUT: Maximum execution time for synchronous workflows (300 seconds)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator
from copy import deepcopy
from typing import Annotated
from uuid import UUID, uuid4

from ag_ui.core import RunAgentInput
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from fastapi.responses import EventSourceResponse, StreamingResponse
from fastapi.sse import format_sse_event
from lfx.events.event_manager import create_default_event_manager
from lfx.graph.graph.base import Graph
from lfx.schema.schema import InputValueRequest
from lfx.schema.workflow import (
    WORKFLOW_EXECUTION_RESPONSES,
    WORKFLOW_STATUS_RESPONSES,
    JobId,
    JobStatus,
    WorkflowExecutionResponse,
    WorkflowJobResponse,
    WorkflowStopRequest,
    WorkflowStopResponse,
)
from lfx.services.deps import injectable_session_scope_readonly
from pydantic_core import ValidationError as PydanticValidationError
from sqlalchemy.exc import OperationalError

from langflow.api.build import generate_flow_events
from langflow.api.utils import extract_global_variables_from_headers
from langflow.api.v1.schemas import FlowDataRequest, RunResponse
from langflow.api.v2.agui_translator import AGUITranslator
from langflow.api.v2.converters import (
    ParsedWorkflowRun,
    create_error_response,
    parse_run_agent_input,
    run_response_to_workflow_response,
)
from langflow.api.v2.workflow_reconstruction import reconstruct_workflow_response_from_job_id
from langflow.exceptions.api import (
    WorkflowQueueFullError,
    WorkflowResourceError,
    WorkflowServiceUnavailableError,
    WorkflowTimeoutError,
    WorkflowValidationError,
)
from langflow.helpers.flow import get_flow_by_id_or_endpoint_name
from langflow.processing.process import process_tweaks, run_graph_internal
from langflow.services.auth.utils import get_current_user_for_workflow
from langflow.services.database.models.flow.model import FlowRead
from langflow.services.database.models.jobs.model import JobType
from langflow.services.database.models.user.model import UserRead
from langflow.services.deps import get_job_service, get_queue_service, get_task_service

# Configuration constants
EXECUTION_TIMEOUT = 300  # 5 minutes default timeout for sync execution


router = APIRouter(prefix="/workflows", tags=["Workflow"])


def _build_run_inputs(parsed: ParsedWorkflowRun) -> list[InputValueRequest] | None:
    """Build the graph input list from the AG-UI chat message, if any.

    The last user message becomes a single chat input; an empty message means
    the flow runs with no chat input (parameters arrive via tweaks instead).
    """
    if not parsed.input_value:
        return None
    return [InputValueRequest(components=[], input_value=parsed.input_value, type="chat")]


@router.post(
    "",
    response_model=None,
    response_model_exclude_none=True,
    responses=WORKFLOW_EXECUTION_RESPONSES,
    summary="Execute Workflow",
    description="Execute a workflow with support for sync, stream, and background modes",
)
async def execute_workflow(
    run_input: RunAgentInput,
    background_tasks: BackgroundTasks,
    http_request: Request,
    current_user: Annotated[UserRead, Depends(get_current_user_for_workflow)],
) -> WorkflowExecutionResponse | WorkflowJobResponse | StreamingResponse:
    """Execute a workflow from a strict AG-UI ``RunAgentInput`` body.

    The execution mode is carried in ``forwardedProps.mode``:
        - **sync**: run inline, return the complete WorkflowExecutionResponse
        - **background**: queue a job, return a WorkflowJobResponse
        - **stream** (default): server-sent AG-UI events (not yet implemented)

    Error Handling Strategy:
        - System errors (404, 500, 503, 504): returned as HTTP error responses
        - Component execution errors: returned as HTTP 200 with errors in the body

    Args:
        run_input: The AG-UI request body.
        background_tasks: FastAPI background tasks for async operations.
        http_request: The HTTP request object for extracting headers.
        current_user: Authenticated user (session cookie or API key).

    Returns:
        - WorkflowExecutionResponse: for synchronous execution (HTTP 200)
        - WorkflowJobResponse: for background execution
        - StreamingResponse: for streaming execution

    Raises:
        HTTPException:
            - 404: Flow not found or user lacks access
            - 400: Invalid flow data or validation error
            - 500: Internal server error
            - 501: Streaming mode not yet implemented
            - 503: Database unavailable
            - 408: Execution timeout exceeded
    """
    parsed = parse_run_agent_input(run_input)
    job_id = uuid4()

    if not parsed.flow_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Flow not found",
                "code": "FLOW_NOT_FOUND",
                "message": "No flow_id provided in forwardedProps.",
            },
        )

    try:
        # Validate flow exists and user has permission
        flow = await get_flow_by_id_or_endpoint_name(parsed.flow_id, current_user.id)

        # Background mode execution
        if parsed.mode == "background":
            return await execute_workflow_background(
                parsed=parsed,
                flow=flow,
                job_id=job_id,
                current_user=current_user,
                http_request=http_request,
            )

        # Synchronous execution
        if parsed.mode == "sync":
            return await execute_sync_workflow_with_timeout(
                parsed=parsed,
                flow=flow,
                job_id=job_id,
                current_user=current_user,
                background_tasks=background_tasks,
                http_request=http_request,
            )

        # Streaming mode (default)
        return _execute_streaming_workflow(
            parsed=parsed,
            flow=flow,
            job_id=job_id,
            current_user=current_user,
            background_tasks=background_tasks,
        )

    except HTTPException as e:
        # Reformat 404 from get_flow_by_id_or_endpoint_name to structured format
        if e.status_code == status.HTTP_404_NOT_FOUND:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "Flow not found",
                    "code": "FLOW_NOT_FOUND",
                    "message": f"Flow '{parsed.flow_id}' does not exist. Verify the flow_id and try again.",
                    "flow_id": parsed.flow_id,
                },
            ) from e
        raise
    except OperationalError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Service unavailable, Please try again.",
                "code": "DATABASE_ERROR",
                "message": f"Failed to fetch flow: {e!s}",
                "flow_id": parsed.flow_id,
            },
        ) from e
    except WorkflowTimeoutError:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail={
                "error": "Execution timeout",
                "code": "EXECUTION_TIMEOUT",
                "message": f"Workflow execution exceeded {EXECUTION_TIMEOUT} seconds",
                "job_id": str(job_id),
                "flow_id": str(parsed.flow_id),
                "timeout_seconds": EXECUTION_TIMEOUT,
            },
        ) from None
    except (PydanticValidationError, WorkflowValidationError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Workflow validation error",
                "code": "INVALID_FLOW_DATA",
                "message": str(e),
                "flow_id": parsed.flow_id,
            },
        ) from e
    except WorkflowServiceUnavailableError as err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Service unavailable",
                "code": "QUEUE_SERVICE_UNAVAILABLE",
                "message": str(err),
                "flow_id": parsed.flow_id,
            },
        ) from err
    except (WorkflowResourceError, WorkflowQueueFullError, MemoryError) as err:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Service busy",
                "code": "SERVICE_BUSY",
                "message": "The service is currently unable to handle the request due to resource limits.",
                "flow_id": parsed.flow_id,
            },
        ) from err
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "code": "INTERNAL_SERVER_ERROR",
                "message": f"An unexpected error occurred: {err!s}",
                "flow_id": parsed.flow_id,
            },
        ) from err


async def execute_sync_workflow_with_timeout(
    parsed: ParsedWorkflowRun,
    flow: FlowRead,
    job_id: UUID,
    current_user: UserRead,
    background_tasks: BackgroundTasks,
    http_request: Request,
) -> WorkflowExecutionResponse:
    """Execute workflow with timeout protection.

    Args:
        parsed: The parsed AG-UI run parameters
        flow: The flow to execute
        job_id: Generated job ID for tracking
        current_user: Authenticated user
        background_tasks: FastAPI background tasks
        http_request: The HTTP request object for extracting headers

    Returns:
        WorkflowExecutionResponse with complete results

    Raises:
        WorkflowTimeoutError: If execution exceeds timeout
        WorkflowValidationError: If flow validation fails
    """
    try:
        return await asyncio.wait_for(
            execute_sync_workflow(
                parsed=parsed,
                flow=flow,
                job_id=job_id,
                current_user=current_user,
                background_tasks=background_tasks,
                http_request=http_request,
            ),
            timeout=EXECUTION_TIMEOUT,
        )
    except asyncio.TimeoutError as e:
        raise WorkflowTimeoutError from e


async def execute_sync_workflow(
    parsed: ParsedWorkflowRun,
    flow: FlowRead,
    job_id: UUID,
    current_user: UserRead,
    background_tasks: BackgroundTasks,  # noqa: ARG001
    http_request: Request,
) -> WorkflowExecutionResponse:
    """Execute workflow synchronously and return complete results.

    This function implements a two-tier error handling strategy:
        1. System-level errors (validation, graph build): Raised as exceptions
        2. Component execution errors: Returned in response body with HTTP 200

    This approach allows clients to receive partial results even when some
    components fail, which is useful for debugging and incremental processing.

    Execution Flow:
        1. Apply tweaks and chat input from the parsed AG-UI request
        2. Validate flow data exists
        3. Extract context from HTTP headers
        4. Build graph from flow data with tweaks applied
        5. Identify terminal nodes for execution
        6. Execute graph and collect results
        7. Convert V1 RunResponse to V2 WorkflowExecutionResponse

    Args:
        parsed: The parsed AG-UI run parameters with tweaks and chat input
        flow: The flow model from database
        job_id: Generated job ID for tracking this execution
        current_user: Authenticated user for permission checks
        background_tasks: FastAPI background tasks (unused in sync mode)
        http_request: The HTTP request object for extracting headers

    Returns:
        WorkflowExecutionResponse: Complete execution results with outputs and metadata

    Raises:
        WorkflowValidationError: If flow data is None or graph build fails
    """
    # Tweaks and chat input come straight from the parsed AG-UI request
    tweaks = parsed.tweaks
    session_id = parsed.session_id

    # Validate flow data - this is a system error, not execution error
    if flow.data is None:
        msg = f"Flow {flow.id} has no data. The flow may be corrupted."
        raise WorkflowValidationError(msg)

    # Extract request-level variables from headers (similar to V1)
    # Headers with prefix X-LANGFLOW-GLOBAL-VAR-* are extracted and made available to components
    request_variables = extract_global_variables_from_headers(http_request.headers)

    # Build context from request variables (similar to V1's _run_flow_internal)
    context = {"request_variables": request_variables} if request_variables else None

    # Build graph - system error if this fails
    try:
        flow_id_str = str(flow.id)
        user_id = str(current_user.id)
        # Use deepcopy to prevent mutation of the original flow.data
        # process_tweaks modifies nested dictionaries in-place
        graph_data = deepcopy(flow.data)
        graph_data = process_tweaks(graph_data, tweaks, stream=False)
        # Pass context to graph (similar to V1's simple_run_flow)
        # This allows components to access request metadata via graph.context
        graph = Graph.from_payload(
            graph_data, flow_id=flow_id_str, user_id=user_id, flow_name=flow.name, context=context
        )
        # Set run_id for tracing/logging (similar to V1's simple_run_flow)
        graph.set_run_id(job_id)
    except Exception as e:
        msg = f"Failed to build graph from flow data: {e!s}"
        raise WorkflowValidationError(msg) from e

    # Get terminal nodes - these are the outputs we want
    terminal_node_ids = graph.get_terminal_nodes()

    # Execute graph - component errors are caught and returned in response body
    job_service = get_job_service()
    await job_service.create_job(job_id=job_id, flow_id=flow_id_str, user_id=current_user.id)
    try:
        task_result, execution_session_id = await job_service.execute_with_status(
            job_id=job_id,
            run_coro_func=run_graph_internal,
            graph=graph,
            flow_id=flow_id_str,
            session_id=session_id,
            inputs=_build_run_inputs(parsed),
            outputs=terminal_node_ids,
            stream=False,
        )

        # Build RunResponse
        run_response = RunResponse(outputs=task_result, session_id=execution_session_id)
        # Convert to WorkflowExecutionResponse
        return run_response_to_workflow_response(
            run_response=run_response,
            flow_id=parsed.flow_id,
            job_id=str(job_id),
            inputs=parsed.tweaks,
            graph=graph,
        )

    except asyncio.CancelledError:
        # Re-raise CancelledError to allow timeout mechanism to work properly
        # This ensures asyncio.wait_for() can properly cancel and raise TimeoutError
        raise
    except asyncio.TimeoutError as e:
        # Re-raise TimeoutError to allow timeout mechanism to work properly
        # This ensures asyncio.wait_for() can properly cancel and raise TimeoutError
        raise WorkflowTimeoutError from e
    except Exception as exc:  # noqa: BLE001
        # Component execution errors - return in response body with HTTP 200
        # This allows partial results and detailed error information per component
        return create_error_response(
            flow_id=parsed.flow_id,
            job_id=job_id,
            inputs=parsed.tweaks,
            error=exc,
        )


def _single_input_value_request(parsed: ParsedWorkflowRun) -> InputValueRequest | None:
    """Build the single chat InputValueRequest the v1 build loop accepts, if any.

    The v1 build path (``generate_flow_events``) takes a single
    ``InputValueRequest`` rather than a list; an empty chat message means no
    input is dispatched and parameters arrive via tweaks only.
    """
    if not parsed.input_value:
        return None
    return InputValueRequest(
        components=[],
        input_value=parsed.input_value,
        type="chat",
        session=parsed.session_id,
    )


async def _agui_event_frames(
    *,
    flow_id: UUID,
    flow_name: str | None,
    background_tasks: BackgroundTasks,
    parsed: ParsedWorkflowRun,
    current_user: UserRead,
    run_id: str,
    thread_id: str,
) -> AsyncIterator[bytes]:
    """Run a flow via the v1 build-vertex loop, translate its events to AG-UI.

    The v1 ``generate_flow_events`` drives the graph vertex-by-vertex, emitting
    ``vertices_sorted``, ``build_start``, ``end_vertex``, ``token``,
    ``add_message``, and ``end`` via the EventManager. The translator maps each
    to AG-UI events; this generator yields them as SSE frames with monotonic
    ``id:`` for ``Last-Event-ID`` resume. A failure during the run becomes a
    ``RUN_ERROR`` event rather than an HTTP error; closing the consumer cancels
    the run.
    """
    queue: asyncio.Queue = asyncio.Queue()
    event_manager = create_default_event_manager(queue)
    translator = AGUITranslator(run_id=run_id, thread_id=thread_id)
    input_request = _single_input_value_request(parsed)
    flow_data = FlowDataRequest(**parsed.data) if parsed.data else None

    async def drive() -> None:
        try:
            await generate_flow_events(
                flow_id=flow_id,
                background_tasks=background_tasks,
                event_manager=event_manager,
                inputs=input_request,
                data=flow_data,
                files=None,
                stop_component_id=parsed.stop_component_id,
                start_component_id=parsed.start_component_id,
                log_builds=False,
                current_user=current_user,
                flow_name=flow_name,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            event_manager.on_error(data={"error": str(exc)})
            with contextlib.suppress(Exception):
                await event_manager.queue.put((None, None, time.time()))
        # generate_flow_events emits on_end and the sentinel on success.

    def _frame(ag_event: object, seq: int) -> bytes:
        return format_sse_event(
            data_str=ag_event.model_dump_json(by_alias=True, exclude_none=True),
            id=str(seq),
        )

    seq = 0
    run_task = asyncio.create_task(drive())
    try:
        for ag_event in translator.start():
            yield _frame(ag_event, seq)
            seq += 1
        while True:
            _, value, _ = await queue.get()
            if value is None:
                break
            payload = json.loads(value.decode("utf-8"))
            for ag_event in translator.translate(payload.get("event", ""), payload.get("data") or {}):
                yield _frame(ag_event, seq)
                seq += 1
    finally:
        if not run_task.done():
            run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task


def _execute_streaming_workflow(
    *,
    parsed: ParsedWorkflowRun,
    flow: FlowRead,
    job_id: UUID,
    current_user: UserRead,
    background_tasks: BackgroundTasks,
) -> EventSourceResponse:
    """Run a workflow live and stream AG-UI events over server-sent events.

    The graph is built inside ``generate_flow_events`` (the v1 build-vertex
    loop) so the same per-vertex events the canvas already knows flow through
    the translator. A failure during the run becomes a ``RUN_ERROR`` event
    rather than an HTTP error.
    """
    return EventSourceResponse(
        _agui_event_frames(
            flow_id=flow.id,
            flow_name=flow.name,
            background_tasks=background_tasks,
            parsed=parsed,
            current_user=current_user,
            run_id=parsed.run_id or str(job_id),
            thread_id=parsed.session_id or str(flow.id),
        ),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class _BackgroundRun:
    """In-memory buffer of a background run's AG-UI SSE frames for re-attach.

    The buffer lives in the process; restarts drop it. Multiple readers can
    re-attach concurrently and tail until the run ends.
    """

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self.frames: list[bytes] = []
        self.done = False
        self._cond = asyncio.Condition()

    async def append(self, frame: bytes) -> None:
        async with self._cond:
            self.frames.append(frame)
            self._cond.notify_all()

    async def finish(self) -> None:
        async with self._cond:
            self.done = True
            self._cond.notify_all()

    async def replay(self, start_index: int) -> AsyncIterator[bytes]:
        """Yield buffered frames from ``start_index`` and tail until done."""
        idx = max(start_index, 0)
        while True:
            async with self._cond:
                while idx >= len(self.frames) and not self.done:
                    await self._cond.wait()
                snapshot = self.frames[idx:]
                finished = self.done
            for frame in snapshot:
                yield frame
            idx += len(snapshot)
            if finished and idx >= len(self.frames):
                return


# Process-local registry of background runs keyed by job_id, bounded by
# ``_MAX_BACKGROUND_RUNS`` (oldest evicted first). Re-attach reads this.
_MAX_BACKGROUND_RUNS = 100
_BACKGROUND_RUNS: dict[str, _BackgroundRun] = {}


def _register_background_run(job_id: str, bg_run: _BackgroundRun) -> None:
    """Register a background run, evicting the oldest entry when full."""
    if len(_BACKGROUND_RUNS) >= _MAX_BACKGROUND_RUNS:
        oldest = next(iter(_BACKGROUND_RUNS))
        _BACKGROUND_RUNS.pop(oldest, None)
    _BACKGROUND_RUNS[job_id] = bg_run


async def _buffer_background_run(
    *,
    bg_run: _BackgroundRun,
    flow: FlowRead,
    parsed: ParsedWorkflowRun,
    job_id: str,
    current_user: UserRead,
) -> None:
    """Run a background flow, buffer its AG-UI frames, and finalize job status."""
    fresh_background_tasks = BackgroundTasks()
    errored = False
    try:
        async for frame in _agui_event_frames(
            flow_id=flow.id,
            flow_name=flow.name,
            background_tasks=fresh_background_tasks,
            parsed=parsed,
            current_user=current_user,
            run_id=parsed.run_id or job_id,
            thread_id=parsed.session_id or str(flow.id),
        ):
            if b'"RUN_ERROR"' in frame:
                errored = True
            await bg_run.append(frame)
    finally:
        await bg_run.finish()
        with contextlib.suppress(Exception):
            await get_job_service().update_job_status(
                job_id,
                JobStatus.FAILED if errored else JobStatus.COMPLETED,
            )


async def execute_workflow_background(
    parsed: ParsedWorkflowRun,
    flow: FlowRead,
    job_id: JobId,
    current_user: UserRead,
    http_request: Request,  # noqa: ARG001
) -> WorkflowJobResponse:
    """Run a workflow in the background, buffering AG-UI events for re-attach.

    A job row is created so ``GET /workflows`` and ``POST /workflows/stop`` keep
    working. The buffer task is scheduled through the queue service under
    ``job_id`` so ``/stop`` can revoke it. Graph construction happens inside
    the v1 build-vertex loop driven by ``_agui_event_frames``.
    """
    try:
        flow_id_str = str(flow.id)
        job_id_str = str(job_id)

        await get_job_service().create_job(
            job_id=job_id,
            flow_id=flow_id_str,
            user_id=current_user.id,
        )

        bg_run = _BackgroundRun(user_id=str(current_user.id))
        _register_background_run(job_id_str, bg_run)

        queue_service = get_queue_service()
        queue_service.create_queue(job_id_str)
        queue_service.start_job(
            job_id_str,
            _buffer_background_run(
                bg_run=bg_run,
                flow=flow,
                parsed=parsed,
                job_id=job_id_str,
                current_user=current_user,
            ),
        )
        return WorkflowJobResponse(job_id=job_id_str, flow_id=parsed.flow_id, status=JobStatus.QUEUED)

    except (WorkflowResourceError, WorkflowServiceUnavailableError, WorkflowQueueFullError):
        raise
    except MemoryError as exc:
        raise WorkflowResourceError from exc


@router.get(
    "",
    response_model=None,
    response_model_exclude_none=True,
    responses=WORKFLOW_STATUS_RESPONSES,
    summary="Get Workflow Status",
    description="Get status of workflow job by job ID",
)
async def get_workflow_status(
    current_user: Annotated[UserRead, Depends(get_current_user_for_workflow)],
    job_id: Annotated[JobId | None, Query(description="Job ID to query")] = None,
    session: Annotated[object, Depends(injectable_session_scope_readonly)] = None,
) -> WorkflowExecutionResponse | WorkflowJobResponse:
    """Get workflow job status and results.

    Args:
        current_user: Authenticated user (session cookie or API key)
        job_id: Optional job ID to query specific job
        session: Database session for querying vertex builds

    Returns:
        WorkflowExecutionResponse or reconstructed results

    Raises:
        HTTPException:
            - 400: Job ID not provided
            - 403: Developer API disabled or unauthorized
            - 404: Job not found
            - 408: Execution timeout
            - 500: Internal server error or Job failure
    """
    if not job_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Missing required parameter",
                "code": "MISSING_PARAMETER",
                "message": "Job ID must be provided",
            },
        )

    job_service = get_job_service()
    try:
        job = await job_service.get_job_by_job_id(job_id=job_id, user_id=current_user.id)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "code": "INTERNAL_SERVER_ERROR",
                "message": f"Failed to retrieve job from database: {exc!s}",
            },
        ) from exc

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Workflow job not found",
                "code": "JOB_NOT_FOUND",
                "message": f"Workflow job {job_id} not found",
                "job_id": str(job_id),
            },
        )

    # Verify this is a workflow job
    if job.type != JobType.WORKFLOW:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Workflow job not found",
                "code": "JOB_NOT_FOUND",
                "message": f"Job {job_id} is not a workflow job (type: {job.type})",
                "job_id": str(job_id),
            },
        )

    # Store context for exception handling scope
    flow_id_str = str(job.flow_id)
    job_id_str = str(job_id)
    try:
        # If job is completed, reconstruct full workflow response from vertex_builds
        if job.status == JobStatus.COMPLETED:
            # Get the flow
            flow = await get_flow_by_id_or_endpoint_name(flow_id_str, current_user.id)

            # Reconstruct response from vertex_build table
            return await reconstruct_workflow_response_from_job_id(
                session=session,
                flow=flow,
                job_id=job_id_str,
                user_id=str(current_user.id),
            )

        if job.status == JobStatus.FAILED:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "error": "Job failed",
                    "code": "JOB_FAILED",
                    "message": f"Job {job_id_str} has failed execution.",
                    "job_id": job_id_str,
                },
            )

        if job.status == JobStatus.TIMED_OUT:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={
                    "error": "Execution timeout",
                    "code": "EXECUTION_TIMEOUT",
                    "message": "Workflow execution timed out",
                    "job_id": job_id_str,
                    "flow_id": flow_id_str,
                },
            )

        # Default response for active statuses (QUEUED, IN_PROGRESS, etc.)
        return WorkflowJobResponse(
            flow_id=flow_id_str,
            job_id=job_id_str,
            status=job.status,
        )

    except HTTPException:
        raise
    except WorkflowTimeoutError as err:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail={
                "error": "Execution timeout",
                "code": "EXECUTION_TIMEOUT",
                "message": f"Workflow execution exceeded {EXECUTION_TIMEOUT} seconds",
                "job_id": job_id_str,
                "flow_id": flow_id_str,
                "timeout_seconds": EXECUTION_TIMEOUT,
            },
        ) from err
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "code": "INTERNAL_SERVER_ERROR",
                "message": f"Failed to process job status: {exc!s}",
            },
        ) from exc


@router.post(
    "/stop",
    summary="Stop Workflow",
    description="Stop a running workflow execution",
)
async def stop_workflow(
    request: WorkflowStopRequest,
    current_user: Annotated[UserRead, Depends(get_current_user_for_workflow)],
) -> WorkflowStopResponse:
    """Stop a running workflow execution by job_id.

    This endpoint allows clients to gracefully or forcefully stop a running workflow.

    Args:
        request: Stop request containing job_id and optional force flag
        current_user: Authenticated user (session cookie or API key)

    Returns:
        WorkflowStopResponse: Confirmation of stop request with final job status

    Raises:
        HTTPException:
            - 403: Developer API disabled or unauthorized
            - 404: Job ID not found
            - 500: Internal server error
    """
    job_id = request.job_id
    job_service = get_job_service()
    task_service = get_task_service()

    try:
        # 1. Fetch Job
        job = await job_service.get_job_by_job_id(job_id, user_id=current_user.id)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "code": "INTERNAL_SERVER_ERROR",
                "message": f"Failed to retrieve job status: {exc!s}",
            },
        ) from exc

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Job not found",
                "code": "JOB_NOT_FOUND",
                "message": f"Job {job_id} not found",
                "job_id": str(job_id),
            },
        )

    # Verify this is a workflow job
    if job.type != JobType.WORKFLOW:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Job not found",
                "code": "JOB_NOT_FOUND",
                "message": f"Job {job_id} is not a workflow job (type: {job.type})",
                "job_id": str(job_id),
            },
        )

    if job.status == JobStatus.CANCELLED:
        return WorkflowStopResponse(job_id=str(job_id), message=f"Job {job_id} is already cancelled.")

    try:
        revoked = await task_service.revoke_task(job_id)
        await job_service.update_job_status(job_id, JobStatus.CANCELLED)

        message = f"Job {job_id} cancelled successfully." if revoked else f"Job {job_id} is already cancelled."
        return WorkflowStopResponse(job_id=str(job_id), message=message)
    except asyncio.CancelledError as exc:
        # Handle system-initiated cancellations that were re-raised
        # The job status has already been updated to FAILED in jobs/service.py
        message_code = exc.args[0] if exc.args else "UNKNOWN"
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Task cancellation error",
                "code": message_code,
                "message": f"Job {job_id} was cancelled unexpectedly by the system",
                "job_id": str(job_id),
            },
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "code": "INTERNAL_SERVER_ERROR",
                "message": f"Failed to stop job: {job_id} - {exc!s}",
            },
        ) from exc


@router.get(
    "/{job_id}/events",
    response_model=None,
    summary="Re-attach to a background run",
    description="Replay buffered AG-UI events for a background run and tail until it ends.",
)
async def reattach_workflow_events(
    job_id: str,
    http_request: Request,
    current_user: Annotated[UserRead, Depends(get_current_user_for_workflow)],
) -> EventSourceResponse:
    """Stream the AG-UI events of a background run, replaying from ``Last-Event-ID``.

    The buffer is process-local. Cross-user access is rejected with 404 to avoid
    leaking the existence of other users' runs.
    """
    bg_run = _BACKGROUND_RUNS.get(job_id)
    if bg_run is None or bg_run.user_id != str(current_user.id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Background run not found",
                "code": "JOB_NOT_FOUND",
                "message": f"No buffered AG-UI events for job {job_id}.",
                "job_id": job_id,
            },
        )

    last_event_id = http_request.headers.get("Last-Event-ID")
    start_index = 0
    if last_event_id:
        try:
            start_index = int(last_event_id) + 1
        except ValueError:
            start_index = 0

    return EventSourceResponse(
        bg_run.replay(start_index),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
