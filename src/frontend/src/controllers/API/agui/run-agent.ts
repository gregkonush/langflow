/**
 * AG-UI workflow run service.
 *
 * Wraps `@ag-ui/client`'s `HttpAgent` for the v2 workflows endpoint. Frontend
 * code builds a `RunAgentInput` via `buildRunInput`, then runs it through the
 * agent returned by `createWorkflowAgent` to receive a typed AG-UI event
 * stream.
 */

import { HttpAgent, type RunAgentInput } from "@ag-ui/client";

/** v2 workflows endpoint mode carried on `forwardedProps.mode`. */
export type WorkflowMode = "stream" | "sync" | "background";

/** Options describing one Langflow workflow run. */
export interface WorkflowRunOptions {
  /** The Langflow flow id to run. Required by the v2 endpoint. */
  flowId: string;
  /** User chat input (last user message) sent on the run. */
  message?: string;
  /** Component-keyed parameter overrides, e.g. `{ChatInput-abc: {input_value: "..."}}`. */
  tweaks?: Record<string, Record<string, unknown>>;
  /** Execution mode; defaults to `stream`. */
  mode?: WorkflowMode;
  /** Thread id (maps to the v2 endpoint's `session_id`). */
  threadId?: string;
  /** Run id; defaults to a fresh uuid per run. */
  runId?: string;
  /** Optional partial-run start vertex id. */
  startComponentId?: string;
  /** Optional partial-run stop vertex id. */
  stopComponentId?: string;
  /** Current flow data (nodes + edges) to run; falls back to the DB copy if omitted. */
  flowData?: { nodes: unknown[]; edges: unknown[] };
  /** Runtime file references the graph build needs (e.g. uploaded file paths). */
  files?: string[];
}

/** The v2 workflows endpoint path. */
export const WORKFLOWS_ENDPOINT = "/api/v2/workflows";

function uuid(): string {
  if (
    typeof crypto !== "undefined" &&
    typeof crypto.randomUUID === "function"
  ) {
    return crypto.randomUUID();
  }
  // Fallback for environments without crypto.randomUUID (e.g. older jest).
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

/**
 * Build a strict AG-UI `RunAgentInput` for a Langflow workflow run.
 *
 * Langflow-specific fields ride on `forwardedProps`: `flow_id`, `mode`, optional
 * `tweaks`, `start_component_id`, `stop_component_id`. The chat input becomes
 * a single user message; an empty `message` yields no messages.
 */
export function buildRunInput(opts: WorkflowRunOptions): RunAgentInput {
  const forwardedProps: Record<string, unknown> = {
    flow_id: opts.flowId,
    mode: opts.mode ?? "stream",
  };
  if (opts.tweaks) forwardedProps.tweaks = opts.tweaks;
  if (opts.startComponentId)
    forwardedProps.start_component_id = opts.startComponentId;
  if (opts.stopComponentId)
    forwardedProps.stop_component_id = opts.stopComponentId;
  if (opts.flowData) forwardedProps.data = opts.flowData;
  if (opts.files && opts.files.length > 0) forwardedProps.files = opts.files;

  const messages = opts.message
    ? [{ id: uuid(), role: "user" as const, content: opts.message }]
    : [];

  return {
    threadId: opts.threadId ?? uuid(),
    runId: opts.runId ?? uuid(),
    state: {},
    messages,
    tools: [],
    context: [],
    forwardedProps,
  };
}

/** Construction options for the workflow agent. */
export interface WorkflowAgentOptions {
  /** Override the endpoint URL (defaults to `/api/v2/workflows`). */
  url?: string;
  /** Extra headers; cookies and `fetch-intercept`'d headers are sent automatically. */
  headers?: Record<string, string>;
  /** Initial thread id; can be set per-run via `buildRunInput` instead. */
  threadId?: string;
}

/**
 * Create an `HttpAgent` preconfigured for the v2 workflows endpoint.
 *
 * Auth is carried by the browser: same-origin cookies are sent automatically,
 * and Langflow's `fetch-intercept` registration adds any custom headers.
 */
export function createWorkflowAgent(
  opts: WorkflowAgentOptions = {},
): HttpAgent {
  return new HttpAgent({
    url: opts.url ?? WORKFLOWS_ENDPOINT,
    headers: opts.headers,
    threadId: opts.threadId,
  });
}
