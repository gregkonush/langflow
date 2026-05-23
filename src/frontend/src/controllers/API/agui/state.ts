/**
 * Canvas-state reducer for AG-UI `STATE_SNAPSHOT` and `STATE_DELTA` events.
 *
 * The backend's `AGUITranslator` seeds the run with an empty
 * `{nodes: {}}` snapshot, then drives per-vertex updates as `STATE_DELTA`s
 * carrying `add` ops on `/nodes/{id}`. This reducer applies those updates to
 * produce the node-state map the canvas renders. Pure — no React, no store.
 */

import { type BaseEvent, EventType } from "@ag-ui/client";

/** Lifecycle status reported for a node by the run. */
export type CanvasNodeStatus = "pending" | "running" | "success" | "error";

/** State of a single node on the canvas during/after a run. */
export interface CanvasNodeState {
  status: CanvasNodeStatus;
  output: unknown;
}

/** Top-level run state the canvas/playground consumes. */
export interface CanvasState {
  nodes: Record<string, CanvasNodeState>;
}

/** Initial state at the start of a run, before any events arrive. */
export const INITIAL_CANVAS_STATE: CanvasState = { nodes: {} };

interface JsonPatchOp {
  op: string;
  path: string;
  value?: unknown;
}

const NODE_PATH = /^\/nodes\/([^/]+)$/;

/**
 * Apply one RFC 6902 op to the canvas state. Supports only the shapes the
 * Langflow translator emits today: `add`/`replace` on `/nodes/{id}`. Anything
 * else is ignored (rather than throwing) so an unexpected payload cannot
 * crash the run.
 */
function applyOp(state: CanvasState, op: JsonPatchOp): CanvasState {
  const match = NODE_PATH.exec(op.path);
  if (!match) return state;
  if (op.op !== "add" && op.op !== "replace") return state;
  const nodeId = match[1];
  return {
    ...state,
    nodes: { ...state.nodes, [nodeId]: op.value as CanvasNodeState },
  };
}

/**
 * Reduce one AG-UI event into the canvas state.
 *
 * `STATE_SNAPSHOT` replaces the entire state with its `snapshot`. `STATE_DELTA`
 * applies each op in order. Every other event leaves the state untouched.
 */
export function applyAGUIStateEvent(
  state: CanvasState,
  event: BaseEvent,
): CanvasState {
  if (event.type === EventType.STATE_SNAPSHOT) {
    const snapshot = (event as unknown as { snapshot: unknown }).snapshot;
    if (
      snapshot &&
      typeof snapshot === "object" &&
      "nodes" in (snapshot as Record<string, unknown>) &&
      typeof (snapshot as { nodes: unknown }).nodes === "object"
    ) {
      return snapshot as CanvasState;
    }
    return state;
  }
  if (event.type === EventType.STATE_DELTA) {
    const ops = (event as unknown as { delta: JsonPatchOp[] }).delta ?? [];
    return ops.reduce(applyOp, state);
  }
  return state;
}
