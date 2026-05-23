/**
 * Bridge: run a workflow through the AG-UI service and update flowStore.
 *
 * This is the Phase 4.3 keystone: while ``ENABLE_V2_WORKFLOWS_AGUI`` is on,
 * `flowStore.buildFlow` calls this function instead of the v1 build path. It
 * starts the AG-UI HttpAgent and folds each event into the existing flow-store
 * methods (`updateBuildStatus`, `addDataToFlowPool`, `setBuildInfo`,
 * `setIsBuilding`) so the canvas and "built successfully" toast keep working.
 */

import { type BaseEvent, EventType } from "@ag-ui/client";
import { handleMessageEvent } from "@/components/core/playgroundComponent/chat-view/utils/message-event-handler";
import { BuildStatus } from "@/constants/enums";
import useAlertStore from "@/stores/alertStore";
import useFlowStore from "@/stores/flowStore";
import type {
  ChatInputType,
  ChatOutputType,
  VertexBuildTypeAPI,
  VertexDataTypeAPI,
} from "@/types/api";
import {
  buildRunInput,
  createWorkflowAgent,
  type WorkflowRunOptions,
} from "./run-agent";

const AGUI_STATUS_TO_BUILD_STATUS: Record<string, BuildStatus> = {
  pending: BuildStatus.TO_BUILD,
  running: BuildStatus.BUILDING,
  success: BuildStatus.BUILT,
  error: BuildStatus.ERROR,
};

interface JsonPatchOp {
  op: string;
  path: string;
  value?: unknown;
}

interface AGUINodeState {
  status: string;
  output: VertexDataTypeAPI | null;
}

function applyStateDelta(
  ops: JsonPatchOp[],
  runId: string,
  nodeIds: Set<string>,
): void {
  const flowStore = useFlowStore.getState();
  for (const op of ops) {
    const match = /^\/nodes\/([^/]+)$/.exec(op.path);
    if (!match) continue;
    if (op.op !== "add" && op.op !== "replace") continue;
    const nodeId = match[1];
    const value = op.value as AGUINodeState | undefined;
    if (!value || typeof value !== "object") continue;

    const buildStatus =
      AGUI_STATUS_TO_BUILD_STATUS[value.status] ?? BuildStatus.BUILDING;
    flowStore.updateBuildStatus([nodeId], buildStatus);
    nodeIds.add(nodeId);

    // Only the final per-vertex emission carries the result data; running
    // states have ``output: null`` and contribute no flow-pool entry.
    if (value.output) {
      const entry: VertexBuildTypeAPI = {
        id: nodeId,
        inactivated_vertices: null,
        next_vertices_ids: [],
        top_level_vertices: [],
        run_id: runId,
        valid: value.status === "success",
        data: value.output,
        timestamp: new Date().toISOString(),
        params: null,
        messages: [] as ChatOutputType[] | ChatInputType[],
        artifacts: null,
      };
      flowStore.addDataToFlowPool(entry, nodeId);
    }
  }
}

/**
 * Run a workflow through the AG-UI service and update flowStore as events
 * arrive. Resolves when the run ends (RUN_FINISHED or RUN_ERROR) or the
 * underlying observable errors.
 */
export async function runFlowAGUI(opts: WorkflowRunOptions): Promise<void> {
  const input = buildRunInput(opts);
  const agent = createWorkflowAgent();
  const flowStore = useFlowStore.getState();
  const setErrorData = useAlertStore.getState().setErrorData;
  const touchedNodeIds = new Set<string>();

  return new Promise<void>((resolve) => {
    const finish = () => {
      flowStore.updateEdgesRunningByNodes([...touchedNodeIds], false);
      flowStore.setIsBuilding(false);
      flowStore.revertBuiltStatusFromBuilding();
      resolve();
    };

    const subscription = agent.run(input).subscribe({
      next: (event: BaseEvent) => {
        if (event.type === EventType.STATE_DELTA) {
          const ops =
            (event as unknown as { delta?: JsonPatchOp[] }).delta ?? [];
          applyStateDelta(ops, input.runId, touchedNodeIds);
        } else if (event.type === EventType.CUSTOM) {
          // Side-channel: the backend mirrors message-shaped events (add_message,
          // token, remove_message, error) as a `langflow.event` CustomEvent so
          // the playground's chat-view utilities can consume them in their v1
          // shape until Phase 5 can rewrite chat-view onto AG-UI events directly.
          const custom = event as unknown as {
            name?: string;
            value?: { event_type?: string; data?: unknown };
          };
          if (custom.name === "langflow.event" && custom.value?.event_type) {
            handleMessageEvent(custom.value.event_type, custom.value.data);
          }
        } else if (event.type === EventType.RUN_FINISHED) {
          flowStore.setBuildInfo({ success: true });
        } else if (event.type === EventType.RUN_ERROR) {
          const message =
            (event as unknown as { message?: string }).message ??
            "Unknown run error";
          flowStore.setBuildInfo({ error: [message], success: false });
          setErrorData({ title: "Workflow run failed", list: [message] });
        }
      },
      error: (err: Error) => {
        flowStore.setBuildInfo({ error: [err.message], success: false });
        setErrorData({ title: "Workflow run failed", list: [err.message] });
        subscription.unsubscribe();
        finish();
      },
      complete: () => {
        subscription.unsubscribe();
        finish();
      },
    });
  });
}
