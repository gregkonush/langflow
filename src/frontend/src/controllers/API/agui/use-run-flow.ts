/**
 * React hook around the AG-UI workflow run service.
 *
 * Provides a minimal imperative API for components that want to start a run,
 * collect typed AG-UI events as they arrive, and abort. Phase 4 wires this (or
 * the underlying service directly) into the canvas / playground stores.
 */

import { type BaseEvent, type HttpAgent } from "@ag-ui/client";
import { useCallback, useRef, useState } from "react";
import { Subscription } from "rxjs";
import {
  buildRunInput,
  createWorkflowAgent,
  type WorkflowAgentOptions,
  type WorkflowRunOptions,
} from "./run-agent";

export interface UseRunFlowState {
  /** All AG-UI events received so far in the current run. */
  events: BaseEvent[];
  /** True from the moment a run is started until it ends (success or error). */
  isRunning: boolean;
  /** Error from a failed run; cleared at the start of the next run. */
  error: Error | null;
}

const INITIAL_STATE: UseRunFlowState = {
  events: [],
  isRunning: false,
  error: null,
};

/**
 * Run AG-UI workflow runs from React.
 *
 * The agent is constructed lazily on first run. Calling `run` again starts a
 * fresh run (resetting `events`); calling `abort` stops the active run.
 */
export function useRunFlow(agentOptions: WorkflowAgentOptions = {}) {
  const agentRef = useRef<HttpAgent | null>(null);
  const subRef = useRef<Subscription | null>(null);
  const [state, setState] = useState<UseRunFlowState>(INITIAL_STATE);

  const getAgent = useCallback((): HttpAgent => {
    if (agentRef.current === null) {
      agentRef.current = createWorkflowAgent(agentOptions);
    }
    return agentRef.current;
  }, [agentOptions]);

  const run = useCallback(
    (opts: WorkflowRunOptions) =>
      new Promise<void>((resolve) => {
        // Cancel any prior in-flight run.
        subRef.current?.unsubscribe();
        setState({ events: [], isRunning: true, error: null });

        const input = buildRunInput(opts);
        subRef.current = getAgent()
          .run(input)
          .subscribe({
            next: (event) => {
              setState((s) => ({ ...s, events: [...s.events, event] }));
            },
            error: (err: Error) => {
              setState((s) => ({ ...s, isRunning: false, error: err }));
              resolve();
            },
            complete: () => {
              setState((s) => ({ ...s, isRunning: false }));
              resolve();
            },
          });
      }),
    [getAgent],
  );

  const abort = useCallback(() => {
    subRef.current?.unsubscribe();
    agentRef.current?.abortRun();
    setState((s) => ({ ...s, isRunning: false }));
  }, []);

  return { ...state, run, abort };
}
