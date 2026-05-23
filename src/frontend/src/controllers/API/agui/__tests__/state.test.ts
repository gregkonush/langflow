import { type BaseEvent, EventType } from "@ag-ui/client";
import {
  applyAGUIStateEvent,
  type CanvasState,
  INITIAL_CANVAS_STATE,
} from "../state";

function snapshot(snap: unknown): BaseEvent {
  return {
    type: EventType.STATE_SNAPSHOT,
    snapshot: snap,
  } as unknown as BaseEvent;
}

function delta(
  ops: Array<{ op: string; path: string; value?: unknown }>,
): BaseEvent {
  return { type: EventType.STATE_DELTA, delta: ops } as unknown as BaseEvent;
}

describe("applyAGUIStateEvent", () => {
  it("starts from an empty node map", () => {
    expect(INITIAL_CANVAS_STATE).toEqual({ nodes: {} });
  });

  it("replaces the entire state on STATE_SNAPSHOT", () => {
    const start: CanvasState = {
      nodes: { stale: { status: "success", output: "old" } },
    };
    const next = applyAGUIStateEvent(
      start,
      snapshot({ nodes: { fresh: { status: "pending", output: null } } }),
    );

    expect(next).toEqual({
      nodes: { fresh: { status: "pending", output: null } },
    });
  });

  it("ignores a STATE_SNAPSHOT without a nodes object", () => {
    const start: CanvasState = {
      nodes: { a: { status: "pending", output: null } },
    };

    expect(applyAGUIStateEvent(start, snapshot({}))).toBe(start);
    expect(applyAGUIStateEvent(start, snapshot(null))).toBe(start);
  });

  it("adds a node via STATE_DELTA 'add' on /nodes/{id}", () => {
    const next = applyAGUIStateEvent(
      INITIAL_CANVAS_STATE,
      delta([
        {
          op: "add",
          path: "/nodes/A",
          value: { status: "running", output: null },
        },
      ]),
    );

    expect(next.nodes.A).toEqual({ status: "running", output: null });
  });

  it("replaces an existing node when 'add' fires on the same path again", () => {
    const start: CanvasState = {
      nodes: { A: { status: "running", output: null } },
    };
    const next = applyAGUIStateEvent(
      start,
      delta([
        {
          op: "add",
          path: "/nodes/A",
          value: { status: "success", output: "ok" },
        },
      ]),
    );

    expect(next.nodes.A).toEqual({ status: "success", output: "ok" });
  });

  it("applies multiple ops in order in one delta", () => {
    const next = applyAGUIStateEvent(
      INITIAL_CANVAS_STATE,
      delta([
        {
          op: "add",
          path: "/nodes/A",
          value: { status: "running", output: null },
        },
        {
          op: "add",
          path: "/nodes/B",
          value: { status: "pending", output: null },
        },
        {
          op: "replace",
          path: "/nodes/A",
          value: { status: "success", output: 1 },
        },
      ]),
    );

    expect(next.nodes).toEqual({
      A: { status: "success", output: 1 },
      B: { status: "pending", output: null },
    });
  });

  it("does not mutate the input state", () => {
    const start: CanvasState = {
      nodes: { A: { status: "pending", output: null } },
    };
    const startSnapshot = JSON.parse(JSON.stringify(start));

    applyAGUIStateEvent(
      start,
      delta([
        {
          op: "add",
          path: "/nodes/A",
          value: { status: "running", output: null },
        },
      ]),
    );

    expect(start).toEqual(startSnapshot);
  });

  it("ignores ops on paths other than /nodes/{id}", () => {
    const start: CanvasState = {
      nodes: { A: { status: "pending", output: null } },
    };
    const next = applyAGUIStateEvent(
      start,
      delta([{ op: "add", path: "/nodes/A/status", value: "running" }]),
    );

    expect(next).toBe(start);
  });

  it("returns the same reference for unrelated events", () => {
    const start: CanvasState = {
      nodes: { A: { status: "running", output: null } },
    };
    const ignored = { type: EventType.RUN_STARTED } as unknown as BaseEvent;

    expect(applyAGUIStateEvent(start, ignored)).toBe(start);
  });
});
