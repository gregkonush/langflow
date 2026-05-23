import {
  buildRunInput,
  createWorkflowAgent,
  WORKFLOWS_ENDPOINT,
} from "../run-agent";

describe("buildRunInput", () => {
  it("puts flow_id and mode into forwardedProps with stream default", () => {
    const input = buildRunInput({ flowId: "flow-1" });

    expect(input.forwardedProps).toMatchObject({
      flow_id: "flow-1",
      mode: "stream",
    });
  });

  it("honors an explicit mode", () => {
    const input = buildRunInput({ flowId: "flow-1", mode: "background" });

    expect((input.forwardedProps as { mode: string }).mode).toBe("background");
  });

  it("wraps a chat message into a single user message", () => {
    const input = buildRunInput({ flowId: "flow-1", message: "hello" });

    expect(input.messages).toHaveLength(1);
    expect(input.messages[0]).toMatchObject({ role: "user", content: "hello" });
  });

  it("omits messages when no chat input is given", () => {
    const input = buildRunInput({ flowId: "flow-1" });

    expect(input.messages).toEqual([]);
  });

  it("includes tweaks and partial-run component ids when provided", () => {
    const input = buildRunInput({
      flowId: "flow-1",
      tweaks: { "ChatInput-abc": { input_value: "x" } },
      startComponentId: "c1",
      stopComponentId: "c2",
    });

    expect(input.forwardedProps).toMatchObject({
      tweaks: { "ChatInput-abc": { input_value: "x" } },
      start_component_id: "c1",
      stop_component_id: "c2",
    });
  });

  it("uses provided threadId and runId when given", () => {
    const input = buildRunInput({
      flowId: "flow-1",
      threadId: "t-9",
      runId: "r-9",
    });

    expect(input.threadId).toBe("t-9");
    expect(input.runId).toBe("r-9");
  });

  it("generates a fresh threadId and runId when omitted", () => {
    const a = buildRunInput({ flowId: "flow-1" });
    const b = buildRunInput({ flowId: "flow-1" });

    expect(a.threadId).toBeTruthy();
    expect(a.runId).toBeTruthy();
    expect(a.threadId).not.toBe(b.threadId);
    expect(a.runId).not.toBe(b.runId);
  });

  it("ships an empty state and empty tools/context arrays", () => {
    const input = buildRunInput({ flowId: "flow-1" });

    expect(input.state).toEqual({});
    expect(input.tools).toEqual([]);
    expect(input.context).toEqual([]);
  });
});

describe("createWorkflowAgent", () => {
  it("defaults to the v2 workflows endpoint", () => {
    const agent = createWorkflowAgent();

    expect(agent.url).toBe(WORKFLOWS_ENDPOINT);
  });

  it("uses a caller-provided url", () => {
    const agent = createWorkflowAgent({ url: "/api/v2/workflows-test" });

    expect(agent.url).toBe("/api/v2/workflows-test");
  });

  it("passes custom headers through to the underlying HttpAgent", () => {
    const agent = createWorkflowAgent({ headers: { "X-Foo": "bar" } });

    expect(agent.headers).toMatchObject({ "X-Foo": "bar" });
  });
});
