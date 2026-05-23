import { type BaseEvent, EventType } from "@ag-ui/client";
import {
  applyAGUIChatEvent,
  type ChatMessage,
  INITIAL_CHAT_THREAD,
} from "../chat";

function ev(type: EventType, payload: Record<string, unknown>): BaseEvent {
  return { type, ...payload } as unknown as BaseEvent;
}

describe("applyAGUIChatEvent", () => {
  it("starts with an empty thread", () => {
    expect(INITIAL_CHAT_THREAD).toEqual([]);
  });

  it("opens a streaming assistant message on TEXT_MESSAGE_START", () => {
    const thread = applyAGUIChatEvent(
      INITIAL_CHAT_THREAD,
      ev(EventType.TEXT_MESSAGE_START, { messageId: "m1", role: "assistant" }),
    );

    expect(thread).toEqual([
      { id: "m1", role: "assistant", content: "", isStreaming: true },
    ]);
  });

  it("appends delta content on TEXT_MESSAGE_CONTENT", () => {
    let thread = applyAGUIChatEvent(
      INITIAL_CHAT_THREAD,
      ev(EventType.TEXT_MESSAGE_START, { messageId: "m1", role: "assistant" }),
    );
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_CONTENT, { messageId: "m1", delta: "Hello, " }),
    );
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_CONTENT, { messageId: "m1", delta: "world!" }),
    );

    expect(thread[0].content).toBe("Hello, world!");
    expect(thread[0].isStreaming).toBe(true);
  });

  it("marks the message complete on TEXT_MESSAGE_END", () => {
    let thread = applyAGUIChatEvent(
      INITIAL_CHAT_THREAD,
      ev(EventType.TEXT_MESSAGE_START, { messageId: "m1", role: "assistant" }),
    );
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_END, { messageId: "m1" }),
    );

    expect(thread[0].isStreaming).toBe(false);
  });

  it("ignores CONTENT for an unknown message id", () => {
    const thread: readonly ChatMessage[] = INITIAL_CHAT_THREAD;
    const next = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_CONTENT, { messageId: "ghost", delta: "..." }),
    );

    expect(next).toBe(thread);
  });

  it("supports multiple sequential messages in the thread", () => {
    let thread: readonly ChatMessage[] = INITIAL_CHAT_THREAD;
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_START, { messageId: "m1", role: "assistant" }),
    );
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_CONTENT, { messageId: "m1", delta: "first" }),
    );
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_END, { messageId: "m1" }),
    );
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_START, { messageId: "m2", role: "assistant" }),
    );
    thread = applyAGUIChatEvent(
      thread,
      ev(EventType.TEXT_MESSAGE_CONTENT, { messageId: "m2", delta: "second" }),
    );

    expect(
      thread.map((m) => ({
        id: m.id,
        content: m.content,
        isStreaming: m.isStreaming,
      })),
    ).toEqual([
      { id: "m1", content: "first", isStreaming: false },
      { id: "m2", content: "second", isStreaming: true },
    ]);
  });

  it("does not mutate the input thread", () => {
    const start = applyAGUIChatEvent(
      INITIAL_CHAT_THREAD,
      ev(EventType.TEXT_MESSAGE_START, { messageId: "m1", role: "assistant" }),
    );
    const startCopy = JSON.parse(JSON.stringify(start));

    applyAGUIChatEvent(
      start,
      ev(EventType.TEXT_MESSAGE_CONTENT, { messageId: "m1", delta: "x" }),
    );

    expect(start).toEqual(startCopy);
  });

  it("returns the same reference for unrelated events", () => {
    const start = applyAGUIChatEvent(
      INITIAL_CHAT_THREAD,
      ev(EventType.TEXT_MESSAGE_START, { messageId: "m1", role: "assistant" }),
    );

    expect(applyAGUIChatEvent(start, ev(EventType.RUN_STARTED, {}))).toBe(
      start,
    );
  });
});
