/**
 * Playground-chat reducer for AG-UI text-message events.
 *
 * The backend translator emits `TEXT_MESSAGE_START`, `TEXT_MESSAGE_CONTENT`,
 * and `TEXT_MESSAGE_END` for streaming assistant messages (token-by-token), or
 * a START / CONTENT / END triple for non-streamed `add_message` events. This
 * reducer folds that lifecycle into a flat thread of messages the playground
 * can render. Pure — no React, no store.
 */

import { type BaseEvent, EventType } from "@ag-ui/client";

/** A single message in the playground chat thread. */
export interface ChatMessage {
  id: string;
  role: string;
  content: string;
  /** True while tokens are still streaming for this message. */
  isStreaming: boolean;
}

/** Initial chat thread state. */
export const INITIAL_CHAT_THREAD: readonly ChatMessage[] = [];

function withMessage(
  thread: readonly ChatMessage[],
  id: string,
  update: (msg: ChatMessage) => ChatMessage,
): readonly ChatMessage[] {
  let found = false;
  const next = thread.map((msg) => {
    if (msg.id !== id) return msg;
    found = true;
    return update(msg);
  });
  return found ? next : thread;
}

/**
 * Reduce one AG-UI event into the chat thread.
 *
 * `TEXT_MESSAGE_START` appends a new streaming message. `TEXT_MESSAGE_CONTENT`
 * appends its delta to the matching open message. `TEXT_MESSAGE_END` marks the
 * matching message complete. Every other event leaves the thread untouched.
 */
export function applyAGUIChatEvent(
  thread: readonly ChatMessage[],
  event: BaseEvent,
): readonly ChatMessage[] {
  if (event.type === EventType.TEXT_MESSAGE_START) {
    const e = event as unknown as { messageId: string; role: string };
    return [
      ...thread,
      {
        id: e.messageId,
        role: e.role ?? "assistant",
        content: "",
        isStreaming: true,
      },
    ];
  }
  if (event.type === EventType.TEXT_MESSAGE_CONTENT) {
    const e = event as unknown as { messageId: string; delta: string };
    return withMessage(thread, e.messageId, (msg) => ({
      ...msg,
      content: msg.content + (e.delta ?? ""),
    }));
  }
  if (event.type === EventType.TEXT_MESSAGE_END) {
    const e = event as unknown as { messageId: string };
    return withMessage(thread, e.messageId, (msg) => ({
      ...msg,
      isStreaming: false,
    }));
  }
  return thread;
}
