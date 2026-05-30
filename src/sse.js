import { assertValidSessionEvent } from "./session-event.js";

export function formatSseEvent(event) {
  assertValidSessionEvent(event);
  return [
    `id: ${event.eventId}`,
    `event: ${event.type}`,
    ...formatDataLines(JSON.stringify(event)),
    ""
  ].join("\n") + "\n";
}

export function formatSseRetry(milliseconds) {
  if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) {
    throw new TypeError("milliseconds must be a non-negative safe integer");
  }
  return `retry: ${milliseconds}\n\n`;
}

function formatDataLines(serializedEvent) {
  return serializedEvent.split(/\r?\n/u).map((line) => `data: ${line}`);
}
