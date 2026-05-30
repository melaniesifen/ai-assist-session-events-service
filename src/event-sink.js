import { assertValidSessionEvent } from "./session-event.js";
import { SessionEventValidationError } from "./errors.js";
import { replayStatusForLastEventId } from "./reconnect.js";

const DEFAULT_BUFFER_SIZE = 100;

export class InMemorySessionEventSink {
  #eventsBySession = new Map();
  #maxEventsPerSession;

  constructor({ maxEventsPerSession = DEFAULT_BUFFER_SIZE } = {}) {
    if (!Number.isSafeInteger(maxEventsPerSession) || maxEventsPerSession < 1) {
      throw new TypeError("maxEventsPerSession must be a positive safe integer");
    }
    this.#maxEventsPerSession = maxEventsPerSession;
  }

  append(event) {
    assertValidSessionEvent(event);
    const sessionEvents = this.#eventsBySession.get(event.sessionId) ?? [];
    if (sessionEvents.some((existing) => existing.eventId === event.eventId)) {
      throw new SessionEventValidationError([
        { path: "eventId", code: "duplicate_event_id", message: "eventId already exists for this session" }
      ]);
    }
    const nextEvents = sessionEvents.concat(event).slice(-this.#maxEventsPerSession);
    this.#eventsBySession.set(event.sessionId, nextEvents);
    return event;
  }

  list(sessionId) {
    return [...(this.#eventsBySession.get(sessionId) ?? [])];
  }

  listAfter(sessionId, lastEventId) {
    return this.replayAfter(sessionId, lastEventId).events;
  }

  replayAfter(sessionId, lastEventId) {
    return replayStatusForLastEventId(this.list(sessionId), lastEventId);
  }

  hasEvent(sessionId, eventId) {
    return this.list(sessionId).some((event) => event.eventId === eventId);
  }
}
