import { assertValidSessionEvent } from "./session-event.js";
import { InMemorySessionEventSink } from "./event-sink.js";

export class InMemorySessionEventPublisher {
  #sink;
  #subscribersBySession = new Map();

  constructor({ sink = new InMemorySessionEventSink() } = {}) {
    this.#sink = sink;
  }

  publish(event) {
    assertValidSessionEvent(event);
    this.#sink.append(event);
    for (const subscriber of this.#subscribersBySession.get(event.sessionId) ?? []) {
      subscriber(event);
    }
    return event;
  }

  subscribe(sessionId, handler, { lastEventId, replay = true } = {}) {
    if (typeof handler !== "function") {
      throw new TypeError("handler must be a function");
    }

    const subscribers = this.#subscribersBySession.get(sessionId) ?? new Set();
    subscribers.add(handler);
    this.#subscribersBySession.set(sessionId, subscribers);

    const replayResult = replay
      ? this.#sink.replayAfter(sessionId, lastEventId)
      : { status: "REPLAY_DISABLED", events: [] };
    for (const event of replayResult.events) {
      handler(event);
    }

    return {
      replayStatus: replayResult.status,
      replayed: replayResult.events,
      unsubscribe: () => {
        const current = this.#subscribersBySession.get(sessionId);
        if (!current) {
          return;
        }
        current.delete(handler);
        if (current.size === 0) {
          this.#subscribersBySession.delete(sessionId);
        }
      }
    };
  }

  list(sessionId) {
    return this.#sink.list(sessionId);
  }
}
