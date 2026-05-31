import { assertValidSessionEvent } from "./session-event.js";
import { InMemorySessionEventSink } from "./event-sink.js";
import { SessionEventPublisherError } from "./errors.js";

export const PUBLISHER_FAILURE_CATEGORIES = Object.freeze({
  VALIDATION: "VALIDATION",
  PERSISTENCE: "PERSISTENCE"
});

export class InMemorySessionEventPublisher {
  #sink;
  #subscribersBySession = new Map();

  constructor({ sink = new InMemorySessionEventSink() } = {}) {
    this.#sink = sink;
  }

  publish(event) {
    const result = this.tryPublish(event);
    if (!result.ok) {
      throw result.error;
    }
    return result.event;
  }

  tryPublish(event) {
    try {
      assertValidSessionEvent(event);
    } catch (error) {
      return publisherFailure(PUBLISHER_FAILURE_CATEGORIES.VALIDATION, "validate", error);
    }

    try {
      this.#sink.append(event);
    } catch (error) {
      return publisherFailure(PUBLISHER_FAILURE_CATEGORIES.PERSISTENCE, "append", error);
    }

    const deliveryFailures = [];
    for (const subscriber of this.#subscribersBySession.get(event.sessionId) ?? []) {
      try {
        subscriber(event);
      } catch (error) {
        deliveryFailures.push(error);
      }
    }

    return { ok: true, event, deliveryFailures };
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

function publisherFailure(category, operation, cause) {
  return {
    ok: false,
    error: new SessionEventPublisherError({ category, operation, cause })
  };
}
