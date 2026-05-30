export function createEventDeduplicator({ initialEventIds = [], maxTrackedEventIds = 1000 } = {}) {
  if (!Number.isSafeInteger(maxTrackedEventIds) || maxTrackedEventIds < 1) {
    throw new TypeError("maxTrackedEventIds must be a positive safe integer");
  }
  const seen = [];
  const seenSet = new Set();

  for (const eventId of initialEventIds) {
    remember(eventId);
  }

  function remember(eventId) {
    if (seenSet.has(eventId)) {
      return;
    }
    seen.push(eventId);
    seenSet.add(eventId);
    while (seen.length > maxTrackedEventIds) {
      seenSet.delete(seen.shift());
    }
  }

  return {
    shouldProcess(event) {
      if (seenSet.has(event.eventId)) {
        return false;
      }
      remember(event.eventId);
      return true;
    },
    hasSeen(eventId) {
      return seenSet.has(eventId);
    }
  };
}

export function detectSequenceGap(previousSequence, nextEvent) {
  if (previousSequence === undefined || nextEvent.sequence === undefined) {
    return { hasGap: false };
  }
  const expectedSequence = previousSequence + 1;
  return {
    hasGap: nextEvent.sequence !== expectedSequence,
    expectedSequence,
    actualSequence: nextEvent.sequence
  };
}

export function replayStatusForLastEventId(events, lastEventId) {
  if (!lastEventId) {
    return { status: "FULL_REPLAY", events };
  }
  const index = events.findIndex((event) => event.eventId === lastEventId);
  if (index === -1) {
    return { status: "REPLAY_UNAVAILABLE", events: [] };
  }
  return { status: "PARTIAL_REPLAY", events: events.slice(index + 1) };
}
