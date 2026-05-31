export class SessionEventValidationError extends Error {
  constructor(issues) {
    super("Invalid SessionEvent envelope");
    this.name = "SessionEventValidationError";
    this.code = "SESSION_EVENT_VALIDATION_FAILED";
    this.issues = issues;
  }
}

export class SessionEventPublisherError extends Error {
  constructor({ category, operation, cause, subscriberFailures = [] }) {
    super(`Session event publisher ${operation} failed`);
    this.name = "SessionEventPublisherError";
    this.code = "SESSION_EVENT_PUBLISHER_FAILED";
    this.category = category;
    this.operation = operation;
    this.subscriberFailures = subscriberFailures;
    if (cause) {
      this.cause = cause;
    }
  }
}
