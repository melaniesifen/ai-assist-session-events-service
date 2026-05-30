export class SessionEventValidationError extends Error {
  constructor(issues) {
    super("Invalid SessionEvent envelope");
    this.name = "SessionEventValidationError";
    this.code = "SESSION_EVENT_VALIDATION_FAILED";
    this.issues = issues;
  }
}
