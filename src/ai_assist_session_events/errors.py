class SessionEventValidationError(ValueError):
    """Raised when a SessionEvent envelope or payload is invalid."""

    code = "SESSION_EVENT_VALIDATION_FAILED"

    def __init__(self, issues):
        super().__init__("Invalid SessionEvent envelope")
        self.issues = issues


class SessionEventPublisherError(RuntimeError):
    """Raised when validation or persistence prevents publishing an event."""

    code = "SESSION_EVENT_PUBLISHER_FAILED"

    def __init__(self, *, category, operation, cause=None, subscriber_failures=None):
        super().__init__(f"Session event publisher {operation} failed")
        self.category = category
        self.operation = operation
        self.cause = cause
        self.subscriber_failures = list(subscriber_failures or [])
