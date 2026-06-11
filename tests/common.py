import copy


BASE_EVENT = {
    "eventId": "evt_001",
    "tenantId": "tenant_001",
    "userId": "user_001",
    "sessionId": "session_001",
    "requestId": "req_001",
    "correlationId": "corr_001",
    "type": "progress",
    "sequence": 1,
    "createdAt": "2026-05-29T00:00:00.000Z",
    "payload": {
        "stage": "context.loading",
        "status": "started",
        "messageCode": "CONTEXT_LOADING",
    },
}


BASE_ENVELOPE = {
    "eventId": "evt_001",
    "tenantId": "tenant_001",
    "userId": "user_001",
    "sessionId": "session_001",
    "requestId": "req_001",
    "correlationId": "corr_001",
    "sequence": 1,
}


GOOGLE_DOCS_RESOURCE_REF = {
    "connector": "google_docs",
    "resourceId": "doc_001",
    "resourceType": "document",
    "displayName": "Quarterly plan",
}


def event_with(**overrides):
    event = copy.deepcopy(BASE_EVENT)
    event.update(overrides)
    return event
