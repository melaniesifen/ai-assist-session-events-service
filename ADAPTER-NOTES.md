# Adapter Notes

## Current MVP Boundary

- SSE is the only MVP stream adapter.
- `SessionEvent` stays transport-neutral and is validated before publish.
- HTTP remains the authority for commands, approvals, and durable mutations.
- Stream logs must use metadata-only records from `createStreamLogRecord`.

## Future WebSocket Trigger Checklist

Add a WebSocket adapter only when at least one product requirement needs it:

- Multi-client session sync.
- Browser extension or sidebar active-session coordination.
- Bidirectional live context updates.
- Visible-region streaming.
- Screen-aware mode.
- Long-running background workflows that need active client coordination.

## Future WebSocket Adapter Constraints

- Authenticate `$connect`.
- Keep a connection registry keyed by tenant, user, session, and connection ID.
- Authorize every subscribe, unsubscribe, and client message.
- Implement heartbeat or ping/pong handling.
- Clean up stale connections.
- Emit the same validated `SessionEvent` envelope used by SSE.
- Do not move apply-action, approval, rejection, or any durable mutation off HTTP.
- Do not trust client-supplied identity fields.
- Do not add orchestration workflow decisions to this repo.
