# Webhooks and durable event consumption

Meldbase does not provide a generic, database-owned webhook dispatcher. An
outbound HTTP request has application-specific authentication, retry, ordering,
idempotency, dead-letter, and recipient-ownership semantics. Instead, durable
databases expose a named, resumable database-wide change feed that an
application-owned worker can use to implement webhooks, queues, search-index
updates, or an outbox relay.

## Delivery contract

`CreateDurableDatabaseChanges` creates a named checkpoint at an exact commit
token. `OpenDurableDatabaseChanges` resumes it after a crash. Batches are in
global commit order and may contain document writes, collection creation, index
publication, or no public changes at all. Empty batches still advance the token
and must be acknowledged.

Call `Ack(token)` only after the external effect for that token is durable in
the destination system. That makes delivery **at least once**: a crash after an
HTTP recipient accepted a request but before the local acknowledgement can
redeliver it. Include the database identity and batch token as the recipient
idempotency key. Do not promise exactly-once delivery across the database and
an external HTTP service.

```go
subscription, err := db.CreateDurableDatabaseChanges(ctx, "billing-webhook", 0, 64)
if err != nil { log.Fatal(err) }
defer subscription.Close()

for {
    select {
    case batch, ok := <-subscription.Batches:
        if !ok { return }

        event := marshalWebhook(db.DatabaseIdentity(), batch)
        if err := deliverAndDurablyRecord(event); err != nil {
            // Do not Ack. Reopen the named subscription after retry/backoff.
            return
        }
        if err := subscription.Ack(batch.Token); err != nil { return }
    case err := <-subscription.Errors:
        if err != nil { return }
        return
    case <-ctx.Done():
        return
    }
}
```

The initial `afterToken` must still be present in retained Commit Log history.
If `OpenDurableDatabaseChanges` returns `ErrHistoryLost`, do not silently start
at the newest event: repair the downstream projection from a verified source
snapshot, create a new checkpoint at the repaired token, and record the
resynchronization.

## Webhook worker guidance

- Own the worker process, credentials, HTTP client timeout, retry/backoff, and
  dead-letter policy in the application boundary.
- Use a stable event envelope with `databaseId`, `token`, transaction ID,
  event schema version, and recipient-specific idempotency key. Do not send raw
  database pages or credentials.
- Keep the checkpoint name stable and unique to one consumer. Multiple
  destinations need separate names and acknowledgements.
- Size the subscription buffer for short local bursts only; it is not a durable
  remote queue. A slow consumer intentionally retains source history until it
  catches up or is explicitly removed.
- Observe consumer lag, retry counts, `ErrHistoryLost`, and destination
  failures. Delete a checkpoint only after the worker has stopped and the
  destination no longer needs replay.

For a follower or snapshot-plus-tail bootstrap, use `BeginArchive` and
`OpenFollower`; that is a replication/recovery protocol, not a webhook
delivery API. The public realtime protocol is likewise for end-user query
subscriptions, not an authoritative outbox.
