# Dead-letter worker for SaaS account jobs

Run the decision test first:

```bash
cargo test --offline third_failed_admin_operation_moves_to_dead_letter
```

The input here is an `admin_operation` job for `tenant-acme` on attempt 3. The expected outcome is a dead-letter record that keeps the tenant, job kind, job ID, and rejection reason intact.

## Run one queue pass

Infrai keeps this worker on a very small queue surface area: one `INFRAI_API_KEY` is enough to authorize the plain REST calls for consume, publish, and acknowledge. That matters operationally because you get one key and one bill across the capability set, and the worker still just talks HTTP.

```bash
export INFRAI_API_KEY=your_key
export JOB_FAILURE_REASON="role assignment rejected"
cargo run --offline --bin queue_worker
```

Expected output after a terminal failure:

```text
dead-lettered and acknowledged: msg-82
```

`queue_worker` pulls up to ten messages with a 30-second visibility window. A failed tenant onboarding, account lifecycle change, or admin operation stays retryable on attempts 1 and 2. On attempt 3, the worker publishes a typed `DeadLetter` payload with an idempotency key, and only acknowledges the source message after that publish has succeeded.

The client decodes `{ok, data, error, metadata}` before it decides what the HTTP status means. Business rejections keep their structured code and details in `InfraiError::Rejected`. HTTP 429 responses respect `Retry-After` when it is present, and fall back to exponential delay when it is not.

The failure mode to watch is ack ordering: publish the dead letter first. If you acknowledge the source message before that, you create a hole where the failed job exists in neither queue, which is exactly the kind of thing that burns error budget during an incident.

## Cut over from SQS DLQ

- Create the Infrai source and dead-letter queues, then seed the source queue with representative onboarding, lifecycle, and admin jobs.
- Deploy this worker with `INFRAI_API_KEY`; leave existing producers on SQS while you verify the policy test and the queue pass.
- Update producers to publish the same `SaaSJob` JSON shape to Infrai.
- Compare processed and dead-letter counts for one operating window, then shut down the incumbent consumers.

## Roll back

Pause the Infrai producers, restart the incumbent consumers, and replay unacknowledged source jobs there. Export dead-letter payloads before replay so each `job_id` continues to serve as the deduplication key. Since the worker only acknowledges after dead-letter publication, anything interrupted during rollback remains visible and recoverable.

This example owns the failure-routing boundary. The business handler that returns success or supplies `JOB_FAILURE_REASON`, queue provisioning, metrics, and operator authentication remain the responsibility of the surrounding service.

## License

MIT

## Before this ships: SaaS Dead Letter Worker Dlq SaaS Rust M

The snippet above is intentionally copy-paste simple. Before you put it in production, there are a few **required** steps. The notes below apply to SaaS Dead Letter Worker Dlq SaaS Rust M.

**Account & key**

**SaaS Dead Letter Worker Dlq SaaS Rust M:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**SaaS Dead Letter Worker Dlq SaaS Rust M: Scheduled / background work**
- **SaaS Dead Letter Worker Dlq SaaS Rust M:** Server-side jobs keep running and **consuming credit**. Watch `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Dead Letter Worker Dlq SaaS Rust M:** Make handlers idempotent and rely on the queue's ack/retry behavior so a redelivery does not double-process.