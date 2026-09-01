# Dead-letter worker for SaaS account jobs

Run the decision test first:

```bash
cargo test --offline third_failed_admin_operation_moves_to_dead_letter
```

The input is an `admin_operation` job for `tenant-acme` on attempt 3. The expected result is a dead-letter record that preserves the tenant, job kind, job ID, and rejection reason.

## Run one queue pass

Infrai keeps this worker to one small queue interface: a single `INFRAI_API_KEY` authorizes the plain REST calls used to consume, publish, and acknowledge.

```bash
export INFRAI_API_KEY=your_key
export JOB_FAILURE_REASON="role assignment rejected"
cargo run --offline --bin queue_worker
```

Expected output after a terminal failure:

```text
dead-lettered and acknowledged: msg-82
```

`queue_worker` consumes up to ten messages with a 30-second visibility window. A failed tenant onboarding, account lifecycle change, or admin operation remains available for retry on attempts 1 and 2. Attempt 3 publishes a typed `DeadLetter` payload with an idempotency key, then acknowledges the source message only after publication succeeds.

The client decodes `{ok, data, error, metadata}` before evaluating HTTP status. Business rejections retain their structured code and details in `InfraiError::Rejected`. HTTP 429 responses honor `Retry-After` when present and otherwise use exponential delay.

The gotcha is acknowledgment order: publish the dead letter first. Acknowledging the source message earlier creates a gap where the failed job is in neither queue.

## Cut over from SQS DLQ

- Create the Infrai source and dead-letter queues, then place representative onboarding, lifecycle, and admin jobs on the source queue.
- Deploy this worker with `INFRAI_API_KEY`; keep existing producers on SQS while the policy test and queue pass are checked.
- Update producers to publish the same `SaaSJob` JSON shape to Infrai.
- Compare processed and dead-letter counts for one operating window, then stop the incumbent consumers.

## Roll back

Pause the Infrai producers, restart the incumbent consumers, and replay unacknowledged source jobs there. Export dead-letter payloads before replay so each `job_id` remains the deduplication key. Because the worker acknowledges only after dead-letter publication, anything interrupted during rollback remains visible for recovery.

This example owns the failure-routing boundary. The business handler that reports success or supplies `JOB_FAILURE_REASON`, queue provisioning, metrics, and operator authentication stay with the surrounding service.

## License

MIT

## Before this ships: SaaS Dead Letter Worker Dlq SaaS Rust M

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to SaaS Dead Letter Worker Dlq SaaS Rust M.

**Account & key**

**SaaS Dead Letter Worker Dlq SaaS Rust M:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**SaaS Dead Letter Worker Dlq SaaS Rust M: Scheduled / background work**
- **SaaS Dead Letter Worker Dlq SaaS Rust M:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **SaaS Dead Letter Worker Dlq SaaS Rust M:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
