# sol-typescript

Source repository for Sol's TypeScript application-runtime packages:

- **`packages/svc`** → `@sol-fab/svc` — Sol's service lifecycle and outbound
  peer-authentication helpers for TypeScript HTTP services.
- **`packages/worker`** → `@sol-fab/worker` — Sol's worker-lifecycle contract
  for TypeScript Kafka workers.
- **`packages/jobs`** → `@sol-fab/jobs` — Sol's durable Postgres job queue: a
  transactional, dedupe-keyed `enqueue` and a leased `runJobs` runner, mirroring
  `framework/sol-jobs`.
- **`packages/outbox`** → `@sol-fab/outbox` — Sol's Postgres transactional
  outbox: a `publish` that joins the caller's transaction and a `runRelay` that
  publishes each key's events in order, deleting a row only after the broker
  acknowledged it. Each relay declares its `kinds` from the same `OutboxContract`
  used to enqueue events. Relays sharing a table must own disjoint kinds; an
  earlier foreign event still blocks later events of the same key. One logical
  relay per kind remains required (there is no replica fencing).
  Mirrors `framework/sol-outbox`.
- **`packages/retry`** → `@sol-fab/retry` — Sol's operation-level retry helper:
  one bounded, jittered policy vocabulary (`baseDelayS`, `maxDelayS`,
  `maxAttempts`, `jitterRatio`) that retries a dependency call in place, never a
  message or a handler. Mirrors `framework/sol-retry`; `@sol-fab/jobs` consumes
  the same vocabulary rather than keeping its own copy.

The packages keep lifecycle and helper ownership narrow: routing and inbound
HTTP stay Fastify/Express, Kafka consumption stays `kafkajs`, DLQ routing stays
`@sol-fab/kafka`, and metrics/tracing naming stays `@sol-fab/obs`.

`@sol-fab/svc` and `@sol-fab/worker` deliberately differ in one respect,
because their OCaml counterparts do: `sol-svc`'s `service.mli` exposes a
`drain_timeout_s` (an HTTP client can hold a connection open indefinitely);
`sol-worker`'s `worker.mli` has no equivalent parameter at all (a worker's
forced-shutdown safety is Kafka redelivery plus the orchestrator's grace
period, not a package-level timeout).

`@sol-fab/svc` also mirrors `sol-svc`'s readiness: `runService` returns
`isReady()`, false from the instant shutdown begins, so the app's `/readyz`
route returns 503 while the listener still serves. The listener keeps serving
for `shutdownDelayMs` (default 5s, `sol-svc`'s `shutdown_delay_s`) before the
drain starts, so a readiness probe observes the flip before the socket closes.

`@sol-fab/svc` exports `declaredPeer`, `peerUrl`, and `peerHeaders` for generated
application bindings. `peerHeaders` reads the projected token file for each call
so token rotation is observed; it uses the local API-key fallback only when
`SOL_ALLOW_PLAINTEXT_PEER_AUTH=1` is explicitly set.

The callee half of the same contract (DEC-063) is
`verifyWorkloadIdentity` / `createWorkloadIdentityGuard`. A request is
authenticated by default and only an explicit `isPublic` exception makes it
external. The guard verifies the projected ServiceAccount token against the
target-projected issuer (`SOL_TRUSTED_WORKLOAD_ISSUER`, discovered through its
OIDC document -- never the incoming token's `iss`), checks signature, audience,
time validity and the Kubernetes ServiceAccount subject, then authorizes the
caller unit against the caller set Sol derived from `calls` and projected as
`SOL_CALLED_BY`. Unauthenticated callers are 401, authenticated-but-undeclared
callers are 403, and a missing audience or trust root fails closed with a
`SOL_UNIT`/`SOL_TRUSTED_WORKLOAD_ISSUER` error. Verification uses Node's built-in
WebCrypto, so the package keeps no runtime dependencies.

`@sol-fab/jobs` is a library, not a fourth primitive (DEC-021): an ordinary
`@sol-fab/worker` binary hosts it by calling `runJobs` instead of consuming a
topic. Kafka says "this happened"; the job table says "this must happen", and
`enqueue` accepts a `PoolClient` so it can join the transaction that caused it.

Jobs are at-least-once: duplicate handlers can overlap after lease expiry or an
external effect followed by a crash. Enqueue dedupe keys suppress insertion only
while their row is retained; put a stable business operation ID in the payload
and use the provider's idempotency key for external effects. Sweep retention is
scoped to the runner's workspace and kinds. Consumers sharing a kind must agree
on codecs and retry/retention policies; no central registry enforces that.

A confirmed claim loss aborts the handler's `context.signal`. Handlers should
pass that signal to cancellable dependencies and join all their work before
returning. The runner waits for cleanup and never finalizes that lost attempt,
even if the handler returns success after abort. Query errors mean uncertain
ownership and are logged/retried; they do not abort execution. Cancellation is
cooperative and cannot stop blocking JavaScript or retract an accepted external
request. An expired but unreclaimed attempt may renew using its attempt fence.

`onOutcome` reports only confirmed durable transitions (`ok`, `retry`, `failed`),
while `onHandlerOutcome` reports handler returns and monotonic execution duration
before finalization. `onQueueError` carries bounded `kind`, `action` and `reason`
fields (`lease_lost` or `database_error`) for metric wiring; job IDs and original
errors stay in warnings. Metrics are process observations, not a persisted audit;
a crash after SQL commits and before callback delivery may undercount. `maxJobs`
counts confirmed terminal transitions. A stop signal drains the current attempt;
it is separate from the attempt's cancellation signal. After an in-flight claim, stop is rechecked before decode/handle. Only an
unstarted claim is released through the fenced retry statement with zero delay.
Its fencing generation is never refunded, so repeated pre-start shutdowns can
exhaust the claim budget without handler execution. An advanced generation makes
release a no-op. One active job is enforced
per `runJobs` loop, not per worker process. Use separate ordinary worker
Deployments for materially different resource/reliability classes.

No `@sol-fab/fn` package exists yet -- `sol-fn`'s OCaml contract (run once,
return, `SIGTERM` before the next invocation) has no measured TypeScript gap
to justify one; see the `sol` repo's `FEAT-036` ticket.

## Status

Published to npm as `@sol-fab/svc`, `@sol-fab/worker`, `@sol-fab/jobs`,
`@sol-fab/outbox` and `@sol-fab/retry`; a tag (`svc-v*`, `worker-v*`,
`jobs-v*`, `outbox-v*`, `retry-v*`) publishes through the OIDC release
workflow. Publish `retry` before a `jobs` release that depends on it. See the
`sol` repo's `pipeline/tickets/` for sequencing.

The `@sol-fab/jobs` and `@sol-fab/outbox` behaviour tests need a Postgres: CI
runs one and sets `POSTGRES_URL`, and without it those cases self-skip.

## Development

```bash
npm install
npm run build --workspaces
npm run test --workspaces
```

`runJobs({ executionTimeoutS })` accepts an optional finite, positive cooperative
execution deadline. Monotonic timing starts before decode, and completion time is
captured before telemetry. Deadline expiry aborts the attempt signal and waits for
the handler promise and cleanup to settle; renewal continues during cleanup.
Only afterward does the original attempt fence permit a timeout retry/failure.
Late success cannot erase timeout, including blocking work that returns before
the timer can run. Confirmed claim loss wins over timeout and suppresses stale
finalization. Renewal errors remain unknown ownership and do not release work.
Blocking JavaScript or ignored aborts can occupy the loop indefinitely; there is
no hard execution limit. The worker `signal` remains a drain request, distinct
from the handler's cancellation signal. JavaScript promises provide no Eio-style
implicit parent cancellation; abandoning `runJobs` does not cancel its work.
