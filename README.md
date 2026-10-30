# GOAT Sidekiq Runtime

Shared runtime components for Sidekiq OSS 7.3.10. Requires Ruby 3.4.7 or later.

These components provide storage naming, queue discovery, explicit Redis pool
scoping, delivery metadata, telemetry, atomic claims, shared queue rate limits,
reliable fetching, and process-heartbeat recovery.
Native producer/server installation, final queue validation, management retries
and atomic scheduled promotion are available. Optional Active Job integration
includes serialization, physical queue routing and confirmed retry writes.
Optional Active Job uniqueness now includes atomic admission, execution ownership,
conflict continuations and owner-safe cleanup.

## Storage and queues

```ruby
require 'goat_sidekiq_runtime'

storage = SidekiqRuntime::Storage.new(namespace: 'app:srt:v1')
SidekiqRuntime::QueueKeys.configure!(storage)
SidekiqRuntime::QueueKeys.source_queue('critical') # => "queue:critical"
SidekiqRuntime::QueueKeys.inflight_key('critical') # => "app:srt:v1:queue:critical:inflight"
```

Physical queues use Sidekiq's native `queue:<name>` lists. Runtime keys share an
immutable namespace; queue discovery combines configured queues, Sidekiq's native
queue set, and the runtime's persistent queue directory. Configure the namespace
once before using runtime components.

## Claims and queue rate limits

`InflightStore` protocol 3 atomically moves Ready jobs into a persistent Inflight
sorted set and records each claim's process owner. ACK and requeue check the
exact claim and owner; orphan reclaim also checks that the owner's native Sidekiq
heartbeat has disappeared. Ambiguous Redis replies propagate without replaying
the transition.

`QueueRateLimit` manages a token bucket shared by consumers of one physical queue
within the runtime namespace and Redis database. Claims spend tokens in the same
Lua script as the Ready-to-Inflight move. Empty queues spend no tokens; a denied
claim returns `InflightStore::RateLimited` and leaves Ready and ownership intact.
Queues are unlimited until a policy is enabled.

```ruby
limiter = SidekiqRuntime::QueueRateLimit.new(sidekiq_config)
limiter.enable('critical', rate: 2.5, burst: 3)
limiter.get('critical')
limiter.disable('critical')
```

## Reliable fetching and recovery

`ReliableFetch.new(capsule)` supports Sidekiq's strict, weighted and random queue
modes. Its `UnitOfWork` acknowledges the exact claim or requeues it on shutdown
with redelivery metadata. Shutdown does not spend the crash recovery budget;
unknown Redis results are reported without replaying the operation.

`ProcessorBoundary` tracks claims held during processor work and reports an
unconfirmed release for manual review. A living owner's heartbeat never
authorizes automatic recovery, even for a long-running job.

`OrphanReclaimer` scans the persistent queue directory and claims in bounded
batches. Recovery atomically rechecks the native Sidekiq process heartbeat,
preserves original arguments and routing, and increments `recovery_count`.
Malformed envelopes and exhausted recovery budgets move to Dead.

`InflightCollection.new(config: sidekiq_config)` discovers reliable queues for
bounded browsing, ownership diagnostics and explicit orphan recovery. Displayed
state is informational; every recovery rechecks ownership and heartbeat in Redis.

Install the producer in every process that writes native Sidekiq jobs:

```ruby
SidekiqRuntime.install_producer!(storage: storage)
```

Install the server before Sidekiq constructs its capsule fetcher. Deployment owns
queues, concurrency and shutdown timeout; set raw queue input explicitly before
startup. Weighted queues require positive integer weights on every entry.

```ruby
Sidekiq.configure_server do |config|
  config[:queues] = %w[critical,3 default,1]
  config.queues = config[:queues]
  SidekiqRuntime.install_server!(
    config,
    redis_config: {url: ENV.fetch('SIDEKIQ_REDIS_URL')},
    storage: storage
  )
end
```

Startup validates the actual default capsule and cached reliable fetcher, then
starts the bounded orphan reclaimer. Quiet and shutdown stop the reclaimer.
Final producer persistence validates queue names and rejects `retry_queue`.
`AtomicScheduledEnqueuer` promotes accepted Scheduled/Retry records atomically
without rerunning producer middleware. Retry/Dead management operations preserve
Sidekiq's remove-then-push semantics and their original physical queue.

## Optional Active Job adapter

Applications using Active Job 7.2.3.1 can load the optional adapter:

```ruby
require 'goat_sidekiq_runtime/active_job'

class RefreshProductJob < ActiveJob::Base
  self.queue_adapter = :sidekiq_runtime
  self.enqueue_after_transaction_commit = :default
  queue_as :products
  retry_on Timeout::Error, wait: 10, attempts: 2
  discard_on ActiveJob::DeserializationError

  def perform(product_id)
    Products.refresh(product_id, reconcile: execution_context.redelivered?)
  end
end
```

Keep the producer/server installation above. The wrapper delegates serialization,
callbacks, retries and discard decisions to Active Job, with native Sidekiq
`retry: 0, dead: true`. AJ retries normally enter Scheduled; an unhandled final
exception enters Dead. The logical `job_id` survives retries, while
`provider_job_id` identifies the accepted Sidekiq delivery. The immutable
`execution_context.redelivered?` distinguishes known retries/recovery from first
submissions and newly created child jobs.

AJ owns after-commit deferral when Rails installs its integration. The adapter
uses a direct Sidekiq client so a preallocated transactional JID cannot confirm a
retry write. Each self-retry must receive an actual write acknowledgement before
the wrapper returns. Callback cancellation, rejected writes, pending/rolled-back
transactions and caught transport exceptions leave the retry unconfirmed and
raise `SidekiqRuntime::ActiveJob::RetryPersistenceError`. Sidekiq retains a Dead
failure, or the exact Inflight claim if failure persistence also fails. Unknown
writes and late commits can create duplicates; delivery remains at-least-once.
The contract tests probe framework deferral APIs without booting Rails or a real
database transaction.

Client middleware may route ordinary AJ jobs from their logical `queue_name` to
a physical queue. Self-retries are validated after middleware and must return to
the consumed physical queue. Ordinary bulk enqueue keeps per-job results without
enqueue callbacks, transaction deferral or batch atomicity.

`sidekiq.notification.active_job_finished` reports execution outcomes including
`succeeded`, `retry_scheduled`, `discarded`, `retry_exhausted`, `handled`, `aborted`,
`retry_unconfirmed` and `failed`. These events describe AJ execution; observer
failures do not confirm retry writes or control Redis ACK.

## Uniqueness and retry lifecycle

Active Job declares the single `until_and_while_executing` policy with:

```ruby
class RefreshProductJob < ActiveJob::Base
  self.queue_adapter = :sidekiq_runtime
  queue_as :products
  runtime_uniqueness enqueue_ttl: 24.hours, runtime_ttl: 60.seconds,
                     key: ->(job) { job.arguments.first }
end
```

The key may return one stable business value; it does not have to return an Array.
For a GlobalID argument, prefer an immutable key such as `job.arguments.first.id`.
AJ serializes the key as a one-element identity Array. Choose stable identity
fields, not request time, trace fields, redelivery flags or mutable model data.
A class that declares this enhancement and chooses an unsupported adapter such
as `:resque` raises before enqueue, including AJ bulk grouping. Plain jobs may
still choose another adapter.

Only Active Job exposes custom uniqueness. Native Sidekiq Jobs use ordinary
enqueue, execution, Retry and Dead behavior. Use `runtime_uniqueness` on Active
Job for the scope **business Job class + physical queue + one identity**.
Enqueue and execution have separate keys and TTLs for that same identity.

The snapshot lives in `sidekiq_runtime.uniqueness` alongside `redelivered` on the
Sidekiq transport payload. It contains `job_class`, `queue`, serialized `identity`,
the scoped `key_prefix`, and both TTLs. AJ also carries this metadata at
`args[0].sidekiq_runtime` so `Base.execute` and later self-retries preserve it.
The outer transport copy is authoritative: `JobMetadata.with_redelivery` updates
both copies, and server middleware synchronizes the AJ copy before execution.
No business argument is added or rewritten. Do not edit this internal metadata.

Uniqueness does not impose a retry or Dead retention policy. Native `retry: false`,
`retry: 0`, and `dead: false` retain Sidekiq's meanings; no reliability policy is
silently upgraded. The AJ wrapper keeps its one default `retry: 0, dead: true`.

The policy is `until_and_while_executing`, not uniqueness until terminal success:

| Phase | Reservation behavior |
|---|---|
| Initial Ready or Scheduled enqueue | One atomic write stores the payload and an enqueue lock owned by its JID. While that lock is valid, a new submission for the same identity is filtered, even if it supplies the same JID. |
| Execution start | The worker first acquires a per-attempt runtime owner token, then deletes the enqueue lock only if the current JID still owns it. After Job A releases its reservation, Job B may be accepted. While B owns the new reservation, Job C is filtered, and A's later Success or Dead cleanup does not remove B's lock. |
| Success or ordinary failure | The runtime token is released. A native Retry or an AJ self-retry waits without creating or extending an enqueue reservation, so a new same-identity Job may be accepted. AJ retries preserve the snapshot and use only the current in-memory retry request to select this path; `redelivered` and matching `job_id` are not retry admission signals. |
| Sidekiq shutdown | Before releasing its runtime token, the current JID attempts to restore its own enqueue reservation for the Inflight/Ready handoff. Restoration can fail when another JID owns the reservation or Redis is unavailable. |
| Runtime-lock conflict | Stores a same-JID Scheduled continuation delayed by `runtime_ttl + 1` seconds without entering business `perform`, increasing AJ `executions`, or changing redelivery. Its enqueue reservation is created when absent, refreshed when owned by that JID, and left unchanged when owned by another JID. This is not an ordinary Sidekiq Retry. When the runtime TTL expires, concurrent execution can occur. |

The enqueue lock for a Scheduled unique job lasts for `configured_enqueue_ttl + ceil(max(scheduled_at - enqueue_time, 0))` seconds. The configured TTL is therefore the duplicate-filtering window *after* the scheduled time, if the job is promoted on time. A longer actual queue wait can outlive that window. Runtime locks also have TTLs and do not guarantee one instance for an arbitrarily long job. Business idempotency remains the Job's responsibility.

Active Job unique enqueues do not support `Sidekiq::Testing.fake!` or
`Sidekiq::Testing.inline!`. They raise `ArgumentError` before client middleware or
Redis persistence. Use `Sidekiq::Testing.disable!` with a dedicated Redis instance
for uniqueness tests. Ordinary jobs retain Sidekiq's fake/inline behavior.

## Management API support

| Operation | Contract |
|---|---|
| Inflight `recover_orphan` | Exact atomic owner-heartbeat recheck; alive/unknown state never permits takeover. Same recovery budget/Dead policy as automatic recovery. |
| Runtime Poller promotion from Scheduled or Retry | Supported. The exact source member moves atomically to Ready without producer middleware or identity evaluation. Retry promotion adds redelivery metadata; Scheduled promotion preserves it. |
| `SortedEntry#add_to_queue` or `#retry` for a unique AJ Scheduled entry | Rejected before Sidekiq removes the source, even for a unique AJ retry in Scheduled. Let the runtime Poller promote it when due. Ordinary Scheduled entries retain native behavior. |
| `ScheduledSet#retry_all` / inherited bulk Ruby management APIs | Outside the runtime management contract. They bypass the per-entry guard and metadata boundary; applications must not expose them. No all-or-nothing management batch is promised. |
| Per-entry native Retry or Dead `#retry` / `#add_to_queue` | Supported for native/AJ envelopes. Mark redelivery and preserve the original identity; bypass fresh reservation admission through this explicit boundary, including entries without `retry_count`. `#retry` decrements an existing counter as upstream does; no counter is fabricated. |
| Official Dead Delete | Supported native deletion. It does not release runtime locks or delete a newer job's reservation. |
| Queue or SortedEntry delete; Queue/SortedSet clear | Native data operations only. Enqueue/runtime locks are not repaired or removed; an orphaned reservation can remain until TTL expiry. |
| SortedEntry reschedule | Native score change only. The original reservation TTL is not extended/recomputed; postponing beyond it can admit duplicates before due time. |

Per-entry Retry/Dead management retains Sidekiq's **non-atomic remove-then-push**
semantics. Middleware rejection or Redis failure after removal can lose the
source; metadata wiring does not turn this into an atomic runtime move. Manual
retry does not create/extend a reservation and cannot clear a newer JID's lock.
Direct Redis writes or user-supplied retry/continuation markers are not supported
ways to bypass admission.

## Metadata and events

`JobMetadata` keeps delivery facts under `sidekiq_runtime`, separate from business
arguments. `ExecutionContext` exposes whether a delivery was recovered.

Set `SidekiqRuntime.event_handler` to a callable accepting an event name and payload
to observe runtime events. `Telemetry.perform(payload, queue: physical_queue)`
publishes attempt timing and outcome while preserving the business block's result
or exception. Event construction and subscriber errors are isolated at `publish`.

## Development

From `goat-sidekiq-runtime/`, install the locked dependencies with `bundle install`.
Run the foundation checks:

```sh
bundle exec rspec spec/unit/storage_spec.rb spec/unit/queue_directory_spec.rb \
  spec/unit/redis_scope_spec.rb spec/unit/telemetry_spec.rb
bundle exec ruby -Ilib -rgoat_sidekiq_runtime -e 'puts "runtime loaded"'
bundle exec rubocop lib goat_sidekiq_runtime.gemspec Gemfile
gem build goat_sidekiq_runtime.gemspec
```

The test helper restricts Redis to a dedicated loopback endpoint outside port
6379 (`GOAT_RUNTIME_TEST_REDIS_URL`, default `redis://127.0.0.1:17779/15`). The
foundation examples use doubles and pool scoping and do not issue Redis commands.
With that dedicated test Redis running, verify the claim protocol and rate limits:

```sh
GOAT_RUNTIME_TEST_REDIS_URL=redis://127.0.0.1:17779/15 \
  bundle exec rspec spec/unit/inflight_store_spec.rb \
  spec/redis/queue_rate_limit_redis_contract_spec.rb
```

Verify fetching, recovery, heartbeat ownership and terminal races:

```sh
GOAT_RUNTIME_TEST_REDIS_URL=redis://127.0.0.1:17779/15 \
  bundle exec rspec spec/unit/reliable_fetch_spec.rb \
  spec/unit/recovery_spec.rb spec/unit/recovery_state_spec.rb \
  spec/unit/inflight_collection_spec.rb \
  spec/redis/process_heartbeat_contract_spec.rb \
  spec/redis/safety_terminal_race_spec.rb
```

Verify the native installation and lifecycle paths:

```sh
GOAT_RUNTIME_TEST_REDIS_URL=redis://127.0.0.1:17779/15 \
  bundle exec rspec spec/unit/queue_configuration_spec.rb \
  spec/unit/atomic_scheduled_enqueuer_spec.rb spec/unit/queue_keys_spec.rb \
  spec/unit/server_configurator_spec.rb spec/unit/client_push_boundary_spec.rb \
  spec/contracts/client_push_boundary_native_producer_contract_spec.rb \
  spec/contracts/native_retry_contract_spec.rb \
  spec/contracts/weighted_native_job_contract_spec.rb \
  spec/contracts/processor_diagnostics_contract_spec.rb \
  spec/contracts/processor_reliable_fetch_contract_spec.rb \
  spec/contracts/sorted_entry_guard_contract_spec.rb
```

Verify ordinary Active Job enqueue, execution and physical retry routing:

```sh
GOAT_RUNTIME_TEST_REDIS_URL=redis://127.0.0.1:17779/15 \
  bundle exec rspec spec/contracts/active_job_adapter_contract_spec.rb \
  spec/contracts/active_job_execution_contract_spec.rb \
  spec/contracts/active_job_queue_routing_contract_spec.rb
```

Verify the complete uniqueness lifecycle (with the same dedicated test Redis):

```sh
GOAT_RUNTIME_TEST_REDIS_URL=redis://127.0.0.1:17779/15 \
  bundle exec rspec spec/unit/atomic_enqueue_spec.rb \
  spec/redis/atomic_enqueue_redis_contract_spec.rb \
  spec/unit/until_and_while_executing_spec.rb spec/unit/server_middleware_spec.rb \
  spec/unit/death_handler_spec.rb spec/contracts/active_job_uniqueness_contract_spec.rb \
  spec/contracts/uniqueness_testing_mode_contract_spec.rb
```

All production runtime components are now present. Real-process acceptance and
its CLI/Redis fixtures enter in PR-07 of this runtime stack.
