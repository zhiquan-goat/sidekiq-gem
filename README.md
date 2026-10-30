# GOAT Sidekiq Runtime

Shared runtime components for Sidekiq OSS 7.3.10. Requires Ruby 3.4.7 or later.

These components provide storage naming, queue discovery, explicit Redis pool
scoping, delivery metadata, telemetry, atomic claims, shared queue rate limits,
reliable fetching, and process-heartbeat recovery.
Native producer/server installation, final queue validation, management retries
and atomic scheduled promotion are available. ActiveJob integration and uniqueness
enter in PR-05 and PR-06 of the seven-part runtime stack.

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
