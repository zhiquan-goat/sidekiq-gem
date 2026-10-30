# GOAT Sidekiq Runtime

Shared runtime components for Sidekiq OSS 7.3.10. Requires Ruby 3.4.7 or later.

These components provide storage naming, queue discovery, explicit Redis pool
scoping, delivery metadata, telemetry, atomic claims, and shared queue rate limits.
Producer and server installation will be introduced in PR-04 of the seven-part
runtime stack.

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
the transition. Fetching and automatic recovery arrive in PR-03.

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
