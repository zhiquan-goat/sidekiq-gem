# GOAT Sidekiq Runtime

Shared runtime components for Sidekiq OSS 7.3.10. Requires Ruby 3.4.7 or later.

This foundation provides storage naming, queue discovery, explicit Redis pool
scoping, delivery metadata, and telemetry. Producer and server installation will
be introduced in PR-04 of the seven-part runtime stack.

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

The upcoming claim protocol will associate each delivery with a claim and a
process owner so that recovery can distinguish stale acknowledgements. Atomic
claim transitions arrive in PR-02, with fetching and recovery in PR-03.

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
