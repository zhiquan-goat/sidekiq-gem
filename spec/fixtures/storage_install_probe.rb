# frozen_string_literal: true

require 'json'
require 'goat_sidekiq_runtime'

queue = 'storage_install_probe'
redis_config = {url: 'redis://127.0.0.1:1/15'}
config = Sidekiq::Config.new(
  death_handlers: [],
  lifecycle_events: Sidekiq::Config::DEFAULTS.fetch(:lifecycle_events).transform_values { [] }
)
config[:queues] = [queue]
config.queues = [queue]

lazy_default = SidekiqRuntime::QueueKeys.storage
first = SidekiqRuntime::Storage.new(namespace: 'storage_probe_a:srt:v1')
equivalent = SidekiqRuntime::Storage.new(namespace: first.namespace)
different = SidekiqRuntime::Storage.new(namespace: 'storage_probe_b:srt:v1')
first_handler = ->(*) {}
equivalent_handler = ->(*) {}
rejected_handler = ->(*) {}

SidekiqRuntime.install_server!(
  config,
  redis_config: redis_config,
  storage: first,
  event_handler: first_handler
)
capsule = config.default_capsule
fetcher = capsule.fetcher
store_key = fetcher.stores_by_queue.fetch(queue).inflight_key

SidekiqRuntime.install_server!(
  config,
  redis_config: redis_config,
  storage: equivalent,
  event_handler: equivalent_handler
)
before_conflict = {
  fetch_class: config[:fetch_class].name,
  scheduled_class: config[:scheduled_enq].name,
  client: config.client_middleware.map { |entry| entry.klass.name },
  server: config.server_middleware.map { |entry| entry.klass.name }
}

error = begin
  SidekiqRuntime.install_server!(
    config,
    redis_config: redis_config,
    storage: different,
    event_handler: rejected_handler
  )
  nil
rescue => exception
  exception
end

after_conflict = {
  fetch_class: config[:fetch_class].name,
  scheduled_class: config[:scheduled_enq].name,
  client: config.client_middleware.map { |entry| entry.klass.name },
  server: config.server_middleware.map { |entry| entry.klass.name }
}

# The parent spec parses this fixture's stdout as its subprocess protocol.
$stdout.write(JSON.generate( # rubocop:disable Rails/Output
  lazy_default_did_not_pin: lazy_default.namespace == 'srt:v1' &&
    SidekiqRuntime::QueueKeys.storage.equal?(first),
  same_namespace_succeeded: SidekiqRuntime.event_handler.equal?(equivalent_handler),
  different_namespace_error: error&.class&.name,
  storage_still_first_object: SidekiqRuntime::QueueKeys.storage.equal?(first),
  event_handler_unchanged: SidekiqRuntime.event_handler.equal?(equivalent_handler),
  fetcher_unchanged: capsule.fetcher.equal?(fetcher),
  configuration_unchanged: before_conflict == after_conflict,
  store_key_unchanged: fetcher.stores_by_queue.fetch(queue).inflight_key == store_key &&
    SidekiqRuntime::QueueKeys.prefix(queue) == 'storage_probe_a:srt:v1:queue:storage_install_probe'
))
