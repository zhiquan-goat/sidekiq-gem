# frozen_string_literal: true

require 'sidekiq'
require 'sidekiq/api'
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/hash/except'
require 'active_support/core_ext/hash/keys'
require 'active_support/core_ext/object/deep_dup'
require 'active_support/core_ext/string/inflections'

# Installs and configures the standalone Sidekiq runtime components.
module SidekiqRuntime
  class << self
    attr_accessor :event_handler

    def publish(event, payload = nil)
      handler = event_handler
      return unless handler

      handler.call(event, block_given? ? yield : payload)
    rescue
      nil
    end

    def install_producer!(storage: QueueKeys.storage, event_handler: nil)
      QueueKeys.configure!(storage)
      self.event_handler = event_handler if event_handler
      Sidekiq::Client.prepend(ClientPushBoundary) unless Sidekiq::Client.ancestors.include?(ClientPushBoundary)
      Sidekiq::SortedEntry.prepend(SortedEntryGuard) unless Sidekiq::SortedEntry.ancestors.include?(SortedEntryGuard)
      true
    end

    def install_server!(config, redis_config:, storage: QueueKeys.storage, event_handler: nil, recovery: nil)
      install_producer!(storage: storage, event_handler: event_handler)
      ServerConfigurator.apply(config, redis_config: redis_config, recovery: recovery)
    end
  end
end

require_relative 'sidekiq_runtime/storage'
require_relative 'sidekiq_runtime/queue_keys'
require_relative 'sidekiq_runtime/queue_directory'
require_relative 'sidekiq_runtime/queue_rate_limit'
require_relative 'sidekiq_runtime/execution_context'
require_relative 'sidekiq_runtime/job_metadata'
require_relative 'sidekiq_runtime/telemetry'
require_relative 'sidekiq_runtime/inflight_store'
require_relative 'sidekiq_runtime/recovery'
require_relative 'sidekiq_runtime/recovery_state'
require_relative 'sidekiq_runtime/redis_scope'
require_relative 'sidekiq_runtime/reliable_fetch'
require_relative 'sidekiq_runtime/processor_boundary'
require_relative 'sidekiq_runtime/orphan_reclaimer'
require_relative 'sidekiq_runtime/atomic_scheduled_enqueuer'
require_relative 'sidekiq_runtime/client_push_boundary'
require_relative 'sidekiq_runtime/sorted_entry_guard'
require_relative 'sidekiq_runtime/queue_configuration'
require_relative 'sidekiq_runtime/server_configurator'
require_relative 'sidekiq_runtime/inflight_collection'
