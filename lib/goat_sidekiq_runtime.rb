# frozen_string_literal: true

require 'sidekiq'

# Shared contracts for the standalone Sidekiq runtime components.
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
  end
end

require_relative 'sidekiq_runtime/storage'
require_relative 'sidekiq_runtime/queue_keys'
require_relative 'sidekiq_runtime/queue_directory'
require_relative 'sidekiq_runtime/execution_context'
require_relative 'sidekiq_runtime/job_metadata'
require_relative 'sidekiq_runtime/telemetry'
require_relative 'sidekiq_runtime/redis_scope'
