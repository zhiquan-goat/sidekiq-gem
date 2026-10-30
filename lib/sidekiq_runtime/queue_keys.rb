# frozen_string_literal: true

require_relative 'storage'

module SidekiqRuntime
  # Builds and validates Redis keys for queues in the shared runtime namespace.
  module QueueKeys
    CONFIGURATION_MUTEX = Mutex.new

    class << self
      def storage = @storage || Storage.default

      def configure!(value)
        raise ArgumentError, 'storage must be SidekiqRuntime::Storage' unless value.is_a?(Storage)

        CONFIGURATION_MUTEX.synchronize do
          if @storage && @storage.namespace != value.namespace
            raise ArgumentError,
              "Sidekiq runtime storage namespace is already configured as #{@storage.namespace.inspect}"
          end

          @storage ||= value
        end
      end

      def valid_queue?(queue) = storage.valid_queue?(queue)
      def validate_queue!(queue) = storage.validate_queue!(queue)
      def prefix(queue) = storage.prefix(queue)
      def directory_key = storage.directory_key
      def unique_prefix = storage.unique_prefix
      def source_queue(queue) = "queue:#{validate_queue!(queue)}"
      def inflight_key(queue) = "#{prefix(queue)}:inflight"
      def rate_limit_key(queue) = "#{prefix(queue)}:rate_limit"
      def configured_queues(config) = config.queues.uniq
    end
  end
end
