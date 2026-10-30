# frozen_string_literal: true

require_relative 'queue_keys'

module SidekiqRuntime
  # Persists queue ownership independently of Sidekiq's ephemeral
  # `queues` set, which Queue#clear removes.
  class QueueDirectory
    def initialize(config)
      @config = config
    end

    def queues
      directory_queues, native_queues = @config.redis do |redis|
        [redis.smembers(QueueKeys.directory_key), redis.smembers('queues')]
      end
      candidates = QueueKeys.configured_queues(queue_config) + native_queues + directory_queues
      candidates.uniq.select { |queue| QueueKeys.valid_queue?(queue) }
    end

    # SSCAN may return more than COUNT; the caller retains overflow and processes
    # at most its queue budget. Claims always register their queue atomically.
    def scan(cursor:, count:)
      @config.redis { |redis| redis.call('SSCAN', QueueKeys.directory_key, cursor, 'COUNT', count) }
    end

    private

    def queue_config
      @config == Sidekiq ? Sidekiq.default_configuration : @config
    end
  end
end
