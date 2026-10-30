# frozen_string_literal: true

require 'sidekiq/client'

module SidekiqRuntime
  # Pins a complete management operation to one explicit Sidekiq Config's own
  # Redis pool. local_redis_pool is a deliberately narrow Sidekiq 7.3.10 seam.
  module RedisScope
    module_function

    def pool(config)
      raise ArgumentError, 'config must be a Sidekiq::Config' unless config.is_a?(Sidekiq::Config)

      config.__send__(:local_redis_pool)
    end

    def with(config)
      Sidekiq::Client.via(pool(config)) { yield }
    end
  end
end
