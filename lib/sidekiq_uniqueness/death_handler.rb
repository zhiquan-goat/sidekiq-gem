# frozen_string_literal: true

module SidekiqUniqueness
  # Releases the final enqueue reservation after Sidekiq exhausts retries.
  class DeathHandler
    def initialize(config)
      @config = config
    end

    def call(job_payload, _exception)
      lock = SidekiqUniqueness.lock_for(
        job_payload: job_payload,
        physical_queue: job_payload.fetch('queue')
      )
      return unless lock

      owner = job_payload.fetch('jid')
      @config.redis do |redis|
        lock.release_enqueue(redis: redis, owner: owner)
      end
    rescue => exception
      @config.logger.warn(
        "[SidekiqUniqueness] Failed to release dead job enqueue lock: #{exception.message}"
      )
      0
    end
  end
end
