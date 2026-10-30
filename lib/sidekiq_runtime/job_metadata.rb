# frozen_string_literal: true

module SidekiqRuntime
  module JobMetadata
    KEY = 'sidekiq_runtime'
    ACTIVE_JOB_WRAPPER = 'SidekiqRuntime::ActiveJob::JobWrapper'
    private_constant :ACTIVE_JOB_WRAPPER

    def self.context(payload)
      ExecutionContext.new(redelivered: payload.fetch(KEY, {})['redelivered'])
    end

    def self.uniqueness(payload)
      payload.dig(KEY, 'uniqueness')
    end

    def self.active_job?(payload)
      payload['class'].to_s == ACTIVE_JOB_WRAPPER
    end

    def self.with_redelivery(payload)
      result = payload.merge(KEY => payload.fetch(KEY, {}).merge('redelivered' => true))
      sync_active_job(result)
    end

    # Outer transport metadata is authoritative after native management/replay.
    # Keep the serialized AJ copy in sync before it reaches Base.execute.
    def self.sync_active_job(payload)
      return payload unless active_job?(payload)

      data = payload.fetch('args').fetch(0).merge(KEY => payload.fetch(KEY, {}))
      payload.merge('args' => [data])
    end
  end
end
