# frozen_string_literal: true

module SidekiqRuntime
  # Normalizes runtime job telemetry into stable event fields.
  module Telemetry
    def self.fields(payload, queue: nil)
      payload = {} unless payload.is_a?(Hash)
      fields = {job_class: (payload['wrapped'] || payload['class']).to_s, jid: payload['jid'], physical_queue: queue || payload['queue']}
      if payload['wrapped'] && payload['args'].is_a?(Array) && payload['args'].first.is_a?(Hash)
        fields[:job_id] = payload['args'].first['job_id']
      end
      fields[:retry_count] = payload['retry_count'] if payload.key?('retry_count')
      fields[:job_payload] = payload if SidekiqRuntime.event_handler
      fields
    end

    def self.perform(payload, queue:)
      started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      enqueued_at = Time.now.to_f
      SidekiqRuntime.publish('sidekiq.notification.attempt_started') do
        fields(payload, queue: queue).merge(
          enqueued_duration_ms: elapsed_ms(payload['enqueued_at'], now: enqueued_at),
          first_enqueued_duration_ms: elapsed_ms(payload['first_enqueued_at'], now: enqueued_at)
        ).compact
      end
      result = yield
      success = true
      result
    rescue => exception
      raise
    ensure
      SidekiqRuntime.publish('sidekiq.notification.attempt_finished') do
        attributes = fields(payload, queue: queue).merge(
          enqueued_duration_ms: elapsed_ms(payload['enqueued_at'], now: enqueued_at),
          first_enqueued_duration_ms: elapsed_ms(payload['first_enqueued_at'], now: enqueued_at),
          success: !!success,
          duration_ms: (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at) * 1000)
        attributes.merge!(exception: [exception.class.name, exception.message], exception_object: exception) if exception
        attributes.compact
      end
    end

    def self.elapsed_ms(value, now:)
      timestamp = Float(value)
      return unless timestamp.finite? && !timestamp.negative?

      duration = (now - timestamp) * 1000
      [duration, 0].max if duration.finite?
    rescue ArgumentError, RangeError, TypeError
      nil
    end
  end
end
