# frozen_string_literal: true

require_relative 'telemetry'

module SidekiqRuntime
  # Owns the single producer persistence boundary. Ordinary Sidekiq payloads
  # keep the upstream raw_push; retries retain their original physical queue.
  module ClientPushBoundary
    EVENT_NAME = 'sidekiq.notification.push_result'

    public_constant :EVENT_NAME

    # Retry intent and its original physical queue stay in memory. Management
    # retries default to the queue already stored on their transport payload.
    def push(item, runtime_retry = false, original_queue = nil)
      unless runtime_retry
        result = super(item)
        publish_filtered(item) unless result
        return result
      end

      normalized = normalize_item(item)
      original_queue = (original_queue || normalized.fetch('queue')).dup
      payload = middleware.invoke(item['class'], normalized, normalized['queue'], @redis_pool) { normalized }
      unless payload
        publish_filtered(normalized)
        return nil
      end

      if payload.fetch('queue') != original_queue
        raise ArgumentError, 'Retry must remain on its original physical queue'
      end
      verify_json(payload)
      raw_push([payload])
      payload.fetch('jid')
    end

    def push_retry(item, original_queue = nil)
      push(item, true, original_queue)
    end

    private

    def raw_push(payloads)
      # Validate after the entire middleware chain, including post-yield edits.
      payloads.each do |payload|
        QueueKeys.validate_queue!(payload['queue'])
        raise ArgumentError, 'retry_queue is not supported by the Sidekiq runtime' unless payload['retry_queue'].nil?
      end

      begin
        result = super
      rescue => exception
        payloads.each do |payload|
          publish_result(
            payload,
            status: 'error',
            delivery_outcome: 'unknown',
            exception: [exception.class.name, exception.message],
            exception_object: exception
          )
        end
        raise
      end

      payloads.each do |payload|
        publish_result(payload, status: 'success', delivery_outcome: 'redis_acknowledged')
      end
      result
    end

    def publish_filtered(item)
      publish_result(item, status: 'filtered', delivery_outcome: 'not_pushed')
    end

    def effective_queue(item)
      queue = item['queue']
      job_class = item['class']
      queue ||= job_class.get_sidekiq_options['queue'] if job_class.respond_to?(:get_sidekiq_options)
      queue.to_s.presence
    end

    def publish_result(payload, **attributes)
      SidekiqRuntime.publish(EVENT_NAME) do
        return unless payload.is_a?(Hash)
        return if payload.key?('retry_count')

        queue = effective_queue(payload)
        return unless queue

        Telemetry.fields(payload, queue: queue).merge(attributes)
      end
    end
  end
end
