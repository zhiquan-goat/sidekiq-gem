# frozen_string_literal: true

require_relative 'telemetry'
require_relative '../sidekiq_uniqueness/atomic_enqueue'

module SidekiqRuntime
  # Owns the single producer persistence boundary. AJ unique jobs
  # use AtomicEnqueue; every other Sidekiq payload keeps the upstream raw_push.
  # Client middleware may change args and queue in its returned payload, but the
  # job class that selected this boundary must remain stable across the chain.
  module ClientPushBoundary
    EVENT_NAME = 'sidekiq.notification.push_result'

    public_constant :EVENT_NAME

    # Retry intent and its original physical queue stay in memory. Management
    # retries default to the queue already stored on their transport payload.
    def push(item, runtime_retry = false, original_queue = nil)
      if defined?(::Sidekiq::Testing) && ::Sidekiq::Testing.enabled? && SidekiqUniqueness.unique_job?(item)
        raise ArgumentError,
          'Unique jobs do not support Sidekiq::Testing.fake! or inline!; use Sidekiq::Testing.disable! with a dedicated Redis instance'
      end

      initial_unique_enqueue = !runtime_retry && initial_unique_enqueue?(item)
      unless initial_unique_enqueue || runtime_retry
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

      if runtime_retry
        if payload.fetch('queue') != original_queue
          raise ArgumentError, 'Retry must remain on its original physical queue'
        end
        verify_json(payload)
        raw_push([payload])
        payload.fetch('jid')
      else
        verify_json(payload)
        persist_unique(payload)
      end
    rescue SidekiqUniqueness::AtomicEnqueue::Rejected => exception
      publish_result(
        payload || normalized || item,
        status: 'rejected',
        delivery_outcome: 'not_pushed',
        rejection_code: exception.code
      )
      raise
    rescue SidekiqUniqueness::AtomicEnqueue::Unknown => exception
      publish_result(
        payload || normalized || item,
        status: 'error',
        delivery_outcome: 'unknown',
        exception: [exception.class.name, exception.message],
        exception_object: exception
      )
      raise
    rescue => exception
      if initial_unique_enqueue
        publish_result(
          payload || normalized || item,
          status: 'error',
          delivery_outcome: 'not_pushed',
          exception: [exception.class.name, exception.message],
          exception_object: exception
        )
      end
      raise
    end

    def push_retry(item, original_queue = nil)
      push(item, true, original_queue)
    end

    def push_bulk(items)
      if SidekiqUniqueness.unique_job?(items)
        raise SidekiqUniqueness::UnsupportedBulk, 'Unique push_bulk is not supported; enqueue jobs individually'
      end

      super
    end

    private

    def raw_push(payloads)
      # Validate after the entire middleware chain, including post-yield edits.
      # Retry/continuation bypasses skip uniqueness, never transport validation.
      payloads.each do |payload|
        QueueKeys.validate_queue!(payload['queue'])
        raise ArgumentError, 'retry_queue is not supported by the Sidekiq runtime' unless payload['retry_queue'].nil?
        unique = JobMetadata.uniqueness(payload)
        if unique && unique.fetch('queue') != payload['queue']
          raise ArgumentError, 'Unique job must remain on its original physical queue'
        end
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

    def persist_unique(payload)
      lock = SidekiqUniqueness.lock_for(
        job_payload: payload,
        physical_queue: payload.fetch('queue')
      )
      unless lock
        raw_push([payload])
        return payload.fetch('jid')
      end

      result = SidekiqUniqueness::AtomicEnqueue.new(@redis_pool).write(payload, lock: lock)
      if result == :duplicate
        publish_result(
          payload,
          status: 'rejected',
          delivery_outcome: 'duplicate_rejected',
          rejection_code: :duplicate_rejected
        )
        return nil
      end

      publish_result(payload, status: 'success', delivery_outcome: 'redis_acknowledged')
      payload.fetch('jid')
    end

    def initial_unique_enqueue?(item)
      item.is_a?(Hash) &&
        !item.key?('retry_count') &&
        !SidekiqUniqueness.internal_continuation?(item) &&
        SidekiqUniqueness.unique_job?(item)
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
        return if payload.key?('retry_count') || SidekiqUniqueness.internal_continuation?(payload)

        queue = effective_queue(payload)
        return unless queue

        Telemetry.fields(payload, queue: queue).merge(attributes)
      end
    end
  end
end
