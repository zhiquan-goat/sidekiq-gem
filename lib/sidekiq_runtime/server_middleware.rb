# frozen_string_literal: true

require_relative '../sidekiq_uniqueness/atomic_enqueue'

module SidekiqRuntime
  # Prepares every Active Job for execution and applies optional uniqueness ownership.
  class ServerMiddleware
    include ::Sidekiq::ServerMiddleware

    class ContinuationPersistenceError < StandardError; end

    def call(job_instance, job_payload, queue)
      prepare_active_job(job_instance, job_payload, queue)
      lock = SidekiqUniqueness.lock_for(
        job_payload: job_payload,
        physical_queue: queue
      )
      return yield unless lock

      jid = job_payload.fetch('jid')
      enqueue_owner = jid
      execution_owner = begin_execution(lock, jid: jid)
      unless execution_owner
        defer_runtime_conflict(job_payload, lock, queue)
        return nil
      end

      yield
    rescue ::Sidekiq::Shutdown
      restore_enqueue(lock, enqueue_owner) if execution_owner
      raise
    ensure
      finish_execution(lock, execution_owner) if execution_owner
    end

    private

    def prepare_active_job(job_instance, job_payload, queue)
      return unless JobMetadata.active_job?(job_payload)

      job_payload['args'] = JobMetadata.sync_active_job(job_payload).fetch('args')
      job_instance.runtime_physical_queue = queue.dup.freeze
    end

    def begin_execution(lock, jid:)
      redis { |connection| lock.begin_execution(redis: connection, jid: jid) }
    end

    def restore_enqueue(lock, enqueue_owner)
      redis { |connection| lock.reserve_enqueue(redis: connection, owner: enqueue_owner) }
    rescue => exception
      logger.warn("[SidekiqRuntime] Failed to restore enqueue lock: #{exception.message}")
      false
    end

    def finish_execution(lock, execution_owner)
      redis { |connection| lock.finish_execution(redis: connection, owner: execution_owner) }
    rescue => exception
      logger.warn("[SidekiqRuntime] Failed to release runtime lock: #{exception.message}")
      0
    end

    def defer_runtime_conflict(job_payload, lock, queue)
      jid = job_payload.fetch('jid')
      delay = lock.runtime_conflict_delay
      scheduled_payload = SidekiqUniqueness.mark_runtime_conflict_continuation(
        job_payload.merge('queue' => queue, 'at' => Time.now.to_f + delay)
      )
      scheduled_payload.delete('enqueued_at')
      result = SidekiqUniqueness::AtomicEnqueue.new(config.redis_pool).write(
        scheduled_payload,
        lock: lock,
        mode: :continuation
      )
      unless result == :accepted
        raise ContinuationPersistenceError, 'Runtime-conflict successor was not accepted by Redis'
      end

      SidekiqRuntime.publish('runtime_conflict.sidekiq_uniqueness') do
        lock.observability_payload.merge(jid: jid, scheduled_jid: jid, delay_seconds: delay,
          decision: 'rescheduled', physical_queue: queue, runtime_redis_key: lock.runtime_redis_key)
      end
    rescue SidekiqUniqueness::AtomicEnqueue::Rejected, SidekiqUniqueness::AtomicEnqueue::Unknown => exception
      raise ContinuationPersistenceError,
        "Runtime-conflict successor persistence failed: #{exception.message}",
        cause: exception
    end
  end
end
