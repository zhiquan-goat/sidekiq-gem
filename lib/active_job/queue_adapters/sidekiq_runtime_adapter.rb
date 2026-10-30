# frozen_string_literal: true

require 'goat_sidekiq_runtime/active_job'

module ActiveJob
  module QueueAdapters
    class SidekiqRuntimeAdapter
      def initialize(config: nil)
        @config = config
      end

      def enqueue_after_transaction_commit?
        true
      end

      def enqueue(job)
        push(job)
      end

      def enqueue_at(job, timestamp)
        push(job, at: timestamp)
      end

      def enqueue_all(jobs)
        if jobs.any?(&:runtime_uniqueness_options)
          raise SidekiqUniqueness::UnsupportedBulk, 'Unique Active Job bulk is not supported; enqueue jobs individually'
        end

        # Same per-job results and error handling as AJ's adapter fallback.
        # No callbacks, transaction deferral, or cross-adapter atomicity.
        jobs.each do |job|
          job.successfully_enqueued = false
          job.scheduled_at ? enqueue_at(job, job.scheduled_at.to_f) : enqueue(job)
          job.successfully_enqueued = true
        rescue EnqueueError => exception
          job.enqueue_error = exception
        end
        jobs.count(&:successfully_enqueued?)
      end

      private

      def push(job, at: nil)
        request = job.__send__(:runtime_retry_request)
        job.provider_job_id = nil
        payload = {'class' => SidekiqRuntime::ActiveJob::JobWrapper,
                   'wrapped' => job.class.name, 'queue' => job.queue_name, 'args' => [job.serialize]}
        payload[SidekiqRuntime::JobMetadata::KEY] = payload['args'][0].fetch(SidekiqRuntime::JobMetadata::KEY)
        payload['at'] = at if at
        jid = if request
          client.push_retry(payload, request.physical_queue)
        else
          client.push(payload)
        end
        unless jid
          raise EnqueueError, 'Sidekiq runtime did not accept the Active Job enqueue'
        end

        job.provider_job_id = jid
        request&.confirm_write!
        jid
      end

      def client
        config = @config || Thread.current[:sidekiq_capsule] || Sidekiq.default_configuration
        pool = if @config
          SidekiqRuntime::RedisScope.pool(@config)
        else
          Thread.current[:sidekiq_redis_pool] || config.redis_pool
        end
        # AJ owns after-commit. Never use TransactionAwareClient here: its
        # preallocated JID does not prove Client accepted the actual write.
        Sidekiq::Client.new(config: config, pool: pool)
      end
    end
  end
end
