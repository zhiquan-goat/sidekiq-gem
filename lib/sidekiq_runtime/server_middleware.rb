# frozen_string_literal: true

module SidekiqRuntime
  # Prepares every Active Job for execution.
  class ServerMiddleware
    include ::Sidekiq::ServerMiddleware

    def call(job_instance, job_payload, queue)
      prepare_active_job(job_instance, job_payload, queue)
      yield
    end

    private

    def prepare_active_job(job_instance, job_payload, queue)
      return unless JobMetadata.active_job?(job_payload)

      job_payload['args'] = JobMetadata.sync_active_job(job_payload).fetch('args')
      job_instance.runtime_physical_queue = queue.dup.freeze
    end
  end
end
