# frozen_string_literal: true

module SidekiqRuntime
  module ActiveJob
    class JobWrapper
      include ::Sidekiq::Job
      sidekiq_options retry: 0, dead: true

      attr_writer :runtime_physical_queue

      def perform(data)
        # Direct/testing execution has no server middleware transport context.
        execution = Execution.new(physical_queue: @runtime_physical_queue || data.fetch('queue_name'))
        fields = {job_class: data['job_class'], job_id: data['job_id'], jid: jid,
                  physical_queue: execution.physical_queue}
        outcome = :failed
        begin
          result = ::ActiveJob::Base.execute(data.merge(
            'provider_job_id' => jid, Execution::MEMORY_KEY => execution
          ))
          outcome = execution.finalize_outcome!
          result
        rescue RetryPersistenceError
          outcome = :retry_unconfirmed
          raise
        ensure
          SidekiqRuntime.publish('sidekiq.notification.active_job_finished', fields.merge(outcome: outcome))
        end
      end
    end
  end
end
