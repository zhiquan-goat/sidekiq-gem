# frozen_string_literal: true

module SidekiqRuntime
  # Keep Sidekiq's remove-then-push management semantics. This is not an atomic
  # move: a push error can occur after the source has already been removed.
  module SortedEntryGuard
    def add_to_queue
      return super unless %w[retry dead].include?(parent.name)

      retry_original(decrement: false)
    end

    def retry
      return super unless %w[retry dead].include?(parent.name)

      retry_original(decrement: true)
    end

    private

    def retry_original(decrement:)
      remove_job do |message|
        payload = JobMetadata.with_redelivery(Sidekiq.load_json(message))
        payload['retry_count'] -= 1 if decrement && payload['retry_count']
        ::Sidekiq::Client.new.push_retry(payload)
      end
    end
  end
end
