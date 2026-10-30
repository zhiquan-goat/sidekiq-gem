# frozen_string_literal: true

module SidekiqRuntime
  # Replays original business arguments and producer identity without loading
  # the business class or calling a recovery hook. Claim movement stays with
  # atomic store; normal shutdown does not spend the crash recovery budget.
  module Recovery
    def self.for_orphan(raw_job, physical_queue:, max_recoveries:)
      payload = Sidekiq.load_json(raw_job)
      valid = payload.is_a?(Hash) && payload['class'].is_a?(String) && !payload['class'].empty? &&
        payload['args'].is_a?(Array) && payload['queue'] == physical_queue && payload['retry_queue'].nil?
      metadata = payload.is_a?(Hash) ? payload[JobMetadata::KEY] : nil
      count = metadata.is_a?(Hash) ? metadata.fetch('recovery_count', 0) : 0
      valid &&= (metadata.nil? || metadata.is_a?(Hash)) && count.is_a?(Integer) && count >= 0
      reason = if !valid
        'invalid_protocol'
      elsif count >= max_recoveries
        'recovery_exhausted'
      end
      if reason
        dead = payload.is_a?(Hash) ? payload.merge(
          'error_class' => 'SidekiqRuntime::RecoveryError', 'error_message' => reason
        ) : payload
        return {job: Sidekiq.dump_json(dead), destination: 'dead', reason: reason, recovery_count: count}
      end

      metadata = (metadata || {}).merge('redelivered' => true, 'recovery_count' => count + 1)
      recovered = JobMetadata.sync_active_job(payload.merge(JobMetadata::KEY => metadata))
      {job: Sidekiq.dump_json(recovered), destination: 'requeued', reason: 'owner_dead', recovery_count: count + 1}
    rescue JSON::ParserError, TypeError, KeyError, NoMethodError
      {job: raw_job, destination: 'dead', reason: 'invalid_protocol', recovery_count: nil}
    end

    def self.prepare(raw_job)
      payload = Sidekiq.load_json(raw_job)
      unless payload.is_a?(Hash) && payload['class'].is_a?(String) && payload['args'].is_a?(Array)
        raise ArgumentError, 'Cannot recover an invalid Sidekiq envelope'
      end
      unless payload['retry_queue'].nil?
        raise ArgumentError, 'Recovery must not set retry_queue'
      end

      Sidekiq.dump_json(JobMetadata.with_redelivery(payload))
    end
  end
end
