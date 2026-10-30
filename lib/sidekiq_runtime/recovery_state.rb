# frozen_string_literal: true

module SidekiqRuntime
  # A display snapshot is informational; mutations always recheck in Redis.
  module RecoveryState
    def self.snapshot(entry, store:)
      store.owner_state(entry)
    rescue => exception
      SidekiqRuntime.publish(OrphanReclaimer::EVENT_NAME, operation: 'snapshot', outcome: 'unknown',
        physical_queue: entry.physical_queue, exception: [exception.class.name, exception.message])
      {known: false}
    end

    def self.classify(entry, snapshot:)
      status = if !snapshot[:known]
        'unknown'
      elsif !snapshot[:present]
        'missing'
      elsif snapshot[:owner_status] == 'owner_missing'
        'owner_missing'
      elsif snapshot[:owner_identity] != entry.owner_identity
        'owner_changed'
      else
        snapshot.fetch(:owner_status, 'unknown')
      end
      metadata = entry.payload&.fetch(JobMetadata::KEY, nil)
      {
        entry: entry, status: status, recoverable: status == 'owner_dead',
        heartbeat_ttl_seconds: snapshot[:heartbeat_ttl_seconds], redis_now: snapshot[:redis_now],
        recovery_count: metadata.is_a?(Hash) ? metadata.fetch('recovery_count', 0) : 0
      }
    end
  end
end
