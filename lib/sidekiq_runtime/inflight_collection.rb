# frozen_string_literal: true

require_relative 'inflight_store'
require_relative 'queue_directory'
require_relative 'queue_keys'

module SidekiqRuntime
  # Presents all reliable queues as one read/admin surface while
  # keeping every queue's durable inflight ZSET and atomic requeue operation
  # independent.
  class InflightCollection
    MAX_BROWSE_WINDOW = 1_000
    private_constant :MAX_BROWSE_WINDOW

    def initialize(config:, queues: nil)
      @config = config
      queues ||= QueueDirectory.new(config).queues
      @stores_by_source_queue = queues.uniq.to_h do |queue|
        store = InflightStore.new(config, queue: queue)
        [store.source_queue, store]
      end.freeze
    end

    def size
      stores.sum(&:size)
    end

    def entries(offset: 0, limit: 100)
      offset = Integer(offset)
      limit = Integer(limit)
      raise ArgumentError, 'offset must not be negative' if offset.negative?
      raise ArgumentError, 'limit must be positive' unless limit.positive?

      requested_count = offset + limit
      if requested_count > MAX_BROWSE_WINDOW
        raise ArgumentError, "inflight browsing is limited to the first #{MAX_BROWSE_WINDOW} entries"
      end

      stores
        .flat_map { |store| store.entries(offset: 0, limit: requested_count) }
        .sort_by { |entry| [entry.claimed_at, entry.source_queue, entry.member] }
        .slice(offset, limit) || []
    end

    def find_entry(entry_id, physical_queue:)
      store = @stores_by_source_queue["queue:#{physical_queue}"]
      store&.find_entry(entry_id)
    end

    def recover_orphan(entry)
      OrphanReclaimer.new(@config).recover(entry, store: store_for(entry))
    end

    def state(entry)
      RecoveryState.classify(entry, snapshot: RecoveryState.snapshot(entry, store: store_for(entry)))
    end

    def snapshots
      stores.to_h do |store|
        value = begin
          store.snapshot
        rescue
          {known: false}
        end
        [store.physical_queue, value]
      end
    end

    private

    def stores
      @stores_by_source_queue.values
    end

    def store_for(entry)
      @stores_by_source_queue.fetch(entry.source_queue)
    rescue KeyError
      raise ArgumentError, "Inflight entry belongs to an unknown queue: #{entry.source_queue}"
    end
  end
end
