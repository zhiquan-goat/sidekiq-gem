# frozen_string_literal: true

module SidekiqRuntime
  # Each tick advances the directory cursor and a bounded number of claims.
  # No business classes, WorkSet or ProcessSet reads participate in authorization.
  class OrphanReclaimer
    EVENT_NAME = 'orphan_reclaimer.sidekiq_runtime'

    def initialize(config, settings: RecoveryConfiguration.for(config))
      @config = config
      @settings = settings
      @directory = QueueDirectory.new(config)
      @cursor = '0'
      @pending = []
      @claim_scans = {}
      @mutex = Mutex.new
      @wake = ConditionVariable.new
      @stopping = false
    end

    def tick
      if @pending.empty?
        @cursor, @pending = @directory.scan(cursor: @cursor, count: @settings.queue_batch)
        @pending.uniq!
      end
      @pending.shift(@settings.queue_batch).each do |queue|
        store = store_for(queue)
        scan = (@claim_scans[queue] ||= {cursor: '0', pending: []})
        if scan[:pending].empty?
          scan[:cursor], scan[:pending] = store.scan(cursor: scan[:cursor], count: @settings.reclaim_batch)
        end
        scan[:pending].shift(@settings.reclaim_batch).each do |entry|
          state = store.owner_state(entry)
          recover(entry, store: store) if state[:known] && state[:present] && state[:owner_status] == 'owner_dead'
        end
        @claim_scans.delete(queue) if scan[:cursor].to_s == '0' && scan[:pending].empty?
      rescue => exception
        publish(operation: 'scan', outcome: 'unknown', physical_queue: queue, exception: exception)
      end
    rescue => exception
      publish(operation: 'directory_scan', outcome: 'unknown', exception: exception)
    end

    # Shared by automatic recovery and the authorized admin fallback. A submitted
    # snapshot/owner assertion never substitutes for the atomic Redis heartbeat check.
    def recover(entry, store: store_for(entry.physical_queue))
      plan = Recovery.for_orphan(entry.job, physical_queue: entry.physical_queue,
        max_recoveries: @settings.max_recoveries)
      outcome = store.reclaim_orphan(entry, job: plan.fetch(:job), destination: plan.fetch(:destination))
      result = {outcome: outcome}
      if %w[requeued dead].include?(outcome)
        result.merge!(reason: plan.fetch(:reason), recovery_count: plan.fetch(:recovery_count))
      end
      publish(**result, operation: 'recover', entry: entry)
      result
    rescue => exception
      # A lost write reply is unknown. Inspect the exact source; absence means
      # resolved, not proof of which ACK/reclaimer won. Never enqueue a new copy.
      state = begin
        store.owner_state(entry)
      rescue
        {known: false}
      end
      result = {outcome: 'unknown', current_state: state}
      publish(operation: 'recover', entry: entry, exception: exception, **result)
      result
    end

    def start
      @mutex.synchronize { @thread ||= Thread.new { run } }
      self
    end

    def stop
      @mutex.synchronize do
        @stopping = true
        @wake.broadcast
      end
    end

    private

    def run
      Thread.current.name = 'runtime-orphan-reclaimer'
      loop do
        break if @mutex.synchronize { @stopping }
        tick
        @mutex.synchronize { @wake.wait(@mutex, @settings.reclaim_interval) unless @stopping }
      end
    end

    def store_for(queue)
      InflightStore.new(@config, queue: queue)
    end

    def publish(entry: nil, exception: nil, **fields)
      SidekiqRuntime.publish(EVENT_NAME) do
        payload = Telemetry.fields(entry&.payload || {}, queue: entry&.physical_queue).merge(fields)
        payload[:entry_id] = entry.entry_id if entry
        payload[:exception] = [exception.class.name, exception.message] if exception
        payload
      end
    end
  end
end
