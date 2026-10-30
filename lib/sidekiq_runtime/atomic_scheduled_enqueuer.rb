# frozen_string_literal: true

require 'sidekiq/component'
require 'sidekiq/scheduled'
require_relative 'queue_keys'
require_relative 'queue_directory'
require_relative 'telemetry'

module SidekiqRuntime
  # One Poller sees every queue in the shared Retry/Scheduled sets. Queue ownership
  # is global; due work is moved without running producer middleware again.
  class AtomicScheduledEnqueuer
    include Sidekiq::Component

    EVENT_NAME = 'promotion.sidekiq_runtime'
    DEFAULT_BUDGET = 100

    ATOMIC_PROMOTE = <<~LUA
      local function key_type(key)
        local result = redis.call('TYPE', key)
        if type(result) == 'table' then
          return result.ok
        end
        return result
      end

      local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
      if not score or tonumber(score) > tonumber(ARGV[2]) then
        return 0
      end
      local destination_type = key_type(KEYS[3])
      local queues_type = key_type(KEYS[2])
      local directory_type = key_type(KEYS[4])
      if destination_type ~= 'none' and destination_type ~= 'list' then
        return redis.error_reply('destination queue must be a list')
      end
      if queues_type ~= 'none' and queues_type ~= 'set' then
        return redis.error_reply('queues key must be a set')
      end
      if directory_type ~= 'none' and directory_type ~= 'set' then
        return redis.error_reply('queue directory must be a set')
      end

      redis.call('SADD', KEYS[4], ARGV[3])
      redis.call('SADD', KEYS[2], ARGV[3])
      redis.call('LPUSH', KEYS[3], ARGV[4])
      redis.call('ZREM', KEYS[1], ARGV[1])
      return 1
    LUA

    private_constant :ATOMIC_PROMOTE, :DEFAULT_BUDGET

    def initialize(config, clock: -> { Time.now.to_f }, budget: DEFAULT_BUDGET)
      @config = config
      @clock = clock
      @budget = Integer(budget)
      raise ArgumentError, 'promotion budget must be positive' unless @budget.positive?

      @offsets = Hash.new(0)
      @done = false
    end

    def enqueue_jobs(sorted_sets = Sidekiq::Scheduled::SETS)
      sorted_sets.each { |source_set| promote_due(source_set) }
    end

    def terminate
      @done = true
    end

    private

    def promote_due(source_set)
      @budget.times do
        return if @done

        now = @clock.call
        member, score = due_member(source_set, now)
        unless member
          @offsets[source_set] = 0
          return
        end
        # Retain the offset between Poller ticks: a large invalid prefix must not
        # starve valid work beyond this tick's budget. Removals shift the next
        # member into this slot; retained members advance the slot instead.
        removed = promote_member(source_set, member, score, now)
        @offsets[source_set] += 1 unless removed
      end
      publish(source_set, 'scan', 'budget_exhausted')
    end

    def due_member(source_set, now)
      values = redis do |connection|
        connection.call('ZRANGE', source_set, '-inf', now, 'BYSCORE',
          'LIMIT', @offsets[source_set], 1, 'WITHSCORES')
      end
      return [nil, nil] if values.empty?

      values.first.is_a?(Array) ? values.first : values
    end

    def promote_member(source_set, member, score, now)
      payload = Sidekiq.load_json(member)
      unless valid_envelope?(payload)
        publish(source_set, 'unclassified', 'invalid', payload)
        return false
      end
      ready = payload.except('at').merge('enqueued_at' => now)
      ready = JobMetadata.with_redelivery(ready) if source_set == 'retry'
      moved = evaluate_once(ATOMIC_PROMOTE,
        keys: [source_set, 'queues', QueueKeys.source_queue(payload['queue']), QueueKeys.directory_key],
        arguments: [member, now, payload['queue'], Sidekiq.dump_json(ready)])
      outcome = moved == 1 ? 'success' : 'race_lost'
      publish(source_set, 'atomic', outcome, payload, due_age_ms: [(now - Float(score)) * 1_000, 0].max)
      true
    rescue JSON::ParserError, TypeError => exception
      publish(source_set, 'unclassified', 'invalid', payload, exception: exception)
      false
    rescue RedisClient::ConnectionError, RedisClient::ProtocolError => exception
      publish(source_set, 'atomic', 'unknown', payload, exception: exception)
      raise
    rescue => exception
      # A poison destination must not prevent other due queues from progressing.
      # If Lua partially wrote, its ordering still retains source or destination.
      publish(source_set, 'atomic', 'error', payload, exception: exception)
      false
    end

    def valid_envelope?(payload)
      payload.is_a?(Hash) && QueueKeys.valid_queue?(payload['queue']) && payload['retry_queue'].nil? &&
        payload['class'].is_a?(String) && payload['class'].present? && payload['args'].is_a?(Array)
    end

    def evaluate_once(script, keys:, arguments:)
      redis { |connection| connection.call_once('EVAL', script, keys.length, *keys, *arguments) }
    end

    def publish(source_set, mode, outcome, payload = nil, **attributes)
      metadata = payload.is_a?(Hash) ? payload : {}
      SidekiqRuntime.publish(EVENT_NAME) { Telemetry.fields(metadata).merge(
        source_set: source_set,
        mode: mode,
        outcome: outcome
      ).merge(attributes) }
    end
  end
end
