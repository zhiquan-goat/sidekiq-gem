# frozen_string_literal: true

require 'sidekiq/component'
require_relative 'inflight_store'
require_relative 'queue_keys'
require_relative 'recovery'
require_relative 'telemetry'

module SidekiqRuntime
  # Reliable fetcher for strict, weighted, or random queues. A raw job stays
  # in its queue's Redis inflight ZSET until Processor acknowledges a
  # successful or persisted retry/dead outcome, or a requeue operation wins.
  class ReliableFetch
    include Sidekiq::Component

    EVENT_NAME = 'reliable_fetch.sidekiq_runtime'
    TIMEOUT = 2
    POLL_INTERVAL_SECONDS = 1.0

    public_constant :EVENT_NAME
    private_constant :TIMEOUT, :POLL_INTERVAL_SECONDS

    UnitOfWork = Struct.new(
      :queue,
      :job,
      :config,
      :store,
      :claim,
      :executor_started_at_ms,
      :executor_clock,
      :released,
      keyword_init: true
    ) do
      def acknowledge
        removed = store.acknowledge(claim)
        self.released = true
        ReliableFetch.publish(
          operation: 'acknowledged',
          outcome: removed == 1 ? 'success' : 'missing',
          claim: claim,
          executor_slot_occupied_duration_ms: executor_slot_occupied_duration_ms
        )
        removed
      rescue => exception
        ReliableFetch.publish(
          operation: 'acknowledged',
          outcome: ambiguous_redis_result?(exception) ? 'unknown' : 'error',
          claim: claim,
          exception: exception,
          executor_slot_occupied_duration_ms: executor_slot_occupied_duration_ms
        )
        raise
      end

      def queue_name
        queue.delete_prefix('queue:')
      end

      def requeue
        redis_operation_attempted = false
        replacement_job = Recovery.prepare(job)
        redis_operation_attempted = true
        moved = store.requeue(claim, replacement_job: replacement_job)
        self.released = true
        ReliableFetch.publish(
          operation: 'shutdown_requeued',
          outcome: moved == 1 ? 'success' : 'missing',
          claim: claim
        )
        moved
      rescue => exception
        ReliableFetch.publish(
          operation: 'shutdown_requeued',
          outcome: redis_operation_attempted && ambiguous_redis_result?(exception) ? 'unknown' : 'error',
          claim: claim,
          exception: exception
        )
        raise
      end

      private

      def ambiguous_redis_result?(exception)
        exception.is_a?(RedisClient::ConnectionError) ||
          exception.is_a?(RedisClient::ProtocolError)
      end

      def executor_slot_occupied_duration_ms
        return unless executor_started_at_ms && executor_clock

        duration_ms = executor_clock.call - executor_started_at_ms
        duration_ms unless duration_ms.negative?
      rescue
        nil
      end
    end

    public_constant :UnitOfWork

    attr_reader :stores_by_queue

    def initialize(capsule, store_class: InflightStore, random: Random, monotonic_clock: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }, executor_clock: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC, :float_millisecond) }, sleeper: ->(seconds) { sleep(seconds) })
      @config = capsule
      @mode = capsule.mode
      @random = random
      @monotonic_clock = monotonic_clock
      @executor_clock = executor_clock
      @sleeper = sleeper
      @queues = capsule.queues.dup.freeze
      raise ArgumentError, 'reliable fetch requires at least one queue' if @queues.empty?
      unless %i[strict weighted random].include?(@mode)
        raise ArgumentError, "reliable fetch does not support #{@mode.inspect} queue mode"
      end
      @strict_queues = @queues.uniq.freeze if @mode == :strict
      @stores_by_queue = @queues.uniq.to_h do |physical_queue|
        QueueKeys.validate_queue!(physical_queue)

        [physical_queue, store_class.new(
          capsule,
          queue: physical_queue,
          owner_identity: identity
        )]
      end.freeze
    end

    def configuration_matches?(capsule)
      @config.equal?(capsule) && @mode == capsule.mode && @queues == capsule.queues
    end

    def retrieve_work
      deadline = @monotonic_clock.call + TIMEOUT
      current_queue = nil
      empty_claims = 0
      rate_limited_claims = 0
      claim_duration_ms = 0.0
      cooldowns = {}

      loop do
        round_started_at = @monotonic_clock.call
        candidate_queues.each do |physical_queue|
          next if cooldowns.fetch(physical_queue, 0) > round_started_at

          cooldowns.delete(physical_queue)

          current_queue = physical_queue
          store = stores_by_queue.fetch(physical_queue)
          claim_started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          claim = store.claim
          claim_duration_ms += (Process.clock_gettime(Process::CLOCK_MONOTONIC) - claim_started_at) * 1_000
          unless claim
            empty_claims += 1
            next
          end
          if claim.is_a?(InflightStore::RateLimited)
            rate_limited_claims += 1
            observed_at = @monotonic_clock.call
            cooldowns[physical_queue] = observed_at + [claim.retry_after_seconds, POLL_INTERVAL_SECONDS].min
            self.class.publish(
              operation: 'rate_limited',
              outcome: 'limited',
              physical_queue: physical_queue,
              retry_after_seconds: claim.retry_after_seconds
            )
            next
          end
          # Retain unsupported low-level writes before JobRetry can use them.
          if claim.payload && !claim.payload['retry_queue'].nil?
            self.class.publish(operation: 'claim_blocked', outcome: 'unsupported_retry_queue', claim: claim)
            next
          end

          return hold_claimed_work(store, claim, empty_claims: empty_claims, claim_duration_ms: claim_duration_ms)
        end

        remaining = deadline - @monotonic_clock.call
        unless remaining.positive?
          self.class.publish(operation: 'poll', outcome: 'idle',
            empty_claims: empty_claims, claim_duration_ms: claim_duration_ms,
            rate_limited_claims: rate_limited_claims,
            poll_duration_ms: (TIMEOUT - remaining) * 1_000)
          return
        end

        wait = [POLL_INTERVAL_SECONDS, remaining].min
        earliest_cooldown = cooldowns.values.min
        if earliest_cooldown
          until_cooldown = earliest_cooldown - @monotonic_clock.call
          next unless until_cooldown.positive?

          wait = [wait, until_cooldown].min
        end
        @sleeper.call(wait)
      end
    rescue => exception
      self.class.publish(
        operation: 'claimed',
        outcome: ambiguous_redis_result?(exception) ? 'unknown' : 'error',
        physical_queue: current_queue,
        exception: exception
      )
      raise
    end

    def hold_claimed_work(store, claim, empty_claims:, claim_duration_ms:)
      executor_started_at_ms = executor_clock_ms
      work = UnitOfWork.new(
        queue: claim.source_queue,
        job: claim.job,
        config: config,
        store: store,
        claim: claim,
        executor_started_at_ms: executor_started_at_ms,
        executor_clock: @executor_clock
      )
      ProcessorBoundary.hold(work)
      self.class.publish(
        operation: 'claimed',
        outcome: 'success',
        claim: claim,
        empty_claims: empty_claims,
        claim_duration_ms: claim_duration_ms,
        pickup_latency_ms: Telemetry.elapsed_ms(claim.payload&.fetch('enqueued_at', nil), now: claim.claimed_at)
      )
      work
    end
    private :hold_claimed_work

    def candidate_queues
      return @strict_queues if @mode == :strict

      @queues.shuffle(random: @random).uniq
    end
    private :candidate_queues

    def executor_clock_ms
      @executor_clock.call
    rescue
      nil
    end
    private :executor_clock_ms

    def ambiguous_redis_result?(exception)
      exception.is_a?(RedisClient::ConnectionError) ||
        exception.is_a?(RedisClient::ProtocolError)
    end
    private :ambiguous_redis_result?

    def bulk_requeue(inprogress)
      return if inprogress.empty?

      requeued = 0
      inprogress.each do |unit_of_work|
        requeued += 1 if unit_of_work.requeue == 1
      rescue => exception
        logger.warn("Reliable inflight requeue did not complete: #{exception.message}")
      end
      logger.info("Confirmed #{requeued} reliable inflight jobs requeued in Redis")
    end

    def self.publish(operation:, outcome:, claim: nil, physical_queue: nil, exception: nil, executor_slot_occupied_duration_ms: nil, **measurements)
      SidekiqRuntime.publish(EVENT_NAME) do
        physical_queue ||= claim&.physical_queue
        payload = Telemetry.fields(claim&.payload || {}, queue: physical_queue).merge(
          operation: operation,
          outcome: outcome,
          physical_queue: physical_queue,
          jid: claim&.jid,
          entry_id: claim&.entry_id,
          owner_identity: claim&.owner_identity
        )
        if executor_slot_occupied_duration_ms
          payload[:executor_slot_occupied_duration_ms] = executor_slot_occupied_duration_ms
        end
        payload.merge!(measurements.compact)
        if exception
          payload[:exception] = [exception.class.name, exception.message]
          payload[:exception_object] = exception
        end
        payload
      end
    end
  end
end
