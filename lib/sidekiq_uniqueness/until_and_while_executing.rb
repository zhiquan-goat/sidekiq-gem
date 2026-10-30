# frozen_string_literal: true

require 'digest'
require 'json'
require 'securerandom'

module SidekiqUniqueness
  # Implements Sidekiq-local queue and execution locks with independent TTLs.
  class UntilAndWhileExecuting
    RESERVE_ENQUEUE = <<~LUA
      local existing = redis.call('get', KEYS[1])
      if not existing then
        redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
        return 1
      end
      if existing == ARGV[1] then
        redis.call('expire', KEYS[1], ARGV[2])
        return 1
      end
      return 0
    LUA

    BEGIN_EXECUTION = <<~LUA
      local acquired = redis.call('set', KEYS[2], ARGV[2], 'NX', 'EX', ARGV[3])
      if not acquired then
        return 0
      end
      if redis.call('get', KEYS[1]) == ARGV[1] then
        redis.call('del', KEYS[1])
      end
      return 1
    LUA

    COMPARE_AND_DELETE = <<~LUA
      if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
      end
      return 0
    LUA

    private_constant :RESERVE_ENQUEUE, :BEGIN_EXECUTION, :COMPARE_AND_DELETE

    def initialize(key_prefix:, identity:, enqueue_ttl:, runtime_ttl:, metadata: {})
      @key_prefix = String(key_prefix)
      @identity_digest = identity_digest(identity, :identity)
      @enqueue_ttl = positive_seconds(enqueue_ttl, :enqueue_ttl)
      @runtime_ttl = positive_seconds(runtime_ttl, :runtime_ttl)
      @metadata = metadata

      raise ArgumentError, 'key_prefix must be present' if @key_prefix.empty?
    end

    attr_reader :enqueue_ttl, :runtime_ttl, :metadata

    def runtime_conflict_delay
      runtime_ttl + 1
    end

    def enqueue_redis_key
      @enqueue_redis_key ||= redis_key('enqueue', @identity_digest)
    end

    def runtime_redis_key
      @runtime_redis_key ||= redis_key('runtime', @identity_digest)
    end

    def reserve_enqueue(redis:, owner:)
      instrument_lock(
        'enqueue_lock.sidekiq_uniqueness',
        redis_key: enqueue_redis_key,
        lock_ttl: enqueue_ttl
      ) do
        truthy_redis_result(
          redis.call('EVAL', RESERVE_ENQUEUE, 1, enqueue_redis_key, owner, enqueue_ttl)
        )
      end
    end

    def release_enqueue(redis:, owner:)
      compare_and_delete(
        redis: redis,
        redis_key: enqueue_redis_key,
        owner: owner,
        event_name: 'enqueue_unlock.sidekiq_uniqueness'
      )
    end

    # The enqueue reservation is deleted only after the runtime lock is held.
    # A runtime conflict therefore leaves the queued reservation intact.
    def begin_execution(redis:, jid:)
      owner = "sidekiq:#{jid}:#{SecureRandom.uuid}"
      acquired = instrument_lock(
        'runtime_lock.sidekiq_uniqueness',
        redis_key: runtime_redis_key,
        lock_ttl: runtime_ttl
      ) do
        truthy_redis_result(
          redis.call(
            'EVAL',
            BEGIN_EXECUTION,
            2,
            enqueue_redis_key,
            runtime_redis_key,
            jid,
            owner,
            runtime_ttl
          )
        )
      end

      owner if acquired
    end

    def finish_execution(redis:, owner:)
      return 0 unless owner

      compare_and_delete(
        redis: redis,
        redis_key: runtime_redis_key,
        owner: owner,
        event_name: 'runtime_unlock.sidekiq_uniqueness'
      )
    end

    def observability_payload
      SidekiqRuntime::Telemetry.fields(metadata).merge(strategy: SidekiqUniqueness::STRATEGY)
    end

    private

    def positive_seconds(value, name)
      seconds = value.to_i
      raise ArgumentError, "#{name} must be positive" unless seconds.positive?

      seconds
    end

    def identity_digest(identity, name)
      raise ArgumentError, "#{name} must not be nil" if identity.nil?

      Digest::SHA256.hexdigest(JSON.generate(identity))
    end

    def redis_key(lock_type, digest)
      "#{@key_prefix}:#{lock_type}:#{digest}"
    end

    def truthy_redis_result(result)
      result == 1 || result == true
    end

    def instrument_lock(event_name, redis_key:, lock_ttl:)
      acquired = yield
      SidekiqRuntime.publish(event_name) do
        observability_payload.merge(redis_key: redis_key, lock_ttl: lock_ttl, acquired: acquired)
      end
      acquired
    rescue => exception
      SidekiqRuntime.publish(event_name) do
        observability_payload.merge(redis_key: redis_key, exception: [exception.class.name, exception.message])
      end
      raise
    end

    def compare_and_delete(redis:, redis_key:, owner:, event_name:)
      deleted = redis.call('EVAL', COMPARE_AND_DELETE, 1, redis_key, owner)
      SidekiqRuntime.publish(event_name) { observability_payload.merge(redis_key: redis_key, deleted: deleted) }
      deleted
    rescue => exception
      SidekiqRuntime.publish(event_name) do
        observability_payload.merge(redis_key: redis_key, exception: [exception.class.name, exception.message])
      end
      raise
    end
  end
end
