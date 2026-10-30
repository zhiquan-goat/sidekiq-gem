# frozen_string_literal: true

require 'digest/sha1'
require 'sidekiq/job_util'
require_relative '../sidekiq_runtime/queue_keys'

module SidekiqUniqueness
  # Atomically persists one native Ready/Scheduled job and its enqueue lock.
  # The caller owns normalization and client middleware execution.
  class AtomicEnqueue
    WRITE = <<~LUA
      local function key_type(key)
        local result = redis.call('TYPE', key)
        if type(result) == 'table' then
          return result.ok
        end
        return result
      end

      if #KEYS ~= 4 then
        return 'rejected:key_count'
      end
      if #ARGV ~= 7 then
        return 'rejected:argument_count'
      end
      if ARGV[1] == '' then
        return 'rejected:queue_missing'
      end
      if ARGV[2] == '' then
        return 'rejected:raw_job_missing'
      end
      if ARGV[3] == '' then
        return 'rejected:jid_missing'
      end
      if ARGV[4] ~= 'ready' and ARGV[4] ~= 'scheduled' then
        return 'rejected:invalid_target'
      end
      if ARGV[4] == 'ready' and KEYS[1] ~= 'queue:' .. ARGV[1] then
        return 'rejected:ready_key_mismatch'
      end
      if ARGV[4] == 'scheduled' and KEYS[1] ~= 'schedule' then
        return 'rejected:schedule_key_mismatch'
      end
      local scheduled_at = tonumber(ARGV[5])
      if ARGV[4] == 'scheduled' and
          (not scheduled_at or scheduled_at ~= scheduled_at or
           scheduled_at == math.huge or scheduled_at == -math.huge or scheduled_at < 0) then
        return 'rejected:invalid_scheduled_at'
      end
      if ARGV[6] ~= 'new' and ARGV[6] ~= 'continuation' then
        return 'rejected:invalid_mode'
      end
      local lock_ttl = tonumber(ARGV[7])
      if not lock_ttl or lock_ttl <= 0 or lock_ttl ~= math.floor(lock_ttl) then
        return 'rejected:invalid_lock_ttl'
      end

      local target_type = key_type(KEYS[1])
      local native_directory_type = key_type(KEYS[2])
      local runtime_directory_type = key_type(KEYS[3])
      local lock_type = key_type(KEYS[4])
      if ARGV[4] == 'ready' and target_type ~= 'none' and target_type ~= 'list' then
        return 'rejected:ready_wrong_type'
      end
      if ARGV[4] == 'scheduled' and target_type ~= 'none' and target_type ~= 'zset' then
        return 'rejected:schedule_wrong_type'
      end
      if native_directory_type ~= 'none' and native_directory_type ~= 'set' then
        return 'rejected:native_directory_wrong_type'
      end
      if runtime_directory_type ~= 'none' and runtime_directory_type ~= 'set' then
        return 'rejected:runtime_directory_wrong_type'
      end
      if lock_type ~= 'none' and lock_type ~= 'string' then
        return 'rejected:lock_wrong_type'
      end

      local lock_owner = redis.call('get', KEYS[4])
      if ARGV[6] == 'new' and lock_owner then
        return 'duplicate:enqueue_lock_exists'
      end

      redis.call('SADD', KEYS[3], ARGV[1])
      redis.call('SADD', KEYS[2], ARGV[1])
      if ARGV[4] == 'scheduled' then
        redis.call('ZADD', KEYS[1], scheduled_at, ARGV[2])
      else
        redis.call('LPUSH', KEYS[1], ARGV[2])
      end

      if not lock_owner then
        redis.call('set', KEYS[4], ARGV[3], 'EX', lock_ttl)
      elseif lock_owner == ARGV[3] then
        redis.call('expire', KEYS[4], lock_ttl)
      end

      return 'accepted:' .. ARGV[4]
    LUA

    SCRIPT_SHA = Digest::SHA1.hexdigest(WRITE)
    ACCEPTED_REPLY = /\Aaccepted:(ready|scheduled)\z/
    REJECTED_REPLY = /\Arejected:([a-z0-9_]+)\z/
    DUPLICATE_REPLY = 'duplicate:enqueue_lock_exists'

    private_constant :WRITE, :SCRIPT_SHA, :ACCEPTED_REPLY, :REJECTED_REPLY,
      :DUPLICATE_REPLY

    # The script deterministically rejected the request before any write.
    class Rejected < StandardError
      attr_reader :code

      def initialize(code)
        @code = code.to_sym
        super("Native Sidekiq enqueue rejected: #{@code}")
      end
    end

    # Redis may have applied none, some, or all of the enqueue script.
    class Unknown < StandardError; end
    class UnexpectedReply < StandardError; end
    private_constant :UnexpectedReply

    include Sidekiq::JobUtil

    def initialize(redis_pool, clock: -> { Time.now.to_f })
      @redis_pool = redis_pool
      @clock = clock
    end

    def write(normalized_payload, lock:, mode: :new)
      payload, raw_job, target, scheduled_at = prepare(normalized_payload)
      reply = write_once(
        payload,
        raw_job,
        target: target,
        scheduled_at: scheduled_at,
        lock: lock,
        mode: mode
      )
      interpret(reply, target: target)
    end

    private

    def prepare(normalized_payload)
      validate!(normalized_payload)
      payload = normalized_payload.dup
      scheduled_at = payload.key?('at') ? finite_time!(payload['at'], :invalid_scheduled_at) : nil
      target = scheduled_at ? :scheduled : :ready
      if scheduled_at
        payload.delete('at')
        payload.delete('enqueued_at')
      else
        payload['enqueued_at'] = finite_time!(@clock.call, :invalid_enqueued_at)
      end
      verify_json(payload)
      [payload, Sidekiq.dump_json(payload), target, scheduled_at]
    rescue Rejected
      raise
    rescue => exception
      raise Rejected.new(:invalid_json), cause: exception
    end

    def validate!(payload)
      reject!(:invalid_payload) unless payload.is_a?(Hash)
      reject!(:invalid_top_level_keys) unless payload.keys.all?(String)
      reject!(:invalid_class) unless nonempty_string?(payload['class'])
      reject!(:invalid_jid) unless nonempty_string?(payload['jid'])
      reject!(:invalid_args) unless payload['args'].is_a?(Array)
      reject!(:invalid_queue) unless SidekiqRuntime::QueueKeys.valid_queue?(payload['queue'])
      reject!(:retry_queue_not_supported) unless payload['retry_queue'].nil?
    end

    def nonempty_string?(value)
      value.is_a?(String) && !value.empty?
    end

    def reject!(code)
      raise Rejected, code
    end

    def finite_time!(value, code)
      value = Float(value)
      reject!(code) unless value.finite? && !value.negative?
      value
    rescue ArgumentError, RangeError, TypeError
      reject!(code)
    end

    def write_once(payload, raw_job, target:, scheduled_at:, lock:, mode:)
      reject!(:invalid_mode) unless %i[new continuation].include?(mode)
      queue = payload.fetch('queue')
      target_key = target == :scheduled ? 'schedule' : SidekiqRuntime::QueueKeys.source_queue(queue)
      keys = [target_key, 'queues', SidekiqRuntime::QueueKeys.directory_key, lock.enqueue_redis_key]
      scheduled_delay = scheduled_at ? [scheduled_at - finite_time!(@clock.call, :invalid_enqueued_at), 0].max.ceil : 0
      effective_ttl = lock.enqueue_ttl + scheduled_delay
      reject!(:invalid_lock_ttl) if effective_ttl > (2**63 - 1)
      arguments = [
        queue,
        raw_job,
        payload.fetch('jid'),
        target.to_s,
        scheduled_at ? scheduled_at.to_s : '',
        mode.to_s,
        effective_ttl.to_s
      ]
      @redis_pool.with do |redis|
        evalsha_or_load(redis, keys, arguments)
      end
    rescue Rejected
      raise
    rescue => exception
      raise Unknown.new('Native Sidekiq enqueue outcome is unknown'), cause: exception
    end

    def evalsha_or_load(redis, keys, arguments)
      redis.call_once('EVALSHA', SCRIPT_SHA, keys.length, *keys, *arguments)
    rescue RedisClient::CommandError => exception
      raise unless exception.message.include?('NOSCRIPT')

      redis.call_once('EVAL', WRITE, keys.length, *keys, *arguments)
    end

    def interpret(reply, target:)
      accepted = ACCEPTED_REPLY.match(reply) if reply.is_a?(String)
      return :accepted if accepted && accepted[1] == target.to_s
      return :duplicate if reply == DUPLICATE_REPLY

      rejection = REJECTED_REPLY.match(reply) if reply.is_a?(String)
      raise Rejected, rejection[1] if rejection

      cause = UnexpectedReply.new("Unexpected native Sidekiq enqueue reply: #{reply.class.name}")
      raise Unknown.new('Native Sidekiq enqueue outcome is unknown'), cause: cause
    end
  end
end
