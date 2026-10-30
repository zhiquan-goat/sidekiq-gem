# frozen_string_literal: true

require 'digest'
require 'digest/sha1'
require 'securerandom'
require_relative 'queue_keys'
require_relative 'queue_rate_limit'
require_relative 'recovery_configuration'

module SidekiqRuntime
  # Keeps a fetched job in a durable Redis sorted set until Sidekiq ACKs it or
  # an owner-checked recovery atomically moves it to Ready or Dead.
  class InflightStore
    EVENT_NAME = 'inflight.sidekiq_runtime'
    PROTOCOL_VERSION = 3
    MEMBER_SEPARATOR = "\n"
    ENTRY_ID_PATTERN = /\A[0-9a-f]{64}\z/
    RATE_LIMITED_RESULT = 'rate_limited'

    CLAIM = <<~LUA.freeze
      #{QueueRateLimit::LUA_HELPERS}

      if #KEYS ~= 6 then
        return redis.error_reply('claim key count is invalid')
      end
      if #ARGV ~= 3 then
        return redis.error_reply('claim argument count is invalid')
      end

      local queue_type = key_type(KEYS[1])
      local inflight_type = key_type(KEYS[2])
      local owners_type = key_type(KEYS[3])
      local directory_type = key_type(KEYS[4])
      local rate_limit_type = key_type(KEYS[5])
      local heartbeat_type = key_type(KEYS[6])
      if heartbeat_type == 'none' then return false end
      if heartbeat_type ~= 'hash' or redis.call('PTTL', KEYS[6]) <= 0 then
        return redis.error_reply('owner heartbeat must be a hash with a positive TTL')
      end
      if queue_type ~= 'none' and queue_type ~= 'list' then
        return redis.error_reply('source queue must be a list')
      end
      if inflight_type ~= 'none' and inflight_type ~= 'zset' then
        return redis.error_reply('inflight key must be a sorted set')
      end
      if owners_type ~= 'none' and owners_type ~= 'hash' then
        return redis.error_reply('inflight owners key must be a hash')
      end
      if directory_type ~= 'none' and directory_type ~= 'set' then
        return redis.error_reply('queue directory must be a set')
      end
      if rate_limit_type ~= 'none' and rate_limit_type ~= 'hash' then
        return redis.error_reply('queue rate limit must be a hash')
      end
      local job = redis.call('LINDEX', KEYS[1], -1)
      if not job then return false end
      local claimed_at = redis_time()

      if rate_limit_type == 'hash' then
        local bucket = read_bucket(KEYS[5])
        local tokens, last_refill = refill_bucket(bucket, claimed_at)
        if tokens < 1 then
          redis.call('HSET', KEYS[5],
            'tokens', serialize_number(tokens), 'last_refill', serialize_number(last_refill))
          local retry_after = (1 - tokens) / bucket[1]
          if retry_after <= 0 or retry_after ~= retry_after or
              retry_after == math.huge or retry_after == -math.huge then
            return redis.error_reply('queue rate limit retry calculation is invalid')
          end
          return {'rate_limited', serialize_number(retry_after)}
        end
        tokens = tokens - 1
        redis.call('HSET', KEYS[5],
          'tokens', serialize_number(tokens), 'last_refill', serialize_number(last_refill))
      end

      local member = ARGV[1] .. '\\n' .. job
      -- Persist before the destructive read. Lua execution is isolated, so
      -- LINDEX and RPOP address the same queue item. If a later command ever
      -- fails, the safe failure mode is a duplicate, not a lost job.
      redis.call('SADD', KEYS[4], ARGV[3])
      redis.call('ZADD', KEYS[2], claimed_at, member)
      redis.call('HSET', KEYS[3], member, ARGV[2])
      redis.call('RPOP', KEYS[1])
      return {job, member, serialize_number(claimed_at)}
    LUA

    CLAIM_HELPERS = <<~LUA.freeze
      local function check_type(key, expected)
        local actual = redis.call('TYPE', key).ok
        if actual ~= 'none' and actual ~= expected then
          error('runtime key must be a ' .. expected .. ': ' .. key)
        end
      end
      local function now_seconds()
        local time = redis.call('TIME')
        return tonumber(time[1]) + tonumber(time[2]) / 1000000
      end
      local function heartbeat_status(owner)
        if not owner or owner == '' then return 'owner_missing', -1 end
        local kind = redis.call('TYPE', owner).ok
        if kind == 'none' then return 'owner_dead', -2 end
        local ttl = redis.call('PTTL', owner)
        if kind ~= 'hash' or ttl <= 0 then return 'unknown', ttl end
        return 'owner_alive', ttl
      end
      local function clear_claim(member)
        local removed = redis.call('ZREM', KEYS[1], member)
        redis.call('HDEL', KEYS[2], member)
        return removed
      end
      check_type(KEYS[1], 'zset')
      check_type(KEYS[2], 'hash')
    LUA

    ACKNOWLEDGE = <<~LUA.freeze
      #{CLAIM_HELPERS}
      if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
      return clear_claim(ARGV[1])
    LUA

    REQUEUE = <<~LUA.freeze
      #{CLAIM_HELPERS}
      check_type(KEYS[3], 'list')
      if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
      if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[3] then return 0 end
      redis.call('RPUSH', KEYS[3], ARGV[2])
      clear_claim(ARGV[1])
      return 1
    LUA

    RECLAIM = <<~LUA.freeze
      #{CLAIM_HELPERS}
      check_type(KEYS[3], 'list')
      check_type(KEYS[4], 'zset')
      if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 'missing' end
      local owner = redis.call('HGET', KEYS[2], ARGV[1])
      if not owner or owner == '' then return 'owner_missing' end
      if owner ~= ARGV[2] then return 'owner_changed' end
      local status = heartbeat_status(owner)
      if status ~= 'owner_dead' then return status end
      local now = now_seconds()
      if ARGV[4] == 'dead' then
        redis.call('ZADD', KEYS[4], now, ARGV[3])
        redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', now - tonumber(ARGV[5]))
        redis.call('ZREMRANGEBYRANK', KEYS[4], 0, -tonumber(ARGV[6]) - 1)
      else
        redis.call('RPUSH', KEYS[3], ARGV[3])
      end
      clear_claim(ARGV[1])
      return ARGV[4]
    LUA

    OWNER_STATE = <<~LUA.freeze
      #{CLAIM_HELPERS}
      local owner = redis.call('HGET', KEYS[2], ARGV[1]) or ''
      local status, ttl = heartbeat_status(owner)
      return {tostring(now_seconds()), redis.call('ZSCORE', KEYS[1], ARGV[1]) or '', owner, status, ttl}
    LUA

    SNAPSHOT = <<~LUA.freeze
      #{CLAIM_HELPERS}
      local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
      return {tostring(now_seconds()), redis.call('ZCARD', KEYS[1]), oldest[2] or ''}
    LUA

    SCRIPT_SHAS = [CLAIM, ACKNOWLEDGE, REQUEUE, RECLAIM, OWNER_STATE, SNAPSHOT]
      .to_h { |script| [script, Digest::SHA1.hexdigest(script)] }.freeze

    Claim = Struct.new(
      :source_queue,
      :job,
      :member,
      :claimed_at,
      :owner_identity,
      keyword_init: true
    ) do
      def entry_id
        Digest::SHA256.hexdigest(member)
      end

      def payload
        return @payload if defined?(@payload)

        @payload = Sidekiq.load_json(job)
        @payload = nil unless @payload.is_a?(Hash)
        @payload
      rescue JSON::ParserError, TypeError
        @payload = nil
      end

      def jid
        payload&.fetch('jid', nil).presence
      end

      def job_class
        (payload&.fetch('wrapped', nil) || payload&.fetch('class', nil)).to_s.presence
      end

      def physical_queue
        source_queue.to_s.delete_prefix('queue:')
      end

      def valid_member?
        stored_claim_id, stored_job = member.to_s.split(MEMBER_SEPARATOR, 2)
        stored_claim_id.present? && stored_job == job
      end
    end

    RateLimited = Data.define(:physical_queue, :retry_after_seconds)

    public_constant :EVENT_NAME, :PROTOCOL_VERSION, :Claim, :RateLimited
    private_constant :CLAIM,
      :ACKNOWLEDGE,
      :REQUEUE, :SCRIPT_SHAS, :CLAIM_HELPERS, :RECLAIM, :OWNER_STATE, :SNAPSHOT,
      :MEMBER_SEPARATOR,
      :ENTRY_ID_PATTERN,
      :RATE_LIMITED_RESULT

    attr_reader :key_prefix, :source_queue

    def initialize(config, queue:, owner_identity: nil, id_generator: -> { SecureRandom.uuid })
      @config = config
      @key_prefix = QueueKeys.prefix(queue)
      @source_queue = QueueKeys.source_queue(queue)
      @owner_identity = owner_identity
      @id_generator = id_generator
    end

    def physical_queue
      source_queue.delete_prefix('queue:')
    end

    def inflight_key
      @inflight_key ||= "#{key_prefix}:inflight"
    end

    def owners_key
      "#{inflight_key}:owners"
    end

    def claim(owner_identity: @owner_identity)
      owner_identity = required_string(owner_identity, :owner_identity)
      claim_id = required_string(@id_generator.call, :claim_id)
      keys = [
        source_queue,
        inflight_key,
        owners_key,
        QueueKeys.directory_key,
        QueueKeys.rate_limit_key(physical_queue),
        owner_identity
      ]
      arguments = [claim_id, owner_identity, physical_queue]
      result = evaluate_once(CLAIM, keys: keys, arguments: arguments)
      return unless result

      if result.first == RATE_LIMITED_RESULT
        retry_after_seconds = Float(result.fetch(1))
        unless retry_after_seconds.finite? && retry_after_seconds.positive?
          raise RedisClient::ProtocolError, 'queue rate limit retry must be a finite positive number'
        end
        return RateLimited.new(physical_queue:, retry_after_seconds:)
      end

      job, member, claimed_at = result
      Claim.new(
        source_queue: source_queue,
        job: job,
        member: member,
        claimed_at: Float(claimed_at),
        owner_identity: owner_identity
      )
    end

    def acknowledge(claim)
      validate_claim!(claim)
      evaluate_once(ACKNOWLEDGE, keys: claim_keys, arguments: [claim.member, claim.owner_identity.to_s])
    end

    # Trusted Sidekiq shutdown primitive. Admin must use recover_orphan instead.
    def requeue(claim, replacement_job: claim.job)
      validate_claim!(claim)
      evaluate_once(
        REQUEUE,
        keys: [*claim_keys, source_queue],
        arguments: [claim.member, replacement_job, claim.owner_identity.to_s]
      )
    end

    def size
      @config.redis { |redis| redis.zcard(inflight_key) }
    end

    def entries(offset: 0, limit: 100)
      offset = Integer(offset)
      limit = Integer(limit)
      raise ArgumentError, 'offset must not be negative' if offset.negative?
      raise ArgumentError, 'limit must be positive' unless limit.positive?

      values = @config.redis do |redis|
        redis.zrange(inflight_key, offset, offset + limit - 1, 'withscores')
      end
      pairs = normalize_scored_members(values)
      owners = @config.redis do |redis|
        pairs.empty? ? [] : redis.hmget(owners_key, *pairs.map(&:first))
      end
      pairs.each_with_index.map do |(member, score), index|
        entry_from(member, score, owners[index])
      end
    end

    def scan(cursor: '0', count: 100)
      next_cursor, values = @config.redis { |redis| redis.call('ZSCAN', inflight_key, cursor, 'COUNT', count) }
      pairs = normalize_scored_members(values)
      owners = @config.redis { |redis| pairs.empty? ? [] : redis.hmget(owners_key, *pairs.map(&:first)) }
      [next_cursor, pairs.each_with_index.map { |(member, score), index| entry_from(member, score, owners[index]) }]
    end

    def find_entry(entry_id)
      entry_id = String(entry_id)
      return unless ENTRY_ID_PATTERN.match?(entry_id)

      pair = @config.redis do |redis|
        redis.zscan(inflight_key, count: 100).find do |member, _score|
          Digest::SHA256.hexdigest(member) == entry_id
        end
      end
      if pair
        owner = @config.redis { |redis| redis.hget(owners_key, pair.first) }
        entry_from(*pair, owner)
      end
    end

    # Preparation has no authority to recover: this script rechecks the owner heartbeat.
    def reclaim_orphan(claim, job:, destination:)
      validate_claim!(claim)
      raise ArgumentError, 'invalid recovery destination' unless %w[requeued dead].include?(destination)

      evaluate_once(RECLAIM, keys: [*claim_keys, source_queue, 'dead'],
        arguments: [claim.member, claim.owner_identity.to_s, job, destination,
          @config[:dead_timeout_in_seconds], @config[:dead_max_jobs]])
    end

    def owner_state(claim)
      validate_claim!(claim)
      now, claimed, owner, status, ttl = evaluate_once(OWNER_STATE, keys: claim_keys, arguments: [claim.member])
      {
        known: status != 'unknown', redis_now: Float(now), present: !claimed.empty?,
        owner_identity: owner, owner_status: status,
        heartbeat_ttl_seconds: ttl.positive? ? ttl / 1_000.0 : nil
      }
    end

    def snapshot
      now, count, oldest = evaluate_once(SNAPSHOT, keys: claim_keys, arguments: [])
      {
        known: true, protocol_version: PROTOCOL_VERSION, redis_now: Float(now), inflight_count: count,
        oldest_inflight_age_seconds: oldest.empty? ? 0 : [Float(now) - Float(oldest), 0].max
      }
    end

    private

    def entry_from(member, score, owner_identity = nil)
      _claim_id, job = member.to_s.split(MEMBER_SEPARATOR, 2)
      Claim.new(
        source_queue: source_queue,
        job: job,
        member: member,
        claimed_at: Float(score),
        owner_identity: owner_identity
      )
    end

    def claim_keys
      [inflight_key, owners_key]
    end

    def normalize_scored_members(values)
      return values if values.empty? || values.first.is_a?(Array)

      values.each_slice(2).to_a
    end

    def evaluate_once(script, keys:, arguments:)
      @config.redis do |redis|
        # redis-client normally retries once after reconnect. A lost CLAIM
        # reply must not claim a second job, and a lost REQUEUE reply must be
        # reported as unknown rather than replayed into a misleading zero.
        begin
          redis.call_once('EVALSHA', SCRIPT_SHAS.fetch(script), keys.length, *keys, *arguments)
        rescue RedisClient::CommandError => exception
          # NOSCRIPT is a confirmed non-execution, unlike a connection failure.
          raise unless exception.message.start_with?('NOSCRIPT')

          redis.call_once('EVAL', script, keys.length, *keys, *arguments)
        end
      end
    end

    def validate_claim!(claim)
      unless claim.is_a?(Claim) && claim.source_queue == source_queue && claim.valid_member?
        raise ArgumentError, 'claim does not belong to this inflight store'
      end
    end

    def required_string(value, name)
      string = String(value)
      raise ArgumentError, "#{name} must be present" if string.empty?
      raise ArgumentError, "#{name} must be single-line" if string.include?(MEMBER_SEPARATOR)

      string
    end
  end
end
