# frozen_string_literal: true

require_relative 'queue_keys'

module SidekiqRuntime
  # Stores and atomically updates a token bucket shared by every consumer of a
  # physical queue in one runtime namespace and Redis logical database.
  class QueueRateLimit
    FIELDS = %w[rate burst tokens last_refill].freeze
    MIN_RATE = Float::MIN
    MAX_BURST = (2**53) - 1

    class InvalidPolicy < StandardError; end

    LUA_HELPERS = <<~LUA
      local function key_type(key)
        local result = redis.call('TYPE', key)
        if type(result) == 'table' then return result.ok end
        return result
      end

      local function finite_number(value)
        local number = tonumber(value)
        if not number or number ~= number or number == math.huge or number == -math.huge then
          error('queue rate limit number must be finite')
        end
        return number
      end

      local function positive_integer(value)
        local number = finite_number(value)
        if number <= 0 or number > 9007199254740991 or number ~= math.floor(number) then
          error('queue rate limit integer must be positive')
        end
        return number
      end

      local function serialize_number(number)
        if number ~= number or number == math.huge or number == -math.huge then
          error('queue rate limit calculation is invalid')
        end
        return string.format('%.17g', number)
      end

      local function serialize_integer(number)
        if number <= 0 or number > 9007199254740991 or number ~= math.floor(number) then
          error('queue rate limit integer must be positive')
        end
        return string.format('%.0f', number)
      end

      local function read_bucket(key)
        local values = redis.call('HMGET', key, 'rate', 'burst', 'tokens', 'last_refill')
        local rate = finite_number(values[1])
        local burst = positive_integer(values[2])
        local tokens = finite_number(values[3])
        local last_refill = finite_number(values[4])
        if rate < 2.2250738585072014e-308 or tokens < 0 or tokens > burst or last_refill < 0 then
          error('queue rate limit policy is invalid')
        end
        return {rate, burst, tokens, last_refill}
      end

      local function redis_time()
        local parts = redis.call('TIME')
        local now = finite_number(parts[1]) + (finite_number(parts[2]) / 1000000)
        return now
      end

      local function refill_bucket(bucket, now)
        local effective_now = math.max(now, bucket[4])
        local elapsed = effective_now - bucket[4]
        local tokens = bucket[3]
        local needed = bucket[2] - tokens
        if elapsed > 0 and needed > 0 then
          if bucket[1] >= (needed / elapsed) then
            tokens = bucket[2]
          else
            tokens = tokens + (elapsed * bucket[1])
          end
        end
        serialize_number(tokens)
        return tokens, effective_now
      end
    LUA

    UPDATE = <<~LUA.freeze
      #{LUA_HELPERS}

      if #KEYS ~= 2 then return redis.error_reply('rate limit key count is invalid') end
      if #ARGV ~= 4 then return redis.error_reply('rate limit argument count is invalid') end

      local bucket_type = key_type(KEYS[1])
      local directory_type = key_type(KEYS[2])
      if bucket_type ~= 'none' and bucket_type ~= 'hash' then
        return redis.error_reply('queue rate limit must be a hash')
      end
      if directory_type ~= 'none' and directory_type ~= 'set' then
        return redis.error_reply('queue directory must be a set')
      end
      if ARGV[1] == '' then return redis.error_reply('queue must be present') end

      local old = nil
      if bucket_type == 'hash' then old = read_bucket(KEYS[1]) end
      local before = {}
      if old then
        before = {serialize_number(old[1]), serialize_integer(old[2]),
          serialize_number(old[3]), serialize_number(old[4])}
      end

      if ARGV[2] == 'disable' then
        if old then redis.call('DEL', KEYS[1]) end
        return {before, {}}
      end
      if ARGV[2] ~= 'enable' then return redis.error_reply('rate limit action is invalid') end

      local new_rate = finite_number(ARGV[3])
      local new_burst = positive_integer(ARGV[4])
      if new_rate < 2.2250738585072014e-308 then
        return redis.error_reply('rate limit configuration is invalid')
      end

      local now = redis_time()
      local tokens = new_burst
      local last_refill = now
      if old then
        tokens, last_refill = refill_bucket(old, now)
        tokens = math.min(tokens, new_burst)
      end

      redis.call('HSET', KEYS[1],
        'rate', serialize_number(new_rate),
        'burst', serialize_integer(new_burst),
        'tokens', serialize_number(tokens),
        'last_refill', serialize_number(last_refill))
      redis.call('SADD', KEYS[2], ARGV[1])
      return {before, {serialize_number(new_rate), serialize_integer(new_burst),
        serialize_number(tokens), serialize_number(last_refill)}}
    LUA

    Policy = Data.define(:rate, :burst, :tokens, :last_refill) do
      def to_h = {rate: rate, burst: burst, tokens: tokens, last_refill: last_refill}
    end
    Update = Data.define(:before, :after)

    public_constant :Policy, :Update, :InvalidPolicy, :LUA_HELPERS, :MIN_RATE, :MAX_BURST
    private_constant :FIELDS, :UPDATE

    def initialize(config)
      @config = config
    end

    def get(queue)
      queue = QueueKeys.validate_queue!(queue)
      values = @config.redis { |redis| redis.hgetall(QueueKeys.rate_limit_key(queue)) }
      return if values.empty?

      policy_from_hash(values)
    end

    def get_many(queues)
      queues = queues.map { |queue| QueueKeys.validate_queue!(queue) }.uniq
      return {} if queues.empty?

      values = @config.redis do |redis|
        redis.multi do |transaction|
          queues.each { |queue| transaction.hgetall(QueueKeys.rate_limit_key(queue)) }
        end
      end
      queues.zip(values).to_h.transform_values do |fields|
        policy_from_hash(fields) unless fields.empty?
      end
    end

    def enable(queue, rate:, burst:)
      queue = QueueKeys.validate_queue!(queue)
      rate = positive_finite_float(rate, :rate)
      burst = positive_integer(burst, :burst)
      evaluate_update(queue, 'enable', rate: rate, burst: burst)
    end

    def disable(queue)
      queue = QueueKeys.validate_queue!(queue)
      evaluate_update(queue, 'disable')
    end

    private

    def evaluate_update(queue, action, rate: '', burst: '')
      before, after = @config.redis do |redis|
        redis.call_once(
          'EVAL', UPDATE, 2,
          QueueKeys.rate_limit_key(queue), QueueKeys.directory_key,
          queue, action, rate, burst
        )
      end
      Update.new(
        before: before.empty? ? nil : policy_from_values(before),
        after: after.empty? ? nil : policy_from_values(after)
      )
    end

    def policy_from_hash(values)
      missing = FIELDS - values.keys
      raise InvalidPolicy, "queue rate limit policy is missing #{missing.join(', ')}" unless missing.empty?

      policy_from_values(FIELDS.map { |field| values.fetch(field) })
    end

    def policy_from_values(values)
      raise InvalidPolicy, 'queue rate limit policy is incomplete' unless values.length == 4

      rate = positive_finite_float(values.fetch(0), :rate)
      burst = positive_integer(values.fetch(1), :burst)
      tokens = finite_float(values.fetch(2), :tokens)
      last_refill = finite_float(values.fetch(3), :last_refill)
      raise InvalidPolicy, 'tokens must be between zero and burst' unless tokens.between?(0, burst)
      raise InvalidPolicy, 'last_refill must not be negative' if last_refill.negative?

      Policy.new(rate:, burst:, tokens:, last_refill:)
    rescue ArgumentError => exception
      raise InvalidPolicy, exception.message
    end

    def positive_finite_float(value, name)
      number = finite_float(value, name)
      raise ArgumentError, "#{name} is below the supported finite range" if number < MIN_RATE

      number
    end

    def positive_integer(value, name)
      number = Integer(value)
      numeric = Float(value)
      raise ArgumentError, "#{name} must be a positive integer no greater than #{MAX_BURST}" unless
        numeric.finite? && numeric == number && number.positive? && number <= MAX_BURST

      number
    rescue TypeError, ArgumentError
      raise ArgumentError, "#{name} must be a positive integer"
    end

    def finite_float(value, name)
      number = Float(value)
      raise ArgumentError, "#{name} must be finite" unless number.finite?

      number
    rescue TypeError, ArgumentError
      raise ArgumentError, "#{name} must be a finite number"
    end
  end
end
