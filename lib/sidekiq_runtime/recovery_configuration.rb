# frozen_string_literal: true

module SidekiqRuntime
  # One immutable engine policy, independent of per-job uniqueness lock TTLs.
  class RecoveryConfiguration
    DEFAULTS = {
      reclaim_interval: 5.0, reclaim_batch: 100, queue_batch: 10, max_recoveries: 3
    }.freeze
    attr_reader(*DEFAULTS.keys)

    def initialize(**options)
      unknown = options.keys - DEFAULTS.keys
      raise ArgumentError, "unknown recovery settings: #{unknown.join(', ')}" unless unknown.empty?

      DEFAULTS.merge(options).each do |name, value|
        value = DEFAULTS[name].is_a?(Integer) ? Integer(value) : Float(value)
        raise ArgumentError, "#{name} must be finite and positive" unless value.finite? && value.positive?
        instance_variable_set("@#{name}", value)
      end
      raise ArgumentError, 'recovery batches must not exceed 1000' if [reclaim_batch, queue_batch].max > 1_000
      freeze
    end

    def ==(other)
      other.is_a?(self.class) && DEFAULTS.keys.all? { |key| public_send(key) == other.public_send(key) }
    end

    def self.for(config)
      config[:runtime_recovery] ||= new
    end
  end
end
