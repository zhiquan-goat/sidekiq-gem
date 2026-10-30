# frozen_string_literal: true

module SidekiqRuntime
  # Queue selection is finalized at startup; live reconfiguration is unsupported.
  module QueueConfiguration
    def self.validate!(config)
      raw = config[:queues]
      raise ArgumentError, 'set config[:queues] before startup' unless raw

      entries = raw.is_a?(Array) ? raw : [raw]
      raise ArgumentError, 'at least one queue must be configured' if entries.empty?
      parsed = entries.map { |entry| parse_entry!(entry) }
      weights = parsed.map(&:last)
      if weights.all?(&:nil?)
        expected = parsed.map(&:first)
        mode = :strict
      else
        if weights.any?(&:nil?)
          raise ArgumentError, 'strict and weighted queue entries cannot be mixed'
        end
        expected = parsed.flat_map { |name, weight| Array.new(weight, name) }
        mode = weights.all? { |weight| weight == 1 } ? :random : :weighted
      end
      capsule = config.default_capsule
      unless capsule.queues == expected && capsule.mode == mode
        raise ArgumentError, 'configured queues do not match the default capsule'
      end
      true
    end

    def self.parse_entry!(entry)
      if entry.is_a?(String) && !entry.include?(',')
        raise ArgumentError, 'queue name must be present' if entry.empty?

        return [entry, nil]
      end

      values = if entry.is_a?(String)
        entry.split(',', -1)
      elsif entry.is_a?(Array)
        entry
      else
        []
      end
      raise ArgumentError, 'weighted queue entries must contain exactly a name and weight' unless values.size == 2

      name, weight = values
      raise ArgumentError, 'queue name must be a non-empty String' unless name.is_a?(String) && !name.empty?
      unless positive_integer_weight?(weight)
        raise ArgumentError, 'queue weight must be an explicit positive integer'
      end

      [name, weight.to_i]
    end
    private_class_method :parse_entry!

    def self.positive_integer_weight?(weight)
      (weight.is_a?(Integer) && weight.positive?) ||
        (weight.is_a?(String) && weight.match?(/\A[1-9][0-9]*\z/))
    end
    private_class_method :positive_integer_weight?
  end
end
