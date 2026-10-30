# frozen_string_literal: true

module SidekiqRuntime
  module ActiveJob
    module Uniqueness
      extend ActiveSupport::Concern

      included do
        class_attribute :runtime_uniqueness_options, instance_writer: false, default: nil
      end

      class_methods do
        def runtime_uniqueness(enqueue_ttl:, runtime_ttl:, key:)
          unless enqueue_ttl.to_i.positive? && runtime_ttl.to_i.positive? && key.respond_to?(:call)
            raise ArgumentError, 'runtime_uniqueness requires positive TTLs and a callable key'
          end

          self.runtime_uniqueness_options = {enqueue_ttl: enqueue_ttl.to_i, runtime_ttl: runtime_ttl.to_i, key: key}.freeze
        end
      end
    end
  end
end
