# frozen_string_literal: true

module SidekiqRuntime
  # Delivery facts, separate from business arguments and mutable transport state.
  class ExecutionContext
    def initialize(redelivered: false)
      @redelivered = redelivered == true
      freeze
    end

    def redelivered?
      @redelivered
    end
  end
end
