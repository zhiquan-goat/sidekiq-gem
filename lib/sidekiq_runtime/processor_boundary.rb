# frozen_string_literal: true

require 'sidekiq/processor'

module SidekiqRuntime
  # ACK runs outside middleware. A surviving process may retain abandoned work;
  # report the exact claim for manual diagnosis without authorizing a replay.
  module ProcessorBoundary
    SCOPE_KEY = :sidekiq_runtime_executor
    private_constant :SCOPE_KEY

    def self.hold(work)
      scope = Thread.current[SCOPE_KEY]
      scope[:work] = work if scope
    end

    private

    def process_one
      previous = Thread.current[SCOPE_KEY]
      scope = {}
      Thread.current[SCOPE_KEY] = scope
      super
    ensure
      Thread.current[SCOPE_KEY] = previous
      work = scope&.fetch(:work, nil)
      if work && !work.released
        ReliableFetch.publish(operation: 'unconfirmed_release', outcome: 'manual_review', claim: work.claim)
      end
    end
  end
end
