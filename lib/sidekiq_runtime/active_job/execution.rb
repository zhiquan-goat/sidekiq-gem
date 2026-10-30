# frozen_string_literal: true

module SidekiqRuntime
  module ActiveJob
    class RetryPersistenceError < StandardError; end

    # One per wrapper execution, carried only in memory and never written to Redis.
    # AJ's retry_job dup shares each RetryRequest; do not deep-copy it. The adapter
    # confirms a request only after its actual push returns a write acknowledgement.
    # The final AJ outcome describes this execution, independently of Redis ACK.
    class Execution
      MEMORY_KEY = :sidekiq_runtime_execution
      attr_accessor :outcome
      attr_reader :physical_queue

      class RetryRequest
        attr_reader :physical_queue

        def initialize(physical_queue)
          @physical_queue = physical_queue.dup.freeze
          @write_confirmed = false
        end

        def write_confirmed?
          @write_confirmed
        end

        def confirm_write!
          @write_confirmed = true
        end
      end

      def initialize(physical_queue:)
        @physical_queue = physical_queue.dup.freeze
        @retry_requests = []
        @outcome = :succeeded
      end

      def request_retry
        RetryRequest.new(physical_queue).tap { |request| @retry_requests << request }
      end

      def finalize_outcome!
        if @retry_requests.any? { |request| !request.write_confirmed? }
          raise RetryPersistenceError, 'Active Job requested a retry without a confirmed successor write'
        end
        @outcome = :retry_scheduled unless @retry_requests.empty?
        @outcome
      end
    end
  end
end
