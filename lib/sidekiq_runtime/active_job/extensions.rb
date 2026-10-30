# frozen_string_literal: true

module SidekiqRuntime
  module ActiveJob
    module Extensions
      def execution_context
        @runtime_execution_context ||= ExecutionContext.new
      end

      def serialize
        metadata = (@runtime_metadata ||= {'redelivered' => execution_context.redelivered?})
        data = super.merge(JobMetadata::KEY => metadata)
        @runtime_retry_request ? JobMetadata.with_redelivery(data) : data
      end

      def deserialize(data)
        super
        @runtime_metadata = data.fetch(JobMetadata::KEY, {}).deep_dup
        @runtime_execution_context = JobMetadata.context(data)
        @runtime_execution = data[Execution::MEMORY_KEY]
      end

      def retry_job(options = {})
        previous = @runtime_retry_request
        # Also mark known AJ retries on other adapters, without promising their
        # transport guarantees. Only the runtime wrapper verifies persistence.
        @runtime_retry_request = if @runtime_execution
          @runtime_execution.request_retry
        else
          Execution::RetryRequest.new(queue_name)
        end
        super
      ensure
        @runtime_retry_request = previous
      end

      def rescue_with_handler(*)
        result = super
        if result && @runtime_execution&.outcome == :succeeded
          @runtime_execution.outcome = :handled
        end
        result
      end

      private

      attr_reader :runtime_retry_request

      # Observe AJ's own decisions at their execution boundary, not through
      # notification subscribers (which are not part of ACK correctness).
      def instrument(operation, payload = {}, &block)
        result = super
        if @runtime_execution
          case operation
          when :discard then @runtime_execution.outcome = :discarded
          when :retry_stopped then @runtime_execution.outcome = :retry_exhausted
          end
        end
        result
      end

      def halted_callback_hook(filter, name)
        @runtime_execution.outcome = :aborted if name == :perform && @runtime_execution
        super
      end
    end
  end
end
