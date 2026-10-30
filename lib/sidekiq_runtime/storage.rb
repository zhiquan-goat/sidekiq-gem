# frozen_string_literal: true

module SidekiqRuntime
  # Names runtime-owned Redis keys within one Sidekiq logical database.
  class Storage
    attr_reader :namespace, :directory_key, :unique_prefix

    def self.default
      @default ||= new
    end

    def initialize(namespace: 'srt:v1')
      @namespace = String(namespace).dup.freeze
      unless /\A[a-zA-Z0-9:_-]+\z/.match?(@namespace)
        raise ArgumentError, 'namespace must contain only letters, digits, colons, underscores, or hyphens'
      end

      @directory_key = "#{@namespace}:queues".freeze
      @unique_prefix = "#{@namespace}:unique".freeze
      freeze
    end

    def valid_queue?(queue)
      queue.is_a?(String) && !queue.empty?
    end

    def validate_queue!(queue)
      raise ArgumentError, "Invalid Sidekiq queue: #{queue.inspect}" unless valid_queue?(queue)

      queue
    end

    def prefix(queue)
      "#{namespace}:queue:#{validate_queue!(queue)}"
    end
  end
end
