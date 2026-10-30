# frozen_string_literal: true

require_relative 'sidekiq_runtime/telemetry'

# One stable producer snapshot; consumers never invoke business identity code.
module SidekiqUniqueness
  STRATEGY = :until_and_while_executing
  INTERNAL_CONTINUATION_ATTRIBUTE = 'sidekiq_uniqueness_internal_continuation'
  RUNTIME_CONFLICT_CONTINUATION = 'runtime_conflict'

  class UnsupportedBulk < ArgumentError; end

  public_constant :STRATEGY
  private_constant :INTERNAL_CONTINUATION_ATTRIBUTE, :RUNTIME_CONFLICT_CONTINUATION

  def self.internal_continuation?(job_payload)
    job_payload.is_a?(Hash) &&
      job_payload[INTERNAL_CONTINUATION_ATTRIBUTE] == RUNTIME_CONFLICT_CONTINUATION
  end

  def self.mark_runtime_conflict_continuation(job_payload)
    job_payload.merge(INTERNAL_CONTINUATION_ATTRIBUTE => RUNTIME_CONFLICT_CONTINUATION)
  end

  def self.unique_job?(job_payload)
    job_payload.is_a?(Hash) && SidekiqRuntime::JobMetadata.uniqueness(job_payload)
  end

  def self.snapshot(job_class:, queue:, identity:, enqueue_ttl:, runtime_ttl:, key_prefix: SidekiqRuntime::QueueKeys.unique_prefix)
    # Class namespaces and queue names can both contain colons.
    {
      'job_class' => job_class, 'queue' => queue, 'identity' => identity,
      'key_prefix' => "#{key_prefix}:#{job_class.bytesize}:#{job_class}:#{queue}",
      'enqueue_ttl' => enqueue_ttl.to_i, 'runtime_ttl' => runtime_ttl.to_i
    }.deep_dup
  end

  def self.lock_for(job_payload:, physical_queue:)
    unique = SidekiqRuntime::JobMetadata.uniqueness(job_payload)
    return unless unique
    unless unique.fetch('queue') == physical_queue
      raise ArgumentError, 'Unique job must remain on its original physical queue'
    end

    UntilAndWhileExecuting.new(
      key_prefix: unique.fetch('key_prefix'),
      identity: unique.fetch('identity'),
      enqueue_ttl: unique.fetch('enqueue_ttl'), runtime_ttl: unique.fetch('runtime_ttl'),
      metadata: job_payload
    )
  end
end
