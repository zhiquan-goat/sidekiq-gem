# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'sidekiq'
require 'time'
require 'goat_sidekiq_runtime/active_job'

module SidekiqRuntimeProcessEvidence
  module_function

  def directory
    ENV.fetch('SIDEKIQ_RUNTIME_PROCESS_SCENARIO_DIR')
  end

  def record(event, attributes)
    entry = attributes.merge(
      'event' => event,
      'pid' => Process.pid,
      'wall_time' => Time.now.utc.iso8601(6),
      'monotonic_time' => Process.clock_gettime(Process::CLOCK_MONOTONIC)
    )
    FileUtils.mkdir_p(directory)
    File.open(File.join(directory, 'journal.jsonl'), File::WRONLY | File::APPEND | File::CREAT, 0o600) do |file|
      file.flock(File::LOCK_EX)
      file.write(JSON.generate(entry))
      file.write("\n")
      file.flush
      file.fsync
    end
  end

  def mark(name, id)
    path = File.join(directory, "#{name}.#{id}")
    File.open(path, 'wb', 0o600) do |file|
      file.write("#{Process.pid}\n")
      file.flush
      file.fsync
    end
  end

  def wait_for(name, id)
    path = File.join(directory, "#{name}.#{id}")
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 180
    until File.exist?(path)
      raise "timed out waiting for #{path}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.02
    end
  end
end

SidekiqRuntime.event_handler = lambda do |event, payload|
  next unless %w[reliable_fetch.sidekiq_runtime orphan_reclaimer.sidekiq_runtime].include?(event)
  next if payload[:operation] == 'poll'

  SidekiqRuntimeProcessEvidence.record(payload[:operation], payload.slice(
    :jid, :job_id, :job_class, :physical_queue, :owner_identity, :outcome,
    :entry_id, :reason, :recovery_count
  ).transform_keys(&:to_s))
end

module SidekiqRuntimeProcessExecution
  def record_execution(attributes, facts)
    id = attributes.fetch('id')
    evidence = attributes.merge(facts)
    SidekiqRuntimeProcessEvidence.record('started', evidence)
    SidekiqRuntimeProcessEvidence.mark('started', id)
    SidekiqRuntimeProcessEvidence.wait_for('release', id) if attributes.fetch('mode') == 'block'
    if attributes.fetch('mode') == 'block_per_process'
      SidekiqRuntimeProcessEvidence.wait_for('release', "#{id}.#{Process.pid}")
    end
    sleep Float(attributes.fetch('work_seconds', 0))
    SidekiqRuntimeProcessEvidence.record('completed', evidence)
  end
end

class SidekiqRuntimeProcessJob
  include Sidekiq::Job
  include SidekiqRuntimeProcessExecution
  sidekiq_options queue: 'runtime_process_fixture', retry: false, dead: true

  def perform(attributes)
    payload = Sidekiq.load_json(_context.job.job)
    record_execution(attributes, 'redelivered' => SidekiqRuntime::JobMetadata.context(payload).redelivered?)
  end
end

class SidekiqRuntimeProcessAJ < ActiveJob::Base
  include SidekiqRuntimeProcessExecution
  self.queue_adapter = :sidekiq_runtime
  runtime_uniqueness enqueue_ttl: 30,
    runtime_ttl: Integer(ENV.fetch('SIDEKIQ_RUNTIME_PROCESS_RUNTIME_TTL', '1')),
    key: ->(job) { job.arguments.first.fetch('id') }

  def perform(attributes)
    record_execution(attributes, 'redelivered' => execution_context.redelivered?, 'job_id' => job_id)
  end
end

# Test-only faults cross the actual Processor fetch/ACK boundaries. One fault per
# scenario; a live owner requires manual restart before orphan recovery.
module SidekiqRuntimeProcessFaults
  def self.once(mode, payload)
    return false unless payload.dig('args', 0, 'mode') == mode
    path = File.join(SidekiqRuntimeProcessEvidence.directory, "fault.#{mode}")
    File.open(path, File::WRONLY | File::CREAT | File::EXCL) { |f| f.write(Process.pid.to_s) }
    SidekiqRuntimeProcessEvidence.record(mode, 'jid' => payload['jid'])
    true
  rescue Errno::EEXIST
    false
  end

  module Ack
    def acknowledge
      raise RedisClient::ConnectionError, 'injected ACK failure' if SidekiqRuntimeProcessFaults.once('ack_failure', Sidekiq.load_json(job))
      super
    end
  end

  module Fetch
    def retrieve_work
      work = super
      if work && SidekiqRuntimeProcessFaults.once('fetch_failure', Sidekiq.load_json(work.job))
        raise RedisClient::ProtocolError, 'injected fetch-return failure'
      end
      work
    end
  end
end
SidekiqRuntime::ReliableFetch::UnitOfWork.prepend(SidekiqRuntimeProcessFaults::Ack)
SidekiqRuntime::ReliableFetch.prepend(SidekiqRuntimeProcessFaults::Fetch)

if ENV['SIDEKIQ_RUNTIME_PROCESS_ACK_BARRIER'] == '1'
  module SidekiqRuntimeProcessAckBarrier
    def acknowledge
      payload = JSON.parse(job)
      attributes = payload.fetch('args').fetch(0)
      if attributes['mode'] == 'ack_barrier'
        id = attributes.fetch('id')
        SidekiqRuntimeProcessEvidence.record('ack_waiting', attributes)
        SidekiqRuntimeProcessEvidence.mark('ack_waiting', id)
        SidekiqRuntimeProcessEvidence.wait_for('ack_release', id)
      end
      super
    end
  end

  SidekiqRuntime::ReliableFetch::UnitOfWork.prepend(SidekiqRuntimeProcessAckBarrier)
end

Sidekiq.configure_server do |config|
  settings = SidekiqRuntime::RecoveryConfiguration.new(
    reclaim_interval: ENV.fetch('SIDEKIQ_RUNTIME_PROCESS_RECLAIM_INTERVAL', '5'))
  SidekiqRuntime.install_server!(config, redis_config: {url: ENV.fetch('SIDEKIQ_REDIS_URL')}, recovery: settings)
  config.on(:startup) do
    SidekiqRuntimeProcessEvidence.record('startup', {
      'recovery' => SidekiqRuntime::RecoveryConfiguration::DEFAULTS.keys.to_h { |key| [key.to_s, settings.public_send(key)] },
      'fetcher' => config.default_capsule.fetcher.class.name,
      'capsules' => config.capsules.keys,
      'namespace' => SidekiqRuntime::QueueKeys.storage.namespace
    })
  end
end
