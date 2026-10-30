# frozen_string_literal: true

require 'spec_helper'
require_relative '../sidekiq_runtime_process_support'

RSpec.describe 'Independent process-heartbeat process safety' do
  it 'preserves the replacement claim and runtime token when a STOPped AJ resumes after automatic recovery' do
    skip 'explicit local process command required' unless ENV['SIDEKIQ_RUNTIME_PROCESS_ACCEPTANCE'] == '1'

    allow(SidekiqRuntime::QueueKeys).to receive(:storage).and_return(SidekiqRuntime::Storage.default)
    harness = SidekiqRuntimeProcessSupport::Harness.new(evidence_dir: ENV.fetch('SIDEKIQ_RUNTIME_EVIDENCE_DIR'))
    directory = harness.scenario('stop-cont-process-heartbeats')
    queue = harness.unique_name('stop_cont')
    path = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 3)
    previous_ttl = ENV['SIDEKIQ_RUNTIME_PROCESS_RUNTIME_TTL']
    ENV['SIDEKIQ_RUNTIME_PROCESS_RUNTIME_TTL'] = '30'
    config = Sidekiq::Config.new.tap do |value|
      value.logger = Logger.new(File::NULL)
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
    store = SidekiqRuntime::InflightStore.new(config,
      queue: queue)
    workers = []
    stopped = nil
    begin
      harness.cleanup(queues: [queue])
      enqueued = harness.enqueue(queue: queue, id: 'paused', mode: 'block_per_process', active_job: true)
      original = JSON.parse(enqueued.fetch('raw'))
      lock = SidekiqUniqueness.lock_for(job_payload: original, physical_queue: queue)
      worker = harness.spawn_worker(name: 'original', directory: directory, config: path, default_recovery: true)
      workers << worker
      harness.wait_for_file(directory, 'started', 'paused')
      first = store.entries.fetch(0)
      token_a = config.redis { |redis| redis.get(lock.runtime_redis_key) }
      expect(token_a).not_to be_nil
      harness.signal(worker, 'STOP', directory: directory, name: 'stop-original')
      stopped = worker
      at_stop = store.owner_state(first)
      expires_at = at_stop.fetch(:redis_now) + at_stop.fetch(:heartbeat_ttl_seconds)
      harness.write_json(directory.join('stop-heartbeat.json'), at_stop)
      harness.snapshot(name: 'stopped', directory: directory, queues: [queue])

      2.times do |i|
        workers << harness.spawn_worker(name: "replacement-#{i}", directory: directory, config: path, default_recovery: true)
      end
      started = wait_safety(timeout: 80) do
        rows = harness.journal(directory).select { |event| event['event'] == 'started' }
        rows if rows.size == 2
      end
      replacement_pid = started.last.fetch('pid')
      expect(replacement_pid).not_to eq(worker)
      replacement = store.entries.fetch(0)
      expect(replacement.member).not_to eq(first.member)
      expect(replacement.owner_identity).not_to eq(first.owner_identity)
      expect(replacement.claimed_at).to be >= expires_at
      expect(replacement.claimed_at - expires_at).to be < 15
      expect(replacement.payload['args'][0].except('sidekiq_runtime')).to eq(original['args'][0].except('sidekiq_runtime'))
      expect(replacement.payload.dig('sidekiq_runtime', 'uniqueness')).to eq(original.dig('sidekiq_runtime', 'uniqueness'))
      expect(replacement.payload.dig('sidekiq_runtime', 'recovery_count')).to eq(1)
      token_b = config.redis { |redis| redis.get(lock.runtime_redis_key) }
      expect(token_b).not_to be_nil
      expect(token_b).not_to eq(token_a)
      harness.snapshot(name: 'replacement-running', directory: directory, queues: [queue])

      harness.signal(worker, 'CONT', directory: directory, name: 'continue-original')
      stopped = nil
      harness.release(directory, 'release', "paused.#{worker}")
      old_ack = wait_safety(timeout: 6) do
        harness.journal(directory).find { |event| event['event'] == 'acknowledged' && event['pid'] == worker }
      end
      expect(old_ack['outcome']).to eq('missing')
      expect(store.entries.map(&:member)).to eq([replacement.member])
      expect(store.owner_state(replacement)).to include(present: true, owner_identity: replacement.owner_identity)
      expect(config.redis { |redis| redis.get(lock.runtime_redis_key) }).to eq(token_b)
      expect(store.acknowledge(first)).to eq(0)
      expect(store.requeue(first)).to eq(0)
      protected = harness.snapshot(name: 'after-old-completion', directory: directory, queues: [queue])
      expect(protected.dig('queues', queue, 'ready')).to be_empty
      expect(protected.fetch('sorted_sets').values).to all(be_empty)
      expect(harness.journal(directory).count { |event| event['event'] == 'completed' }).to eq(1)

      harness.release(directory, 'release', "paused.#{replacement_pid}")
      final = harness.wait_for_empty([queue], timeout: 10)
      expect(final.dig('queues', queue).values_at('ready', 'inflight', 'owners')).to all(be_empty)
      expect(config.redis { |redis| redis.get(lock.runtime_redis_key) }).to be_nil
      events = harness.journal(directory)
      expect(events.count { |event| event['event'] == 'recover' && event['outcome'] == 'requeued' }).to eq(1)
      expect(events.count { |event| event['event'] == 'completed' }).to eq(2)
      expect(started.map { |event| event['redelivered'] }).to eq([false, true])
      expect(started.map { |event| event['job_id'] }.uniq).to eq([original.dig('args', 0, 'job_id')])
      startups = events.select { |event| event['event'] == 'startup' }
      expect(startups.size).to eq(3)
      startups.each do |startup|
        expect(startup['recovery']).to eq(SidekiqRuntime::RecoveryConfiguration::DEFAULTS.transform_keys(&:to_s))
        expect(startup).to include('fetcher' => 'SidekiqRuntime::ReliableFetch', 'capsules' => ['default'], 'namespace' => 'srt:v1')
      end
      harness.write_json(directory.join('assertions.json'), result: 'passed', first: first.to_h,
        replacement: replacement.to_h, heartbeat_at_stop: at_stop, old_ack: old_ack,
        token_a: token_a, token_b: token_b, started: started, final: final,
        recovery_delay_after_deadline: replacement.claimed_at - expires_at)
      workers.each_with_index do |pid, i|
        harness.signal(pid, 'TERM', directory: directory, name: "term-#{i}")
        expect(harness.wait_process(pid, directory: directory, name: "worker-#{i}")).to include(success: true)
      end
      workers.clear
    ensure
      Process.kill('CONT', stopped) if stopped
      workers.each { |pid| harness.terminate_if_alive(pid) }
      if lock
        config.redis { |redis| redis.del(lock.enqueue_redis_key, lock.runtime_redis_key) }
      end
      harness.cleanup(queues: [queue])
      ENV['SIDEKIQ_RUNTIME_PROCESS_RUNTIME_TTL'] = previous_ttl
    end
  end

  def wait_safety(timeout:)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      value = yield
      return value if value
      raise "safety condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
      sleep 0.1
    end
  end
end
