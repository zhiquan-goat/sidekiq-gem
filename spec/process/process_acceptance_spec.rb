# frozen_string_literal: true

require 'spec_helper'
require_relative '../sidekiq_runtime_process_support'

RSpec.describe 'Sidekiq runtime real process acceptance' do # rubocop:disable RSpec/DescribeClass
  unless ENV['SIDEKIQ_RUNTIME_PROCESS_ACCEPTANCE'] == '1'
    it 'runs only through the explicit serial evidence command' do
      skip 'use bin/test process with a dedicated Redis test endpoint'
    end
  else
    # One harness keeps a single append-only command index for the full evidence run.
    before(:context) do # rubocop:disable RSpec/BeforeAfterAll
      @sidekiq_runtime_process_harness = SidekiqRuntimeProcessSupport::Harness.new(
        evidence_dir: ENV.fetch('SIDEKIQ_RUNTIME_EVIDENCE_DIR')
      ) # rubocop:disable RSpec/InstanceVariable
    end
    let(:harness) { @sidekiq_runtime_process_harness } # rubocop:disable RSpec/InstanceVariable

    it 'keeps the process heartbeat during quiet and graceful shutdown', :ci_smoke do
      directory = harness.scenario('long-quiet-graceful')
      queue = harness.unique_name('long')
      observer_queue = harness.unique_name('observer')
      config = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 20)
      observer_config = harness.write_config(directory.join('observer'), queues: [observer_queue], concurrency: 1, timeout: 2)
      worker = observer = nil
      begin
        harness.cleanup(queues: [queue, observer_queue])
        harness.enqueue(queue: queue, id: 'long', mode: 'block')
        worker = harness.spawn_worker(name: 'worker', directory: directory, config: config)
        harness.wait_for_file(directory, 'started', 'long')
        before = harness.snapshot(name: 'before-quiet', directory: directory, queues: [queue])
        observer = harness.spawn_worker(name: 'observer', directory: directory, config: observer_config)
        harness.signal(worker, 'TSTP', directory: directory, name: 'quiet')
        sleep 7
        quiet = harness.snapshot(name: 'after-quiet', directory: directory, queues: [queue])
        harness.signal(worker, 'TERM', directory: directory, name: 'term')
        sleep 7
        waiting = harness.snapshot(name: 'after-shutdown', directory: directory, queues: [queue])
        [quiet, waiting].each do |snapshot|
          expect(snapshot.dig('queues', queue, 'inflight').map { |e| e['member'] })
            .to eq(before.dig('queues', queue, 'inflight').map { |e| e['member'] })
          expect(snapshot.dig('queues', queue, 'ready')).to be_empty
          expect(snapshot.dig('queues', queue, 'owner_heartbeats').values.first.fetch('ttl_ms')).to be > 0
        end
        expect(waiting.dig('queues', queue, 'owner_heartbeats').values.first.fetch('beat').to_f)
          .to be > before.dig('queues', queue, 'owner_heartbeats').values.first.fetch('beat').to_f
        harness.release(directory, 'release', 'long')
        expect(harness.wait_process(worker, directory: directory, name: 'worker')).to include(success: true)
        worker = nil
        final = harness.wait_for_empty([queue])
        expect(harness.journal(directory).count { |e| e['event'] == 'started' }).to eq(1)
        expect_terminal_empty(final, queue)
        harness.write_json(directory.join('assertions.json'), result: 'passed', quiet_wait: 7, shutdown_wait: 7)
      ensure
        harness.terminate_if_alive(worker)
        harness.terminate_if_alive(observer)
        harness.cleanup(queues: [queue, observer_queue])
      end
    end

    it 'uses real Manager hard shutdown to requeue original arguments without spending crash budget' do
      directory = harness.scenario('term-timeout-requeue')
      queue = harness.unique_name('term_timeout')
      config = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 1)
      worker = nil
      begin
        harness.cleanup(queues: [queue])
        enqueued = harness.enqueue(queue: queue, id: 'timeout', mode: 'block')
        worker = harness.spawn_worker(name: 'worker', directory: directory, config: config)
        harness.wait_for_file(directory, 'started', 'timeout')
        harness.signal(worker, 'TERM', directory: directory, name: 'term')
        expect(harness.wait_process(worker, directory: directory, name: 'worker')).to include(success: true)
        worker = nil
        after = harness.snapshot(name: 'after-shutdown', directory: directory, queues: [queue])
        state = after.dig('queues', queue)
        replacement = JSON.parse(state.fetch('ready').fetch(0))
        expect(replacement).to include(JSON.parse(enqueued.fetch('raw')))
        expect(replacement.dig('sidekiq_runtime', 'redelivered')).to be true
        expect(replacement.dig('sidekiq_runtime', 'recovery_count')).to be_nil
        expect(state.values_at('inflight', 'owners')).to all(be_empty)
        harness.write_json(directory.join('assertions.json'), result: 'passed', ready: replacement)
      ensure
        harness.terminate_if_alive(worker)
        harness.cleanup(queues: [queue])
      end
    end

    it 'automatically replays a KILLed AJ through two reclaimers with original logical identity and uniqueness snapshot', :ci_smoke do
      directory = harness.scenario('kill-aj-two-reclaimers')
      queue = harness.unique_name('kill_aj')
      config = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 3)
      worker = nil
      replacements = []
      begin
        harness.cleanup(queues: [queue])
        enqueued = harness.enqueue(queue: queue, id: 'kill-aj', mode: 'block', active_job: true)
        original = JSON.parse(enqueued.fetch('raw'))
        worker = harness.spawn_worker(name: 'worker', directory: directory, config: config)
        harness.wait_for_file(directory, 'started', 'kill-aj')
        before = harness.snapshot(name: 'before-kill', directory: directory, queues: [queue])
        harness.signal(worker, 'KILL', directory: directory, name: 'kill')
        expect(harness.wait_process(worker, directory: directory, name: 'worker')).to include(termsig: 9)
        worker = nil
        2.times { |i| replacements << harness.spawn_worker(name: "replacement-#{i}", directory: directory, config: config) }
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 90
        loop do
          break if harness.journal(directory).count { |e| e['event'] == 'started' && e['id'] == 'kill-aj' } == 2
          raise 'automatic AJ recovery timed out' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          sleep 0.1
        end
        recovered = harness.snapshot(name: 'automatic-replacement', directory: directory, queues: [queue])
        payload = JSON.parse(recovered.dig('queues', queue, 'inflight', 0, 'raw'))
        expect(payload['args'][0].except('sidekiq_runtime')).to eq(original['args'][0].except('sidekiq_runtime'))
        expect(payload.dig('sidekiq_runtime', 'uniqueness')).to eq(original.dig('sidekiq_runtime', 'uniqueness'))
        expect(payload.dig('sidekiq_runtime', 'recovery_count')).to eq(1)
        expect(payload['jid']).to eq(original['jid'])
        expect(recovered.dig('queues', queue, 'inflight', 0, 'member')).not_to eq(before.dig('queues', queue, 'inflight', 0, 'member'))
        harness.release(directory, 'release', 'kill-aj')
        final = harness.wait_for_empty([queue])
        events = harness.journal(directory)
        started = events.select { |e| e['event'] == 'started' && e['id'] == 'kill-aj' }
        expect(started.map { |e| e['redelivered'] }).to eq([false, true])
        expect(started.map { |e| e['job_id'] }.uniq).to eq([original['args'][0]['job_id']])
        expect(events.count { |e| e['event'] == 'completed' && e['id'] == 'kill-aj' }).to eq(1)
        expect(events.count { |e| e['event'] == 'recover' && e['outcome'] == 'requeued' }).to eq(1)
        expect_terminal_empty(final, queue)
        harness.write_json(directory.join('assertions.json'), result: 'passed', original: original, recovered: payload, started: started)
      ensure
        harness.terminate_if_alive(worker)
        replacements.each { |pid| harness.terminate_if_alive(pid) }
        harness.cleanup(queues: [queue])
      end
    end

    it 'automatically repeats a durable business completion interrupted before ACK' do
      directory = harness.scenario('kill-before-ack')
      queue = harness.unique_name('kill_ack')
      config = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 3)
      worker = replacement = nil
      begin
        harness.cleanup(queues: [queue])
        harness.enqueue(queue: queue, id: 'ack-window', mode: 'ack_barrier')
        worker = harness.spawn_worker(name: 'worker', directory: directory, config: config, ack_barrier: true)
        harness.wait_for_file(directory, 'ack_waiting', 'ack-window')
        harness.snapshot(name: 'before-kill', directory: directory, queues: [queue])
        harness.signal(worker, 'KILL', directory: directory, name: 'kill')
        expect(harness.wait_process(worker, directory: directory, name: 'worker')).to include(termsig: 9)
        worker = nil
        replacement = harness.spawn_worker(name: 'replacement', directory: directory, config: config)
        final = harness.wait_for_empty([queue], timeout: 90)
        completed = harness.journal(directory).select { |e| e['event'] == 'completed' }
        expect(completed.size).to eq(2)
        expect(completed.map { |e| e['redelivered'] }).to eq([false, true])
        expect_terminal_empty(final, queue)
        harness.write_json(directory.join('assertions.json'), result: 'passed', business_completions: 2, automatic: true)
      ensure
        harness.terminate_if_alive(worker)
        harness.terminate_if_alive(replacement)
        harness.cleanup(queues: [queue])
      end
    end

    %w[ack_failure fetch_failure].each do |fault|
      it "diagnoses #{fault} and recovers after manual owner restart", :ci_smoke do
        directory = harness.scenario("live-process-#{fault}")
        queue = harness.unique_name(fault)
        config = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 3)
        worker = nil
        begin
          harness.cleanup(queues: [queue])
          harness.enqueue(queue: queue, id: fault, mode: fault)
          worker = harness.spawn_worker(name: 'worker', directory: directory, config: config)
          deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
          until File.exist?(directory.join("fault.#{fault}"))
            raise 'fault not reached' if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
            sleep 0.1
          end
          sleep 2
          stranded = harness.snapshot(name: 'after-fault', directory: directory, queues: [queue])
          expect(stranded.dig('queues', queue, 'inflight').size).to eq(1)
          expect(stranded.dig('queues', queue, 'ready')).to be_empty
          expect(Process.kill(0, worker)).to eq(1)
          diagnostic = harness.journal(directory).find { |event| event['event'] == 'unconfirmed_release' }
          expect(diagnostic).to include('outcome' => 'manual_review', 'owner_identity' => stranded.dig('queues', queue, 'inflight', 0, 'owner'))
          expect(diagnostic['entry_id']).not_to be_nil
          original_pid = worker
          harness.signal(worker, 'TERM', directory: directory, name: 'manual-restart')
          expect(harness.wait_process(worker, directory: directory, name: 'original')).to include(success: true)
          worker = harness.spawn_worker(name: 'replacement', directory: directory, config: config)
          final = harness.wait_for_empty([queue], timeout: 90)
          events = harness.journal(directory)
          starts = events.select { |e| e['event'] == 'started' }
          expect(starts.size).to eq(fault == 'ack_failure' ? 2 : 1)
          expect(starts.last['redelivered']).to be true
          expect(starts.last['pid']).to eq(worker)
          expect(worker).not_to eq(original_pid)
          expect(events.count { |e| e['event'] == 'recover' && e['outcome'] == 'requeued' }).to eq(1)
          expect_terminal_empty(final, queue)
          harness.write_json(directory.join('assertions.json'), result: 'passed', manually_restarted: true, original_pid: original_pid, pid: worker, starts: starts)
        ensure
          harness.terminate_if_alive(worker)
          harness.cleanup(queues: [queue])
        end
      end
    end

    it 'shares one queue bucket across two workers and observes live rate changes without restart' do
      directory = harness.scenario('shared-queue-rate-limit')
      queue = harness.unique_name('shared_rate')
      config = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 5)
      workers = []

      begin
        harness.cleanup(queues: [queue])
        harness.update_rate_limit(queue: queue, enabled: true, rate: 2, burst: 2)
        shared = 8.times.map do |index|
          harness.enqueue(queue: queue, id: "shared-#{index}", mode: 'timed', work_seconds: 0.2)
        end
        workers << harness.spawn_worker(name: 'worker-a', directory: directory, config: config)
        workers << harness.spawn_worker(name: 'worker-b', directory: directory, config: config)
        harness.wait_for_empty([queue])

        shared_jids = shared.map { |entry| entry.fetch('jid') }
        shared_claims = harness.journal(directory).select do |event|
          event.fetch('event') == 'claimed' && shared_jids.include?(event.fetch('jid'))
        end
        shared_starts = harness.journal(directory).select do |event|
          event.fetch('event') == 'started' && event.fetch('id').start_with?('shared-')
        end
        elapsed = shared_claims.last.fetch('monotonic_time') - shared_claims.first.fetch('monotonic_time')
        expect(shared_claims.size).to eq(8)
        expect(shared_claims.size).to be <= 2 + (2 * elapsed) + 1
        expect(shared_starts.map { |event| event.fetch('pid') }.uniq.size).to eq(2)

        harness.update_rate_limit(queue: queue, enabled: true, rate: 100, burst: 3)
        harness.enqueue(queue: queue, id: 'high-warmup', mode: 'timed', work_seconds: 0.05)
        harness.wait_for_empty([queue], timeout: 10)

        harness.update_rate_limit(queue: queue, enabled: true, rate: 0.01, burst: 1)
        3.times do |index|
          harness.enqueue(queue: queue, id: "raise-#{index}", mode: 'timed', work_seconds: 0.05)
        end
        harness.wait_for_file(directory, 'started', 'raise-0')
        harness.wait_for_journal(directory, event: 'completed', id: 'raise-0')
        before_raise = harness.snapshot(name: 'before-raise', directory: directory, queues: [queue])
        expect(before_raise.dig('queues', queue, 'ready').size).to eq(2)
        harness.update_rate_limit(queue: queue, enabled: true, rate: 100, burst: 3)
        harness.wait_for_empty([queue], timeout: 10)

        harness.update_rate_limit(queue: queue, enabled: true, rate: 0.01, burst: 1)
        2.times do |index|
          harness.enqueue(queue: queue, id: "disable-#{index}", mode: 'timed', work_seconds: 0.05)
        end
        harness.wait_for_file(directory, 'started', 'disable-0')
        harness.wait_for_journal(directory, event: 'completed', id: 'disable-0')
        before_disable = harness.snapshot(name: 'before-disable', directory: directory, queues: [queue])
        expect(before_disable.dig('queues', queue, 'ready').size).to eq(1)
        harness.update_rate_limit(queue: queue, enabled: false)
        final = harness.wait_for_empty([queue], timeout: 10)

        exits = workers.each_with_index.map do |pid, index|
          harness.signal(pid, 'TERM', directory: directory, name: "worker-#{index}-term")
          harness.wait_process(pid, directory: directory, name: "worker-#{index}")
        end
        workers.clear
        events = harness.journal(directory)
        expect(exits).to all(include(success: true, exit_status: 0))
        expect(events.count { |event| event.fetch('event') == 'completed' }).to eq(14)
        expect(final.dig('queues', queue, 'rate_limit')).to be_nil
        expect_terminal_empty(final, queue)
        harness.write_json(
          directory.join('assertions.json'),
          result: 'passed',
          shared_claim_count: shared_claims.size,
          shared_claim_elapsed_seconds: elapsed,
          shared_claim_upper_bound_with_one_token_timing_tolerance: 2 + (2 * elapsed) + 1,
          participating_worker_pids: shared_starts.map { |event| event.fetch('pid') }.uniq,
          live_rate_decrease_left_ready_jobs: 2,
          live_rate_raise_drained_previous_ready_within_seconds: 10,
          live_disable_observed: true,
          worker_statuses: exits
        )
      ensure
        workers.each { |pid| harness.terminate_if_alive(pid) }
        harness.cleanup(queues: [queue])
      end
    end

    it 'retains checkpointed Ready, Inflight, owner, and directory across Redis restart' do
      directory = harness.scenario('redis-restart')
      queue = harness.unique_name('restart')
      config = harness.write_config(directory, queues: [queue], concurrency: 1, timeout: 5)
      worker = nil

      begin
        harness.cleanup(queues: [queue])
        harness.enqueue(queue: queue, id: 'restart-inflight', mode: 'block')
        worker = harness.spawn_worker(name: 'worker', directory: directory, config: config)
        harness.wait_for_claim(queue)
        harness.wait_for_file(directory, 'started', 'restart-inflight')
        harness.signal(worker, 'KILL', directory: directory, name: 'kill')
        harness.wait_process(worker, directory: directory, name: 'worker')
        worker = nil
        harness.enqueue(queue: queue, id: 'restart-ready', mode: 'block')

        before = harness.snapshot(
          name: 'before-restart', directory: directory,
          queues: [queue]
        )
        redis_before = harness.server_config(name: 'before-restart', directory: directory)
        expect(harness.save_checkpoint(directory: directory)).to eq('save' => 'OK')

        harness.restart_redis

        redis_after = harness.server_config(name: 'after-restart', directory: directory)
        after = harness.snapshot(
          name: 'after-restart', directory: directory,
          queues: [queue]
        )

        expect(redis_after.fetch('run_id')).not_to eq(redis_before.fetch('run_id'))
        expect(after.dig('queues', queue).except('owner_heartbeats')).to eq(before.dig('queues', queue).except('owner_heartbeats'))
        expect(after['sorted_sets']).to eq(before['sorted_sets'])
        harness.release(directory, 'release', 'restart-inflight')
        harness.release(directory, 'release', 'restart-ready')
        worker = harness.spawn_worker(name: 'after-restart-worker', directory: directory, config: config)
        final = harness.wait_for_empty([queue], timeout: 90)
        expect_terminal_empty(final, queue)
        expect(harness.journal(directory).count { |event| event['event'] == 'completed' }).to eq(2)
        harness.signal(worker, 'TERM', directory: directory, name: 'after-restart-term')
        expect(harness.wait_process(worker, directory: directory, name: 'after-restart-worker')).to include(success: true)
        worker = nil
        harness.write_json(
          directory.join('assertions.json'),
          result: 'passed',
          redis_run_id_changed: true,
          redis_process_ids: [redis_before.fetch('process_id'), redis_after.fetch('process_id')],
          checkpoint: 'SAVE',
          automatic_recovery_after_restart: true,
          script_cache_reloaded_after_restart: true,
          rebuild_or_disk_loss_tested: false
        )
      ensure
        harness.terminate_if_alive(worker)
        harness.cleanup(queues: [queue])
      end
    end

    def expect_terminal_empty(snapshot, queue)
      state = snapshot.fetch('queues').fetch(queue)
      expect(state.fetch('ready')).to be_empty
      expect(state.fetch('inflight')).to be_empty
      expect(state.fetch('owners')).to be_empty
      expect(snapshot.fetch('sorted_sets').values).to all(be_empty)
    end
  end
end
