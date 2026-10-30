# frozen_string_literal: true

require 'digest'
require 'fileutils'
require 'json'
require 'open3'
require 'pathname'
require 'securerandom'
require 'time'
require 'timeout'
require 'yaml'

module SidekiqRuntimeProcessSupport
  class Harness
    attr_reader :evidence_dir, :root

    def initialize(evidence_dir:, root: File.expand_path('..', __dir__))
      @root = Pathname(root).expand_path
      @evidence_dir = Pathname(evidence_dir).expand_path
      FileUtils.mkdir_p(@evidence_dir)
      @commands = []
      @sequence = 0
    end

    def unique_name(label)
      "runtime_process_#{label}_#{SecureRandom.hex(4)}"
    end

    def scenario(name)
      path = evidence_dir.join(name)
      raise "scenario evidence directory must be empty: #{path}" if path.directory? && !path.empty?

      FileUtils.mkdir_p(path)
      path
    end

    def write_config(directory, queues:, concurrency:, timeout:)
      configuration = {
        'queues' => queues,
        'concurrency' => concurrency,
        'timeout' => timeout
      }
      path = Pathname(directory).join('sidekiq.yml')
      durable_write(path, YAML.dump(configuration))
      path
    end

    def spawn_worker(name:, directory:, config:, ack_barrier: false, default_recovery: false)
      stdout = Pathname(directory).join("#{name}.stdout.log")
      stderr = Pathname(directory).join("#{name}.stderr.log")
      environment = {'SIDEKIQ_RUNTIME_PROCESS_SCENARIO_DIR' => Pathname(directory).to_s,
        'SIDEKIQ_RUNTIME_PROCESS_RECLAIM_INTERVAL' => '0.5'}
      environment['SIDEKIQ_RUNTIME_PROCESS_RECLAIM_INTERVAL'] = nil if default_recovery
      environment['SIDEKIQ_RUNTIME_PROCESS_ACK_BARRIER'] = '1' if ack_barrier
      command = [runner, 'process-worker', '-C', Pathname(config).to_s, '-e', 'test']
      pid = Process.spawn(
        environment,
        *command,
        chdir: root.to_s,
        out: stdout.to_s,
        err: stderr.to_s
      )
      record_command(
        kind: 'worker',
        name: name,
        argv: command,
        pid: pid,
        stdout: relative_evidence(stdout),
        stderr: relative_evidence(stderr)
      )
      pid
    end

    def enqueue(queue:, id:, mode:, work_seconds: 0, active_job: false)
      probe(
        'enqueue',
        {
          queue: queue,
          active_job: active_job,
          arguments: {id: id, mode: mode, work_seconds: work_seconds}
        },
        record: true
      )
    end

    def update_rate_limit(queue:, enabled:, rate: nil, burst: nil)
      probe(
        'rate-limit-update',
        {queue: queue, enabled: enabled, rate: rate, burst: burst}.compact,
        record: true
      )
    end

    def cleanup(queues:)
      probe('cleanup', {queues: queues}, record: true)
    end

    def snapshot(name:, directory:, queues:)
      value = probe('snapshot', {queues: queues}, record: true)
      write_json(Pathname(directory).join("#{name}.snapshot.json"), value)
      value
    end

    def server_config(name:, directory:)
      value = probe('server-config', {}, record: true)
      write_json(Pathname(directory).join("#{name}.redis-server.json"), value)
      value
    end

    def save_checkpoint(directory:)
      value = probe('save', {}, record: true)
      write_json(Pathname(directory).join('save-checkpoint.json'), value)
      value
    end

    def wait_for_claim(queue, timeout: 40)
      wait_until(timeout: timeout) do
        value = probe('snapshot', {queues: [queue]}, record: false)
        state = value.fetch('queues').fetch(queue)
        state.fetch('inflight').one? ? value : nil
      end
    end

    def wait_for_empty(queues, timeout: 60)
      wait_until(timeout: timeout) do
        value = probe('snapshot', {queues: queues}, record: false)
        empty = value.fetch('queues').values.all? do |state|
          state.fetch('ready').empty? && state.fetch('inflight').empty? && state.fetch('owners').empty?
        end
        sorted_empty = value.fetch('sorted_sets').values.all?(&:empty?)
        empty && sorted_empty ? value : nil
      end
    end

    def wait_for_file(directory, name, id, timeout: 40)
      path = Pathname(directory).join("#{name}.#{id}")
      wait_until(timeout: timeout) { path.exist? ? path : nil }
    end

    def wait_for_journal(directory, event:, id:, timeout: 40)
      wait_until(timeout: timeout) do
        journal(directory).find do |entry|
          entry['event'] == event && entry['id'] == id
        end
      end
    end

    def release(directory, name, id)
      durable_write(Pathname(directory).join("#{name}.#{id}"), "release\n")
    end

    def signal(pid, signal, directory:, name:)
      Process.kill(signal, pid)
      entry = {
        signal: signal,
        pid: pid,
        wall_time: Time.now.utc.iso8601(6),
        monotonic_time: Process.clock_gettime(Process::CLOCK_MONOTONIC)
      }
      write_json(Pathname(directory).join("#{name}.signal.json"), entry)
      record_command(kind: 'process_signal', name: name, pid: pid, signal: signal)
      entry
    end

    def wait_process(pid, directory:, name:, timeout: 45)
      status = Timeout.timeout(timeout) do
        loop do
          waited, process_status = Process.waitpid2(pid, Process::WNOHANG)
          break process_status if waited

          sleep 0.05
        end
      end
      result = {
        pid: pid,
        success: status.success?,
        exit_status: status.exitstatus,
        termsig: status.termsig,
        finished_at: Time.now.utc.iso8601(6)
      }
      write_json(Pathname(directory).join("#{name}.exit.json"), result)
      record_command({kind: 'process_exit', name: name}.merge(result))
      result
    rescue Timeout::Error
      Process.kill('KILL', pid)
      Process.wait(pid)
      raise "worker #{pid} did not exit within #{timeout}s"
    end

    def terminate_if_alive(pid)
      return unless pid

      Process.kill(0, pid)
      Process.kill('KILL', pid)
      Process.wait(pid)
    rescue Errno::ESRCH, Errno::ECHILD
      nil
    end

    def journal(directory)
      path = Pathname(directory).join('journal.jsonl')
      return [] unless path.exist?

      path.readlines(chomp: true).reject(&:empty?).map { |line| JSON.parse(line) }
    end

    def restart_redis
      command = ENV.fetch('GOAT_RUNTIME_TEST_REDIS_RESTART_COMMAND')
      raise ArgumentError, 'restart command must be an absolute executable path' unless
        Pathname(command).absolute? && File.executable?(command)

      host_command('redis-restart', command)
      wait_until(timeout: 30) do
        probe('ping', {}, record: false)
      rescue RedisClient::ConnectionError
        nil
      end
    end

    def host_command(name, *command)
      stdout, stderr, status = Open3.capture3(*command, chdir: root.to_s)
      sequence = next_sequence
      stdout_path = evidence_dir.join(format('%03d-%s.stdout.log', sequence, name))
      stderr_path = evidence_dir.join(format('%03d-%s.stderr.log', sequence, name))
      durable_write(stdout_path, stdout)
      durable_write(stderr_path, stderr)
      record_command(
        kind: 'host',
        name: name,
        argv: command,
        exit_status: status.exitstatus,
        success: status.success?,
        stdout: relative_evidence(stdout_path),
        stderr: relative_evidence(stderr_path)
      )
      raise "#{command.join(' ')} failed with #{status.exitstatus}: #{stderr}" unless status.success?

      stdout
    end

    def write_json(path, value)
      durable_write(path, JSON.pretty_generate(value) + "\n")
    end

    private

    def probe(action, payload, record:)
      command = [runner, 'redis-probe', action, JSON.generate(payload)]
      stdout, stderr, status = Open3.capture3(*command, chdir: root.to_s)
      if record
        sequence = next_sequence
        stdout_path = evidence_dir.join(format('%03d-redis-probe-%s.stdout.log', sequence, action))
        stderr_path = evidence_dir.join(format('%03d-redis-probe-%s.stderr.log', sequence, action))
        durable_write(stdout_path, stdout)
        durable_write(stderr_path, stderr)
        record_command(
          kind: 'redis_probe',
          name: action,
          argv: [runner, 'redis-probe', action, "payload_sha256=#{Digest::SHA256.hexdigest(JSON.generate(payload))}"],
          exit_status: status.exitstatus,
          success: status.success?,
          stdout: relative_evidence(stdout_path),
          stderr: relative_evidence(stderr_path)
        )
      end
      raise "Redis probe #{action} failed with #{status.exitstatus}: #{stderr}" unless status.success?

      JSON.parse(stdout)
    end

    def wait_until(timeout:)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      loop do
        result = yield
        return result if result
        raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        sleep 0.05
      end
    end

    def record_command(entry)
      @commands << entry.merge(recorded_at: Time.now.utc.iso8601(6))
      write_json(evidence_dir.join('commands.json'), @commands)
    end

    def next_sequence
      @sequence += 1
    end

    def runner
      root.join('bin/test').to_s
    end

    def relative_evidence(path)
      Pathname(path).relative_path_from(evidence_dir).to_s
    end

    def durable_write(path, contents)
      FileUtils.mkdir_p(Pathname(path).dirname)
      File.open(path, 'wb', 0o600) do |file|
        file.write(contents)
        file.flush
        file.fsync
      end
      path
    end
  end
end
