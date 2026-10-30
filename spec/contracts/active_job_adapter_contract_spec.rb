require_relative '../support/active_job_runtime'
require 'open3'

RSpec.describe ActiveJob::QueueAdapters::SidekiqRuntimeAdapter do
  include ActiveJobRuntimeSupport

  it 'loads native runtime without loading Active Job or Rails' do
    output, status = Open3.capture2e(RbConfig.ruby, '-Ilib', '-rgoat_sidekiq_runtime', '-e',
      'abort "optional framework loaded" if defined?(ActiveJob) || defined?(Rails); puts "native-only OK"')
    expect(status.success?).to be(true), output
    expect(output).to include('native-only OK')
  end

  it 'resolves the standard adapter name after optional loading' do
    job = build_job
    job.queue_adapter = :sidekiq_runtime
    expect(job.queue_adapter).to be_a(described_class)
    expect(job.queue_adapter.enqueue_after_transaction_commit?).to be(true)
  end

  it 'runs serialized arguments through real AJ and Processor with the actual provider JID' do
    record_class = Class.new do
      include GlobalID::Identification
      attr_reader :id
      def initialize(id) = @id = id
      def self.find(id) = new(id.to_i)
    end
    stub_const('SpecAJRecord', record_class)
    old_app = GlobalID.app
    GlobalID.app = 'runtime-spec'
    job_class = build_job
    args = [SpecAJRecord.new(42), {symbol: :value, date: Date.new(2026, 10, 23)}, 3]
    job = job_class.perform_later(*args)
    wire = persisted('ready').fetch(0)
    expect(wire).to include('wrapped' => job_class.name, 'retry' => 0, 'dead' => true, 'jid' => job.provider_job_id)
    expect(wire.dig('args', 0, 'job_id')).to eq(job.job_id)
    expect(wire.dig('args', 0, 'arguments', 0)).to include('_aj_globalid' => 'gid://runtime-spec/SpecAJRecord/42')
    process_next
    execution = job_class.records.fetch(0)
    expect(execution).to include(job_id: job.job_id, jid: job.provider_job_id, executions: 1)
    expect(execution[:args][0].id).to eq(42)
    expect(execution[:args][1..]).to eq(args[1..])
    expect(execution[:context].redelivered?).to be(false)
    expect(execution[:context]).to be_frozen
    expect { execution[:context].instance_variable_set(:@redelivered, true) }.to raise_error(FrozenError)
    expect(store.size).to eq(0)
    expect(outcomes).to eq([:succeeded])
    expect(SidekiqRuntime::Telemetry.fields(wire)).to include(job_class: job_class.name, job_id: job.job_id)
  ensure
    GlobalID.app = old_app if old_app
  end

  it 'keeps delayed work in Scheduled until due and preserves physical queue overrides' do
    job_class = build_job
    job_class.queue_name = 'unused_default'
    job = job_class.set(queue: queue, wait: 60).perform_later('later')
    expect(persisted('ready')).to be_empty
    expect(persisted('schedule').fetch(0)['jid']).to eq(job.provider_job_id)
    promote(now: Time.now.to_f)
    expect(persisted('ready')).to be_empty
    promote(now: Time.now.to_f + 120)
    process_next
    expect(job_class.records.fetch(0)[:args]).to eq(['later'])
    expect(store.size).to eq(0)
  end

  [nil, false].each do |refusal|
    it "makes perform_later false on real middleware refusal #{refusal.inspect}, even with transactional_push" do
      job_class = build_job
      backend = transaction_probe(job_class, open: false)
      middleware = Class.new { define_method(:call) { |*| refusal } }
      config.client_middleware { |chain| chain.add(middleware) }
      with_transactional_push do
        job = nil
        expect(job_class.perform_later('filtered') { |value| job = value }).to be(false)
        expect(job.enqueue_error).to be_a(ActiveJob::EnqueueError)
        expect(job.provider_job_id).to be_nil
        expect(backend.registrations).to eq(1)
        expect(persisted('ready')).to be_empty
      end
    end
  end

  it 'reports a lost reply as a transport exception with a real persisted message' do
    job_class = build_job
    lose_client_reply
    expect { job_class.perform_later('unknown') }.to raise_error(RedisClient::ReadTimeoutError)
    expect(persisted('ready').size).to eq(1)
  end

  it 'pins explicit config pool and middleware instead of an ambient pool' do
    job_class = build_job
    wrong_pool = Object.new
    Sidekiq::Client.via(wrong_pool) { expect(job_class.perform_later('pinned')).to be_a(job_class) }
    expect(persisted('ready').size).to eq(1)
  end

  it 'uses the current capsule middleware with the default adapter' do
    job_class = build_job
    job_class.queue_adapter = :sidekiq_runtime
    capsule = config.default_capsule
    middleware = Class.new { def call(*) = nil }
    capsule.client_middleware { |chain| chain.add(middleware) }
    previous = Thread.current[:sidekiq_capsule]
    Thread.current[:sidekiq_capsule] = capsule
    expect(job_class.perform_later('filtered')).to be(false)
    expect(persisted('ready')).to be_empty
  ensure
    Thread.current[:sidekiq_capsule] = previous
  end

  it 'uses exactly one AJ deferral for immediate and delayed jobs and drops rolled-back callbacks (framework probe)' do
    job_class = build_job
    backend = transaction_probe(job_class)
    with_transactional_push do
      first = job_class.perform_later('now')
      second = job_class.set(wait: 60).perform_later('later')
      expect(first).to be_successfully_enqueued
      expect(first.provider_job_id).to be_nil
      expect(persisted('ready') + persisted('schedule')).to be_empty
      expect(backend.pending.size).to eq(2)
      backend.commit
      expect(backend.registrations).to eq(2)
      expect(persisted('ready').map { |j| j['jid'] }).to eq([first.provider_job_id])
      expect(persisted('schedule').map { |j| j['jid'] }).to eq([second.provider_job_id])
      backend.open = true
      job_class.perform_later('rollback')
      job_class.set(wait: 60).perform_later('rollback delayed')
      backend.rollback
      expect(persisted('ready').size).to eq(1)
      expect(persisted('schedule').size).to eq(1)
    end
  end

  it 'keeps post-commit rejection visible on the job (framework probe)' do
    job_class = build_job
    backend = transaction_probe(job_class)
    middleware = Class.new { def call(*) = nil }
    config.client_middleware { |chain| chain.add(middleware) }
    job = job_class.perform_later('deferred')
    expect(job).to be_successfully_enqueued
    backend.commit
    expect(job).not_to be_successfully_enqueued
    expect(job.enqueue_error).to be_a(ActiveJob::EnqueueError)
    expect(persisted('ready')).to be_empty
  end

  it 'propagates post-commit transport failure (framework probe)' do
    job_class = build_job
    backend = transaction_probe(job_class)
    job_class.perform_later('unknown commit')
    lose_client_reply
    expect { backend.commit }.to raise_error(RedisClient::ReadTimeoutError)
    expect(persisted('ready').size).to eq(1)
  end

  it 'preserves framework ordinary bulk behavior and per-job results' do
    job_class = build_job
    jobs = [job_class.new('one'), job_class.new('two').set(wait: 60)]
    expect(ActiveJob.perform_all_later(jobs)).to be_nil
    expect(jobs).to all(be_successfully_enqueued)
    expect(persisted('ready').map { |j| j['jid'] }).to eq([jobs[0].provider_job_id])
    expect(persisted('schedule').map { |j| j['jid'] }).to eq([jobs[1].provider_job_id])
  end

  it 'leaves native TransactionAwareClient deferral intact (framework probe)' do
    job_class = build_job
    backend = transaction_probe(job_class)
    native = Class.new do
      include Sidekiq::Job
      def perform(*) = nil
    end
    stub_const('SpecAJNativeTransactionJob', native)
    with_transactional_push do
      native.sidekiq_options queue: queue
      jid = Sidekiq::Client.via(SidekiqRuntime::RedisScope.pool(config)) { native.perform_async('native') }
      expect(jid).to be_a(String)
      expect(persisted('ready')).to be_empty
      expect(backend.pending.size).to eq(1)
      backend.commit
      expect(persisted('ready').map { |j| j['jid'] }).to eq([jid])
    end
  end
end
