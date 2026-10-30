# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/processor'

RSpec.describe 'Actual Processor abandonment diagnosis' do
  let(:events) { [] }
  around do |example|
    previous = SidekiqRuntime.event_handler
    SidekiqRuntime.event_handler = ->(event, fields) { events << [event, fields] }
    example.run
  ensure
    SidekiqRuntime.event_handler = previous
  end

  let(:queue) { "processor_heartbeat_#{SecureRandom.hex(6)}" }
  let(:config) do
    Sidekiq::Config.new(error_handlers: [->(*) {}], death_handlers: [],
      lifecycle_events: Sidekiq::Config::DEFAULTS[:lifecycle_events].transform_values { [] }).tap do |c|
      c.logger = Logger.new(File::NULL)
      c[:queues] = [queue]
      c.queues = [queue]
      SidekiqRuntime.install_server!(c, redis_config: {url: ENV.fetch('SIDEKIQ_REDIS_URL')})
    end
  end
  let(:fetcher) { config.default_capsule.fetcher }
  let(:store) { fetcher.stores_by_queue.fetch(queue) }
  let(:processor) { Sidekiq::Processor.new(config.default_capsule) {} }
  let(:raw) { Sidekiq.dump_json('class' => 'HeartbeatProcessorJob', 'queue' => queue, 'args' => [], 'jid' => 'heartbeat-jid', 'retry' => 0) }

  before do
    register_runtime_owner(config)
    stub_const('HeartbeatProcessorJob', Class.new do
      include Sidekiq::Job
      def perform; end
    end)
    config.redis { |redis| redis.lpush("queue:#{queue}", raw) }
  end

  after do
    config.redis do |redis|
      redis.del(store.source_queue, store.inflight_key, store.owners_key)
      redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue)
    end
  end

  it 'reports the exact abandoned claim when ACK fails outside middleware' do
    allow(store).to receive(:acknowledge).and_raise(RedisClient::ConnectionError, 'ACK unavailable')
    expect { processor.send(:process_one) }.to raise_error(RedisClient::ConnectionError)
    claim = store.entries.first
    expect(events).to include([SidekiqRuntime::ReliableFetch::EVENT_NAME,
      include(operation: 'unconfirmed_release', outcome: 'manual_review', entry_id: claim.entry_id, owner_identity: claim.owner_identity)])
    expect(SidekiqRuntime::OrphanReclaimer.new(config).recover(claim)).to include(outcome: 'owner_alive')
    config.redis { |r| r.del(claim.owner_identity) }
    expect(SidekiqRuntime::OrphanReclaimer.new(config).recover(claim)).to include(outcome: 'requeued')
  end

  it 'reports a claim when get_one swallows a fetch-return failure before binding the job' do
    allow(fetcher).to receive(:retrieve_work).and_wrap_original do |method|
      method.call
      raise 'fetch return failed'
    end
    allow(processor).to receive(:sleep)
    processor.send(:process_one)
    expect(processor.job).to be_nil
    expect(store.size).to eq(1)
    expect(events.map(&:last)).to include(include(operation: 'unconfirmed_release'))
  end

  it 'does not report abandonment after confirmed ACK or quiet drain' do
    allow_any_instance_of(HeartbeatProcessorJob).to receive(:perform) do
      config[:lifecycle_events][:quiet].each(&:call)
      expect(store.owner_state(store.entries.first)).to include(owner_status: 'owner_alive')
    end
    processor.send(:process_one)
    expect(store.size).to eq(0)
    expect(events.map(&:last)).not_to include(include(operation: 'unconfirmed_release'))
  end

  it 'uses the AJ business class in claimed and ACK telemetry without changing wire class' do
    payload = {'class' => 'SidekiqRuntime::ActiveJob::JobWrapper', 'wrapped' => 'BusinessAJ',
      'args' => [{'job_id' => 'logical-id'}], 'queue' => queue, 'jid' => 'wire-jid'}
    claim = SidekiqRuntime::InflightStore::Claim.new(source_queue: "queue:#{queue}", job: Sidekiq.dump_json(payload), member: "test-claim\n#{Sidekiq.dump_json(payload)}")
    expect(claim.job_class).to eq('BusinessAJ')
    SidekiqRuntime::ReliableFetch.publish(operation: 'claimed', outcome: 'success', claim: claim)
    expect(events.last.last).to include(job_class: 'BusinessAJ', job_id: 'logical-id', jid: 'wire-jid')
    expect(claim.payload['class']).to eq('SidekiqRuntime::ActiveJob::JobWrapper')
  end
end
