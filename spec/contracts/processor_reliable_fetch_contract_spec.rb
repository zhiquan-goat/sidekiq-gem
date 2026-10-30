require 'spec_helper'
require 'sidekiq/processor'

RSpec.describe Sidekiq::Processor do
  before { register_runtime_owner(config, 'processor-owner') }

  let(:config) do
    Sidekiq::Config.new(error_handlers: [->(*) {}], death_handlers: []).tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
      value.default_capsule.queues = [queue]
    end
  end
  let(:queue) { "sneakers_spec_processor_#{SecureRandom.hex(6)}" }
  let(:payload) do
    {'class' => 'SpecProcessorJob', 'queue' => queue, 'jid' => 'processor-jid',
     'retry' => 1, 'dead' => true, 'args' => [123]}
  end
  let(:store) do
    SidekiqRuntime::InflightStore.new(config,
      queue: queue, owner_identity: 'processor-owner')
  end
  let(:processor) { Sidekiq::Processor.new(config.default_capsule) {} }
  before do
    stub_const('SpecProcessorJob', Class.new do
      include Sidekiq::Job

      def perform(_id)
      end
    end)
  end

  after do
    config.redis do |redis|
      redis.del("queue:#{queue}", store.inflight_key, store.owners_key)
      %w[retry schedule dead].each do |key|
        redis.zscan(key).each do |member, _score|
          redis.zrem(key, member) if Sidekiq.load_json(member)['queue'] == queue
        end
      end
    end
  end

  it 'preserves the exact durable claim when the job is interrupted' do
    allow_any_instance_of(SpecProcessorJob).to receive(:perform).and_raise(Sidekiq::Shutdown)
    work = claim_work
    processor.send(:process, work)
    expect(store.entries.map(&:member)).to eq([work.claim.member])
  end

  it 'retains Inflight when both local and global retry persistence fail' do
    allow_any_instance_of(SpecProcessorJob).to receive(:perform).and_raise('job failed')
    config.default_capsule.redis do |redis|
      allow(redis).to receive(:zadd).with('retry', anything, anything).and_raise(RedisClient::ConnectionError, 'retry unavailable')
    end
    work = claim_work
    expect { processor.send(:process, work) }.to raise_error(RedisClient::ConnectionError)
    expect(store.entries.map(&:member)).to eq([work.claim.member])
  end

  it 'allows global retry to persist after the first local write fails' do
    allow_any_instance_of(SpecProcessorJob).to receive(:perform).and_raise('job failed')
    config.default_capsule.redis do |redis|
      writes = 0
      allow(redis).to receive(:zadd).and_wrap_original do |method, *arguments|
        writes += 1
        raise RedisClient::ConnectionError, 'first retry write failed' if writes == 1

        method.call(*arguments)
      end
    end
    expect { processor.send(:process, claim_work) }.to raise_error(RedisClient::ConnectionError)
    expect(store.size).to eq(0)
    expect(persisted('retry').size).to eq(1)
  end

  def claim_work
    raw = Sidekiq.dump_json(payload)
    config.redis { |redis| redis.lpush("queue:#{queue}", raw) }
    claim = store.claim
    SidekiqRuntime::ReliableFetch::UnitOfWork.new(queue: "queue:#{queue}", job: raw,
      config: config, store: store, claim: claim)
  end

  def persisted(key)
    config.redis do |redis|
      redis.zrange(key, 0, -1).map { |raw| Sidekiq.load_json(raw) }.select { |job| job['queue'] == queue }
    end
  end
end
