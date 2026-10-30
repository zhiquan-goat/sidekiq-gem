require 'spec_helper'
require 'sidekiq/processor'

RSpec.describe 'Weighted native job lifecycle through the Sneakers runtime' do # rubocop:disable RSpec/DescribeClass
  before { register_runtime_owner(config) }

  let(:queues) do
    %w[critical common bulk].map { |suffix| "sneakers_spec_weighted_#{suffix}_#{SecureRandom.hex(6)}" }
  end
  let(:raw_queues) { [[queues[0], 5], [queues[1], 3], [queues[2], 1]] }
  let(:config) do
    Sidekiq::Config.new(error_handlers: [->(*) {}]).tap do |value|
      SidekiqRuntime.install_server!(value, redis_config: {url: ENV.fetch('SIDEKIQ_REDIS_URL')})
      value[:queues] = raw_queues
      value.default_capsule.queues = raw_queues
    end
  end
  let(:fetcher) { SidekiqRuntime::ReliableFetch.new(config.default_capsule, random: Random.new(12_345)) }

  before do
    queue = queues.last
    stub_const('SpecWeightedNativeJob', Class.new do
      include Sidekiq::Job

      sidekiq_options queue: queue

      class << self
        attr_accessor :executions
      end

      def perform(value)
        self.class.executions ||= []
        self.class.executions << value
      end
    end)
  end

  after do
    config.redis do |redis|
      queues.each do |queue|
        store = fetcher.stores_by_queue.fetch(queue)
        redis.del("queue:#{queue}", store.inflight_key, store.owners_key)
        redis.srem('queues', queue)
      end
      if redis.call('TYPE', SidekiqRuntime::QueueKeys.directory_key) == 'set'
        redis.srem(SidekiqRuntime::QueueKeys.directory_key, queues)
      end
    end
  end

  it 'claims, ACKs and shutdown-requeues through real Redis with weighted configuration' do
    expect(SidekiqRuntime::ServerConfigurator.validate!(config)).to be true
    expect(config.default_capsule.mode).to eq(:weighted)
    expect(config.queues.tally).to eq(queues[0] => 5, queues[1] => 3, queues[2] => 1)
    expect(fetcher.stores_by_queue.keys).to contain_exactly(*queues)

    Sidekiq::Client.via(config.redis_pool) { SpecWeightedNativeJob.perform_async('ack') }
    acknowledged_work = fetcher.retrieve_work
    expect(acknowledged_work.queue_name).to eq(queues.last)
    Sidekiq::Processor.new(config.default_capsule) {}.send(:process, acknowledged_work)

    store = fetcher.stores_by_queue.fetch(queues.last)
    expect(SpecWeightedNativeJob.executions).to eq(['ack'])
    expect(store.size).to eq(0)
    expect(config.redis { |redis| redis.hlen(store.owners_key) }).to eq(0)

    Sidekiq::Client.via(config.redis_pool) { SpecWeightedNativeJob.perform_async('shutdown') }
    shutdown_work = fetcher.retrieve_work
    original_job = shutdown_work.job
    expect(shutdown_work.requeue).to eq(1)

    requeued = config.redis { |redis| redis.lrange("queue:#{queues.last}", 0, -1) }
    expect(requeued.map { |raw| Sidekiq.load_json(raw) }).to eq([
      SidekiqRuntime::JobMetadata.with_redelivery(Sidekiq.load_json(original_job))
    ])
    expect(store.size).to eq(0)
    expect(config.redis { |redis| redis.hlen(store.owners_key) }).to eq(0)
  end
end
