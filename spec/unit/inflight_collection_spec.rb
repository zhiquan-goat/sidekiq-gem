require 'spec_helper'
require 'sidekiq/api'

RSpec.describe SidekiqRuntime::InflightCollection do
  let(:config) { double('Sidekiq config') }
  let(:queue_a) { 'sneakers_a' }
  let(:queue_b) { 'sneakers_b' }
  let(:store_a) { instance_double(SidekiqRuntime::InflightStore, source_queue: "queue:#{queue_a}") }
  let(:store_b) { instance_double(SidekiqRuntime::InflightStore, source_queue: "queue:#{queue_b}") }

  subject(:collection) { described_class.new(config: config, queues: [queue_a, queue_b]) }

  before do
    allow(SidekiqRuntime::InflightStore).to receive(:new).and_call_original
    allow(SidekiqRuntime::InflightStore).to receive(:new)
      .with(
        config,
        queue: queue_a
      )
      .and_return(store_a)
    allow(SidekiqRuntime::InflightStore).to receive(:new)
      .with(
        config,
        queue: queue_b
      )
      .and_return(store_b)
  end

  it 'globally orders and paginates entries from every queue' do
    oldest = claim(source_queue: "queue:#{queue_b}", member: 'oldest', claimed_at: 10)
    middle = claim(source_queue: "queue:#{queue_a}", member: 'middle', claimed_at: 20)
    newest = claim(source_queue: "queue:#{queue_b}", member: 'newest', claimed_at: 30)
    allow(store_a).to receive(:entries).with(offset: 0, limit: 3).and_return([middle])
    allow(store_b).to receive(:entries).with(offset: 0, limit: 3).and_return([newest, oldest])

    expect(collection.entries(offset: 1, limit: 2)).to eq([middle, newest])
  end

  it 'rejects a deep page before reading any Redis queue' do
    expect(store_a).not_to receive(:entries)
    expect(store_b).not_to receive(:entries)

    expect do
      collection.entries(offset: 1_000, limit: 1)
    end.to raise_error(ArgumentError, /limited to the first 1000 entries/)
  end

  it 'does not search a queue absent from the collection' do
    expect(store_a).not_to receive(:find_entry)
    expect(store_b).not_to receive(:find_entry)

    expect(collection.find_entry('entry-id', physical_queue: 'unknown')).to be_nil
  end

  it 'does not fall back to other queues when the requested queue does not contain the entry' do
    entry = claim(source_queue: "queue:#{queue_b}", member: 'target', claimed_at: 10)
    expect(store_a).to receive(:find_entry).with(entry.entry_id).and_return(nil)
    expect(store_b).not_to receive(:find_entry)

    expect(collection.find_entry(entry.entry_id, physical_queue: queue_a)).to be_nil
  end

  context 'with persistent queue discovery' do
    let(:redis_url) { ENV.fetch('SIDEKIQ_REDIS_URL') }
    let(:queue) { "plain_directory_#{SecureRandom.hex(6)}" }
    let(:runtime_config) do
      Sidekiq::Config.new.tap do |value|
        value.queues = [queue]
        value.redis = {url: redis_url}
      end
    end
    let(:observer_config) do
      Sidekiq::Config.new.tap do |value|
        value.queues = []
        value.redis = {url: redis_url}
      end
    end
    before { register_runtime_owner(runtime_config, 'collection-owner') }

    let(:real_store) do
      SidekiqRuntime::InflightStore.new(
        runtime_config,
        owner_identity: 'collection-owner',
        queue: queue
      )
    end
    let(:raw_job) do
      Sidekiq.dump_json('class' => 'CollectionDirectoryJob', 'args' => [],
        'jid' => SecureRandom.hex(12), 'queue' => queue)
    end

    after do
      runtime_config.redis do |redis|
        redis.del(real_store.source_queue, real_store.inflight_key, real_store.owners_key)
        redis.srem('queues', queue)
        redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue)
      end
    end

    it 'finds and requeues an unconfigured claim after native Queue#clear' do
      runtime_config.redis do |redis|
        redis.lpush(real_store.source_queue, raw_job)
        redis.sadd('queues', queue)
      end
      claim = real_store.claim

      expect(runtime_config.redis { |redis| redis.sismember('queues', queue) }).to eq(1)
      SidekiqRuntime::RedisScope.with(runtime_config) { Sidekiq::Queue.new(queue).clear }

      expect(runtime_config.redis { |redis| redis.sismember('queues', queue) }).to eq(0)
      observed = described_class.new(config: observer_config)
      expect(observed.size).to eq(1)
      expect(observed.state(claim)).to include(status: 'owner_alive', recoverable: false)
      expect(observed.find_entry(claim.entry_id, physical_queue: queue)&.member).to eq(claim.member)
      runtime_config.redis { |redis| redis.del(claim.owner_identity) }
      expect(observed.recover_orphan(claim)).to include(outcome: 'requeued')
      requeued = runtime_config.redis { |redis| redis.lrange(real_store.source_queue, 0, -1) }
      expect(Sidekiq.load_json(requeued.first)).to include(Sidekiq.load_json(raw_job))
    end

    it 'raises instead of reporting an empty collection for a broken directory' do
      with_wrong_type_sidekiq_queue_directory(runtime_config) do
        expect { described_class.new(config: observer_config) }
          .to raise_error(RedisClient::CommandError)
      end
    end
  end

  def claim(source_queue:, member:, claimed_at:)
    SidekiqRuntime::InflightStore::Claim.new(
      source_queue: source_queue,
      job: '{}',
      member: "claim-id\n#{member}",
      claimed_at: claimed_at
    )
  end
end
