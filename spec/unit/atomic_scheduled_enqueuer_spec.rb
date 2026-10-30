require 'spec_helper'

RSpec.describe SidekiqRuntime::AtomicScheduledEnqueuer do
  let(:config) do
    Sidekiq::Config.new.tap { |value| value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')} }
  end
  let(:now) { 1_800_000_000.0 }
  let(:queue) { "orders_promotion_#{SecureRandom.hex(6)}" }
  let(:ready_key) { "queue:#{queue}" }
  let(:source) { "spec:promotion:#{SecureRandom.hex(6)}" }
  let(:payload) do
    {'class' => 'UnloadedPromotionJob', 'args' => [123], 'jid' => 'promotion-jid', 'queue' => queue,
     'retry' => 1, 'dead' => true, 'sneakers_chain_id' => 'chain'}
  end
  let(:member) { Sidekiq.dump_json(payload) }
  let(:enqueuer) { described_class.new(config, clock: -> { now }, budget: 2) }

  after do
    config.redis do |redis|
      redis.del(source, ready_key)
      redis.srem('queues', queue)
      redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue) if
        redis.call('TYPE', SidekiqRuntime::QueueKeys.directory_key) == 'set'
    end
  end

  it 'atomically promotes accepted work without loading its class or invoking producer middleware' do
    config.redis { |redis| redis.zadd(source, now - 1, member) }
    enqueuer.enqueue_jobs([source])
    ready = config.redis { |redis| redis.lrange(ready_key, 0, -1) }
    expect(ready.size).to eq(1)
    expect(Sidekiq.load_json(ready.first)).to eq(payload.merge('enqueued_at' => now))
    expect(config.redis { |redis| redis.zcard(source) }).to eq(0)
    expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(1)
  end

  it 'makes progress beyond more bad records than one tick can inspect' do
    config.redis do |redis|
      5.times { |index| redis.zadd(source, now - 20 + index, "bad-json-#{index}") }
      redis.zadd(source, now - 1, member)
    end
    3.times { enqueuer.enqueue_jobs([source]) }
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(1)
    expect(config.redis { |redis| redis.zcard(source) }).to eq(5)
  end

  it 'keeps future and malformed entries, and continues past a poison destination' do
    other = "queue:#{queue}_other"
    config.redis do |redis|
      redis.set(ready_key, 'wrong-type')
      redis.zadd(source, now - 2, member)
      redis.zadd(source, now - 1, Sidekiq.dump_json(payload.merge('queue' => "#{queue}_other")))
      redis.zadd(source, now + 10, Sidekiq.dump_json(payload.merge('jid' => 'future')))
    end
    enqueuer.enqueue_jobs([source])
    expect(config.redis { |redis| redis.zscore(source, member) }).not_to be_nil
    expect(config.redis { |redis| redis.llen(other) }).to eq(1)
    expect(config.redis { |redis| redis.zcard(source) }).to eq(2)
  ensure
    if other
      config.redis do |redis|
        redis.del(other)
        redis.srem('queues', "#{queue}_other")
      end
    end
  end

  it 'retains the source if a Lua command fails after writing queue metadata' do
    broken = described_class.const_get(:ATOMIC_PROMOTE).sub("redis.call('LPUSH', KEYS[3], ARGV[4])", "redis.call('NOT_A_REDIS_COMMAND')")
    config.redis { |redis| redis.zadd(source, now - 1, member) }
    expect do
      config.redis do |redis|
        redis.call_once('EVAL', broken, 4, source, 'queues', ready_key,
          SidekiqRuntime::QueueKeys.directory_key, member, now, queue, member)
      end
    end.to raise_error(RedisClient::CommandError)
    expect(config.redis { |redis| redis.zscore(source, member) }).not_to be_nil
  end

  it 'retains the source when the directory has the wrong type' do
    config.redis { |redis| redis.zadd(source, now - 1, member) }

    with_wrong_type_sidekiq_queue_directory(config) do
      enqueuer.enqueue_jobs([source])

      expect(config.redis { |redis| redis.zscore(source, member) }).not_to be_nil
      expect(config.redis { |redis| redis.exists(ready_key) }).to eq(0)
      expect(config.redis { |redis| redis.sismember('queues', queue) }).to eq(0)
    end
  end

  it 'allows only one concurrent exact promotion to remove the source' do
    config.redis { |redis| redis.zadd(source, now - 1, member) }
    threads = 2.times.map { Thread.new { described_class.new(config, clock: -> { now }).enqueue_jobs([source]) } }
    threads.each(&:value)
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(1)
  end

  it 'does not duplicate ready work when a successful script reply is lost' do
    config.redis { |redis| redis.zadd(source, now - 1, member) }
    lost = true
    allow(enqueuer).to receive(:evaluate_once).and_wrap_original do |method, *args, **options|
      result = method.call(*args, **options)
      if lost
        lost = false
        raise RedisClient::ConnectionError, 'lost reply'
      end
      result
    end
    expect { enqueuer.enqueue_jobs([source]) }.to raise_error(RedisClient::ConnectionError)
    enqueuer.enqueue_jobs([source])
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(1)
    expect(config.redis { |redis| redis.zcard(source) }).to eq(0)
  end

  it 'retains invalid retry_queue members and continues to a valid member' do
    invalid = payload.merge('retry_queue' => 'somewhere_else', 'jid' => 'invalid-jid')
    config.redis do |redis|
      redis.zadd(source, now - 2, Sidekiq.dump_json(invalid))
      redis.zadd(source, now - 1, member)
    end
    enqueuer.enqueue_jobs([source])

    expect(config.redis { |redis| redis.zscore(source, Sidekiq.dump_json(invalid)) }).not_to be_nil
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(1)
  end
end
