require 'spec_helper'

RSpec.describe SidekiqRuntime::QueueRateLimit, 'Redis contract' do # rubocop:disable RSpec/SpecFilePathFormat
  before { register_runtime_owner(config) }

  let(:queue) { "sneakers_spec_rate_limit_#{SecureRandom.hex(6)}" }
  let(:key) { SidekiqRuntime::QueueKeys.rate_limit_key(queue) }
  let(:ready_key) { SidekiqRuntime::QueueKeys.source_queue(queue) }
  let(:inflight_key) { SidekiqRuntime::QueueKeys.inflight_key(queue) }
  let(:config) do
    Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
  end
  let(:limiter) { described_class.new(config) }

  after do
    config.redis do |redis|
      redis.del(key, ready_key, inflight_key, "#{inflight_key}:owners", "#{inflight_key}:leases")
      directory = SidekiqRuntime::QueueKeys.directory_key
      redis.srem(directory, queue) if redis.call('TYPE', directory) == 'set'
    end
  end

  it 'is unlimited by default and persists an enabled full bucket without a TTL' do
    expect(limiter.get(queue)).to be_nil

    update = limiter.enable(queue, rate: '2.5', burst: '3')

    expect(update.before).to be_nil
    expect(update.after.to_h).to include(rate: 2.5, burst: 3, tokens: 3.0)
    expect(limiter.get(queue).to_h).to eq(update.after.to_h)
    expect(config.redis { |redis| redis.ttl(key) }).to eq(-1)
    expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(1)
  end

  it 'settles the old bucket, clamps the new burst, and does not gift tokens on repeated saves' do
    now = Time.now.to_f
    config.redis do |redis|
      redis.hset(key,
        'rate', '0.01', 'burst', '10', 'tokens', '4', 'last_refill', now + 60)
    end

    first = limiter.enable(queue, rate: 5, burst: 2)
    second = limiter.enable(queue, rate: 5, burst: 2)

    expect(first.before.to_h).to include(rate: 0.01, burst: 10, tokens: 4.0)
    expect(first.before.last_refill).to be_within(0.001).of(now + 60)
    expect(first.after.to_h).to include(rate: 5.0, burst: 2, tokens: 2.0)
    expect(first.after.last_refill).to be_within(0.001).of(now + 60)
    expect(second.before.tokens).to eq(2.0)
    expect(second.after.tokens).to eq(2.0)
  end

  it 'refills an update with the old rate before applying a faster new rate' do
    config.redis do |redis|
      redis.hset(key,
        'rate', '0.5', 'burst', '10', 'tokens', '0', 'last_refill', Time.now.to_f - 1)
    end

    update = limiter.enable(queue, rate: 100, burst: 10)

    expect(update.after.rate).to eq(100)
    expect(update.after.tokens).to be_between(0.4, 1.0)
  end

  it 'deletes all bucket state on disable and re-enables with a full bucket' do
    limiter.enable(queue, rate: 1, burst: 2)
    config.redis { |redis| redis.hset(key, 'tokens', '0') }

    disabled = limiter.disable(queue)
    expect(limiter.disable(queue)).to eq(described_class::Update.new(before: nil, after: nil))
    enabled = limiter.enable(queue, rate: 3, burst: 4)

    expect(disabled.before.tokens).to eq(0)
    expect(disabled.after).to be_nil
    expect(enabled.before).to be_nil
    expect(enabled.after.tokens).to eq(4)
  end

  it 'rejects invalid Ruby inputs before Redis and leaves the previous policy unchanged' do
    limiter.enable(queue, rate: 1, burst: 2)
    previous = limiter.get(queue)

    [0, -1, 1e-320, Float::INFINITY, Float::NAN, 'invalid'].each do |rate|
      expect { limiter.enable(queue, rate: rate, burst: 1) }.to raise_error(ArgumentError)
    end
    [0, -1, 1.5, '1.5', described_class::MAX_BURST + 1, 'invalid'].each do |burst|
      expect { limiter.enable(queue, rate: 1, burst: burst) }.to raise_error(ArgumentError)
    end
    expect(limiter.get(queue)).to eq(previous)
  end

  it 'stores a large safe burst as an exact decimal integer' do
    burst = 100_000_000_000_000

    update = limiter.enable(queue, rate: 1, burst: burst)

    expect(update.after.burst).to eq(burst)
    expect(config.redis { |redis| redis.hget(key, 'burst') }).to eq(burst.to_s)
  end

  it 'shares one fixed burst across independent configs, stores, owners, and clients' do
    second_config = Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
    stores = [config, second_config].each_with_index.map do |store_config, index|
      SidekiqRuntime::InflightStore.new(
        store_config,
        queue: queue,
        owner_identity: register_runtime_owner(config, "independent-owner-#{index}")
      )
    end
    jobs = 6.times.map do |index|
      Sidekiq.dump_json(build_sidekiq_runtime_job_payload(
        jid: "shared-rate-jid-#{index}", physical_queue: queue
      ))
    end
    config.redis { |redis| jobs.each { |job| redis.lpush(ready_key, job) } }
    limiter.enable(queue, rate: described_class::MIN_RATE, burst: 3)
    results = Queue.new

    threads = 6.times.map do |index|
      Thread.new { results << stores.fetch(index % stores.size).claim }
    end
    threads.each(&:join)
    values = 6.times.map { results.pop }

    expect(values.count { |value| value.is_a?(SidekiqRuntime::InflightStore::Claim) }).to eq(3)
    expect(values.count { |value| value.is_a?(SidekiqRuntime::InflightStore::RateLimited) }).to eq(3)
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(3)
    expect(config.redis { |redis| redis.zcard(inflight_key) }).to eq(3)
    expect(config.redis { |redis| redis.hlen("#{inflight_key}:owners") }).to eq(3)
  end

  it 'does not delete or replace malformed policy data' do
    config.redis { |redis| redis.hset(key, 'rate', 'broken') }

    expect { limiter.get(queue) }.to raise_error(described_class::InvalidPolicy)
    expect { limiter.enable(queue, rate: 1, burst: 1) }.to raise_error(RedisClient::CommandError)
    expect { limiter.disable(queue) }.to raise_error(RedisClient::CommandError)
    expect(config.redis { |redis| redis.hgetall(key) }).to eq('rate' => 'broken')
  end

  it 'rejects a wrong Redis key type without changing the directory' do
    config.redis { |redis| redis.set(key, 'wrong-type') }

    expect { limiter.enable(queue, rate: 1, burst: 1) }.to raise_error(RedisClient::CommandError)
    expect(config.redis { |redis| redis.get(key) }).to eq('wrong-type')
    expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(0)
  end
end
