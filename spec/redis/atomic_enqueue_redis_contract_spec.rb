require 'spec_helper'

RSpec.describe SidekiqUniqueness::AtomicEnqueue, 'Redis contract' do # rubocop:disable RSpec/SpecFilePathFormat
  let(:queue) { "sneakers_spec_atomic_#{SecureRandom.hex(6)}" }
  let(:ready_key) { SidekiqRuntime::QueueKeys.source_queue(queue) }
  let(:config) do
    Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
  end
  let(:writer) { described_class.new(config.redis_pool, clock: -> { 1_789_000_000.25 }) }
  let(:payload) do
    {
      'class' => 'SpecAtomicRedisJob',
      'args' => [123],
      'queue' => queue,
      'jid' => 'atomic-redis-jid',
      'retry' => 1,
      'dead' => true,
      'created_at' => 1_788_000_000.5
    }
  end
  let(:lock) { build_lock([123]) }

  before { cleanup }
  after { cleanup }

  it 'atomically persists Ready, both directories, and the enqueue lock without requiring a dead option' do
    candidate = payload.except('dead')
    expect(writer.write(candidate, lock: lock)).to eq(:accepted)

    persisted = config.redis { |redis| Sidekiq.load_json(redis.lindex(ready_key, 0)) }
    expect(persisted).to eq(candidate.merge('enqueued_at' => 1_789_000_000.25))
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq(payload['jid'])
    expect(config.redis { |redis| redis.sismember('queues', queue) }).to eq(1)
    expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(1)
  end

  it 'persists Scheduled without at/enqueued_at and leaves Ready empty' do
    scheduled = payload.merge('jid' => 'scheduled-jid', 'at' => 1_789_000_100.5, 'enqueued_at' => 123)
    expect(writer.write(scheduled, lock: lock)).to eq(:accepted)

    expect(scheduled_payloads.size).to eq(1)
    raw, score = scheduled_payloads.fetch(0)
    expect(score).to eq(1_789_000_100.5)
    expect(Sidekiq.load_json(raw)).to eq(scheduled.except('at', 'enqueued_at'))
    expect(config.redis { |redis| redis.exists(ready_key) }).to eq(0)
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq('scheduled-jid')
    expect(config.redis { |redis| redis.ttl(lock.enqueue_redis_key) }).to be_between(700, 701)
  end

  it 'keeps the enqueue lock through a delay longer than its configured TTL' do
    short_lock = build_lock([123], enqueue_ttl: 5)
    scheduled = payload.merge('at' => 1_789_000_100.5)

    expect(writer.write(scheduled, lock: short_lock)).to eq(:accepted)
    expect(config.redis { |redis| redis.ttl(short_lock.enqueue_redis_key) }).to be_between(104, 106)
    expect(writer.write(scheduled.merge('jid' => 'duplicate-before-due'), lock: short_lock)).to eq(:duplicate)
    expect(scheduled_payloads.size).to eq(1)
  end

  it 'uses only the configured TTL for immediate and overdue scheduled jobs' do
    [payload, payload.merge('at' => 0), payload.merge('at' => 1_789_000_000.0)].each_with_index do |candidate, index|
      candidate_lock = build_lock([index], enqueue_ttl: 5)
      expect(writer.write(candidate.merge('jid' => "zero-delay-#{index}"), lock: candidate_lock)).to eq(:accepted)
      expect(config.redis { |redis| redis.ttl(candidate_lock.enqueue_redis_key) }).to be_between(4, 5)
    end
  end

  it 'coalesces concurrent duplicates while admitting distinct identities' do
    results = []
    mutex = Mutex.new
    threads = 8.times.map do |index|
      Thread.new do
        candidate = payload.merge('jid' => "duplicate-jid-#{index}")
        result = described_class.new(config.redis_pool).write(candidate, lock: lock)
        mutex.synchronize { results << result }
      end
    end
    threads.each(&:join)

    expect(results.tally).to eq(accepted: 1, duplicate: 7)
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(1)

    distinct_lock = build_lock([456])
    expect(writer.write(payload.merge('jid' => 'distinct-jid', 'args' => [456]), lock: distinct_lock))
      .to eq(:accepted)
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(2)
  end

  it 'persists an accepted continuation without overwriting another enqueue owner' do
    config.redis { |redis| redis.set(lock.enqueue_redis_key, 'another-owner', ex: 600) }
    continuation = SidekiqUniqueness.mark_runtime_conflict_continuation(
      payload.merge('at' => 1_789_000_100.5)
    )

    expect(writer.write(continuation, lock: lock, mode: :continuation)).to eq(:accepted)
    expect(scheduled_payloads.size).to eq(1)
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq('another-owner')
  end

  it 'rejects a wrong target or lock type without writing a task or directory entry' do
    [ready_key, lock.enqueue_redis_key].each do |key|
      config.redis do |redis|
        key == ready_key ? redis.set(key, 'wrong-type') : redis.lpush(key, 'wrong-type')
      end
      expect { writer.write(payload, lock: lock) }
        .to raise_error(described_class::Rejected) do |error|
          expect(error.code).to eq(key == ready_key ? :ready_wrong_type : :lock_wrong_type)
        end
      config.redis { |redis| redis.del(key) }
      expect(config.redis { |redis| redis.llen(ready_key) }).to eq(0)
      expect(config.redis { |redis| redis.sismember('queues', queue) }).to eq(0)
      expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(0)
    end
  end

  it 'reports a lost accepted reply as unknown without replaying the write or deleting the lock' do
    calls = []
    uncertain_pool = Object.new
    base_pool = config.redis_pool
    uncertain_pool.define_singleton_method(:with) do |&block|
      base_pool.with do |redis|
        connection = Object.new
        connection.define_singleton_method(:call_once) do |*arguments|
          calls << arguments.first
          redis.call_once(*arguments)
          raise RedisClient::ConnectionError, 'injected accepted reply loss'
        end
        block.call(connection)
      end
    end

    expect { described_class.new(uncertain_pool).write(payload, lock: lock) }
      .to raise_error(described_class::Unknown)
    expect(calls).to satisfy { |commands| commands == ['EVALSHA'] || commands == %w[EVALSHA EVAL] }
    expect(config.redis { |redis| redis.llen(ready_key) }).to eq(1)
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq(payload['jid'])
  end

  def build_lock(identity, enqueue_ttl: 600)
    SidekiqUniqueness::UntilAndWhileExecuting.new(
      key_prefix: "sneakers:sidekiq:unique:SpecAtomicRedisJob:#{queue}",
      identity: identity,
      enqueue_ttl: enqueue_ttl,
      runtime_ttl: 45,
    )
  end

  def scheduled_payloads
    config.redis do |redis|
      redis.zscan('schedule').filter_map do |raw, score|
        [raw, score.to_f] if Sidekiq.load_json(raw)['queue'] == queue
      end
    end
  end

  def cleanup
    config.redis do |redis|
      redis.del(ready_key)
      redis.zscan('schedule').each do |raw, _score|
        redis.zrem('schedule', raw) if Sidekiq.load_json(raw)['queue'] == queue
      end
      redis.srem('queues', queue) if redis.call('TYPE', 'queues') == 'set'
      directory = SidekiqRuntime::QueueKeys.directory_key
      redis.srem(directory, queue) if redis.call('TYPE', directory) == 'set'
      lock_keys = redis.call('KEYS', "sneakers:sidekiq:unique:SpecAtomicRedisJob:#{queue}:*")
      redis.del(*lock_keys) if lock_keys.any?
    end
  end
end
