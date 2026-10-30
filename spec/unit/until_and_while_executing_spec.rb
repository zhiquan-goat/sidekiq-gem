require 'spec_helper'

RSpec.describe SidekiqUniqueness::UntilAndWhileExecuting do
  let(:config) do
    Sidekiq::Config.new(death_handlers: [], error_handlers: []).tap do |sidekiq_config|
      sidekiq_config.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
  end
  let(:key_prefix) { "spec:sidekiq_uniqueness:#{SecureRandom.hex(8)}" }
  let(:lock) do
    described_class.new(
      key_prefix: key_prefix,
      identity: ['ordinary', 123, 10.0],
      enqueue_ttl: 15.minutes,
      runtime_ttl: 1.minute,
      metadata: {'class' => 'SpecJob', 'jid' => 'jid-1', 'queue' => 'ordinary'}
    )
  end

  after do
    config.redis { |redis| redis.del(lock.enqueue_redis_key, lock.runtime_redis_key) }
  end



  it 'reserves once, refreshes the same JID, and rejects a different owner' do
    expect(config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'jid-1') }).to be true
    expect(config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'jid-1') }).to be true
    expect(config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'jid-2') }).to be false

    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq('jid-1')
    expect(config.redis { |redis| redis.ttl(lock.enqueue_redis_key) })
      .to be_within(1).of(lock.enqueue_ttl)
  end

  it 'atomically acquires runtime before removing the current enqueue owner' do
    config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'jid-1') }

    execution_owner = config.redis { |redis| lock.begin_execution(redis: redis, jid: 'jid-1') }

    expect(execution_owner).to start_with('sidekiq:jid-1:')
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to be_nil
    expect(config.redis { |redis| redis.get(lock.runtime_redis_key) }).to eq(execution_owner)
    expect(config.redis { |redis| redis.ttl(lock.runtime_redis_key) })
      .to be_within(1).of(lock.runtime_ttl)
  ensure
    config.redis { |redis| lock.finish_execution(redis: redis, owner: execution_owner) } if execution_owner
  end

  it 'keeps enqueue ownership when runtime is occupied' do
    config.redis do |redis|
      redis.set(lock.runtime_redis_key, 'existing-runtime-owner', ex: lock.runtime_ttl)
      lock.reserve_enqueue(redis: redis, owner: 'jid-1')
    end

    expect(config.redis { |redis| lock.begin_execution(redis: redis, jid: 'jid-1') }).to be_nil
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq('jid-1')
    expect(config.redis { |redis| redis.get(lock.runtime_redis_key) }).to eq('existing-runtime-owner')
  end


  it 'does not let an expired old owner release a replacement reservation' do
    config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'old-owner') }
    config.redis { |redis| redis.call('PEXPIRE', lock.enqueue_redis_key, 1) }
    sleep 0.01
    expect(config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'new-owner') }).to be true

    expect(config.redis { |redis| lock.release_enqueue(redis: redis, owner: 'old-owner') }).to eq(0)
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq('new-owner')
  end

  it 'deletes locks only for the current owner' do
    config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'jid-1') }

    expect(config.redis { |redis| lock.release_enqueue(redis: redis, owner: 'jid-2') }).to eq(0)
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq('jid-1')

    execution_owner = config.redis { |redis| lock.begin_execution(redis: redis, jid: 'jid-1') }
    config.redis { |redis| redis.set(lock.runtime_redis_key, 'new-owner', ex: lock.runtime_ttl) }

    expect(config.redis { |redis| lock.finish_execution(redis: redis, owner: execution_owner) }).to eq(0)
    expect(config.redis { |redis| redis.get(lock.runtime_redis_key) }).to eq('new-owner')
  end

  it 'emits bounded generic lock metadata' do
    events = []
    subscriber = ActiveSupport::Notifications.subscribe('enqueue_lock.sidekiq_uniqueness') do |*args|
      events << ActiveSupport::Notifications::Event.new(*args)
    end

    config.redis { |redis| lock.reserve_enqueue(redis: redis, owner: 'jid-1') }

    expect(events.first.payload).to include(
      job_class: 'SpecJob',
      physical_queue: 'ordinary',
      strategy: :until_and_while_executing,
      acquired: true,
      lock_ttl: 15.minutes.to_i
    )
    expect(events.first.payload).not_to have_key(:arguments)
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

end
