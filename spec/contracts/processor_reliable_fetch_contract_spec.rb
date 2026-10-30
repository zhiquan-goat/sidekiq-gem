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
  let(:lock) do
    instance_double(SidekiqUniqueness::UntilAndWhileExecuting, begin_execution: nil, reserve_enqueue: true, runtime_conflict_delay: 61,
      observability_payload: {}, runtime_redis_key: 'spec-runtime-lock',
      enqueue_redis_key: "spec:#{queue}:enqueue-lock", enqueue_ttl: 600)
  end

  before do
    stub_const('SpecProcessorJob', Class.new do
      include Sidekiq::Job

      def perform(_id)
      end
    end)
  end

  after do
    config.redis do |redis|
      redis.del("queue:#{queue}", store.inflight_key, store.owners_key, lock.enqueue_redis_key)
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

  it 'persists one runtime-conflict successor and ACKs even when the observer fails' do
    configure_conflict
    work = claim_work
    started_at = Time.now.to_f
    subscriber = ActiveSupport::Notifications.subscribe('runtime_conflict.sidekiq_uniqueness') { raise 'subscriber failed' }
    expect_any_instance_of(SpecProcessorJob).not_to receive(:perform)
    processor.send(:process, work)
    expect(store.size).to eq(0)
    successors = persisted('schedule')
    expect(successors.size).to eq(1)
    expect(persisted('retry')).to be_empty
    successor = successors.first
    expect(successor).to include('jid' => payload['jid'], 'args' => [123])
    expect(successor).not_to have_key('at')
    expect(successor).not_to have_key('enqueued_at')
    expect(SidekiqUniqueness.internal_continuation?(successor)).to be true
    score = config.redis { |redis| redis.zscore('schedule', Sidekiq.dump_json(successor)) }
    expect(score).to be_between(started_at + 61, Time.now.to_f + 61)
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it 'persists a conflict successor even if another enqueue owner refuses the reservation refresh' do
    configure_conflict
    config.redis { |redis| redis.set(lock.enqueue_redis_key, 'another-owner', ex: 600) }
    expect(lock).not_to receive(:reserve_enqueue)
    expect_any_instance_of(SpecProcessorJob).not_to receive(:perform)
    processor.send(:process, claim_work)
    expect(persisted('schedule').map { |job| job['jid'] }).to eq([payload['jid']])
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq('another-owner')
    expect(store.size).to eq(0)
  end

  [
    SidekiqUniqueness::AtomicEnqueue::Rejected.new(:schedule_wrong_type),
    SidekiqUniqueness::AtomicEnqueue::Unknown.new('reply lost')
  ].each do |failure|
    it "persists Retry instead of successful completion when successor fails with #{failure.class}" do
      configure_conflict
      allow_any_instance_of(SidekiqUniqueness::AtomicEnqueue).to receive(:write).and_raise(failure)
      expect { processor.send(:process, claim_work) }
        .to raise_error(SidekiqRuntime::ServerMiddleware::ContinuationPersistenceError) { |error| expect(error.cause).to equal(failure) }
      expect(store.size).to eq(0)
      expect(persisted('schedule')).to be_empty
      expect(persisted('retry').size).to eq(1)
    end
  end

  it 'retains the real successor after a lost reply, allowing an additional durable retry' do
    configure_conflict
    allow_any_instance_of(SidekiqUniqueness::AtomicEnqueue).to receive(:write).and_wrap_original do |method, candidate, **keywords|
      method.call(candidate, **keywords)
      raise SidekiqUniqueness::AtomicEnqueue::Unknown, 'successor reply lost'
    end
    expect { processor.send(:process, claim_work) }
      .to raise_error(SidekiqRuntime::ServerMiddleware::ContinuationPersistenceError)
    expect(persisted('schedule').size).to eq(1)
    expect(persisted('retry').size).to eq(1)
    expect(store.size).to eq(0)
  end

  def claim_work
    raw = Sidekiq.dump_json(payload)
    config.redis { |redis| redis.lpush("queue:#{queue}", raw) }
    claim = store.claim
    SidekiqRuntime::ReliableFetch::UnitOfWork.new(queue: "queue:#{queue}", job: raw,
      config: config, store: store, claim: claim)
  end

  def configure_conflict
    config.server_middleware { |chain| chain.add(SidekiqRuntime::ServerMiddleware) }
    allow(SidekiqUniqueness).to receive(:lock_for).and_return(lock)
  end

  def persisted(key)
    config.redis do |redis|
      redis.zrange(key, 0, -1).map { |raw| Sidekiq.load_json(raw) }.select { |job| job['queue'] == queue }
    end
  end
end
