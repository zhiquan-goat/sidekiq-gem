# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'Process heartbeat recovery' do
  let(:queue) { "heartbeat_#{SecureRandom.hex(6)}" }
  let(:owner) { "#{queue}:owner" }
  let(:config) { Sidekiq::Config.new.tap { |c| c.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}; c.logger = Logger.new(File::NULL) } }
  let(:store) { store_for(queue, owner) }
  let(:reclaimer) { SidekiqRuntime::OrphanReclaimer.new(config) }
  let(:payload) { build_sidekiq_runtime_job_payload(physical_queue: queue) }
  let(:queues) { [queue] }

  before { register_runtime_owner(config, owner) }
  after do
    config.redis do |redis|
      queues.each do |name|
        target = store_for(name, owner)
        redis.del(target.source_queue, target.inflight_key, target.owners_key)
        redis.srem(SidekiqRuntime::QueueKeys.directory_key, name)
      end
      redis.del(owner, "#{owner}:other")
      redis.zrange('dead', 0, -1).each { |raw| redis.zrem('dead', raw) if raw.include?(queue) }
    end
  end

  it 'leaves Ready untouched until the first valid process heartbeat' do
    config.redis { |r| r.del(owner); r.lpush(store.source_queue, Sidekiq.dump_json(payload)) }
    expect(store.claim).to be_nil
    expect(store.size).to eq(0)
    expect(config.redis { |r| r.llen(store.source_queue) }).to eq(1)
    register_runtime_owner(config, owner)
    expect(store.claim.payload).to eq(payload)
  end

  it 'does not consume work with a malformed or nonexpiring heartbeat' do
    config.redis { |r| r.persist(owner); r.lpush(store.source_queue, Sidekiq.dump_json(payload)) }
    expect { store.claim }.to raise_error(RedisClient::CommandError, /heartbeat/)
    config.redis { |r| r.del(owner); r.set(owner, 'invalid', ex: 60) }
    expect { store.claim }.to raise_error(RedisClient::CommandError, /heartbeat/)
    expect(config.redis { |r| r.llen(store.source_queue) }).to eq(1)
    expect(store.size).to eq(0)
  end

  it 'uses Redis time and does not reclaim a long job while its owner is alive' do
    allow(Time).to receive(:now).and_return(Time.at(1))
    claim = claim_payload
    expect(claim.claimed_at).to be > 1
    config.redis { |r| r.zadd(store.inflight_key, 1, claim.member) }
    expect(store.owner_state(claim)).to include(owner_status: 'owner_alive', present: true)
    expect(reclaimer.recover(claim)).to eq(outcome: 'owner_alive')
    expect(store.snapshot).to include(inflight_count: 1, protocol_version: 3)
  end

  it 'recovers after native heartbeat expiry and rejects a stale ACK against the replacement' do
    claim = claim_payload
    config.redis { |r| r.call('PEXPIRE', owner, 1) }
    sleep 0.01
    expect(reclaimer.recover(claim)).to eq(outcome: 'requeued', reason: 'owner_dead', recovery_count: 1)
    register_runtime_owner(config, owner)
    replacement = store.claim
    expect(store.acknowledge(claim)).to eq(0)
    expect(store.entries.map(&:member)).to eq([replacement.member])
    expect(replacement.payload['args']).to eq(payload['args'])
    expect(replacement.payload['retry_count']).to be_nil
  end

  it 'rechecks the heartbeat atomically after an orphan snapshot becomes stale' do
    events = []
    subscriber = ActiveSupport::Notifications.subscribe(SidekiqRuntime::OrphanReclaimer::EVENT_NAME) { |event| events << event.payload }
    claim = claim_payload
    config.redis { |r| r.del(owner) }
    expect(store.owner_state(claim)).to include(owner_status: 'owner_dead')
    register_runtime_owner(config, owner)
    expect(reclaimer.recover(claim)).to eq(outcome: 'owner_alive')
    expect(events.last).to include(operation: 'recover', outcome: 'owner_alive')
    expect(events.last.keys).not_to include(:reason, :recovery_count)
    expect(store.entries.first.job).to eq(claim.job)
    expect(store.size).to eq(1)
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end

  it 'allows only one of two reclaimers to move an exact claim' do
    claim = claim_payload
    config.redis { |r| r.del(owner) }
    gate = Queue.new
    threads = 2.times.map { Thread.new { gate.pop; reclaimer.recover(claim) } }
    2.times { gate << true }
    expect(threads.map(&:value).map { |result| result[:outcome] }).to contain_exactly('requeued', 'missing')
    expect(config.redis { |r| [r.llen(store.source_queue), r.hlen(store.owners_key)] }).to eq([1, 0])
  end

  it 'does not recover unknown heartbeats, missing owners or changed ownership' do
    claim = claim_payload
    config.redis { |r| r.persist(owner) }
    expect(reclaimer.recover(claim)).to include(outcome: 'unknown')
    config.redis { |r| r.del(owner); r.set(owner, 'wrong-type', ex: 60) }
    expect(reclaimer.recover(claim)).to include(outcome: 'unknown')
    config.redis { |r| r.hset(store.owners_key, claim.member, "#{owner}:other") }
    expect(reclaimer.recover(claim)).to eq(outcome: 'owner_changed')
    expect(store.acknowledge(claim)).to eq(0)
    config.redis { |r| r.hdel(store.owners_key, claim.member) }
    expect(reclaimer.recover(claim)).to eq(outcome: 'owner_missing')
    expect(store.size).to eq(1)
  end

  it 'keeps an unreadable heartbeat unknown rather than authorizing recovery' do
    claim = claim_payload
    allow(config).to receive(:redis).and_raise(RedisClient::ConnectionError)
    expect(reclaimer.recover(claim)).to include(outcome: 'unknown', current_state: {known: false})
    allow(config).to receive(:redis).and_call_original
    expect(store.size).to eq(1)
  end

  it 'does not replay a recovery whose write reply was lost' do
    claim = claim_payload
    config.redis { |r| r.del(owner) }
    allow(store).to receive(:reclaim_orphan).and_wrap_original do |original, *args, **kwargs|
      original.call(*args, **kwargs)
      raise RedisClient::ConnectionError, 'reply lost after write'
    end
    expect(reclaimer.recover(claim, store: store)).to include(outcome: 'unknown', current_state: include(present: false))
    expect(reclaimer.recover(claim)).to eq(outcome: 'missing')
    expect(config.redis { |r| r.llen(store.source_queue) }).to eq(1)
  end

  it 'bounds crash recovery and retains original arguments in Dead' do
    claim = claim_payload
    3.times do |index|
      config.redis { |r| r.del(owner) }
      expect(reclaimer.recover(claim)).to include(outcome: 'requeued', recovery_count: index + 1)
      register_runtime_owner(config, owner)
      claim = store.claim
      expect(claim.payload['args']).to eq(payload['args'])
    end
    config.redis { |r| r.del(owner) }
    expect(reclaimer.recover(claim)).to include(outcome: 'dead', reason: 'recovery_exhausted')
    dead = config.redis { |r| r.zrange('dead', 0, -1) }.map { |raw| Sidekiq.load_json(raw) }.find { |job| job['queue'] == queue }
    expect(dead).to include('args' => payload['args'], 'error_message' => 'recovery_exhausted')
    expect(store.size).to eq(0)
  end

  it 'moves an invalid protocol envelope to Dead without executing business code' do
    claim = claim_payload(payload.merge('retry_queue' => 'forbidden'))
    config.redis { |r| r.del(owner) }
    expect(reclaimer.recover(claim)).to include(outcome: 'dead', reason: 'invalid_protocol')
    expect(config.redis { |r| r.llen(store.source_queue) }).to eq(0)
  end

  it 'advances bounded scans beyond healthy old claims and discovers unconfigured queues' do
    allow(SidekiqRuntime::QueueKeys).to receive(:directory_key).and_return("#{queue}:directory")
    healthy = 20.times.map { claim_payload }
    orphan_owner = "#{owner}:other"
    register_runtime_owner(config, orphan_owner)
    target = store_for(queue, orphan_owner)
    config.redis { |r| r.lpush(target.source_queue, Sidekiq.dump_json(payload)) }
    target.claim
    removed = "#{queue}_unconfigured"
    queues << removed
    other = store_for(removed, orphan_owner)
    config.redis { |r| r.lpush(other.source_queue, Sidekiq.dump_json(payload.merge('queue' => removed))) }
    other.claim
    config.redis { |r| r.del(orphan_owner) }
    settings = SidekiqRuntime::RecoveryConfiguration.new(queue_batch: 1, reclaim_batch: 2)
    scanner = SidekiqRuntime::OrphanReclaimer.new(config, settings: settings)
    100.times do
      scanner.tick
      break if store.size == healthy.size && other.size.zero?
    end
    expect(store.entries.map(&:member)).to match_array(healthy.map(&:member))
    expect(other.size).to eq(0)
    expect(config.redis { |r| [r.llen(store.source_queue), r.llen(other.source_queue)] }).to eq([1, 1])
  end

  it 'clears claims on ACK or trusted shutdown without spending the crash budget' do
    claim = claim_payload
    work = SidekiqRuntime::ReliableFetch::UnitOfWork.new(queue: store.source_queue, job: claim.job, store: store, claim: claim)
    expect(work.requeue).to eq(1)
    replacement = store.claim
    expect(replacement.payload.dig('sidekiq_runtime', 'redelivered')).to be true
    expect(replacement.payload.dig('sidekiq_runtime', 'recovery_count')).to be_nil
    expect(store.acknowledge(replacement)).to eq(1)
    expect(store.snapshot).to include(inflight_count: 0)
  end

  def store_for(name, identity)
    SidekiqRuntime::InflightStore.new(config, queue: name, owner_identity: identity)
  end

  def claim_payload(value = payload)
    config.redis { |r| r.lpush(store.source_queue, Sidekiq.dump_json(value)) }
    store.claim
  end
end
