# frozen_string_literal: true

require 'spec_helper'
require 'timeout'
require 'json'

RSpec.describe 'Independent exact-claim terminal safety' do
  let(:queue) { "safety_terminal_#{SecureRandom.hex(5)}" }
  let(:config) do
    Sidekiq::Config.new.tap do |value|
      value.logger = Logger.new(File::NULL)
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
      value.queues = [queue]
    end
  end
  let(:store) { new_store(config, 'original-owner') }
  before do
    register_runtime_owner(config, 'original-owner')
    register_runtime_owner(config, 'replacement-owner')
  end
  let(:raw) { Sidekiq.dump_json(build_sidekiq_runtime_job_payload(physical_queue: queue)) }

  after do
    config.redis do |redis|
      redis.del(store.source_queue, store.inflight_key, store.owners_key)
      redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue)
    end
  end

  it 'conserves one source across concurrent ACK, shutdown and two reclaimers, including late old operations' do
    observations = []
    16.times do |iteration|
      register_runtime_owner(config, 'original-owner')
      config.redis { |redis| redis.lpush(store.source_queue, raw) }
      claim = store.claim
      config.redis { |redis| redis.del(claim.owner_identity) }
      work = SidekiqRuntime::ReliableFetch::UnitOfWork.new(store: store, claim: claim, job: claim.job)
      actions = {
        ack: -> { store.acknowledge(claim) }, shutdown: -> { work.requeue },
        reclaimer_a: -> { SidekiqRuntime::OrphanReclaimer.new(config).recover(claim)[:outcome] },
        reclaimer_b: -> { SidekiqRuntime::OrphanReclaimer.new(config).recover(claim)[:outcome] }
      }
      ready = Queue.new
      gate = Queue.new
      threads = actions.to_a.shuffle(random: Random.new(20261023 + iteration)).to_h do |name, action|
        [name, Thread.new { ready << true; gate.pop; action.call }]
      end
      results = Timeout.timeout(10) do
        4.times { ready.pop }
        4.times { gate << true }
        threads.transform_values(&:value)
      end
      winners = results.count { |_name, value| value == 1 || value == 'requeued' }
      expect(winners).to eq(1)
      expect(results.values_at(:reclaimer_a, :reclaimer_b) - %w[requeued missing]).to be_empty
      expect_empty_claim_state
      ready_count = config.redis { |redis| redis.llen(store.source_queue) }
      expect(ready_count).to eq(results[:ack] == 1 ? 0 : 1)
      if ready_count == 1
        replacement = new_store(config, 'replacement-owner').claim
        expect(replacement.payload['args']).to eq(claim.payload['args'])
        expect(replacement.payload.dig('sidekiq_runtime', 'redelivered')).to be(true)
        expect(replacement.payload.dig('sidekiq_runtime', 'recovery_count')).to eq(results[:shutdown] == 1 ? nil : 1)
        before = store.owner_state(replacement)
        expect(store.acknowledge(claim)).to eq(0)
        expect(work.requeue).to eq(0)
        expect(store.entries.map(&:member)).to eq([replacement.member])
        expect(store.owner_state(replacement).except(:redis_now, :heartbeat_ttl_seconds)).to eq(before.except(:redis_now, :heartbeat_ttl_seconds))
        expect(config.redis { |redis| redis.llen(store.source_queue) }).to eq(0)
        expect(store.acknowledge(replacement)).to eq(1)
      end
      observations << {iteration: iteration, results: results, ready_count: ready_count}
      expect_empty_claim_state
    ensure
      threads&.each_value { |thread| thread.kill if thread.alive? }
      threads&.each_value(&:join)
    end
    save_safety_evidence('terminal-races', observations)
  end

  it 'does not remove an identical-payload replacement after an ACK reply is lost' do
    config.redis { |redis| redis.lpush(store.source_queue, raw) }
    original = store.claim
    requests = []
    fault_config = Object.new
    base = config
    fault_config.define_singleton_method(:redis) do |&block|
      base.redis do |connection|
        proxy = Object.new
        proxy.define_singleton_method(:call_once) do |*args|
          connection.call_once(*args)
          requests << args.first
          raise RedisClient::ReadTimeoutError, 'ACK committed; reply lost'
        end
        block.call(proxy)
      end
    end
    ambiguous = new_store(fault_config, 'original-owner')
    expect { ambiguous.acknowledge(original) }.to raise_error(RedisClient::ReadTimeoutError, /reply lost/)
    expect(requests.size).to eq(1)
    expect_empty_claim_state
    config.redis { |redis| redis.lpush(store.source_queue, raw) }
    replacement = new_store(config, 'replacement-owner').claim
    expect(replacement.job).to eq(original.job)
    expect(replacement.member).not_to eq(original.member)
    expect(store.acknowledge(original)).to eq(0)
    expect(SidekiqRuntime::OrphanReclaimer.new(config).recover(original)[:outcome]).to eq('missing')
    expect(store.entries.map(&:member)).to eq([replacement.member])
    expect(store.owner_state(replacement)).to include(present: true, owner_identity: 'replacement-owner')
    expect(config.redis { |redis| redis.llen(store.source_queue) }).to eq(0)
    expect(store.acknowledge(replacement)).to eq(1)
    expect_empty_claim_state
    save_safety_evidence('lost-ack-reply', requests: requests, original: original.to_h, replacement: replacement.to_h)
  end

  def new_store(redis_config, owner)
    SidekiqRuntime::InflightStore.new(redis_config, queue: queue, owner_identity: owner)
  end

  def expect_empty_claim_state
    expect(config.redis { |redis| [redis.zcard(store.inflight_key), redis.hlen(store.owners_key)] }).to eq([0, 0])
  end

  def save_safety_evidence(name, value)
    return unless ENV['SIDEKIQ_RUNTIME_EVIDENCE_DIR']
    require 'fileutils'
    FileUtils.mkdir_p(ENV.fetch('SIDEKIQ_RUNTIME_EVIDENCE_DIR'))
    File.write(File.join(ENV.fetch('SIDEKIQ_RUNTIME_EVIDENCE_DIR'), "#{name}.json"), JSON.pretty_generate(value))
  end
end
