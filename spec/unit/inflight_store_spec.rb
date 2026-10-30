require 'spec_helper'

RSpec.describe SidekiqRuntime::InflightStore do
  before { register_runtime_owner(config, 'spec-owner') }

  let(:redis_url) { ENV.fetch('SIDEKIQ_REDIS_URL') }
  let(:config) do
    Sidekiq::Config.new(death_handlers: [], error_handlers: []).tap do |sidekiq_config|
      sidekiq_config.redis = {url: redis_url}
    end
  end
  let(:token) { SecureRandom.hex(8) }
  let(:queue) { "reliable_fetch_spec_#{token}" }
  let(:source_queue) { SidekiqRuntime::QueueKeys.source_queue(queue) }
  let(:now) { 1_721_234_567.5 }
  let(:claim_ids) { Enumerator.new { |ids| loop.with_index { |_unused, index| ids << "claim-#{index}" } } }
  let(:store) do
    described_class.new(
      config,
      owner_identity: 'spec-owner',
      queue: queue,
      id_generator: -> { claim_ids.next }
    )
  end
  let(:raw_job) { build_raw_job('jid-reliable-fetch') }

  after do
    config.redis do |redis|
      redis.del(source_queue, store.inflight_key, store.owners_key,
        SidekiqRuntime::QueueKeys.rate_limit_key(queue))
      redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue) if
        redis.call('TYPE', SidekiqRuntime::QueueKeys.directory_key) == 'set'
    end
  end

  it 'atomically moves queue work into its persistent inflight ZSET' do
    config.redis { |redis| redis.lpush(source_queue, raw_job) }

    claim = store.claim

    expect(claim.job).to eq(raw_job)
    expect(claim.member).to eq("claim-0\n#{raw_job}")
    expect(claim.claimed_at).to be_within(1).of(Time.now.to_f)
    expect(claim.owner_identity).to eq('spec-owner')
    expect(config.redis { |redis| redis.hget(store.owners_key, claim.member) }).to eq('spec-owner')
    expect(config.redis { |redis| redis.llen(source_queue) }).to eq(0)
    expect(config.redis { |redis| redis.zrange(store.inflight_key, 0, -1) }).to eq([claim.member])
    expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(1)
    score = config.redis { |redis| redis.zscore(store.inflight_key, claim.member) }
    expect(score.to_f).to eq(claim.claimed_at)
    expect(config.redis { |redis| redis.ttl(store.inflight_key) }).to eq(-1)
  end

  it 'does not create an inflight entry or register an empty queue' do
    previous_members = config.redis { |redis| redis.smembers(SidekiqRuntime::QueueKeys.directory_key) }
    expect(store.claim).to be_nil
    expect(store.size).to eq(0)
    expect(config.redis { |redis| redis.smembers(SidekiqRuntime::QueueKeys.directory_key) }).to eq(previous_members)
  end

  it 'does not spend a configured token when the queue is empty' do
    limiter = SidekiqRuntime::QueueRateLimit.new(config)
    limiter.enable(store.physical_queue, rate: 1, burst: 1)

    expect(store.claim).to be_nil
    expect(limiter.get(store.physical_queue).tokens).to eq(1)
  end

  it 'leaves Ready and ownership unchanged when the shared bucket denies a claim' do
    limiter = SidekiqRuntime::QueueRateLimit.new(config)
    limiter.enable(store.physical_queue, rate: 0.01, burst: 1)
    config.redis do |redis|
      redis.lpush(source_queue, build_raw_job('jid-rate-first'), build_raw_job('jid-rate-second'))
    end

    first = store.claim
    limited = store.claim

    expect(first).to be_a(described_class::Claim)
    expect(limited).to be_a(described_class::RateLimited)
    expect(limited.physical_queue).to eq(store.physical_queue)
    expect(limited.retry_after_seconds).to be_between(99.0, 100.0)
    expect(config.redis { |redis| redis.llen(source_queue) }).to eq(1)
    expect(store.entries.map(&:member)).to eq([first.member])
    expect(config.redis { |redis| redis.hlen(store.owners_key) }).to eq(1)
  end

  it 'does not treat a corrupt configured policy as unlimited' do
    config.redis do |redis|
      redis.lpush(source_queue, raw_job)
      redis.hset(SidekiqRuntime::QueueKeys.rate_limit_key(store.physical_queue),
        'rate', 'broken', 'burst', '1', 'tokens', '1', 'last_refill', Time.now.to_f)
    end

    expect { store.claim }.to raise_error(RedisClient::CommandError, /finite/)
    expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([raw_job])
    expect(store.size).to eq(0)
  end

  it 'keeps Ready intact when the directory has the wrong type' do
    job = build_raw_job('jid-managed-wrong-type', queue: queue)
    config.redis { |redis| redis.lpush(source_queue, job) }

    with_wrong_type_sidekiq_queue_directory(config) do
      expect { store.claim }.to raise_error(RedisClient::CommandError)
      expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([job])
      expect(store.size).to eq(0)
    end
  end

  it 'keeps Ready after a failure following directory registration' do
    job = build_raw_job('jid-managed-partial-write', queue: queue)
    broken = described_class.const_get(:CLAIM).sub(
      "redis.call('ZADD', KEYS[2], claimed_at, member)",
      "redis.call('NOT_A_REDIS_COMMAND')"
    )
    config.redis { |redis| redis.lpush(source_queue, job) }

    expect do
      config.redis do |redis|
        redis.call_once('EVAL', broken, 6,
          source_queue, store.inflight_key, store.owners_key,
          SidekiqRuntime::QueueKeys.directory_key, SidekiqRuntime::QueueKeys.rate_limit_key(queue), 'spec-owner',
           'claim-failed-after-directory', 'spec-owner', queue)
      end
    end.to raise_error(RedisClient::CommandError)

    expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([job])
    expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(1)
    expect(store.size).to eq(0)
  end

  it 'loads an uncached script only after a confirmed NOSCRIPT, never after an ambiguous reply' do
    redis = instance_double(Sidekiq::RedisClientAdapter::CompatClient)
    connection = double('config')
    allow(connection).to receive(:redis).and_yield(redis)
    local_store = described_class.new(connection, owner_identity: 'owner', queue: queue)
    expect(redis).to receive(:call_once).with('EVALSHA', anything, 6, any_args)
      .ordered.and_raise(RedisClient::CommandError, 'NOSCRIPT No matching script')
    expect(redis).to receive(:call_once).with('EVAL', anything, 6, any_args).ordered.and_return(false)
    expect(local_store.claim).to be_nil
  end

  it 'keeps a claimed job visible when Redis applies CLAIM but its reply is lost' do
    job_a = build_raw_job('jid-a')
    job_b = build_raw_job('jid-b')
    config.redis do |redis|
      redis.lpush(source_queue, job_a)
      redis.lpush(source_queue, job_b)
    end

    ambiguous_config = reply_lost_config(config)
    ambiguous_store = described_class.new(
      ambiguous_config,
      owner_identity: 'spec-owner',
      queue: queue,
      id_generator: -> { 'claim-reply-lost' }
    )

    expect { ambiguous_store.claim }
      .to raise_error(RedisClient::ConnectionError, 'simulated lost reply')
    expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([job_b])
    expect(store.entries.map(&:job)).to eq([job_a])
    expect(store.entries.first.owner_identity).to eq('spec-owner')
    expect(config.redis { |redis| redis.sismember(SidekiqRuntime::QueueKeys.directory_key, queue) }).to eq(1)

    claim_b = store.claim
    expect(claim_b.job).to eq(job_b)
    expect(store.acknowledge(claim_b)).to eq(1)
    expect(store.entries.map(&:job)).to eq([job_a])
  end

  it 'tracks identical raw jobs as distinct claims' do
    config.redis do |redis|
      redis.lpush(source_queue, raw_job)
      redis.lpush(source_queue, raw_job)
    end

    first_claim = store.claim
    second_claim = store.claim

    expect(first_claim.member).not_to eq(second_claim.member)
    expect(store.size).to eq(2)
    expect(store.acknowledge(second_claim)).to eq(1)
    expect(store.acknowledge(second_claim)).to eq(0)
    expect(store.entries.map(&:member)).to eq([first_claim.member])
  end

  it 'atomically requeues a replacement at most once' do
    claim = claim_job(raw_job)
    recovered_job = Sidekiq.dump_json(
      Sidekiq.load_json(raw_job).tap { |payload| payload['args'][0]['recovered'] = true }
    )

    expect(store.requeue(claim, replacement_job: recovered_job)).to eq(1)
    expect(store.requeue(claim, replacement_job: recovered_job)).to eq(0)
    expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([recovered_job])
    expect(store.size).to eq(0)
  end

  it 'reports an unknown requeue result without replaying after its reply is lost' do
    claim = claim_job(raw_job)
    ambiguous_store = described_class.new(
      reply_lost_config(config),
      queue: queue
    )

    expect { ambiguous_store.requeue(claim) }
      .to raise_error(RedisClient::ConnectionError, 'simulated lost reply')
    expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([raw_job])
    expect(store.size).to eq(0)
  end

  it 'validates destination types before removing a queued job' do
    config.redis do |redis|
      redis.lpush(source_queue, raw_job)
      redis.set(store.inflight_key, 'wrong-type')
    end

    expect { store.claim }.to raise_error(RedisClient::CommandError)
    expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([raw_job])
  end


  it 'keeps the inflight marker when the requeue destination is not a list' do
    claim = claim_job(raw_job)
    config.redis { |redis| redis.set(source_queue, 'wrong-type') }

    expect { store.requeue(claim) }.to raise_error(RedisClient::CommandError)
    expect(store.entries.map(&:member)).to eq([claim.member])
    expect(config.redis { |redis| redis.get(source_queue) }).to eq('wrong-type')
  end

  it 'lists oldest entries first, supports lookup, and reports oldest age' do
    first_store = described_class.new(
      config,
      owner_identity: 'spec-owner',
      queue: queue,
      id_generator: -> { 'claim-oldest' }
    )
    config.redis { |redis| redis.lpush(source_queue, build_raw_job('jid-oldest')) }
    oldest = first_store.claim
    config.redis { |redis| redis.zadd(store.inflight_key, Time.now.to_f - 10, oldest.member) }
    newest = claim_job(build_raw_job('jid-newest'))

    expect(store.entries(offset: 0, limit: 1).map(&:jid)).to eq(['jid-oldest'])
    expect(store.entries(offset: 1, limit: 1).map(&:jid)).to eq(['jid-newest'])
    expect(store.find_entry(newest.entry_id).member).to eq(newest.member)
    expect(store.snapshot).to include(inflight_count: 2,
      oldest_inflight_age_seconds: be_within(1).of(10))
  end

  it 'displays a damaged ownerless claim but does not authorize recovery' do
    member = "damaged-claim\n#{raw_job}"
    config.redis { |redis| redis.zadd(store.inflight_key, now, member) }
    claim = store.entries.first
    expect(claim.owner_identity).to be_nil
    expect(store.reclaim_orphan(claim, job: raw_job, destination: 'requeued')).to eq('owner_missing')
  end

  it 'does not consume ready work when the owner sidecar has the wrong type' do
    config.redis do |redis|
      redis.lpush(source_queue, raw_job)
      redis.set(store.owners_key, 'bad-type')
    end
    expect { store.claim }.to raise_error(RedisClient::CommandError)
    expect(config.redis { |redis| redis.lrange(source_queue, 0, -1) }).to eq([raw_job])
  end

  def build_raw_job(jid, queue: source_queue.delete_prefix('queue:'))
    Sidekiq.dump_json(
      'class' => SidekiqRuntimeSpecSupport::JOB_CLASS_NAME,
      'queue' => queue,
      'jid' => jid,
      'retry' => 1,
      'dead' => true,
      'args' => [build_business_payload]
    )
  end

  def build_business_payload
    build_sidekiq_runtime_business_payload(chain_id: 'chain-reliable-fetch')
  end

  def claim_job(job)
    config.redis { |redis| redis.lpush(source_queue, job) }
    store.claim
  end

  def reply_lost_config(base_config)
    proxy_class = Class.new do
      def initialize(redis)
        @redis = redis
      end

      def call_once(*arguments)
        @redis.call_once(*arguments)
        raise RedisClient::ConnectionError, 'simulated lost reply'
      end

      def method_missing(name, *arguments, **options, &block)
        @redis.public_send(name, *arguments, **options, &block)
      end

      def respond_to_missing?(name, include_private = false)
        @redis.respond_to?(name, include_private) || super
      end
    end

    Struct.new(:base_config, :proxy_class) do
      def redis
        base_config.redis { |redis| yield proxy_class.new(redis) }
      end
    end.new(base_config, proxy_class)
  end
end
