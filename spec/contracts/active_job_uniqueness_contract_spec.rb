# frozen_string_literal: true

require_relative '../support/active_job_runtime'

RSpec.describe 'Active Job uniqueness contract' do
  include ActiveJobRuntimeSupport

  before do
    config.server_middleware { |chain| chain.add(SidekiqRuntime::ServerMiddleware) }
    config.death_handlers << SidekiqUniqueness::DeathHandler.new(config)
    @extra_queues = []
  end

  after do
    config.redis do |redis|
      ([queue] + @extra_queues).each do |name|
        keys = redis.call('KEYS', "#{SidekiqRuntime::QueueKeys.unique_prefix}:*:#{name}:*")
        redis.del(*keys) if keys.any?
        redis.del("queue:#{name}")
        redis.srem('queues', name)
        redis.srem(SidekiqRuntime::QueueKeys.directory_key, name)
      end
    end
  end

  def unique_job(name = 'SpecRuntimeUniqueAJJob', &block)
    build_job(name) do
      runtime_uniqueness enqueue_ttl: 600, runtime_ttl: 45, key: ->(job) { job.arguments.first }
      class_eval(&block) if block
    end
  end

  def lock_for(payload)
    SidekiqUniqueness.lock_for(job_payload: payload, physical_queue: payload.fetch('queue'))
  end

  it 'returns false for a duplicate even with transactional_push enabled, and isolates real classes and queues' do
    first_class = unique_job
    other_class = unique_job('SpecRuntimeOtherUniqueAJJob')
    other_queue = "#{queue}_other"
    @extra_queues << other_queue
    transaction_probe(first_class, open: false)
    with_transactional_push do
      first = first_class.perform_later(42)
      duplicate = first_class.new(42)
      expect(duplicate.enqueue).to be(false)
      expect(duplicate.enqueue_error).to be_a(ActiveJob::EnqueueError)
      expect(other_class.perform_later(42)).to be_a(other_class)
      expect(first_class.set(queue: other_queue).perform_later(42)).to be_a(first_class)
      expect(persisted('ready').map { |payload| payload['jid'] }).to include(first.provider_job_id)
    end
    expect(persisted('ready').size).to eq(2)
    config.redis { |redis| expect(redis.llen("queue:#{other_queue}")).to eq(1) }
  end

  it 'isolates class and queue scopes containing ambiguous colon boundaries' do
    parent = unique_job('ReviewScopeParent')
    child = unique_job('ReviewScopeParent::Child')
    other_queue = ":Child:#{queue}"
    @extra_queues << other_queue

    first = child.perform_later(42)
    second = parent.set(queue: other_queue).perform_later(42)
    expect(first).to be_a(child)
    expect(second).to be_a(parent)
    expect(child.perform_later(42)).to be(false)
    expect(parent.set(queue: other_queue).perform_later(42)).to be(false)

    first_lock = lock_for(persisted('ready').fetch(0))
    second_payload = config.redis { |redis| Sidekiq.load_json(redis.lindex("queue:#{other_queue}", 0)) }
    second_lock = lock_for(second_payload)
    config.redis do |redis|
      first_owner = first_lock.begin_execution(redis: redis, jid: first.provider_job_id)
      expect(first_owner).not_to be_nil
      expect(redis.get(first_lock.enqueue_redis_key)).to be_nil
      expect(redis.get(second_lock.enqueue_redis_key)).to eq(second.provider_job_id)
      second_owner = second_lock.begin_execution(redis: redis, jid: second.provider_job_id)
      expect(second_owner).not_to be_nil
      expect(redis.get(second_lock.enqueue_redis_key)).to be_nil
      expect(first_lock.finish_execution(redis: redis, owner: first_owner)).to eq(1)
      expect(redis.get(second_lock.runtime_redis_key)).to eq(second_owner)
      expect(second_lock.finish_execution(redis: redis, owner: second_owner)).to eq(1)
    end
  end

  it 'reports a lost atomic reply as unknown while preserving the actual written unique job' do
    klass = unique_job
    SidekiqRuntime::RedisScope.pool(config).with do |redis|
      allow(redis).to receive(:call_once).and_wrap_original do |method, *args|
        result = method.call(*args)
        raise RedisClient::ReadTimeoutError, 'reply lost after unique write' if result == 'accepted:ready'
        result
      end
    end
    expect { klass.perform_later(42) }.to raise_error(SidekiqUniqueness::AtomicEnqueue::Unknown)
    expect(persisted('ready').size).to eq(1)
    payload = persisted('ready').first
    expect(config.redis { |redis| redis.get(lock_for(payload).enqueue_redis_key) }).to eq(payload['jid'])
    expect(@events.select { |name, _| name == 'sidekiq.notification.push_result' }.last.last[:delivery_outcome]).to eq('unknown')
  end

  %i[success dead retry].each do |outcome|
    it "keeps B's enqueue owner when A finishes via #{outcome}, while C is rejected" do
      error_class = Class.new(StandardError)
      stub_const('SpecUniqueFailure', error_class)
      klass = unique_job
      klass.retry_on(error_class, wait: 30, attempts: 2) if outcome == :retry
      successor = nil
      duplicate = nil
      example = self
      runtime_config = config
      klass.action = lambda do |id|
        successor = klass.perform_later(id)
        duplicate = klass.perform_later(id)
        b = example.persisted('ready').find { |payload| payload['jid'] == successor.provider_job_id }
        runtime_config.redis { |redis| redis.expire(example.lock_for(b).enqueue_redis_key, 90) }
        raise error_class, 'failed A' unless outcome == :success
      end
      first = klass.perform_later(42)
      outcome == :dead ? expect { process_next }.to(raise_error(error_class)) : process_next
      expect(successor).to be_a(klass)
      expect(duplicate).to be(false)
      b = persisted('ready').find { |payload| payload['jid'] == successor.provider_job_id }
      config.redis do |redis|
        expect(redis.get(lock_for(b).enqueue_redis_key)).to eq(successor.provider_job_id)
        expect(redis.ttl(lock_for(b).enqueue_redis_key)).to be_between(88, 90)
        expect(redis.exists(lock_for(b).runtime_redis_key)).to eq(0)
      end
      expect(store.entries).to be_empty
      if outcome == :retry
        retry_payload = persisted('schedule').fetch(0)
        expect(retry_payload.dig('args', 0, 'job_id')).to eq(first.job_id)
        expect(retry_payload.dig('sidekiq_runtime', 'uniqueness')).to eq(b.dig('sidekiq_runtime', 'uniqueness'))
        expect(retry_payload.dig('sidekiq_runtime', 'redelivered')).to be(true)
      elsif outcome == :dead
        expect(persisted('dead').first['jid']).to eq(first.provider_job_id)
      end
    end
  end

  it 'does not create a reservation for an AJ retry; a new same-identity submission is accepted during its wait' do
    klass = unique_job do
      retry_on RuntimeError, wait: 30, attempts: 2
    end
    klass.action = ->(*) { raise 'first failure' if executions == 1 }
    first = klass.perform_later(42)
    original = persisted('ready').first
    process_next
    retry_payload = persisted('schedule').fetch(0)
    expect(config.redis { |redis| redis.exists(lock_for(original).enqueue_redis_key) }).to eq(0)
    expect(retry_payload.dig('sidekiq_runtime', 'uniqueness')).to eq(original.dig('sidekiq_runtime', 'uniqueness'))
    expect(retry_payload['jid']).not_to eq(first.provider_job_id)
    expect(klass.perform_later(42)).to be_a(klass)
  end

  it 'snapshots a GlobalID-based scalar identity once and never reevaluates it on retry/server/recovery' do
    record_class = Class.new do
      include GlobalID::Identification
      attr_accessor :id, :label
      def initialize(id, label = 'initial') = (@id, @label = id, label)
      def self.find(id) = new(id, 'reloaded')
    end
    stub_const('SpecUniqueRecord', record_class)
    old_app = GlobalID.app
    GlobalID.app = 'runtime-contract'
    key_calls = 0
    klass = unique_job do
      runtime_uniqueness enqueue_ttl: 600, runtime_ttl: 45, key: ->(job) { key_calls += 1; job.arguments.first.id }
      retry_on RuntimeError, wait: 30, attempts: 2
    end
    klass.action = ->(*) { raise 'retry' if executions == 1 }
    job = klass.perform_later(record_class.new('42'))
    wire = persisted('ready').first
    expect(wire.dig('sidekiq_runtime', 'uniqueness', 'identity')).to eq(['42'])
    expect(wire.dig('args', 0, 'arguments', 0)).to include('_aj_globalid')
    recovered = Sidekiq.load_json(SidekiqRuntime::Recovery.prepare(Sidekiq.dump_json(wire)))
    expect(recovered.dig('args', 0, 'arguments')).to eq(wire.dig('args', 0, 'arguments'))
    expect(recovered.dig('args', 0, 'sidekiq_runtime')).to eq(recovered['sidekiq_runtime'])
    process_next
    promote
    process_next
    expect(key_calls).to eq(1)
    expect(klass.records.map { |r| r[:job_id] }).to eq([job.job_id, job.job_id])
    expect(klass.records.map { |r| r[:context].redelivered? }).to eq([false, true])
    expect(klass.records.last[:args].first.label).to eq('reloaded')
  ensure
    GlobalID.instance_variable_set(:@app, old_app)
  end

  [false, true].each do |redelivered|
    it "keeps redelivered=#{redelivered} and AJ executions unchanged across a runtime conflict" do
      klass = unique_job
      klass.retry_on RuntimeError, wait: 10, attempts: 2
      klass.action = ->(*) { raise 'retry' if executions == 1 } if redelivered
      klass.perform_later(42)
      if redelivered
        process_next
        promote
      end
      original = persisted('ready').first
      lock = lock_for(original)
      config.redis { |redis| redis.set(lock.runtime_redis_key, 'other-execution', ex: 45) }
      started_at = Time.now.to_f
      count = klass.records.size
      process_next
      expect(klass.records.size).to eq(count)
      continued = persisted('schedule').fetch(0)
      expect(continued['jid']).to eq(original['jid'])
      expect(continued['args']).to eq(original['args'])
      expect(continued.dig('sidekiq_runtime', 'redelivered')).to eq(redelivered)
      score = config.redis { |redis| redis.zscore('schedule', Sidekiq.dump_json(continued)).to_f }
      expect(score - started_at).to be_between(45.9, 47)
      config.redis { |redis| redis.del(lock.runtime_redis_key) }
      promote
      process_next
      expect(klass.records.last[:executions]).to eq(redelivered ? 2 : 1)
      expect(klass.records.last[:context].redelivered?).to eq(redelivered)
    end
  end

  it 'covers the Scheduled wait in the reservation TTL and refuses early native promotion before source deletion' do
    klass = unique_job
    job = klass.set(wait: 3_600).perform_later(42)
    payload = persisted('schedule').fetch(0)
    lock = lock_for(payload)
    before = config.redis { |redis| redis.call('ZRANGE', 'schedule', 0, -1, 'WITHSCORES') }
    expect(config.redis { |redis| redis.ttl(lock.enqueue_redis_key) }).to be_between(4_198, 4_201)
    SidekiqRuntime::RedisScope.with(config) do
      entry = Sidekiq::ScheduledSet.new.find_job(job.provider_job_id)
      %i[retry add_to_queue].each do |operation|
        expect { entry.public_send(operation) }.to raise_error(SidekiqRuntime::SortedEntryGuard::UnsupportedScheduledPromotion)
      end
    end
    expect(config.redis { |redis| redis.call('ZRANGE', 'schedule', 0, -1, 'WITHSCORES') }).to eq(before)
    expect(persisted('ready')).to be_empty
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq(job.provider_job_id)
  end

  it 'preflights a mixed adapter batch before its ordinary first item can write' do
    ordinary = build_job('SpecBulkOrdinaryAJJob')
    unique = unique_job
    expect { ActiveJob.perform_all_later(ordinary.new(1), unique.new(2), ordinary.new(3)) }
      .to raise_error(SidekiqUniqueness::UnsupportedBulk)
    expect(persisted('ready')).to be_empty
    expect(persisted('schedule')).to be_empty
    expect(config.redis { |redis| redis.call('KEYS', "#{SidekiqRuntime::QueueKeys.unique_prefix}:*:#{queue}:*") }).to be_empty
  end

  it 'rejects unsupported adapters before single and bulk enqueue' do
    klass = unique_job
    klass.queue_adapter = :test
    expect { klass.perform_later(42) }.to raise_error(ArgumentError, /sidekiq_runtime adapter/)
    expect { ActiveJob.perform_all_later(klass.new(42)) }.to raise_error(ArgumentError, /sidekiq_runtime adapter/)
    expect(persisted('ready')).to be_empty
  end

  it 'does not treat redelivered or a matching job_id as a current self-retry signal' do
    klass = unique_job
    first = klass.perform_later(42)
    copied = ActiveJob::Base.deserialize(SidekiqRuntime::JobMetadata.with_redelivery(first.serialize))
    expect(copied.enqueue).to be(false)
    expect(persisted('ready').size).to eq(1)
  end

  it 'rejects a unique AJ self-retry queue change before writing a successor' do
    klass = unique_job
    klass.action = ->(*) { retry_job wait: 30, queue: "#{queue_name}_other" }
    klass.perform_later(42)
    expect { process_next }.to raise_error(ArgumentError, /original physical queue/)
    expect(persisted('schedule')).to be_empty
    expect(persisted('dead').size).to eq(1)
  end

  it 'reads transport redelivery metadata at the server boundary even when the AJ copy is stale' do
    klass = unique_job
    klass.perform_later(42)
    payload = persisted('ready').first
    payload['sidekiq_runtime']['redelivered'] = true
    expect(payload.dig('args', 0, 'sidekiq_runtime', 'redelivered')).to be(false)
    config.redis do |redis|
      redis.lset("queue:#{queue}", 0, Sidekiq.dump_json(payload))
    end
    process_next
    expect(klass.records.first[:context].redelivered?).to be(true)
  end

  it 'rejects retry queue changes made after client middleware returns, including ordinary AJ retries' do
    klass = build_job('SpecRetryRoutingAJJob')
    klass.action = ->(*) { retry_job wait: 30 }
    mutator = Class.new do
      def call(_class, payload, _queue, _pool)
        yield.tap do |result|
          result['queue'] += '_other' if result.dig('sidekiq_runtime', 'redelivered')
        end
      end
    end
    config.client_middleware { |chain| chain.add(mutator) }
    klass.perform_later(42)
    expect { process_next }.to raise_error(ArgumentError, /original physical queue/)
    expect(persisted('schedule')).to be_empty
    expect(persisted('dead').size).to eq(1)
  end

  it 'marks official AJ Dead retry without retry_count, preserves the snapshot, and leaves B reservation alone' do
    klass = unique_job
    klass.action = ->(*) { raise 'dead A' }
    first = klass.perform_later(42)
    expect { process_next }.to raise_error(RuntimeError, 'dead A')
    dead = persisted('dead').first
    lock = lock_for(dead)
    # Sidekiq can retain failures without a retry counter; management must not
    # fabricate one to route them around first-submission admission.
    config.redis do |redis|
      redis.zrem('dead', Sidekiq.dump_json(dead))
      dead.delete('retry_count')
      redis.zadd('dead', Time.now.to_f, Sidekiq.dump_json(dead))
    end
    second = klass.perform_later(42)
    SidekiqRuntime::RedisScope.with(config) do
      expect(Sidekiq::DeadSet.new.find_job(first.provider_job_id).retry).to eq(first.provider_job_id)
    end
    retried = persisted('ready').find { |item| item['jid'] == first.provider_job_id }
    expect(retried).not_to have_key('retry_count')
    expect(retried.dig('args', 0, 'arguments')).to eq(dead.dig('args', 0, 'arguments'))
    expect(retried.dig('args', 0, 'sidekiq_runtime')).to eq(retried['sidekiq_runtime'])
    expect(retried.dig('sidekiq_runtime', 'redelivered')).to be(true)
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq(second.provider_job_id)
    klass.action = nil
    process_next # B first, then A's managed retry
    process_next
    expect(klass.records.last[:context].redelivered?).to be(true)
    expect(persisted('dead')).to be_empty
  end
end
