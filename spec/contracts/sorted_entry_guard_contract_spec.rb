# frozen_string_literal: true

require_relative '../support/active_job_runtime'
require 'sidekiq/api'

RSpec.describe SidekiqRuntime::SortedEntryGuard do
  let(:config) do
    Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
  end
  let(:queue) { "sorted_entry_guard_#{SecureRandom.hex(6)}" }
  let(:unique_job) do
    Class.new(ActiveJob::Base) do
      runtime_uniqueness enqueue_ttl: 600, runtime_ttl: 45, key: ->(job) { job.arguments.first }
      def perform(_identity); end
    end
  end
  let(:ordinary_job) do
    Class.new do
      include Sidekiq::Job

      def perform(_identity)
      end
    end
  end

  let(:lock_keys) { [] }

  around do |example|
    SidekiqRuntime::RedisScope.with(config) { example.run }
  end

  before do
    stub_const('SpecSortedEntryUniqueJob', unique_job)
    stub_const('SpecSortedEntryOrdinaryJob', ordinary_job)
    unique_job.queue_adapter = ActiveJob::QueueAdapters::SidekiqRuntimeAdapter.new(config: config)
    unique_job.queue_name = queue
    unique_job.logger = Logger.new(File::NULL)
    ordinary_job.sidekiq_options('queue' => queue, 'retry' => 2, 'dead' => true)
    cleanup
  end

  after { cleanup }

  %i[add_to_queue retry].each do |operation|
    it "rejects unique Scheduled ##{operation} before changing its source or lock" do
      identity = "#{operation}-unique"
      jid = Sidekiq::Client.via(config.redis_pool) { unique_job.set(wait: 3_600).perform_later(identity).provider_job_id }
      raw, score = sorted_pair('schedule', jid)
      payload = Sidekiq.load_json(raw)
      lock = SidekiqUniqueness.lock_for(
        job_payload: payload,
        physical_queue: queue
      )
      lock_keys.concat([lock.enqueue_redis_key, lock.runtime_redis_key])
      lock_owner, lock_ttl = config.redis do |redis|
        [redis.get(lock.enqueue_redis_key), redis.ttl(lock.enqueue_redis_key)]
      end

      entry = Sidekiq::ScheduledSet.new.find_job(jid)
      expect { entry.public_send(operation) }
        .to raise_error(described_class::UnsupportedScheduledPromotion, /runtime Poller/)

      expect(sorted_pair('schedule', jid)).to eq([raw, score])
      expect(ready_payloads).to be_empty
      config.redis do |redis|
        expect(redis.get(lock.enqueue_redis_key)).to eq(lock_owner).and eq(jid)
        expect(redis.ttl(lock.enqueue_redis_key)).to be_between(lock_ttl - 1, lock_ttl)
      end
    end

    it "keeps ordinary Scheduled ##{operation} behavior" do
      jid = Sidekiq::Client.via(config.redis_pool) do
        ordinary_job.perform_in(3_600, "#{operation}-ordinary")
      end

      expect(Sidekiq::ScheduledSet.new.find_job(jid).public_send(operation)).to eq(jid)
      expect(sorted_pair('schedule', jid)).to be_nil
      expect(ready_payloads.map { |payload| payload['jid'] }).to eq([jid])
    end
  end

  it 'keeps Active Job unique RetrySet retry behavior' do
    jid = add_sorted_payload('retry', retry_count: 1)

    expect(Sidekiq::RetrySet.new.find_job(jid).retry).to eq(jid)
    expect(sorted_pair('retry', jid)).to be_nil
    expect(ready_payloads).to contain_exactly(include('jid' => jid, 'retry_count' => 0, 'sidekiq_runtime' => include('redelivered' => true)))
  end

  it 'keeps official Active Job unique Dead retry behavior' do
    jid = add_sorted_payload('dead', retry_count: 2)

    expect(Sidekiq::DeadSet.new.find_job(jid).retry).to eq(jid)
    expect(sorted_pair('dead', jid)).to be_nil
    expect(ready_payloads).to contain_exactly(include('jid' => jid, 'retry_count' => 1, 'sidekiq_runtime' => include('redelivered' => true)))
  end

  it 'supports native Dead Retry without a counter and leaves a newer same-identity reservation intact' do
    jid = add_sorted_payload('dead', retry_count: nil)
    raw, = sorted_pair('dead', jid)
    payload = Sidekiq.load_json(raw)
    lock = SidekiqUniqueness.lock_for(job_payload: payload, physical_queue: queue)
    lock_keys.concat([lock.enqueue_redis_key, lock.runtime_redis_key])
    second = Sidekiq::Client.via(config.redis_pool) { unique_job.perform_later('dead-identity').provider_job_id }
    expect(Sidekiq::DeadSet.new.find_job(jid).retry).to eq(jid)
    retried = ready_payloads.find { |item| item['jid'] == jid }
    expect(retried['retry_count']).to be_nil
    expect(retried['sidekiq_runtime']).to eq(payload['sidekiq_runtime'].merge('redelivered' => true))
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq(second)
  end

  it 'keeps Dead Delete a native data operation without clearing a newer reservation' do
    jid = add_sorted_payload('dead', retry_count: 0)
    second = Sidekiq::Client.via(config.redis_pool) { unique_job.perform_later('dead-identity').provider_job_id }
    payload = ready_payloads.fetch(0)
    lock = SidekiqUniqueness.lock_for(job_payload: payload, physical_queue: queue)
    lock_keys.concat([lock.enqueue_redis_key, lock.runtime_redis_key])
    Sidekiq::DeadSet.new.find_job(jid).delete
    expect(sorted_pair('dead', jid)).to be_nil
    expect(config.redis { |redis| redis.get(lock.enqueue_redis_key) }).to eq(second)
  end

  private

  def add_sorted_payload(set, retry_count:)
    enqueued = unique_job.perform_later("#{set}-identity")
    jid = enqueued.provider_job_id
    raw = config.redis { |redis| redis.rpop("queue:#{queue}") }
    payload = Sidekiq.load_json(raw)
    lock = SidekiqUniqueness.lock_for(job_payload: payload, physical_queue: queue)
    lock_keys.concat([lock.enqueue_redis_key, lock.runtime_redis_key])
    config.redis { |redis| lock.release_enqueue(redis: redis, owner: jid) }
    raw = Sidekiq.dump_json(payload.merge('retry_count' => retry_count))
    config.redis { |redis| redis.zadd(set, Time.now.to_f, raw) }
    jid
  end

  def sorted_pair(set, jid)
    config.redis do |redis|
      redis.zscan(set).find do |raw, _score|
        Sidekiq.load_json(raw)['jid'] == jid
      rescue JSON::ParserError, TypeError
        false
      end
    end
  end

  def ready_payloads
    config.redis do |redis|
      redis.lrange("queue:#{queue}", 0, -1).map { |raw| Sidekiq.load_json(raw) }
    end
  end

  def cleanup
    config.redis do |redis|
      redis.del("queue:#{queue}", *lock_keys)
      redis.srem('queues', queue) if redis.call('TYPE', 'queues') == 'set'
      redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue) if
        redis.call('TYPE', SidekiqRuntime::QueueKeys.directory_key) == 'set'
      %w[schedule retry dead].each do |set|
        redis.zscan(set).each do |raw, _score|
          redis.zrem(set, raw) if Sidekiq.load_json(raw)['queue'] == queue
        rescue JSON::ParserError, TypeError
          next
        end
      end
    end
  end
end
