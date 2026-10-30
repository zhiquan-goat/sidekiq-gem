# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/processor'
require 'net/http'

RSpec.describe 'Native Sidekiq base retry contract' do # rubocop:disable RSpec/DescribeClass
  let(:queue) { "sneakers_spec_native_retry_#{SecureRandom.hex(5)}" }
  let(:config) do
    Sidekiq::Config.new(error_handlers: [->(*) {}], death_handlers: []).tap do |value|
      SidekiqRuntime.install_server!(
        value,
        redis_config: {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
      )
      value.default_capsule.queues = [queue]
    end
  end
  let(:fetcher) { SidekiqRuntime::ReliableFetch.new(config.default_capsule) }
  let(:processor) { Sidekiq::Processor.new(config.default_capsule) {} }

  before do
    physical_queue = queue
    stub_const('SpecNativeRetryJob', Class.new do
      include Sidekiq::Job

      sidekiq_options queue: physical_queue, retry: 1, dead: true

      class << self
        attr_accessor :attempts
      end
      self.attempts = Hash.new(0)

      def perform(mode)
        self.class.attempts[mode] += 1
        attempt = self.class.attempts.fetch(mode)
        case mode
        when 'temporary_once'
          raise Net::ReadTimeout, 'temporary' if attempt == 1
        when 'temporary_always'
          raise Net::ReadTimeout, 'temporary'
        end
      end
    end)
    stub_const('SpecNativeNoRetryJob', Class.new(SpecNativeRetryJob) do
      sidekiq_options queue: physical_queue, retry: 0, dead: true
      self.attempts = Hash.new(0)
    end)
    register_runtime_owner(config)
    cleanup
  end

  after { cleanup }

  it 'persists one temporary Retry before ACK, promotes it, then succeeds and ACKs' do
    jid = enqueue(SpecNativeRetryJob, 'temporary_once')
    expect { process_next }.to raise_error(Net::ReadTimeout)
    retry_payload = persisted('retry').fetch(0)
    expect(retry_payload).to include('jid' => jid, 'retry_count' => 0, 'retry' => 1)
    expect(inflight_size).to eq(0)

    promote_retries
    expect { process_next }.not_to raise_error

    expect(SpecNativeRetryJob.attempts['temporary_once']).to eq(2)
    expect(persisted('retry')).to be_empty
    expect(persisted('dead')).to be_empty
    expect(inflight_size).to eq(0)
  end

  it 'moves a second temporary failure to Dead because the retry budget is one' do
    jid = enqueue(SpecNativeRetryJob, 'temporary_always')
    expect { process_next }.to raise_error(Net::ReadTimeout)
    promote_retries
    expect { process_next }.to raise_error(Net::ReadTimeout)

    expect(persisted('retry')).to be_empty
    expect(persisted('dead').fetch(0)).to include('jid' => jid, 'retry_count' => 1, 'retry' => 1)
    expect(inflight_size).to eq(0)
  end

  it 'retains Dead for an explicit retry-zero subclass' do
    jid = enqueue(SpecNativeNoRetryJob, 'temporary_always')
    expect { process_next }.to raise_error(Net::ReadTimeout)

    expect(persisted('retry')).to be_empty
    expect(persisted('dead').fetch(0)).to include('jid' => jid, 'retry' => 0)
    expect(inflight_size).to eq(0)
  end

  private

  def enqueue(job_class, mode)
    Sidekiq::Client.via(config.redis_pool) { job_class.perform_async(mode) }
  end

  def process_next
    work = fetcher.retrieve_work
    raise 'expected Ready work' unless work

    Sidekiq::Client.via(config.redis_pool) { processor.send(:process, work) }
  ensure
    processor.instance_variable_set(:@job, nil)
  end

  def promote_retries
    described_class = SidekiqRuntime::AtomicScheduledEnqueuer
    described_class.new(config, clock: -> { Time.now.to_f + 60 }).enqueue_jobs(['retry'])
  end

  def persisted(key)
    config.redis do |redis|
      redis.zrange(key, 0, -1).filter_map do |raw|
        payload = Sidekiq.load_json(raw)
        payload if payload['queue'] == queue
      end
    end
  end

  def inflight_size
    fetcher.stores_by_queue.fetch(queue).size
  end

  def cleanup
    config.redis do |redis|
      store = fetcher.stores_by_queue.fetch(queue)
      redis.del("queue:#{queue}", store.inflight_key, store.owners_key)
      redis.srem('queues', queue) if redis.call('TYPE', 'queues') == 'set'
      redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue) if
        redis.call('TYPE', SidekiqRuntime::QueueKeys.directory_key) == 'set'
      %w[retry schedule dead].each do |key|
        redis.zscan(key).each do |raw, _score|
          redis.zrem(key, raw) if Sidekiq.load_json(raw)['queue'] == queue
        end
      end
    end
  end
end
