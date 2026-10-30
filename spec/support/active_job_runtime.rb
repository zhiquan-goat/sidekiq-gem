# frozen_string_literal: true

require 'spec_helper'
require 'goat_sidekiq_runtime/active_job'
require 'active_job/enqueue_after_transaction_commit'
require 'sidekiq/processor'
require 'sidekiq/transaction_aware_client'
require 'logger'

module ActiveJobRuntimeSupport
  def self.included(group)
    group.let(:queue) { "sneakers_spec_aj_#{SecureRandom.hex(6)}" }
    group.let(:config) do
      Sidekiq::Config.new(error_handlers: [->(*) {}], death_handlers: []).tap do |value|
        value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
        value.logger = Logger.new(File::NULL)
        SidekiqRuntime.install_producer!(storage: SidekiqRuntime::QueueKeys.storage)
        value.default_capsule.queues = [queue]
      end
    end
    group.let(:adapter) { ActiveJob::QueueAdapters::SidekiqRuntimeAdapter.new(config: config) }
    group.let(:store) do
      SidekiqRuntime::InflightStore.new(config,
        queue: queue, owner_identity: 'aj-spec-owner')
    end
    group.let(:processor) { Sidekiq::Processor.new(config.default_capsule) {} }
    group.before do
      register_runtime_owner(config, 'aj-spec-owner')
      @events = []
      @old_handler = SidekiqRuntime.event_handler
      SidekiqRuntime.event_handler = ->(event, payload) { @events << [event, payload] }
    end
    group.after do
      SidekiqRuntime.event_handler = @old_handler
      config.redis do |redis|
        redis.del("queue:#{queue}", store.inflight_key, store.owners_key)
        redis.srem('queues', queue)
        redis.srem(SidekiqRuntime::QueueKeys.directory_key, queue)
        %w[retry schedule dead].each do |key|
          redis.zscan(key).each do |member, _score|
            redis.zrem(key, member) if Sidekiq.load_json(member)['queue'] == queue
          end
        end
      end
    end
  end

  def build_job(name = 'SpecRuntimeAJJob', &declarations)
    klass = Class.new(ActiveJob::Base) do
      class_attribute :records, :action
      self.records = []
      self.logger = Logger.new(File::NULL)

      def perform(*args)
        self.class.records << {args: args, job_id: job_id, jid: provider_job_id,
                               executions: executions, context: execution_context}
        instance_exec(*args, &self.class.action) if self.class.action
      end
    end
    stub_const(name, klass)
    klass.queue_adapter = adapter
    klass.queue_name = queue
    klass.class_eval(&declarations) if declarations
    klass
  end

  def persisted(key)
    config.redis do |redis|
      raws = key == 'ready' ? redis.lrange("queue:#{queue}", 0, -1) : redis.zrange(key, 0, -1)
      raws.map { |raw| Sidekiq.load_json(raw) }.select { |job| job['queue'] == queue }
    end
  end

  def process_next
    @last_claim = store.claim
    raise 'No ready job' unless @last_claim

    work = SidekiqRuntime::ReliableFetch::UnitOfWork.new(queue: "queue:#{queue}", job: @last_claim.job,
      config: config, store: store, claim: @last_claim)
    processor.send(:process, work)
  end

  def promote(now: Time.now.to_f + 60)
    SidekiqRuntime::AtomicScheduledEnqueuer.new(config, clock: -> { now }).enqueue_jobs(['schedule'])
  end

  def outcomes
    @events.select { |event, _| event == 'sidekiq.notification.active_job_finished' }.map { |_, fields| fields[:outcome] }
  end

  def filter_retries(value = nil)
    klass = Class.new do
      define_method(:call) do |_job, payload, _queue, _pool, &block|
        payload.dig('args', 0, 'executions').to_i.positive? ? value : block.call
      end
    end
    config.client_middleware { |chain| chain.add(klass) }
  end

  # Framework contract probe only: use the real AJ deferral module and real
  # Sidekiq TransactionAwareClient, substituting AR's transaction callback API.
  # No Rails boot / database transaction is claimed by these tests.
  def transaction_probe(job_class, open: true)
    backend = Module.new do
      class << self
        attr_accessor :pending, :open, :registrations

        def version
          Gem::Version.new('7.2.3.1')
        end

        def after_all_transactions_commit(&block)
          self.registrations += 1
          open ? pending << block : block.call
        end

        def commit
          self.open = false
          callbacks = pending.dup
          pending.clear
          callbacks.each(&:call)
        end

        def rollback
          self.open = false
          pending.clear
        end
      end
    end
    stub_const('ActiveRecord', backend)
    backend.pending = []
    backend.registrations = 0
    backend.open = open
    job_class.include(ActiveJob::EnqueueAfterTransactionCommit)
    job_class.enqueue_after_transaction_commit = :default
    backend
  end

  def with_transactional_push
    options = Sidekiq.default_job_options.dup
    transient = Sidekiq::JobUtil::TRANSIENT_ATTRIBUTES.dup
    Sidekiq.transactional_push!
    yield
  ensure
    Sidekiq.default_job_options.replace(options)
    Sidekiq::JobUtil::TRANSIENT_ATTRIBUTES.replace(transient)
  end

  def lose_client_reply
    SidekiqRuntime::RedisScope.pool(config).with do |redis|
      allow(redis).to receive(:pipelined).and_wrap_original do |method, *args, &block|
        method.call(*args, &block)
        raise RedisClient::ReadTimeoutError, 'simulated reply loss after real Redis write'
      end
    end
  end
end
