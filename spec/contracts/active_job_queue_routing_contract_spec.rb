# frozen_string_literal: true

require_relative '../support/active_job_runtime'

RSpec.describe 'Active Job physical retry queue' do
  include ActiveJobRuntimeSupport

  let(:logical_queue) { "#{queue}_logical" }
  let(:other_queue) { "#{queue}_other" }

  let(:config) do
    Sidekiq::Config.new(error_handlers: [->(*) {}], death_handlers: []).tap do |value|
      value.logger = Logger.new(File::NULL)
      SidekiqRuntime.install_server!(value,
        redis_config: {url: ENV.fetch('SIDEKIQ_REDIS_URL')}, storage: SidekiqRuntime::QueueKeys.storage)
      value.default_capsule.queues = [queue]
    end
  end

  after do
    config.redis do |redis|
      [logical_queue, other_queue].each do |name|
        redis.del("queue:#{name}")
        redis.srem('queues', name)
        redis.srem(SidekiqRuntime::QueueKeys.directory_key, name)
      end
      %w[schedule retry dead].each do |key|
        redis.zrange(key, 0, -1).each do |raw|
          redis.zrem(key, raw) if [logical_queue, other_queue].include?(Sidekiq.load_json(raw)['queue'])
        end
      end
    end
  end

  %i[retry_on retry_job retry_job_with_logical_change].each do |retry_method|
    it "allows #{retry_method} when middleware keeps the consumed physical queue" do
      route_retries_to(queue)
      klass = build_job
      klass.queue_name = logical_queue
      klass.retry_on(RuntimeError, wait: 10, attempts: 2, jitter: 0) if retry_method == :retry_on
      retry_options = {wait: 10}
      retry_options[:queue] = other_queue if retry_method == :retry_job_with_logical_change
      klass.action = lambda do |*|
        if executions == 1
          raise 'try again' if retry_method == :retry_on
          retry_job(**retry_options)
        end
      end
      original = klass.perform_later(42)
      expect(persisted('ready').first.dig('args', 0, 'queue_name')).to eq(logical_queue)

      process_next

      successor = persisted('schedule').fetch(0)
      expect(successor['queue']).to eq(queue)
      expect(successor['jid']).not_to eq(original.provider_job_id)
      expect(successor.dig('args', 0, 'job_id')).to eq(original.job_id)
      expect(successor.dig('args', 0, 'queue_name')).to eq(retry_options.fetch(:queue, logical_queue))
      expect(store.size).to eq(0)
      expect(persisted('dead')).to be_empty
      promote
      process_next
      expect(klass.records.size).to eq(2)
      expect(klass.records.map { |record| record[:context].redelivered? }).to eq([false, true])
      expect(outcomes).to eq([:retry_scheduled, :succeeded])
      finished = @events.select { |name, _| name == 'sidekiq.notification.active_job_finished' }
      expect(finished.map { |_, fields| fields[:physical_queue] }).to eq([queue, queue])
    end
  end

  %i[other_queue logical_queue].each do |destination|
    it "rejects a retry routed to #{destination} instead of the consumed queue" do
      route_retries_to(public_send(destination))
      klass = build_job
      klass.queue_name = logical_queue
      klass.action = ->(*) { retry_job(wait: 10) }
      klass.perform_later(42)

      expect { process_next }.to raise_error(ArgumentError, 'Retry must remain on its original physical queue')

      scheduled = config.redis { |redis| redis.zrange('schedule', 0, -1).map { |raw| Sidekiq.load_json(raw) } }
      expect(scheduled.select { |payload| payload['wrapped'] == klass.name }).to be_empty
      expect(persisted('dead').first['error_class']).to eq('ArgumentError')
      expect(store.size).to eq(0)
    end
  end

  def route_retries_to(destination)
    consumed_queue = queue
    routing = Class.new do
      define_method(:call) do |_job_class, payload, _queue, _pool, &block|
        block.call.tap do |result|
          result['queue'] = result.dig('args', 0, 'executions').to_i.zero? ? consumed_queue : destination
        end
      end
    end
    config.client_middleware { |chain| chain.add(routing) }
  end
end
