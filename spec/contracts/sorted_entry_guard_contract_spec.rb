# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/api'

RSpec.describe SidekiqRuntime::SortedEntryGuard do
  let(:config) do
    Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
  end
  let(:queue) { "sorted_entry_guard_#{SecureRandom.hex(6)}" }
  let(:ordinary_job) do
    Class.new do
      include Sidekiq::Job

      def perform(_identity)
      end
    end
  end

  around do |example|
    SidekiqRuntime::RedisScope.with(config) { example.run }
  end

  before do
    stub_const('SpecSortedEntryOrdinaryJob', ordinary_job)
    ordinary_job.sidekiq_options('queue' => queue, 'retry' => 2, 'dead' => true)
    cleanup
  end

  after { cleanup }

  %i[add_to_queue retry].each do |operation|
    it "keeps ordinary Scheduled ##{operation} behavior" do
      jid = Sidekiq::Client.via(config.redis_pool) do
        ordinary_job.perform_in(3_600, "#{operation}-ordinary")
      end

      expect(Sidekiq::ScheduledSet.new.find_job(jid).public_send(operation)).to eq(jid)
      expect(sorted_pair('schedule', jid)).to be_nil
      expect(ready_payloads.map { |payload| payload['jid'] }).to eq([jid])
    end
  end

  private

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
      redis.del("queue:#{queue}")
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
