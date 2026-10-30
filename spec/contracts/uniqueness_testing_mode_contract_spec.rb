# frozen_string_literal: true

require 'spec_helper'
require 'goat_sidekiq_runtime/active_job'
require 'logger'

RSpec.describe 'Uniqueness in Sidekiq testing modes' do
  let(:config) do
    Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
      value.logger = Logger.new(File::NULL)
    end
  end
  let(:pool) { SidekiqRuntime::RedisScope.pool(config) }

  before(:context) do
    require 'sidekiq/testing'
    # Loading the testing helper enables fake mode; other contracts use real Redis.
    Sidekiq::Testing.disable!
  end

  before do
    stub_const('SpecTestingActiveJobUniqueJob', Class.new(ActiveJob::Base) do
      self.logger = Logger.new(File::NULL)
      runtime_uniqueness enqueue_ttl: 60, runtime_ttl: 60, key: ->(job) { job.arguments.first }

      def perform(_id); end
    end)
    SpecTestingActiveJobUniqueJob.queue_adapter = ActiveJob::QueueAdapters::SidekiqRuntimeAdapter.new(config: config)
  end

  %i[fake inline].each do |mode|
    it "rejects Active Job unique enqueue in #{mode} mode before accessing Redis" do
      expect(pool).not_to receive(:with)

      Sidekiq::Testing.public_send("#{mode}!") do
        expect do
          SpecTestingActiveJobUniqueJob.perform_later(42)
        end.to raise_error(ArgumentError, /Sidekiq::Testing\.disable!.*dedicated Redis/)
      end
    end
  end
end
