require 'spec_helper'

RSpec.describe SidekiqRuntime::ServerMiddleware do
  subject(:middleware) { described_class.new }

  let(:job_instance) { Object.new }
  let(:lock) do
    instance_double(
      SidekiqUniqueness::UntilAndWhileExecuting,
      begin_execution: 'runtime-owner',
      reserve_enqueue: true,
      finish_execution: 1
    )
  end
  let(:redis) { double('Redis connection') }
  let(:logger) { instance_double(Logger, warn: nil) }
  let(:config) { instance_double(Sidekiq::Config, logger: logger) }
  let(:job_payload) do
    {
      'class' => 'SpecServerSidekiqJob',
      'jid' => 'jid-123',
      'queue' => 'spec_queue',
      'args' => [{'value' => 1}]
    }
  end

  before do
    allow(SidekiqUniqueness).to receive(:lock_for).and_return(lock)
    allow(config).to receive(:redis).and_yield(redis)
    middleware.config = config
  end

  it 'releases runtime and propagates a business error without restoring enqueue' do
    expect(lock).not_to receive(:reserve_enqueue)
    expect(lock).to receive(:finish_execution).with(redis: redis, owner: 'runtime-owner')

    expect do
      middleware.call(job_instance, job_payload, 'spec_queue') { raise 'business failure' }
    end.to raise_error(RuntimeError, 'business failure')
  end

  it 'restores enqueue and releases runtime on Sidekiq shutdown' do
    expect(lock).to receive(:reserve_enqueue).with(redis: redis, owner: 'jid-123')
    expect(lock).to receive(:finish_execution).with(redis: redis, owner: 'runtime-owner')

    expect do
      middleware.call(job_instance, job_payload, 'spec_queue') { raise Sidekiq::Shutdown }
    end.to raise_error(Sidekiq::Shutdown)
  end
end
