require 'spec_helper'

RSpec.describe SidekiqUniqueness::DeathHandler do
  let(:job_class) { Class.new }
  let(:lock) { instance_double(SidekiqUniqueness::UntilAndWhileExecuting, release_enqueue: 1) }
  let(:redis) { double('Redis connection') }
  let(:logger) { instance_double(Logger, warn: nil) }
  let(:config) { instance_double(Sidekiq::Config, logger: logger) }
  let(:job_payload) do
    {
      'class' => 'SpecDeathSidekiqJob',
      'jid' => 'jid-dead',
      'queue' => 'spec_queue',
      'args' => [{'value' => 1}]
    }
  end

  before do
    stub_const('SpecDeathSidekiqJob', job_class)
    allow(SidekiqUniqueness).to receive(:lock_for).and_return(lock)
    allow(config).to receive(:redis).and_yield(redis)
  end

  it 'releases the dead Job enqueue reservation through the server config' do
    expect(lock).to receive(:release_enqueue).with(redis: redis, owner: 'jid-dead')

    described_class.new(config).call(job_payload, StandardError.new('dead'))
  end

  it 'does not mask Sidekiq death processing when cleanup fails' do
    allow(lock).to receive(:release_enqueue).and_raise('Redis unavailable')
    expect(logger).to receive(:warn).with(/Redis unavailable/)

    expect(described_class.new(config).call(job_payload, StandardError.new('dead'))).to eq(0)
  end

  it 'ignores a dead Job without a uniqueness policy' do
    stub_const('OrdinaryDeadSidekiqJob', Class.new)
    job_payload['class'] = 'OrdinaryDeadSidekiqJob'
    allow(SidekiqUniqueness).to receive(:lock_for).and_return(nil)

    expect(config).not_to receive(:redis)
    expect(described_class.new(config).call(job_payload, StandardError.new('dead'))).to be_nil
  end
end
