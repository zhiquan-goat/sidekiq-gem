# frozen_string_literal: true

require 'spec_helper'

RSpec.describe SidekiqRuntime::Telemetry do
  let(:events) { [] }
  let(:payload) { {'class' => 'ObservedJob', 'jid' => 'observed-jid', 'queue' => 'observed'} }

  around do |example|
    previous = SidekiqRuntime.event_handler
    SidekiqRuntime.event_handler = ->(name, fields) { events << [name, fields] }
    example.run
  ensure
    SidekiqRuntime.event_handler = previous
  end

  it 'isolates event construction and subscriber failures in the single publication boundary' do
    expect { SidekiqRuntime.publish('example') { raise 'field construction failed' } }.not_to raise_error
    SidekiqRuntime.event_handler = ->(*) { raise 'subscriber failed' }
    expect { SidekiqRuntime.publish('example') { {value: 1} } }.not_to raise_error
    SidekiqRuntime.event_handler = nil
    expect { SidekiqRuntime.publish('example') { raise 'unused fields' } }.not_to raise_error
  end

  it 'executes the business block exactly once and preserves its result when fields fail' do
    allow(described_class).to receive(:fields).and_raise('unavailable telemetry')
    attempts = 0
    result = described_class.perform(payload, queue: 'observed') { attempts += 1; :completed }
    expect([result, attempts]).to eq([:completed, 1])
  end

  it 'preserves the original business exception when the observer also fails' do
    SidekiqRuntime.event_handler = ->(*) { raise 'observer error' }
    failure = ArgumentError.new('business failure')
    expect { described_class.perform(payload, queue: 'observed') { raise failure } }
      .to raise_error { |error| expect(error).to equal(failure) }
  end

  it 'does not prevent lock construction or mutation when its event fields fail' do
    unique = SidekiqUniqueness.snapshot(job_class: 'ObservedJob', queue: 'observed', identity: [1],
      enqueue_ttl: 30, runtime_ttl: 10, key_prefix: "spec:telemetry:#{SecureRandom.hex(6)}")
    allow(described_class).to receive(:fields).and_raise('unavailable telemetry')
    lock = SidekiqUniqueness.lock_for(job_payload: payload.merge('sidekiq_runtime' => {'uniqueness' => unique}), physical_queue: 'observed')
    redis = double('Redis')
    expect(redis).to receive(:call).once.and_return(1)
    expect(lock.reserve_enqueue(redis: redis, owner: 'observed-jid')).to be true
  end
end
