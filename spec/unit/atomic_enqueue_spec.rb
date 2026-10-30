require 'spec_helper'

RSpec.describe SidekiqUniqueness::AtomicEnqueue do
  let(:redis_pool) { double('Redis pool') }
  let(:redis) { double('Redis connection') }
  let(:writer) { described_class.new(redis_pool, clock: -> { 1_789_000_000.25 }) }
  let(:lock) do
    SidekiqUniqueness::UntilAndWhileExecuting.new(
      key_prefix: 'sneakers:sidekiq:unique:SpecAtomicJob:sneakers_spec_atomic',
      identity: [123],
      enqueue_ttl: 600,
      runtime_ttl: 45,
    )
  end
  let(:payload) do
    {
      'class' => 'SpecAtomicJob',
      'args' => [123],
      'queue' => 'sneakers_spec_atomic',
      'jid' => 'atomic-jid',
      'retry' => 1,
      'dead' => true,
      'created_at' => 1_788_000_000.5
    }
  end

  it 'loads a missing script once without mutating the input' do
    original = payload.deep_dup
    calls = []
    allow(redis_pool).to receive(:with).once.and_yield(redis)
    allow(redis).to receive(:call_once) do |command, *_arguments|
      calls << command
      raise RedisClient::CommandError, 'NOSCRIPT No matching script' if command == 'EVALSHA'
      'accepted:ready'
    end
    expect(writer.write(payload, lock: lock)).to eq(:accepted)
    expect(calls).to eq(%w[EVALSHA EVAL])
    expect(payload).to eq(original)
  end

  it 'does not retransmit a non-NOSCRIPT Redis failure' do
    exception = RedisClient::CommandError.new('WRONGTYPE injected')
    allow(redis_pool).to receive(:with).once.and_yield(redis)
    expect(redis).to receive(:call_once).once.and_raise(exception)

    expect { writer.write(payload, lock: lock) }
      .to raise_error(described_class::Unknown) { |error| expect(error.cause).to equal(exception) }
  end

  it 'does not replay an EVAL whose reply is lost' do
    connection_error = RedisClient::ConnectionError.new('reply lost after write')
    calls = []
    allow(redis_pool).to receive(:with).once.and_yield(redis)
    allow(redis).to receive(:call_once) do |command, *_arguments|
      calls << command
      raise RedisClient::CommandError, 'NOSCRIPT cache miss' if command == 'EVALSHA'
      raise connection_error
    end

    expect { writer.write(payload, lock: lock) }
      .to raise_error(described_class::Unknown) { |error| expect(error.cause).to equal(connection_error) }
    expect(calls).to eq(%w[EVALSHA EVAL])
  end

  it 'rejects invalid payloads without checking out Redis' do
    expect(redis_pool).not_to receive(:with)
    invalid = {
      payload.merge('queue' => '') => :invalid_queue,
      payload.merge('args' => {}) => :invalid_args,
      payload.merge('retry_queue' => 'elsewhere') => :retry_queue_not_supported,
      payload.merge('at' => Float::INFINITY) => :invalid_scheduled_at
    }

    aggregate_failures do
      invalid.each do |candidate, code|
        expect { writer.write(candidate, lock: lock) }
          .to raise_error(described_class::Rejected) { |error| expect(error.code).to eq(code) }
      end
    end
  end
end
