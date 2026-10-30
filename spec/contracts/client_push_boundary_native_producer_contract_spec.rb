require 'spec_helper'

RSpec.describe SidekiqRuntime::ClientPushBoundary, 'native producer contract' do
  let(:queue) { "sneakers_spec_boundary_#{SecureRandom.hex(6)}" }
  let(:ready_key) { SidekiqRuntime::QueueKeys.source_queue(queue) }
  let(:config) do
    Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')}
    end
  end
  let(:client) { Sidekiq::Client.new(config: config) }
  let(:ordinary_job) do
    Class.new do
      include Sidekiq::Job

      def perform(_id)
      end
    end
  end

  before do
    stub_const('SpecBoundaryOrdinaryJob', ordinary_job)
    ordinary_job.sidekiq_options('queue' => queue, 'retry' => 1, 'dead' => true)
    cleanup
  end

  after { cleanup }

  it 'supports scalar and array scheduled bulk timestamps with native wire format' do
    scalar_at = Time.now.to_f + 60
    scalar = client.push_bulk(
      'class' => ordinary_job, 'queue' => queue, 'args' => [[40], [41]], 'at' => scalar_at
    )
    array_at = [Time.now.to_f + 90, Time.now.to_f + 120]
    array = client.push_bulk(
      'class' => ordinary_job, 'queue' => queue, 'args' => [[42], [43]], 'at' => array_at
    )

    expect(scalar + array).to all(be_present)
    scheduled = scheduled_payloads
    expect(scheduled.map(&:last)).to contain_exactly(scalar_at, scalar_at, *array_at)
    expect(scheduled.map(&:first)).to all(satisfy { |payload| !payload.key?('at') && !payload.key?('enqueued_at') })
  end

  it 'preserves native bulk validation and explicit JID rules' do
    expect do
      client.push_bulk('class' => ordinary_job, 'queue' => queue, 'args' => [[1]], 'at' => 'later')
    end.to raise_error(ArgumentError, /must be a Numeric/)
    expect do
      client.push_bulk('class' => ordinary_job, 'queue' => queue, 'args' => [[1], [2]], 'at' => [1])
    end.to raise_error(ArgumentError, /same size/)
    expect do
      client.push_bulk('class' => ordinary_job, 'queue' => queue, 'args' => [[1], [2]], 'jid' => 'forced')
    end.to raise_error(ArgumentError, /more than one job/)

    single = client.push_bulk(
      'class' => ordinary_job, 'queue' => queue, 'args' => [[50]], 'jid' => 'requested-but-regenerated'
    )
    expect(single.one?).to be(true)
    expect(single.first).not_to eq('requested-but-regenerated')
  end

  it 'keeps ordinary bulk on Sidekiq raw_push with its native result shape' do
    expect(SidekiqUniqueness::AtomicEnqueue).not_to receive(:new)
    expect(client).to receive(:raw_push).with(an_instance_of(Array)).twice.and_call_original
    results = client.push_bulk(
      'class' => ordinary_job,
      'queue' => queue,
      'args' => [[1], [2], [3]],
      'batch_size' => 2
    )

    expect(results.size).to eq(3)
    expect(results).to all(be_present)
    expect(ready_payloads.map { |payload| payload['class'] }).to all(eq('SpecBoundaryOrdinaryJob'))
  end

  it 'rejects retry_queue on single and bulk retry or continuation writes' do
    [ordinary_job].each do |klass|
      [{'retry_count' => 0}, SidekiqUniqueness.mark_runtime_conflict_continuation({})].each do |markers|
        item = {'class' => klass, 'queue' => queue, 'args' => [9], 'retry_queue' => 'elsewhere'}.merge(markers)
        expect { client.push(item) }.to raise_error(ArgumentError, /retry_queue/)
        expect { client.push_bulk(item.merge('args' => [[9], [10]])) }
          .to raise_error(ArgumentError, /retry_queue|Unique push_bulk/)
      end
    end
    expect(ready_payloads).to be_empty
    expect(scheduled_payloads).to be_empty
  end

  it 'rejects retry routing added after downstream middleware has returned' do
    mutator = Class.new do
      include Sidekiq::ClientMiddleware

      def call(*)
        yield.tap { |payload| payload['retry_queue'] = 'elsewhere' }
      end
    end
    config.client_middleware { |chain| chain.prepend(mutator) }
    expect { client.push('class' => ordinary_job, 'queue' => queue, 'args' => [9]) }
      .to raise_error(ArgumentError, /retry_queue/)
    expect { client.push_bulk('class' => ordinary_job, 'queue' => queue, 'args' => [[9], [10]]) }
      .to raise_error(ArgumentError, /retry_queue/)
    expect { client.push(ordinary_item(9)) }
      .to raise_error(ArgumentError, /retry_queue/)
    expect(ready_payloads).to be_empty
  end

  def ordinary_item(value, jid: SecureRandom.hex(12))
    {'class' => ordinary_job, 'args' => [value], 'queue' => queue, 'jid' => jid}
  end

  def ready_payloads
    config.redis { |redis| redis.lrange(ready_key, 0, -1).map { |raw| Sidekiq.load_json(raw) } }
  end

  def scheduled_payloads
    config.redis do |redis|
      redis.zscan('schedule').filter_map do |raw, score|
        payload = Sidekiq.load_json(raw)
        [payload, score.to_f] if payload['queue'] == queue
      end
    end
  end

  def cleanup
    config.redis do |redis|
      redis.del(ready_key)
      redis.zscan('schedule').each do |raw, _score|
        redis.zrem('schedule', raw) if Sidekiq.load_json(raw)['queue'] == queue
      end
      redis.srem('queues', queue) if redis.call('TYPE', 'queues') == 'set'
      directory = SidekiqRuntime::QueueKeys.directory_key
      redis.srem(directory, queue) if redis.call('TYPE', directory) == 'set'

    end
  end
end
