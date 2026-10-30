require 'spec_helper'
require 'sidekiq/transaction_aware_client'

RSpec.describe SidekiqRuntime::ClientPushBoundary do
  let(:base_client_class) do
    Class.new do
      attr_writer :pre_push_error, :push_result

      def push(item)
        raise @pre_push_error if @pre_push_error
        return unless @push_result

        raw_push([item])
      end

      private

      def raw_push(payloads)
        return @push_result.call(payloads) if @push_result.respond_to?(:call)

        @push_result
      end
    end
  end
  let(:client_class) do
    Class.new(base_client_class).tap { |klass| klass.prepend(described_class) }
  end
  let(:client) { client_class.new }
  let(:business_payload) do
    build_sidekiq_runtime_business_payload
  end
  let(:job_item) do
    build_sidekiq_runtime_job_payload(
      jid: 'jid-123',
      business_payload: business_payload
    )
  end

  it 'records an initial scheduled push only after the underlying write succeeds' do
    push_completed = false
    client.push_result = lambda do |_payloads|
      push_completed = true
      'jid-123'
    end

    events, result = capture_push_events do
      client.push(job_item.merge('at' => Time.now.to_f + 61))
    end

    expect(push_completed).to be true
    expect(result).to eq('jid-123')
    expect(events.one?).to be true
    expect(events.first.payload).to include(
      status: 'success',
      delivery_outcome: 'redis_acknowledged',
      jid: 'jid-123',
      physical_queue: SidekiqRuntimeSpecSupport::PRIMARY_QUEUE,
      job_class: SidekiqRuntimeSpecSupport::JOB_CLASS_NAME
    )
  end

  it 'records a middleware-filtered push without claiming Redis persistence' do
    client.push_result = nil

    events, result = capture_push_events do
      client.push(job_item)
    end

    expect(result).to be_nil
    expect(events.first.payload).to include(
      status: 'filtered',
      delivery_outcome: 'not_pushed'
    )
  end

  it 'records an error with an unknown delivery outcome and preserves the exception' do
    exception = RedisClient::ConnectionError.new('connection closed after write')
    client.push_result = ->(_payloads) { raise exception }

    events = []
    expect do
      capture_push_events(events: events) do
        client.push(job_item)
      end
    end.to raise_error(RedisClient::ConnectionError, 'connection closed after write')

    expect(events.first.payload).to include(
      status: 'error',
      delivery_outcome: 'unknown',
      exception: ['RedisClient::ConnectionError', 'connection closed after write'],
      exception_object: exception
    )
  end

  it 'does not report a pre-push error as an ambiguous Redis delivery' do
    client.pre_push_error = ArgumentError.new('strict arguments rejected')
    events = []

    expect do
      capture_push_events(events: events) do
        client.push(job_item)
      end
    end.to raise_error(ArgumentError, 'strict arguments rejected')

    expect(events).to be_empty
  end

  it 'does not turn a successful Redis push into producer failure when telemetry raises' do
    client.push_result = 'jid-123'
    allow(SidekiqRuntime::Telemetry).to receive(:fields).and_raise('telemetry unavailable')

    expect(client.push(job_item)).to eq('jid-123')
  end

  it 'does not observe a job moving from the RetrySet back to the queue' do
    client.push_result = 'retry-jid'

    events, result = capture_push_events do
      client.push(job_item.merge('retry_count' => 0))
    end

    expect(result).to eq('retry-jid')
    expect(events).to be_empty
  end

  it 'does not record success when the transactional wrapper only registers its callback' do
    raw_push_completed = false
    chain = Sidekiq::Middleware::Chain.new(Sidekiq.default_configuration)
    redis = double('Redis connection', sadd: 1, lpush: 1)
    redis_pool = double('Redis pool')
    allow(redis_pool).to receive(:with).and_yield(redis)
    allow(redis).to receive(:pipelined) do |&block|
      block.call(redis)
      raw_push_completed = true
      true
    end
    underlying_client = Sidekiq::Client.new(pool: redis_pool, chain: chain)
    callbacks = []
    transactional_client = Sidekiq::TransactionAwareClient.allocate
    transactional_client.instance_variable_set(:@redis_client, underlying_client)
    transactional_client.instance_variable_set(
      :@transaction_backend,
      ->(&callback) { callbacks << callback }
    )

    events = []
    returned_jid = nil
    capture_push_events(events: events) do
      returned_jid = transactional_client.push(job_item.dup)
      expect(raw_push_completed).to be false
      expect(events).to be_empty

      callbacks.fetch(0).call
    end

    expect(returned_jid).to eq('jid-123')
    expect(raw_push_completed).to be true
    expect(events.map { |event| event.payload[:status] }).to eq(['success'])
  end

  def capture_push_events(events: [])
    subscriber = ActiveSupport::Notifications.subscribe(described_class::EVENT_NAME) do |*args|
      events << ActiveSupport::Notifications::Event.new(*args)
    end
    result = yield
    [events, result]
  ensure
    ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
  end
end
