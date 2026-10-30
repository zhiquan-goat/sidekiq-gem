require 'spec_helper'

RSpec.describe SidekiqRuntime::ReliableFetch do
  let(:handler) { spy('event handler') }
  before do
    @previous_handler = SidekiqRuntime.event_handler
    SidekiqRuntime.event_handler = handler
  end
  after { SidekiqRuntime.event_handler = @previous_handler }

  let(:queue) { SidekiqRuntimeSpecSupport::PRIMARY_QUEUE }
  let(:logger) { instance_double(Logger, info: nil, warn: nil) }
  let(:capsule) { instance_double(Sidekiq::Capsule, mode: :strict, queues: [queue], logger: logger) }
  let(:store) { instance_double(SidekiqRuntime::InflightStore) }
  let(:store_class) { class_double(SidekiqRuntime::InflightStore) }
  let(:raw_job) { Sidekiq.dump_json(build_sidekiq_runtime_job_payload) }
  let(:claim) do
    SidekiqRuntime::InflightStore::Claim.new(source_queue: "queue:#{queue}", job: raw_job,
      member: "claim-id\n#{raw_job}", claimed_at: Time.now.to_f, owner_identity: 'owner')
  end
  let(:fetch) { described_class.new(capsule, store_class: store_class, sleeper: ->(_seconds) {}) }

  before do
    stub_sidekiq_runtime_job
    allow(store_class).to receive(:new).with(capsule,
      queue: queue,
      owner_identity: an_instance_of(String)).and_return(store)
    allow(store).to receive(:claim).and_return(claim)
  end

  it 'retains unsupported low-level retry routing before Processor sees it' do
    routed = Sidekiq.load_json(raw_job).merge('retry_queue' => 'elsewhere')
    claim.job = Sidekiq.dump_json(routed)
    ticks = [0.0, 0.0, 2.0].each
    blocked_fetch = described_class.new(capsule, store_class: store_class,
      monotonic_clock: -> { ticks.next })
    expect(store).not_to receive(:acknowledge)
    expect(store).not_to receive(:requeue)
    allow(handler).to receive(:call)
    expect(handler).to receive(:call).with(described_class::EVENT_NAME,
      hash_including(operation: 'claim_blocked', outcome: 'unsupported_retry_queue'))
    expect(blocked_fetch.retrieve_work).to be_nil
  end

  it 'measures Ready-to-claim latency on the existing claimed event' do
    claim.job = Sidekiq.dump_json(Sidekiq.load_json(raw_job).merge('enqueued_at' => claim.claimed_at - 1.5))
    expect(handler).to receive(:call).with(described_class::EVENT_NAME,
      hash_including(operation: 'claimed', pickup_latency_ms: 1_500, empty_claims: 0,
        claim_duration_ms: be >= 0))
    expect(fetch.retrieve_work.job).to eq(claim.job)
  end

  it 'does not move a claim when payload preparation fails' do
    allow(SidekiqRuntime::Recovery).to receive(:prepare).and_raise(ArgumentError, 'invalid recovery payload')
    work = fetch.retrieve_work
    expect(store).not_to receive(:requeue)
    expect { work.requeue }.to raise_error(ArgumentError, 'invalid recovery payload')
  end

  it 'records an unknown ACK outcome without retrying the store call' do
    work = fetch.retrieve_work
    expect(store).to receive(:acknowledge).once.and_raise(RedisClient::ConnectionError, 'lost reply')
    expect(handler).to receive(:call).with(described_class::EVENT_NAME,
      hash_including(operation: 'acknowledged', outcome: 'unknown'))
    expect { work.acknowledge }.to raise_error(RedisClient::ConnectionError)
  end

  it 'reports an ambiguous Redis requeue' do
    work = fetch.retrieve_work
    allow(store).to receive(:requeue).and_raise(RedisClient::ProtocolError, 'lost reply')
    expect(handler).to receive(:call).with(described_class::EVENT_NAME,
      hash_including(operation: 'shutdown_requeued', outcome: 'unknown'))
    expect { work.requeue }.to raise_error(RedisClient::ProtocolError)
  end

  it 'keeps ACK successful if timing is unavailable' do
    ticks = [10_000.0].each
    timed = described_class.new(capsule, store_class: store_class, executor_clock: -> { ticks.next })
    work = timed.retrieve_work
    allow(store).to receive(:acknowledge).and_return(1)
    expect(work.acknowledge).to eq(1)
  end

  it 'rejects empty, unsupported and invalid consumption topology' do
    allow(capsule).to receive(:queues).and_return([])
    expect { fetch }.to raise_error(ArgumentError)
    allow(capsule).to receive(:queues).and_return([queue])
    allow(capsule).to receive(:mode).and_return(:unsupported)
    expect { fetch }.to raise_error(ArgumentError, /does not support/)
    allow(capsule).to receive(:mode).and_return(:strict)
    allow(capsule).to receive(:queues).and_return([''])
    expect { fetch }.to raise_error(ArgumentError, /queue/)
  end

  it 'checks configured queues in strict order without blocking on an empty priority queue' do
    priority = SidekiqRuntimeSpecSupport::PRIORITY_QUEUE
    allow(capsule).to receive(:queues).and_return([priority, queue])
    priority_store = instance_double(SidekiqRuntime::InflightStore)
    allow(store_class).to receive(:new).with(capsule,
      queue: priority,
      owner_identity: an_instance_of(String)).and_return(priority_store)
    expect(priority_store).to receive(:claim).ordered.and_return(nil)
    expect(store).to receive(:claim).ordered.and_return(claim)
    expect(fetch.retrieve_work.queue_name).to eq(queue)
  end

  it 'skips a rate-limited strict queue and continues to the next physical queue' do
    priority = SidekiqRuntimeSpecSupport::PRIORITY_QUEUE
    allow(capsule).to receive(:queues).and_return([priority, queue])
    priority_store = instance_double(SidekiqRuntime::InflightStore)
    allow(store_class).to receive(:new).with(capsule,
      queue: priority,
      owner_identity: an_instance_of(String)).and_return(priority_store)
    limited = SidekiqRuntime::InflightStore::RateLimited.new(
      physical_queue: priority, retry_after_seconds: 12.5
    )
    expect(priority_store).to receive(:claim).ordered.and_return(limited)
    expect(store).to receive(:claim).ordered.and_return(claim)
    allow(handler).to receive(:call)
    expect(handler).to receive(:call).with(described_class::EVENT_NAME,
      hash_including(operation: 'rate_limited', outcome: 'limited', physical_queue: priority,
        retry_after_seconds: 12.5))

    expect(fetch.retrieve_work.queue_name).to eq(queue)
  end

  it 'uses the shorter retry delay and caps a local cooldown at one second' do
    limited = SidekiqRuntime::InflightStore::RateLimited.new(
      physical_queue: queue, retry_after_seconds: 10
    )
    allow(store).to receive(:claim).and_return(limited, claim)
    ticks = [0.0, 0.0, 0.0, 0.0, 0.0, 1.0].each
    sleeps = []
    allow(handler).to receive(:call)

    work = described_class.new(capsule, store_class: store_class,
      monotonic_clock: -> { ticks.next }, sleeper: ->(seconds) { sleeps << seconds }).retrieve_work

    expect(work.claim).to eq(claim)
    expect(sleeps).to eq([1.0])
  end

  it 'does not turn a high-rate retry delay into a fixed one-second sleep' do
    limited = SidekiqRuntime::InflightStore::RateLimited.new(
      physical_queue: queue, retry_after_seconds: 0.025
    )
    allow(store).to receive(:claim).and_return(limited, claim)
    ticks = [0.0, 0.0, 0.0, 0.0, 0.0, 0.025].each
    sleeps = []
    allow(handler).to receive(:call)

    described_class.new(capsule, store_class: store_class,
      monotonic_clock: -> { ticks.next }, sleeper: ->(seconds) { sleeps << seconds }).retrieve_work

    expect(sleeps).to eq([0.025])
  end

  it 'reprobes immediately when later queue scanning outlives the retry delay' do
    priority = SidekiqRuntimeSpecSupport::PRIORITY_QUEUE
    capsule = real_capsule([priority, queue])
    priority_store = Object.new
    limited = SidekiqRuntime::InflightStore::RateLimited.new(
      physical_queue: priority, retry_after_seconds: 0.01
    )
    priority_claims = [limited, claim_for(priority)].each
    priority_store.define_singleton_method(:claim) { priority_claims.next }
    empty_store = Object.new
    empty_store.define_singleton_method(:claim) { nil }
    ticks = [0.0, 0.0, 0.0, 0.02, 0.02, 0.02].each
    sleeps = []
    allow(handler).to receive(:call)

    work = described_class.new(
      capsule,
      store_class: store_factory({priority => priority_store, queue => empty_store}),
      monotonic_clock: -> { ticks.next }, sleeper: ->(seconds) { sleeps << seconds }
    ).retrieve_work

    expect(work.queue_name).to eq(priority)
    expect(sleeps).to be_empty
  end

  it 'checks duplicate strict queues once per round while preserving first-occurrence order' do
    priority = SidekiqRuntimeSpecSupport::PRIORITY_QUEUE
    strict_capsule = real_capsule([priority, priority, queue])
    calls = []
    stores = strict_capsule.queues.uniq.to_h do |physical_queue|
      current_claim = physical_queue == queue ? claim_for(queue) : nil
      store = Object.new
      store.define_singleton_method(:claim) do
        calls << physical_queue
        current_claim
      end
      [physical_queue, store]
    end
    strict_fetcher = described_class.new(strict_capsule, store_class: store_factory(stores))

    expect(strict_fetcher.retrieve_work.queue_name).to eq(queue)
    expect(calls).to eq([priority, queue])
  end

  it 'uses the complete Sidekiq 5:3:1 expansion for weighted selection' do
    capsule = real_capsule([
      ['sneakers_spec_critical', 5],
      ['sneakers_spec_common', 3],
      ['sneakers_spec_bulk', 1]
    ])
    stores = claim_stores(capsule.queues.uniq)
    creations = []
    fetcher = described_class.new(capsule,
      store_class: store_factory(stores, creations: creations), random: Random.new(12_345))
    allow(handler).to receive(:call)

    selections = 9_000.times.each_with_object(Hash.new(0)) do |_index, counts|
      counts[fetcher.retrieve_work.queue_name] += 1
    end

    expect(capsule.mode).to eq(:weighted)
    expect(capsule.queues.tally).to eq(
      'sneakers_spec_critical' => 5,
      'sneakers_spec_common' => 3,
      'sneakers_spec_bulk' => 1
    )
    expect(creations.tally.values).to all(eq(1))
    expect(fetcher.stores_by_queue.keys).to contain_exactly(*capsule.queues.uniq)
    expect(selections.fetch('sneakers_spec_critical')).to be_between(4_500, 5_500)
    expect(selections.fetch('sneakers_spec_common')).to be_between(2_600, 3_400)
    expect(selections.fetch('sneakers_spec_bulk')).to be_between(700, 1_300)
  end

  it 'uses all-one Sidekiq weights as random selection' do
    capsule = real_capsule([
      ['sneakers_spec_critical', 1],
      ['sneakers_spec_common', 1],
      ['sneakers_spec_bulk', 1]
    ])
    fetcher = described_class.new(capsule,
      store_class: store_factory(claim_stores(capsule.queues)), random: Random.new(54_321))
    allow(handler).to receive(:call)

    selections = 3_000.times.each_with_object(Hash.new(0)) do |_index, counts|
      counts[fetcher.retrieve_work.queue_name] += 1
    end

    expect(capsule.mode).to eq(:random)
    expect(selections.values).to all(be_between(800, 1_200))
  end

  it 'checks each empty weighted queue at most once in one selection round' do
    capsule = real_capsule([
      ['sneakers_spec_critical', 5],
      ['sneakers_spec_common', 3],
      ['sneakers_spec_bulk', 1]
    ])
    calls = Hash.new(0)
    stores = capsule.queues.uniq.to_h do |physical_queue|
      store = Object.new
      store.define_singleton_method(:claim) do
        calls[physical_queue] += 1
        nil
      end
      [physical_queue, store]
    end
    ticks = [0.0, 0.0, 2.0].each
    fetcher = described_class.new(capsule, store_class: store_factory(stores), random: Random.new(1),
      monotonic_clock: -> { ticks.next }, sleeper: ->(_seconds) {})

    expect(fetcher.retrieve_work).to be_nil
    expect(calls).to eq(
      'sneakers_spec_critical' => 1,
      'sneakers_spec_common' => 1,
      'sneakers_spec_bulk' => 1
    )
  end

  it 'shares a weighted fetcher without mutating its capsule or expanded queue snapshot' do
    capsule = real_capsule([
      ['sneakers_spec_critical', 5],
      ['sneakers_spec_common', 3],
      ['sneakers_spec_bulk', 1]
    ])
    original_queues = capsule.queues.dup
    fetcher = described_class.new(capsule,
      store_class: store_factory(claim_stores(capsule.queues.uniq)), random: Random.new(98_765))
    allow(handler).to receive(:call)
    results = Queue.new

    workers = 8.times.map do
      Thread.new do
        50.times { results << fetcher.retrieve_work.queue_name }
      end
    end
    workers.each(&:value)

    internal_queues = fetcher.instance_variable_get(:@queues)
    expect(results.size).to eq(400)
    expect(results.size.times.map { results.pop }).to all(be_in(capsule.queues.uniq))
    expect(capsule.queues).to eq(original_queues)
    expect(internal_queues).to eq(original_queues)
    expect(internal_queues).to be_frozen
    expect(fetcher.send(:candidate_queues)).to contain_exactly(*capsule.queues.uniq)
    expect(internal_queues).to eq(original_queues)
  end

  it 'respects the bounded polling timeout' do
    allow(store).to receive(:claim).and_return(nil)
    ticks = [0.0, 0.0, 0.0, 1.0, 1.0, 2.0, 2.0].each
    sleeps = []
    expect(handler).to receive(:call).once.with(described_class::EVENT_NAME,
      hash_including(operation: 'poll', outcome: 'idle', empty_claims: 3,
        rate_limited_claims: 0, poll_duration_ms: 2_000))
    work = described_class.new(capsule, store_class: store_class,
      monotonic_clock: -> { ticks.next }, sleeper: ->(seconds) { sleeps << seconds }).retrieve_work
    expect(work).to be_nil
    expect(sleeps).to eq([1.0, 1.0])
  end

  it 'continues shutdown recovery after one uncertain result' do
    failed = instance_double(described_class::UnitOfWork)
    succeeded = instance_double(described_class::UnitOfWork)
    expect(failed).to receive(:requeue).and_raise(RedisClient::ConnectionError)
    expect(succeeded).to receive(:requeue).and_return(1)
    expect(logger).to receive(:info).with('Confirmed 1 reliable inflight jobs requeued in Redis')
    fetch.bulk_requeue([failed, succeeded])
  end

  def real_capsule(raw_queues)
    Sidekiq::Config.new.tap do |configuration|
      configuration.queues = raw_queues
    end.default_capsule
  end

  def claim_stores(queues)
    queues.to_h do |physical_queue|
      current_claim = claim_for(physical_queue)
      store = Object.new
      store.define_singleton_method(:claim) do
        current_claim
      end
      [physical_queue, store]
    end
  end

  def claim_for(physical_queue)
    job = Sidekiq.dump_json(build_sidekiq_runtime_job_payload(
      jid: "jid-#{physical_queue}", physical_queue: physical_queue
    ))
    SidekiqRuntime::InflightStore::Claim.new(
      source_queue: "queue:#{physical_queue}",
      job: job,
      member: "claim-#{physical_queue}\n#{job}",
      claimed_at: Time.now.to_f,
      owner_identity: 'owner'
    )
  end

  def store_factory(stores, creations: nil)
    Class.new do
      define_singleton_method(:new) do |_capsule, queue:, **|
        physical_queue = queue
        creations << physical_queue if creations
        stores.fetch(physical_queue)
      end
    end
  end
end
