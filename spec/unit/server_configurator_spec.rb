# frozen_string_literal: true

require 'spec_helper'
require 'sidekiq/fetch'

RSpec.describe SidekiqRuntime::ServerConfigurator do
  let(:redis_config) { {url: ENV.fetch('SIDEKIQ_REDIS_URL')} }

  def config_for(queues)
    Sidekiq::Config.new(
      death_handlers: [],
      lifecycle_events: Sidekiq::Config::DEFAULTS.fetch(:lifecycle_events).transform_values { [] }
    ).tap do |config|
      config[:queues] = queues
      config.queues = queues
      config.concurrency = 3
    end
  end

  it 'installs server bindings once and preserves client middleware order across repeated installation' do
    config = config_for(%w[sneakers_common sneakers_catalog])
    before_runtime = Class.new
    after_runtime = Class.new
    config.client_middleware { |chain| chain.add(before_runtime) }
    SidekiqRuntime.install_producer!(storage: SidekiqRuntime::QueueKeys.storage)
    config.client_middleware { |chain| chain.add(after_runtime) }
    SidekiqRuntime.install_server!(config, redis_config: redis_config,
      storage: SidekiqRuntime::QueueKeys.storage)
    SidekiqRuntime.install_producer!(storage: SidekiqRuntime::QueueKeys.storage)
    SidekiqRuntime.install_server!(config, redis_config: redis_config,
      storage: SidekiqRuntime::QueueKeys.storage)

    expect(config.queues).to eq(%w[sneakers_common sneakers_catalog])
    expect(config.concurrency).to eq(3)
    expect(config[:fetch_class]).to eq(SidekiqRuntime::ReliableFetch)
    expect(config[:scheduled_enq]).to eq(SidekiqRuntime::AtomicScheduledEnqueuer)
    expect(config.client_middleware.map(&:klass))
      .to eq([before_runtime, after_runtime])
    expect(Sidekiq::SortedEntry.ancestors.count(SidekiqRuntime::SortedEntryGuard)).to eq(1)
    expect(config[:lifecycle_events].fetch(:startup).first.call).to be true
    config[:lifecycle_events].fetch(:shutdown).each(&:call)
  end

  it 'keeps explicit weighted Sidekiq input for queues outside old application prefixes' do
    config = config_for(%w[sneakers_critical,5 sneakers_common,3 sneakers_bulk,1])
    SidekiqRuntime.install_server!(config, redis_config: redis_config,
      storage: SidekiqRuntime::QueueKeys.storage)
    expect(config.default_capsule.mode).to eq(:weighted)
    expect(config.queues.tally).to eq(
      'sneakers_critical' => 5, 'sneakers_common' => 3, 'sneakers_bulk' => 1
    )
    expect(described_class.validate!(config)).to be true

    config.queues = ['another_app']
    expect { described_class.validate!(config) }.to raise_error(ArgumentError, /default capsule/)
  end

  it 'rejects a cached fetcher whose weighted queue snapshot no longer matches the capsule' do
    config = config_for(%w[orders,3 bulk,1])
    SidekiqRuntime.install_server!(config, redis_config: redis_config,
      storage: SidekiqRuntime::QueueKeys.storage)
    fetcher = config.default_capsule.fetcher

    config[:queues] = %w[orders,1 bulk,3]
    config.queues = config[:queues]

    expect { described_class.validate!(config) }.to raise_error(ArgumentError, /reliable fetch for its configured queues/)
    expect(config.default_capsule.fetcher).to equal(fetcher)
  end

  it 'rejects a BasicFetch cached before runtime installation without replacing it' do
    config = config_for(['sneakers_common'])
    fetcher = config.default_capsule.fetcher
    expect(fetcher).to be_a(Sidekiq::BasicFetch)

    SidekiqRuntime.install_server!(config, redis_config: redis_config,
      storage: SidekiqRuntime::QueueKeys.storage)

    expect { config[:lifecycle_events].fetch(:startup).first.call }
      .to raise_error(ArgumentError, /default capsule/)
    expect(config.default_capsule.fetcher).to equal(fetcher)
  end

  it 'rejects recovery settings replacement after installation or after fetcher construction' do
    config = config_for(['one'])
    SidekiqRuntime.install_server!(config, redis_config: redis_config)
    fetcher = config.default_capsule.fetcher
    expect(SidekiqRuntime.install_server!(config, redis_config: redis_config, recovery: SidekiqRuntime::RecoveryConfiguration.new))
      .to equal(config)
    config[:runtime_recovery] = SidekiqRuntime::RecoveryConfiguration.new
    expect(described_class.validate!(config)).to be true
    replacement = SidekiqRuntime::RecoveryConfiguration.new(reclaim_interval: 10)
    expect { SidekiqRuntime.install_server!(config, redis_config: redis_config, recovery: replacement) }
      .to raise_error(ArgumentError, /cannot change/)
    config[:runtime_recovery] = replacement
    expect { described_class.validate!(config) }.to raise_error(ArgumentError, /changed after installation/)
    expect(config.default_capsule.fetcher).to equal(fetcher)
    expect(Sidekiq::Processor.ancestors.count(SidekiqRuntime::ProcessorBoundary)).to eq(1)
  end

  it 'rejects settings changed before lazy fetcher construction as well as repeated installation' do
    config = config_for(['one'])
    SidekiqRuntime.install_server!(config, redis_config: redis_config)
    config[:runtime_recovery] = SidekiqRuntime::RecoveryConfiguration.new(reclaim_interval: 10)
    expect { described_class.validate!(config) }.to raise_error(ArgumentError, /changed after installation/)
    expect { SidekiqRuntime.install_server!(config, redis_config: redis_config) }
      .to raise_error(ArgumentError, /cannot change/)
  end

  it 'requires an explicit Redis config before changing the server' do
    config = config_for(['sneakers_common'])
    expect(config).not_to receive(:redis=)
    expect { described_class.apply(config, redis_config: nil) }
      .to raise_error(ArgumentError, /redis_config/)
  end
end
