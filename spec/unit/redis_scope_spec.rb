require 'spec_helper'

RSpec.describe SidekiqRuntime::RedisScope do
  let(:config_a) do
    Sidekiq::Config.new.tap { |value| value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL')} }
  end
  let(:config_b) do
    Sidekiq::Config.new.tap do |value|
      value.redis = {url: ENV.fetch('SIDEKIQ_REDIS_URL').sub(%r{/1\z}, '/2')}
    end
  end

  it 'pins nested work to the explicit config and restores an ambient pool on success' do
    pool_a = config_a.__send__(:local_redis_pool)
    pool_b = config_b.__send__(:local_redis_pool)

    Sidekiq::Client.via(pool_a) do
      expect(Thread.current[:sidekiq_redis_pool]).to equal(pool_a)
      described_class.with(config_b) do
        expect(Thread.current[:sidekiq_redis_pool]).to equal(pool_b)
      end
      expect(Thread.current[:sidekiq_redis_pool]).to equal(pool_a)
    end
    expect(Thread.current[:sidekiq_redis_pool]).to be_nil
  end

  it 'restores the ambient pool when the operation raises' do
    pool_a = config_a.__send__(:local_redis_pool)
    Sidekiq::Client.via(pool_a) do
      expect {
        described_class.with(config_b) { raise 'scope failure' }
      }.to raise_error('scope failure')
      expect(Thread.current[:sidekiq_redis_pool]).to equal(pool_a)
    end
  end

  it 'temporarily overrides and then restores an ambient capsule pool' do
    capsule_a = config_a.default_capsule
    pool_b = config_b.__send__(:local_redis_pool)
    Thread.current[:sidekiq_capsule] = capsule_a

    expect(config_b.redis_pool).to equal(capsule_a.redis_pool)
    described_class.with(config_b) do
      expect(config_b.redis_pool).to equal(pool_b)
    end
    expect(config_b.redis_pool).to equal(capsule_a.redis_pool)
  ensure
    Thread.current[:sidekiq_capsule] = nil
  end

  it 'accepts only a real Sidekiq Config' do
    expect { described_class.with(Sidekiq) {} }
      .to raise_error(ArgumentError, 'config must be a Sidekiq::Config')
  end
end
