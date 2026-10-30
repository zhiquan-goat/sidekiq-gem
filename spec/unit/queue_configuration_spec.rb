require 'spec_helper'

RSpec.describe SidekiqRuntime::QueueConfiguration do
  def configured(raw)
    Sidekiq::Config.new.tap do |config|
      config[:queues] = raw
      config.queues = raw
    end
  end

  it 'accepts strict CLI queues, weighted strings and YAML tuples at startup' do
    [%w[critical default], %w[critical,3 default,1], [['critical', 3], ['default', 1]],
      %w[critical,1 default,1]].each do |raw|
      expect(described_class.validate!(configured(raw))).to be true
    end
  end

  it 'rejects malformed raw weights even when Sidekiq has coerced them' do
    [%w[default,2x], [['default', 1.5]], [['default', 0]], [['default', -1]],
      ['default', ['bulk', 1]], []].each do |raw|
      expect { described_class.validate!(configured(raw)) }.to raise_error(ArgumentError)
    end
  end

  it 'requires raw queue input and matching parsed queues at startup' do
    expect { described_class.validate!(Sidekiq::Config.new) }.to raise_error(ArgumentError, /config\[:queues\]/)
    config = configured(%w[critical,3 default,1])
    config.queues = ['other']
    expect { described_class.validate!(config) }.to raise_error(ArgumentError, /default capsule/)
  end
end
