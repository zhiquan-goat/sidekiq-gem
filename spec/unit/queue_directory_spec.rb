require 'spec_helper'

RSpec.describe SidekiqRuntime::QueueDirectory do
  let(:redis) { double('queue directory Redis') }
  let(:config) { double('Sidekiq config', queues: ['sneakers_configured', 'external_configured']) }

  subject(:directory) { described_class.new(config) }

  before do
    allow(config).to receive(:redis).and_yield(redis)
  end

  it 'merges and de-duplicates every discovery source' do
    allow(redis).to receive(:smembers).with(SidekiqRuntime::QueueKeys.directory_key)
      .and_return(['sneakers_directory', 'sneakers_configured', 'external_directory'])
    allow(redis).to receive(:smembers).with('queues')
      .and_return(['sneakers_native', 'sneakers_directory', 'external_native'])

    expect(directory.queues).to contain_exactly(
      'sneakers_configured',
      'external_configured',
      'sneakers_directory',
      'external_directory',
      'sneakers_native',
      'external_native'
    )
  end
end
