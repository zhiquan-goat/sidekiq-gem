# frozen_string_literal: true

require 'spec_helper'

RSpec.describe SidekiqRuntime::Storage do
  it 'uses a generic namespace for every valid queue' do
    storage = described_class.default
    expect(storage.valid_queue?('runtime_orders')).to be true
    expect(storage.valid_queue?('ordinary_app')).to be true
    expect(storage.prefix('ordinary_app')).to eq('srt:v1:queue:ordinary_app')
  end

  it 'derives directory, unique and inflight keys from one application namespace' do
    storage = described_class.new(namespace: 'snk:srt:v1')
    expect(storage.prefix('critical')).to eq('snk:srt:v1:queue:critical')
    expect(storage.prefix('work_bulk')).to eq('snk:srt:v1:queue:work_bulk')
    expect(storage.directory_key).to eq('snk:srt:v1:queues')
    expect(storage.unique_prefix).to eq('snk:srt:v1:unique')
  end

  it 'rejects invalid queue names and glob metacharacters in the namespace' do
    storage = described_class.new
    expect { storage.validate_queue!('') }.to raise_error(ArgumentError, /Invalid Sidekiq queue/)
    expect { storage.validate_queue!(nil) }.to raise_error(ArgumentError, /Invalid Sidekiq queue/)
    expect { described_class.new(namespace: 'srt:*') }.to raise_error(ArgumentError, /namespace/)
  end
end
